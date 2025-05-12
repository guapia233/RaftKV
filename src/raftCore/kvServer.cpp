#include "kvServer.h"
#include <rpcprovider.h>
#include "mprpcconfig.h"

// 在调试模式下打印当前 KV 数据库中的内容 
void KvServer::DprintfKVDB() {
    if (!Debug) {
        return;
    }
    std::lock_guard<std::mutex> lg(m_mtx); // 加锁：防止在打印数据时其他线程修改 m_skipList，确保线程安全

    // DEFER 宏，作用：在作用域结束前自动执行里面的语句（类似于 Go 的 defer，或 C++17 的 scope_exit）
    DEFER {
        m_skipList.display_list();
    };
}

// 以下三个函数用于在 Raft 提交日志之后，真正将客户端的操作应用到上层数据库中
void KvServer::ExecuteAppendOpOnKVDB(Op op) {
    m_mtx.lock();

    m_skipList.insert_set_element(op.Key, op.Value); // 插入键值对

    // 幂等控制：更新 m_lastRequestId 中该客户端的最新请求编号（用于之后判断重复请求）
    m_lastRequestId[op.ClientId] = op.RequestId; 

    m_mtx.unlock();

    DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
    m_mtx.lock();
    *value = "";
    *exist = false;
    if (m_skipList.search_element(op.Key, *value)) {
        *exist = true;
    }
 
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();

    DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
    m_mtx.lock();
    m_skipList.insert_set_element(op.Key, op.Value);
     
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();
 
    DprintfKVDB();
}

// 处理来自 clerk 的 Get 请求
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
    // 1. 构造 Get 操作并交给 Raft 层
    Op op;
    op.Operation = "Get";
    op.Key = args->key();
    op.Value = "";
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    int raftIndex = -1;
    int _ = -1;
    bool isLeader = false;
    
    // 执行 Start 函数，返回 预期的日志索引 logIndex 和 当前节点是否为 Leader
    m_raftNode->Start(op, &raftIndex, &_, &isLeader);  

    // 如果当前节点不是 Leader，直接返回错误
    if (!isLeader) {
        reply->set_err(ErrWrongLeader);
        return;
    }

    // 2. 为这个 raftIndex 准备一个等待队列
    // Raft 提交应用后会调用 GetCommandFromRaft() 把结果写入这个队列中，当前线程则会等待这个队列中消息的到来
    m_mtx.lock();
    // 使用 waitApplyCh 保存日志索引对应的 LockQueue<Op> 通道
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];

    m_mtx.unlock();  // 直接解锁，等待任务执行完成，不能一直拿锁等待

    // 3. 等待 Raft 提交并将结果写回队列（或超时）
    Op raftCommitOp;
    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) { // 超时了
        int _ = -1;
        bool isLeader = false;
        m_raftNode->GetState(&_, &isLeader);
        // 4.超时容错逻辑：检查是否已经执行过该请求（幂等性保障）
        if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
            // 超时了说明 raft 集群不保证已经 commitIndex 过该日志，但如果是已经提交过的 get 请求，是可以再执行的，不会违反线性一致性
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if (exist) {
                reply->set_err(OK);
                reply->set_value(value);
            } else {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        } else {
            reply->set_err(ErrWrongLeader);  // 返回该信息的目的是让 clerk 换一个节点重试
        }
    } else {
        // 5.正常返回逻辑：Raft 成功提交且我们收到了消息
        // 没超时，即 raft 已经提交了该 command，可以正式开始执行了
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if (exist) {
                reply->set_err(OK);
                reply->set_value(value);
            } else {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }
    // 6. 收尾：释放资源
    m_mtx.lock();   
    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp;
    m_mtx.unlock();
}

/**
 * 该函数负责处理 Raft 完成共识后的日志条目，并将其：
 * 1.应用到状态机（即 KV 数据库）
 * 2.做好去重处理（防止客户端重试造成多次写入）
 * 3.触发快照机制（防止日志无限增长）
 * 4.唤醒等待该日志的 RPC 请求线程
 */
void KvServer::GetCommandFromRaft(ApplyMsg message) {
    Op op;
    // 将日志中存储的内容反序列化成结构体 Op，包含操作类型、键值、客户端 ID、请求 ID 等
    op.parseFromString(message.Command);

    DPrintf(
        "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
        "Opreation {%s}, Key :{%s}, Value :{%s}",
        m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    
    // 如果这条日志已经被包含在快照中（即日志已经“历史化”了），就无需再应用一次
    if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
        return;
    }

    // 幂等性保证：如果是重复的请求就不执行
    if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
        // 执行命令
        if (op.Operation == "Put") {
            ExecutePutOpOnKVDB(op);
        }
        if (op.Operation == "Append") {
            ExecuteAppendOpOnKVDB(op);
        }
    }

    // 到这里 kvDB 已经制作了快照
    if (m_maxRaftState != -1) {
        IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
        // 如果 raft 的 log 太大（超过指定的比例），就制作快照
    }

    // 发送消息唤醒之前在 KvServer::Get() 等待该日志 commit 的 RPC 调用线程
    SendMessageToWaitChan(op, message.CommandIndex);
}

// 判断请求是否重复
bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
        return false;
    }
    return RequestId <= m_lastRequestId[ClientId];
}

// get 和 PutAppend 执行的是不一样的
// PutAppend 在收到 raft 消息之后执行，具体函数里面只判断幂等性（是否重复）
// get 函数收到 raft 消息之后在，因为 get 无论是否重复都可以再执行
void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
    // 构造 op 请求对象
    Op op;
    op.Operation = args->op();
    op.Key = args->key();
    op.Value = args->value();
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();
    int raftIndex = -1;
    int _ = -1;
    bool isleader = false;

    // 启动 Raft 日志复制流程
    m_raftNode->Start(op, &raftIndex, &_, &isleader);

    // 如果不是 Leader，直接返回
    if (!isleader) {
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , "
            "but "
            "not leader",
            m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);

        reply->set_err(ErrWrongLeader);
        return;
    }
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
        "leader ",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
    
    // 设置等待通道（waitApplyCh）
    m_mtx.lock();
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];

    m_mtx.unlock();  // 直接解锁，等待任务执行完成，不能一直拿锁等待

    // timeout
    Op raftCommitOp;
    // 通过超时 pop 来限定命令执行时间，如果超时时间到了还没拿到消息，说明命令执行超时了
    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) { // 超时了
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
            "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
            m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

        if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
            reply->set_err(
                OK);  // 虽然超时了,但因为是重复的请求，所以返回 ok，就算没有超时，在真正执行的时候也要判断是否重复
        } else {
            reply->set_err(ErrWrongLeader);  /// 返回此消息的目的是让 clerk 重新尝试
        }
    } else { // 没超时，命令可能真正地在 raft 集群成功执行了
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- "
            "Index:%d , "
            "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
            m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
            // 有可能 leader 发生变更导致日志被覆盖，因此必须检查
            reply->set_err(OK);
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }

    m_mtx.lock();

    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp;
    m_mtx.unlock();
}

void KvServer::ReadRaftApplyCommandLoop() {
    while (true) {
        // 如果只操作applyChan不用拿锁，因为applyChan自己带锁
        auto message = applyChan->Pop();  // 阻塞弹出
        DPrintf(
            "---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] "
            "收到了下raft的消息",
            m_me);
        // listen to every command applied by its raft ,delivery to relative RPC Handler

        if (message.CommandValid) {
            GetCommandFromRaft(message);
        }
        if (message.SnapshotValid) {
            GetSnapShotFromRaft(message);
        }
    }
}

// raft会与persist层交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
//  关于快照raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
//  因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
//  snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
void KvServer::ReadSnapShotToInstall(std::string snapshot) {
    if (snapshot.empty()) {
        // bootstrap without any state?
        return;
    }
    parseFromString(snapshot);

    //    r := bytes.NewBuffer(snapshot)
    //    d := labgob.NewDecoder(r)
    //
    //    var persist_kvdb map[string]string  //理应快照
    //    var persist_lastRequestId map[int64]int //快照这个为了维护线性一致性
    //
    //    if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_lastRequestId) != nil {
    //                DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!",kv.me)
    //        } else {
    //        kv.kvDB = persist_kvdb
    //        kv.lastRequestId = persist_lastRequestId
    //    }
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
    std::lock_guard<std::mutex> lg(m_mtx);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        return false;
    }
    waitApplyCh[raftIndex]->Push(op);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    return true;
}

void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
    if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {
        // Send SnapShot Command
        auto snapshot = MakeSnapShot();
        m_raftNode->Snapshot(raftIndex, snapshot);
    }
}

void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
        ReadSnapShotToInstall(message.Snapshot);
        m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
    }
}

std::string KvServer::MakeSnapShot() {
    std::lock_guard<std::mutex> lg(m_mtx);
    std::string snapshotData = getSnapshotData();
    return snapshotData;
}

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) {
    KvServer::PutAppend(request, response);
    done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) {
    KvServer::Get(request, response);
    done->Run();
}

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port) : m_skipList(6) {
    std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);

    m_me = me;
    m_maxRaftState = maxraftstate;

    applyChan = std::make_shared<LockQueue<ApplyMsg> >();

    m_raftNode = std::make_shared<Raft>();
    ////////////////clerk层面 kvserver开启rpc接受功能
    //    同时raft与raft节点之间也要开启rpc功能，因此有两个注册
    std::thread t([this, port]() -> void {
        // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
        RpcProvider provider;
        provider.NotifyService(this);
        provider.NotifyService(
            this->m_raftNode.get());  // todo：这里获取了原始指针，后面检查一下有没有泄露的问题 或者 shareptr释放的问题
        // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
        provider.Run(m_me, port);
    });
    t.detach();

    ////开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
    ////这里使用睡眠来保证
    std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
    sleep(6);
    std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;
    // 获取所有raft节点ip、port ，并进行连接  ,要排除自己
    MprpcConfig config;
    config.LoadConfigFile(nodeInforFileName.c_str());
    std::vector<std::pair<std::string, short> > ipPortVt;
    for (int i = 0; i < INT_MAX - 1; ++i) {
        std::string node = "node" + std::to_string(i);

        std::string nodeIp = config.Load(node + "ip");
        std::string nodePortStr = config.Load(node + "port");
        if (nodeIp.empty()) {
            break;
        }
        ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));  // 沒有atos方法，可以考慮自己实现
    }
    std::vector<std::shared_ptr<RaftRpcUtil> > servers;
    // 进行连接
    for (int i = 0; i < ipPortVt.size(); ++i) {
        if (i == m_me) {
            servers.push_back(nullptr);
            continue;
        }
        std::string otherNodeIp = ipPortVt[i].first;
        short otherNodePort = ipPortVt[i].second;
        auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
        servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));

        std::cout << "node" << m_me << " 连接node" << i << "success!" << std::endl;
    }
    sleep(ipPortVt.size() - me);  // 等待所有节点相互连接成功，再启动raft
    m_raftNode->init(servers, m_me, persister, applyChan);
    // kv的server直接与raft通信，但kv不直接与raft通信，所以需要把ApplyMsg的chan传递下去用于通信，两者的persist也是共用的

    //////////////////////////////////

    // You may need initialization code here.
    // m_kvDB; //kvdb初始化
    m_skipList;
    waitApplyCh;
    m_lastRequestId;
    m_lastSnapShotRaftLogIndex = 0;  // todo:感覺這個函數沒什麼用，不如直接調用raft節點中的snapshot值？？？
    auto snapshot = persister->ReadSnapshot();
    if (!snapshot.empty()) {
        ReadSnapShotToInstall(snapshot);
    }
    std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this);  // 马上向其他节点宣告自己就是leader
    t2.join();  // 由於ReadRaftApplyCommandLoop一直不會結束，达到一直卡在这的目的
}
