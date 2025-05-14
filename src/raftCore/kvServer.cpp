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

    Op raftCommitOp;
    // 通过超时 pop 来限定命令执行时间，如果超时时间到了还没返回 true，说明 Raft 一直没 commit 成功，命令执行超时了
    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) { 
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
            "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
            m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

        if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
            reply->set_err(OK);  // 虽然超时了，但因为是重复的请求，所以无所谓，返回 ok
        } else {
            reply->set_err(ErrWrongLeader);  // 不是重复的请求，因此要返回此消息，让 clerk 重新尝试
        }
    } else { // 没超时，命令可能真正地在 raft 集群成功执行了，日志已成功 commit
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- "
            "Index:%d , "
            "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
            m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
        
        // 有可能 leader 刚启动了写入操作，还没来得及复制到多数节点，就发生了 leader 变更，导致该日志被丢弃，因此必须检查
        // 在 Raft 中，旧 leader 的请求被丢弃是一个允许的正常行为，因为只有被多数节点复制并最终提交的日志项，才会被应用到状态机
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
            reply->set_err(OK);
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }

    m_mtx.lock();

    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp; // 删除本次请求对应的等待队列（一次性的通信通道）
    m_mtx.unlock();
}

// 从 Raft 层接收已经应用（Apply）的日志消息并将其处理，保持状态机的一致性
void KvServer::ReadRaftApplyCommandLoop() {
    while (true) {
        // 如果只操作 applyChan 不用拿锁，因为 applyChan 自己带锁
        auto message = applyChan->Pop();  // // 阻塞等待 Raft 应用日志
        DPrintf(
            "---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] "
            "收到了下raft的消息",
            m_me);

        // 监听 raft 所应用的每一个命令，并传递给相应的 RPC Handler
        if (message.CommandValid) {
            GetCommandFromRaft(message); // 日志消息，交给状态机应用
        }
        if (message.SnapshotValid) {
            GetSnapShotFromRaft(message); // 快照消息，执行快照恢复
        }
    }
}

/**
 * Raft 模块和 KVServer 模块都需要与持久化层（persist）交互：
 *  -Raft 需要持久化它的日志、term、votedFor 以及快照
 *  -KVServer 需要在启动时恢复状态机状态（即：KV 数据库和幂等性信息），这依赖读取快照
 * 
 * Raft 层在两种情况下与快照交互（只“保存”或“读取” snapshot 本体，不会解释它的内容）：
 *  -保存 snapshot：KVServer 检测到日志太多或空间压力大时，会调用 Snapshot()，传入一个字节串的 snapshot，Raft 层负责将它保存到磁盘
 *  -发送 InstallSnapshot RPC（给落后的 follower）：Leader 需要把这个 snapshot 从磁盘读出来，然后打包成 InstallSnapshot RPC 发给 follower
 * 
 * 因此 snapshot 的具体格式是由 kvServer 层来定的，raft 只负责传递这个东西
 */
void KvServer::ReadSnapShotToInstall(std::string snapshot) {
    if (snapshot.empty()) {
        return;
    }
    parseFromString(snapshot);
}

// 当 Raft 日志在状态机中被应用后，将这个结果通过管道（channel）发送给正在等待这个请求结果的客户端线程 
// 即根据 Raft 日志的索引 raftIndex，将该操作 op 推送到对应的等待通道 waitApplyCh[raftIndex] 中，唤醒对应客户端的等待线程
bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
    std::lock_guard<std::mutex> lg(m_mtx); // 加锁保护共享资源 waitApplyCh，防止多个线程并发修改
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    // 判断是否存在某个线程正在等待 raftIndex 对应的日志应用结果
    // 如果没有（比如超时或者提前取消等待），直接返回 false
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        return false;
    }

    // 如果存在等待者，就将操作结果 op 推送到对应的等待通道中，典型的 “生产者-消费者” 模式，用于线程间通信
    waitApplyCh[raftIndex]->Push(op);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    return true;
}

// 判断是否需要生成快照并通知 Raft 层
void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
    // 前者是当前 Raft 的持久化状态大小（如日志、term 等），后者是 KVServer 设定的最大持久化状态限制的十分之一
    // 如果当前状态超过阈值（此处是 1/10，说明比例比较激进），就调用 MakeSnapShot() 构造快照，然后通过 Snapshot() 命令提交给 Raft 层
    if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {
        auto snapshot = MakeSnapShot(); // 生成快照内容
        m_raftNode->Snapshot(raftIndex, snapshot); // 发送给 Raft 层，让其截断日志
    }
}

// 从 Raft 层接收到快照（Leader InstallSnapshot）后所做的处理
void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
    std::lock_guard<std::mutex> lg(m_mtx);

    // 条件性安装快照：Raft 层判断该快照是否比当前状态新，如果是，就接收该快照
    if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
        ReadSnapShotToInstall(message.Snapshot); // 应用该快照
        m_lastSnapShotRaftLogIndex = message.SnapshotIndex; // 记录快照所对应的日志 index，防止重复应用旧日志
    }
}

// 构造当前 KV 状态的序列化快照数据
std::string KvServer::MakeSnapShot() {
    std::lock_guard<std::mutex> lg(m_mtx);
    std::string snapshotData = getSnapshotData();
    return snapshotData;
}

/**
 * 以下两个函数是 Raft KVServer 的 RPC 服务接口函数，用于接收客户端的远程调用请求并调用实际逻辑处理函数，
 * 属于 protobuf RPC 框架中的典型服务端回调实现
 */
void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) {

    // 调用类内另一个重载函数，负责实际的业务逻辑处理
    KvServer::PutAppend(request, response);
    // 由 protobuf RPC 框架提供的 异步通知机制，告诉框架“响应处理完成，可以返回结果给客户端了”
    done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) {
    KvServer::Get(request, response);
    done->Run();
}

/** 
 * KvServer 构造函数的完整实现，负责完成一个 Raft + kvServer 分布式节点的初始化启动流程：
 * 1.初始化成员变量（如 skip list、日志持久化）
 * 2.创建 RPC 服务线程（用于接收 client 和 raft 节点请求）
 * 3.读取配置文件并连接其他 Raft 节点
 * 4.初始化 Raft 节点本身
 * 5.从持久化中恢复快照（snapshot）
 * 6.启动后台线程读取 Raft Apply 消息并处理
 */
KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port) : m_skipList(6) {
    std::shared_ptr<Persister> persister = std::make_shared<Persister>(me); // 初始化 Persister（日志和快照的持久化层）

    m_me = me; // 初始化当前节点编号、日志大小阈值
    m_maxRaftState = maxraftstate;

    // 构建 applyChan，即 Raft 向 kvServer 通知状态提交的通道（用来传递 ApplyMsg）
    applyChan = std::make_shared<LockQueue<ApplyMsg> >(); 

    m_raftNode = std::make_shared<Raft>(); // 创建 Raft 节点对象

    // 启动一个线程用于 RPC 服务注册
    // clerk 和 kvServer 需要开启 RPC 功能，同时 raft 与 raft 节点之间也要开启 RPC 功能，因此有两个注册
    std::thread t([this, port]() -> void {
        // provider 是一个 RPC 网络服务对象，把 UserService 对象发布到 RPC 节点上
        RpcProvider provider;
        provider.NotifyService(this); // 注册 KVServer 对应的服务（处理客户端 的 Get/Put RPC 请求）
        provider.NotifyService(       // 注册 Raft 协议服务（Raft 节点之间通信的 RPC）
            this->m_raftNode.get()); 

        // 开启 RPC 服务监听，Run 以后，线程进入阻塞状态，等待远程的 RPC 调用请求
        provider.Run(m_me, port);
    });
    t.detach();

    // 注意必须要保证所有节点都开启了 RPC 接受功能之后才能开启 RPC 远程调用能力，这里使用睡眠来保证
    // 即人为等待其他节点也完成初始化，确保在连接 RPC 时它们都已准备好（实际工程应使用更健壮的“握手机制”，避免硬编码等待）
    std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
    sleep(6);
    std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;
   
    // 从配置文件中加载所有节点的 IP 和端口信息并进行连接 
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
        ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));   
    }
    std::vector<std::shared_ptr<RaftRpcUtil> > servers;
    // 构造 RaftRpcUtil，与其他节点建立 RPC 连接（注意跳过自己）
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
