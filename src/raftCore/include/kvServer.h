#ifndef SKIP_LIST_ON_RAFT_KVSERVER_H
#define SKIP_LIST_ON_RAFT_KVSERVER_H

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include "kvServerRPC.pb.h"
#include "raft.h"
#include "skipList.h"

// 继承自 raftKVRpcProctoc::kvServerRpc，意味着它实现了 RPC 服务端接口，是 Raft KV 协议的服务实现
class KvServer : raftKVRpcProctoc::kvServerRpc {
   private:
    std::mutex m_mtx;
    int m_me;
    std::shared_ptr<Raft> m_raftNode; // 封装的 Raft 节点实例
    std::shared_ptr<LockQueue<ApplyMsg> > applyChan; // kvServer 和 raft 节点的通信管道，用于接收日志应用
    int m_maxRaftState; // 日志大小阈值，超过就考虑生成快照

    std::string m_serializedKVData; // 中转变量，保存序列化后的 kv 数据
    SkipList<std::string, std::string> m_skipList; // 上层数据库，使用跳表替代 map
    std::unordered_map<std::string, std::string> m_kvDB;

    // 将每个 log index 映射到一个 Op 队列，用于异步地等待 Raft apply 的响应，用于协调客户端 RPC 调用与状态机实际应用之间的同步
    std::unordered_map<int, LockQueue<Op> *> waitApplyCh;

    // 一个 kvServer 可能连接多个客户端，记录每个客户端最近一次请求的 requestId，用于幂等控制（防止重复执行）
    std::unordered_map<std::string, int> m_lastRequestId;  

    // 最近一次执行快照的 raftIndex
    int m_lastSnapShotRaftLogIndex;

   public:
    KvServer() = delete;

    KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

    void StartKVServer();

    void DprintfKVDB();

    void ExecuteAppendOpOnKVDB(Op op);

    void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

    void ExecutePutOpOnKVDB(Op op);

    // 将 GetArgs 改为 RPC 调用的（不要与 Get 命令混淆），因为是远程客户端，服务器宕机对客户端来说是无感的
    void Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply);  

    // 当 kvServer 从 applyChan 接收到 Raft commit 消息后的回调函数
    // 它会从 message.command 中解析出一个 Op（PUT / APPEND / GET 操作），然后调用对应的函数，比如 ExecuteAppendOpOnKVDB(op);

    void GetCommandFromRaft(ApplyMsg message); 
    
    // 幂等性控制：判断这个(clientId, requestId)是否重复处理过，防止 Put/Append 被重复应用
    bool ifRequestDuplicate(std::string ClientId, int RequestId);

    // clerk 使用 RPC 远程调用
    void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

    // 一直等待 raft 传来的 applyCh
    void ReadRaftApplyCommandLoop();

    void ReadSnapShotToInstall(std::string snapshot);

    bool SendMessageToWaitChan(const Op &op, int raftIndex);

    // 检查是否需要制作快照，需要的话就向 raft 之下制作快照
    void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

    // Handler the SnapShot from kv.rf.applyCh
    void GetSnapShotFromRaft(ApplyMsg message);

    std::string MakeSnapShot();

   public:  // RPC 实现，处理来自客户端的请求
    void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                   ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) override;

    void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
             ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) override;

    /////////////////serialiazation start ///////////////////////////////
    // notice ： func serialize
   private:
    friend class boost::serialization::access;

    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    // 并没有用 Boost 自动把 m_skipList 整个对象序列化，而是手动把它转成字符串，然后序列化这个字符串，再在恢复时用 load_file() 重新构建跳表
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)  // 这里面写需要序列化和反序列化的字段
    {
        ar & m_serializedKVData;

        // ar & m_kvDB;
        ar & m_lastRequestId;
    }

    /**
     * 当 Raft 日志太长时，调用 getSnapshotData() 生成快照
     * 快照包括：m_skipList（转存为字符串 m_serializedKVData）和 m_lastRequestId
     * 利用 boost::archive 对自身对象序列化
     * 快照内容再交由 Raft 保存
     */
    std::string getSnapshotData() {
        m_serializedKVData = m_skipList.dump_file();
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);
        oa << *this;
        m_serializedKVData.clear();
        return ss.str();
    }

    void parseFromString(const std::string &str) {
        std::stringstream ss(str);
        boost::archive::text_iarchive ia(ss);
        ia >> *this;
        m_skipList.load_file(m_serializedKVData);
        m_serializedKVData.clear();
    }

    /////////////////serialiazation end ///////////////////////////////
};

#endif  // SKIP_LIST_ON_RAFT_KVSERVER_H
