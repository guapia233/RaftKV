#ifndef RAFT_H
#define RAFT_H

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "ApplyMsg.h"
#include "Persister.h"
#include "boost/any.hpp"
#include "boost/serialization/serialization.hpp"
#include "config.h"
#include "monsoon.h"
#include "raftRpcUtil.h"
#include "util.h"

// 方便网络分区的时候 debug，网络异常的时候为 disconnected，只要网络正常就为 AppNormal，防止 matchIndex[] 数组异常减小
constexpr int Disconnected = 0;
constexpr int AppNormal = 1;

// 投票状态
constexpr int Killed = 0;
constexpr int Voted = 1;   // 本轮已经投过票了
constexpr int Expire = 2;  // 投票（消息、竞选者）过期
constexpr int Normal = 3;

class Raft : public raftRpcProctoc::raftRpc {
   private:
    std::mutex m_mtx;
    std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;  // 需要与其他 raft 节点通信，这里保存与其他节点通信的 rpc 入口
    std::shared_ptr<Persister> m_persister;             // 持久层，负责 raft 数据的持久化
    int m_me;                                           // 标识自己的编号，raft 是以集群启动的
    int m_currentTerm;                                  // 记录当前的任期
    int m_votedFor;                                     // 记录当前任期给谁投过票
    std::vector<raftRpcProctoc::LogEntry> m_logs;  // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

    // 这两个状态所有节点都在维护，易丢失
    int m_commitIndex;  // 提交的 index
    int m_lastApplied;  // 已经汇报给状态机（上层应用）的 log 的 index

    // 这两个状态是由服务器来维护，易丢失
    // 这两个状态的下标从1开始，因为通常 commitIndex 和 lastApplied 从0开始，应该是一个无效的 index，因此下标从1开始
    std::vector<int> m_matchIndex;  // 下一个要发送给 follower 的日志条目的索引
    std::vector<int> m_nextIndex;   // follower 返回给 leader 已经收到了多少日志条目的索引

    enum Status { Follower, Candidate, Leader };

    // 当前身份
    Status m_status;

    std::shared_ptr<LockQueue<ApplyMsg>> applyChan;  // client 从这里取日志（2B），client 与 raft 通信的接口
    // ApplyMsgQueue chan ApplyMsg // raft 内部使用的 chan，applyChan 是用于和服务层交互，最后好像没用上

    std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;  // 选举超时
    std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;  // 心跳超时

    // 用于传入快照点，储存快照中的最后一个日志的 Index 和 Term
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;

    // 协程
    std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

   public:
    // 日志同步 + 心跳
    void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);
    // 定期向状态机写入日志
    void applierTicker();
    // 记录某个时刻的状态
    bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);
    // 发起选举
    void doElection();
    // 发起心跳，只有 leader 才需要发起心跳
    void doHeartBeat();

    // 每隔一段时间检查睡眠时间内有没有重置定时器，没有则说明超时了
    // 如果有则设置合适睡眠时间：睡眠到重置时间+超时时间
    void electionTimeOutTicker();  // 监控是否发起选举

    // 获取应用日志
    std::vector<ApplyMsg> getApplyLogs();
    // 获取新命令的索引
    int getNewCommandIndex();
    // 获取当前日志信息
    void getPrevLogInfo(int server, int *preIndex, int *preTerm);
    // 查看当前节点是否是 leader
    void GetState(int *term, bool *isLeader);
    // 安装快照
    void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                         raftRpcProctoc::InstallSnapshotResponse *reply);
    // 查看是否该发送心跳了，如果需要发起就调用 doHeartBeat()
    void leaderHearBeatTicker();
    // leader 发送快照
    void leaderSendSnapShot(int server);
    // leader 更新 CommitIndex
    void leaderUpdateCommitIndex();
    // 判断对象 index 日志是否匹配，用来判断 leader 日志和 follower 是否匹配
    bool matchLog(int logIndex, int logTerm);
    // 持久化当前状态
    void persist();
    // 变成 candidate 请求其他节点投票
    void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);
    // 判断当前节点是否有最新日志
    bool UpToDate(int index, int term);
    // 获取最后一个日志条目索引
    int getLastLogIndex();
    // 获取最后一个日志条目任期
    int getLastLogTerm();
    // // 获取最后一个日志条目索引和任期
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
    // 获取指定日志条目索引的任期
    int getLogTermFromLogIndex(int logIndex);
    // 获取 raft 状态的大小
    int GetRaftStateSize();
    // 将日志索引转换为日志条目在 m_logs 数组中的位置
    int getSlicesIndexFromLogIndex(int logIndex);

    // 请求其他节点给自己投票
    bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                         std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);
    // 发送追加日志条目
    bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                           std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);

    // 给上层 KV-server 层发送消息
    void pushMsgToKvServer(ApplyMsg msg);
    // 读取持久化数据
    void readPersist(std::string data);
    // 持久化数据
    std::string persistData();

    void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);

    // Snapshot the service says it has created a snapshot that has
    // all info up to and including index. this means the
    // service no longer needs the log through (and including)
    // that index. Raft should now trim its log as much as possible.
    // index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
    // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
    // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
    void Snapshot(int index, std::string snapshot);

   public:  // 重写基类方法,因为 rpc 远程调用真正调用的是这些方法
    // 序列化，反序列化等操作 rpc 框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可
    void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                       ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
    void InstallSnapshot(google::protobuf::RpcController *controller,
                         const ::raftRpcProctoc::InstallSnapshotRequest *request,
                         ::raftRpcProctoc::InstallSnapshotResponse *response,
                         ::google::protobuf::Closure *done) override;
    void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                     ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;

   public:
    void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
              std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

   private:
    // for persist

    class BoostPersistRaftNode {
       public:
        friend class boost::serialization::access;
        // When the class Archive corresponds to an output archive, the
        // & operator is defined similar to <<.  Likewise, when the class Archive
        // is a type of input archive the & operator is defined similar to >>.
        template <class Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar & m_currentTerm;
            ar & m_votedFor;
            ar & m_lastSnapshotIncludeIndex;
            ar & m_lastSnapshotIncludeTerm;
            ar & m_logs;
        }
        int m_currentTerm;
        int m_votedFor;
        int m_lastSnapshotIncludeIndex;
        int m_lastSnapshotIncludeTerm;
        std::vector<std::string> m_logs;
        std::unordered_map<std::string, int> umap;

       public:
    };
};

#endif  // RAFT_H