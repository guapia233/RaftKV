#ifndef RAFTRPC_H
#define RAFTRPC_H

#include "raftRPC.pb.h"
 

///@brief Raft 节点之间通信的 RPC 工具类，用于封装和简化 Raft 节点之间的 RPC 调用逻辑
// 每个 Raft 节点需要与其他所有节点保持通信，因此每个节点都需要维护一个 RaftRpcUtil 数组，分别存放与集群中每一个节点通信的 RaftRpcUtil 实例
// 每一个 RaftRpcUtil 实例包含：
//     一个 MprpcChannel 对象（用于通信）；
//     一个 RPC Stub（通过 MprpcChannel 进行远程调用的接口）；
class RaftRpcUtil {
 private:
  // raftRpc_Stub 是由 protobuf 根据 .proto 文件自动生成的 RPC 客户端 stub 类，它封装了远程调用的逻辑（如发送 RPC 请求、等待响应等）
  // 每个 stub_ 绑定到一个远程地址（由构造函数提供的 IP 和端口）
  raftRpcProctoc::raftRpc_Stub *stub_;

 public:
  /**
   * Raft 节点需要主动向其他节点发起的三种 RPC 调用
   * 模仿了 MIT 6.824 分布式系统课程 的 Raft 实现方式，在那里，RPC 通信也是用类似的 Stub 和 Channel 调用形式
   * 但是当别的节点想调用本节点的 RPC 服务时，它是不能直接调用我这里的 RaftRpcUtil 类，因为这个类只是客户端调用用的 stub，不具备服务端接收 RPC 的能力
   * 为了让其他节点能 RPC 调用我这边的方法，我必须继承由 Protobuf 自动生成的服务端类 raftRpc_::Service 并实现其中定义的函数
   */

  // 主动调用其他节点的 AppendEntries RPC（心跳 / 日志复制）
  bool AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response);
  // 向其他节点发送快照（用于当对方落后太多，无法通过日志补齐时）
  bool InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args, raftRpcProctoc::InstallSnapshotResponse *response);
  // 在 Raft 选举阶段调用，用于请求其他节点为自己投票
  bool RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response);

  /**
   *
   * @param ip  远端 IP
   * @param port  远端端口
   */
  RaftRpcUtil(std::string ip, short port);
  ~RaftRpcUtil();
};

#endif  // RAFTRPC_H
