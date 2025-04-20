## 实现内容
- 基于跳表数据结构实现跳表数据库 SkipListPro，用于完成键值对的高效存储与查询。
- 实现 Raft 协议中的心跳与选举机制，通过定时线程池触发心跳与选举任务，维护集群中的领导者状态，并跟踪日志的提交情况。
- 基于 Protobuf 和自定义协议，实现轻量级 RPC 通信框架 MprRpc，支持各节点之间的远程调用与数据传输。
- 实现日志读写与提交，由领导节点负责处理客户端的读写请求，并将对应日志复制到跟随者节点。在超过半数节点复制成功后，即提交日志，应用命令至状态机，并向客户端返回响应。
- 实现客户端协议，包括在客户端协议中引入由 IP 和请求序号组成的唯一请求 ID 以保证线性一致性，同时还支持客户端重试等功能。 
 

## 运行指南

0. 运行环境：
- Ubuntu 22.04 LTS
- protoc-3.12.4
- cmake-3.26.4
- gcc-11.4.0
- muduo-2.0.2
- boost-1.74

1. 编译
```
cd KVRaft // 进入项目目录  
mkdir cmake-build // 创建编译目录  
cd cmake-build
cmake ..
make
```

2. 启动 rpc
```
cd KVRaft/bin // 进入 bin 目录  
./provider // 启动 provider
./consumer // 新建一个终端，再启动 consumer 
``` 

3. 使用 raft 集群
```
./raftCoreRun -n 3 -f test.conf
```

4. 使用 kv
```
./callerMain
```
  


 
