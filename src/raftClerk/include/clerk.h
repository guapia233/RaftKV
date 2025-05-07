#ifndef SKIP_LIST_ON_RAFT_CLERK_H
#define SKIP_LIST_ON_RAFT_CLERK_H
#include <arpa/inet.h>
#include <netinet/in.h>
#include <raftServerRpcUtil.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include <vector>
#include "kvServerRPC.pb.h"
#include "mprpcconfig.h"

class Clerk {
   private:
    std::vector<std::shared_ptr<raftServerRpcUtil>> m_servers;  // 保存所有 raft 节点的 fd  
    std::string m_clientId; // 客户端唯一标识
    int m_requestId; // 当前客户端的请求序号，用于保证幂等
    int m_recentLeaderId;  // 最近认为的 Leader 索引

    // 用于返回随机的 clientId
    std::string Uuid() {
        return std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand());
    }  

    // MakeClerk todo
    void PutAppend(std::string key, std::string value, std::string op);

   public:
    // 对外暴露的三个功能和初始化
    void Init(std::string configFileName);
    std::string Get(std::string key);

    void Put(std::string key, std::string value);
    void Append(std::string key, std::string value);

   public:
    Clerk();
};

#endif  // SKIP_LIST_ON_RAFT_CLERK_H
