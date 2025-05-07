#include "clerk.h"
#include "raftServerRpcUtil.h"
#include "util.h"

#include <string>
#include <vector>

// 向 Raft 集群读取指定 key 的值
std::string Clerk::Get(std::string key) {
    m_requestId++;
    auto requestId = m_requestId; // 为本次操作生成单调递增的全局请求序号
    int server = m_recentLeaderId; // 从最可能是 Leader 的节点开始
    
    // 填充参数
    raftKVRpcProctoc::GetArgs args;
    args.set_key(key);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);

    // 会一直重试，因为 requestId 没有改变，可能会因为 RPC 失败或者不是 Leader 导致重试，kvServer 层来保证不重复执行（线性一致性）
    while (true) {
        raftKVRpcProctoc::GetReply reply;
        bool ok = m_servers[server]->Get(&args, &reply);
        // 若 RPC 调用失败或返回 ErrWrongLeader（表明该节点不是 Leader），则轮询下一个节点
        if (!ok || reply.err() == ErrWrongLeader) {  
            // 以最近认为的 Leader 开始，逐个尝试调用 Get RPC
            server = (server + 1) % m_servers.size();
            continue;
        }
        if (reply.err() == ErrNoKey) { // 若返回 ErrNoKey，说明 key 不存在，返回空字符串
            return "";
        }
        if (reply.err() == OK) { // 若返回 OK，说明请求成功，记录下新的 Leader 
            m_recentLeaderId = server;
            return reply.value();
        }
    }

    return "";
}

// 对集群执行 Put 或 Append 操作，内部循环重试，直到由当前 Leader 成功提交
void Clerk::PutAppend(std::string key, std::string value, std::string op) {
    m_requestId++; // 为本次操作生成单调递增的全局请求序号
    auto requestId = m_requestId; // 幂等保证，Leader 收到(clientId, requestId)时，如已处理过会直接返回 OK，不会再次插入日志 
    auto server = m_recentLeaderId; // 先从最近认为的 Leader 开始试

    // 无限重试，直到 Leader 提交该指令
    while (true) { 
        // 构造 RPC 请求
        raftKVRpcProctoc::PutAppendArgs args; 
        args.set_key(key);
        args.set_value(value);
        args.set_op(op);
        args.set_clientid(m_clientId);
        args.set_requestid(requestId);
        
        raftKVRpcProctoc::PutAppendReply reply; // 创建准备接收的响应
        bool ok = m_servers[server]->PutAppend(&args, &reply); // 发送 RPC
        if (!ok || reply.err() == ErrWrongLeader) { // 如果 RPC 失败或发给了非 Leader 节点，就换节点重试
            DPrintf("【Clerk::PutAppend】原以为的leader：{%d}请求失败，向新leader{%d}重试  ，操作：{%s}", server,
                    server + 1, op.c_str());
            if (!ok) {
                DPrintf("重试原因 ，rpc失败 ，");
            }
            if (reply.err() == ErrWrongLeader) {
                DPrintf("重试原因：非leader");
            }
            server = (server + 1) % m_servers.size();  // 轮询下一个节点
            continue; 
        }
        if (reply.err() == OK) {  // 即真正的 Leader 已成功提交
            m_recentLeaderId = server; // 记住新 Leader，下次优先发给它
            return;
        }
    }
}

void Clerk::Put(std::string key, std::string value) { PutAppend(key, value, "Put"); }

void Clerk::Append(std::string key, std::string value) { PutAppend(key, value, "Append"); }

// 初始化客户端，与 Raft 集群所有节点建立 RPC 通信连接
void Clerk::Init(std::string configFileName) {
    // 获取所有 raft 节点的 ip 和 port 并进行连接
    MprpcConfig config; // MprpcConfig 是自研 MprRpc 框架的配置解析器
    config.LoadConfigFile(configFileName.c_str()); // 解析配置文件（配置文件里就是各节点的 IP 和端口）

    // 构造节点列表
    std::vector<std::pair<std::string, short>> ipPortVt;
    for (int i = 0; i < INT_MAX - 1; ++i) {
        std::string node = "node" + std::to_string(i);
        std::string nodeIp = config.Load(node + "ip");
        std::string nodePortStr = config.Load(node + "port");
        if (nodeIp.empty()) {
            break;
        }
        ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str())); 
    }
    // 为每个节点建立 RPC 通道对象
    for (const auto& item : ipPortVt) {
        std::string ip = item.first;
        short port = item.second;
        auto* rpc = new raftServerRpcUtil(ip, port);
        m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
    }
}

Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}
