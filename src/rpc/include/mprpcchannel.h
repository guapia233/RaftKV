#ifndef MPRPCCHANNEL_H
#define MPRPCCHANNEL_H

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <algorithm>
#include <algorithm>  // 包含 std::generate_n() 和 std::generate() 函数的头文件
#include <functional>
#include <iostream>
#include <map>
#include <random>  // 包含 std::uniform_int_distribution 类型的头文件
#include <string>
#include <unordered_map>
#include <vector>
using namespace std;

// MprpcChannel 是自定义的 RPC 通信通道，继承自 protobuf 的 RpcChannel
// 所有通过 stub 调用远程方法时，都是通过 Channel 来统一处理
// 真正负责发送和接收的前后处理工作，如消息的组织方式，向哪个节点发送等等
class MprpcChannel : public google::protobuf::RpcChannel {
   public:
    // 重写 protobuf 的 CallMethod 方法
    // 所有通过 stub 调用的 rpc 方法最终都会调用 CallMethod，统一做 rpc 方法调用的数据数据序列化和网络发送
    void CallMethod(const google::protobuf::MethodDescriptor *method, 
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request, 
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;

    MprpcChannel(string ip, short port, bool connectNow);

   private:
    int m_clientFd; // TCP 套接字，用于通信
    const std::string m_ip;  // 保存目标 ip 和端口，如果断了可以尝试重连
    const uint16_t m_port;
    /// @brief 连接 ip 和端口,并设置 m_clientFd
    /// @param ip ip 地址，本机字节序
    /// @param port 端口，本机字节序
    /// @return 成功返回空字符串，否则返回失败信息

    // 创建 socket 连接并设置 m_clientFd
    bool newConnect(const char *ip, uint16_t port, string *errMsg);
};

#endif  // MPRPCCHANNEL_H