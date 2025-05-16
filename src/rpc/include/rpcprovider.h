#pragma once
#include <google/protobuf/descriptor.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/TcpServer.h>
#include <functional>
#include <string>
#include <unordered_map>
#include "google/protobuf/service.h"

// todo：如果 rpc 客户端优化成长连接，那么 rpc 服务器这边最好提供一个定时器，用以断开很久没有请求的连接
// 服务端注册和提供服务的核心类：服务端由 RpcProvider 提供服务，接收请求、调用本地方法、返回结果
class RpcProvider {
   public:
    // 提供给外部使用的，可以发布 rpc 方法的函数接口
    void NotifyService(google::protobuf::Service *service);

    // 启动 rpc 服务节点，开始提供 rpc 远程网络调用服务
    void Run(int nodeIndex, short port);

   private:
    // 组合 EventLoop
    muduo::net::EventLoop m_eventLoop; // 事件主循环
    std::shared_ptr<muduo::net::TcpServer> m_muduo_server; // TcpServer 是 muduo 的 TCP 服务器类，负责监听端口、接受连接

    // 用来描述一个注册进来的 service
    struct ServiceInfo {
        // 保存服务对象
        google::protobuf::Service *m_service; 
        // 保存该 service 中每个方法的名字和其描述符 MethodDescriptor
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap;  
    };

    // 存储所有注册成功的 service 实例
    std::unordered_map<std::string, ServiceInfo> m_serviceMap;

    // 新的 socket 连接回调，有客户端连接时触发
    void OnConnection(const muduo::net::TcpConnectionPtr &);

    // 已建立连接用户的读写事件回调，有客户端发来数据时触发
    void OnMessage(const muduo::net::TcpConnectionPtr &, muduo::net::Buffer *, muduo::Timestamp);

    // Closure 的回调操作，用于序列化 rpc 的响应和网络发送回客户端
    void SendRpcResponse(const muduo::net::TcpConnectionPtr &, google::protobuf::Message *);

   public:
    ~RpcProvider();
};