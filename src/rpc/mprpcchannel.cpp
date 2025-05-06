#include "mprpcchannel.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include "mprpccontroller.h"
#include "rpcheader.pb.h"
#include "util.h"
/**
 * header_size + header(service_name method_name args_size) + args
 * 所有通过 stub 代理对象调用的 rpc 方法，都会到这里统一通过 rpcChannel 来调用方法，统一做数据序列化和网络发送
 */
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                              google::protobuf::RpcController* controller, const google::protobuf::Message* request,
                              google::protobuf::Message* response, google::protobuf::Closure* done) {
        // 如果之前连接断了（文件描述符为 -1），就尝试重新建立 TCP 连接，失败时设置 controller 的错误信息并返回
        if (m_clientFd == -1) { 
        std::string errMsg;
        bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
        if (!rt) {
            DPrintf("[func-MprpcChannel::CallMethod]重连接ip：{%s} port{%d}失败", m_ip.c_str(), m_port);
            controller->SetFailed(errMsg);
            return;
        } else {
            DPrintf("[func-MprpcChannel::CallMethod]连接ip：{%s} port{%d}成功", m_ip.c_str(), m_port);
        }
    }

    // 通过 MethodDescriptor* 中的 ServiceDescriptor* 获取要调用的服务名和方法名
    const google::protobuf::ServiceDescriptor* sd = method->service();
    std::string service_name = sd->name();     // service_name
    std::string method_name = method->name();  // method_name

    // 将 protobuf 类型的请求对象 request 序列化成字符串，得到其大小 args_size 用于 header 填写
    uint32_t args_size{};
    std::string args_str;
    if (request->SerializeToString(&args_str)) {
        args_size = args_str.size();
    } else {
        controller->SetFailed("serialize request error!");
        return;
    }

    // 构造自定义 RpcHeader 头部
    // 自定义的 RpcHeader 是在自己框架中定义的一个 protobuf 类型，用来描述一次调用元信息，包括：服务名、方法名、参数长度
    RPC::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);

    // 将 RpcHeader 序列化
    std::string rpc_header_str;
    if (!rpcHeader.SerializeToString(&rpc_header_str)) {
        controller->SetFailed("serialize rpc header error!");
        return;
    }

    // 使用 protobuf 的 CodedOutputStream 来构建发送的数据流
    std::string send_rpc_str;  // 用来保存最终发送的数据
    {
        // 创建一个 StringOutputStream 用于写入 send_rpc_str
        google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
        google::protobuf::io::CodedOutputStream coded_output(&string_output);

        // 先写入 header 的长度（变长编码）
        coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));

        // 然后写入 rpc_header 本身
        coded_output.WriteString(rpc_header_str);
    }

    // 最后把参数内容拼接到发送数据末尾 
    send_rpc_str += args_str;

    // 最终格式：header_size + header(service_name method_name args_size) + args

    // 打印调试信息
    //    std::cout << "============================================" << std::endl;
    //    std::cout << "header_size: " << header_size << std::endl;
    //    std::cout << "rpc_header_str: " << rpc_header_str << std::endl;
    //    std::cout << "service_name: " << service_name << std::endl;
    //    std::cout << "method_name: " << method_name << std::endl;
    //    std::cout << "args_str: " << args_str << std::endl;
    //    std::cout << "============================================" << std::endl;

    // 使用 send 函数循环发送 RPC 请求
    // 如果 send 失败，说明连接可能断了，就关闭旧连接并重连，然后重发，如果还是失败，直接 return
    while (-1 == send(m_clientFd, send_rpc_str.c_str(), send_rpc_str.size(), 0)) {
        char errtxt[512] = {0};
        sprintf(errtxt, "send error! errno:%d", errno);
        std::cout << "尝试重新连接，对方ip：" << m_ip << " 对方端口" << m_port << std::endl;
        close(m_clientFd);
        m_clientFd = -1;
        std::string errMsg;
        bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
        if (!rt) {
            controller->SetFailed(errMsg);
            return;
        }
    }
    
    // 从时间节点来说，这里将请求发送过去之后，RPC 服务的提供者就会开始处理，接收响应的时候就代表已经返回响应了
  
    // 接收 RPC 请求的响应结果，缓存在 recv_buf 中
    char recv_buf[1024] = {0};
    int recv_size = 0;
    if (-1 == (recv_size = recv(m_clientFd, recv_buf, 1024, 0))) {
        close(m_clientFd);
        m_clientFd = -1;
        char errtxt[512] = {0};
        sprintf(errtxt, "recv error! errno:%d", errno);
        controller->SetFailed(errtxt);
        return;
    }

    // 反序列化 RPC 调用的响应结果，把 server 返回的数据反序列化填入调用者提供的 response 对象 
    // 使用 ParseFromArray（而非 ParseFromString）是为了避免 \0 截断问题 
    if (!response->ParseFromArray(recv_buf, recv_size)) {
        char errtxt[1050] = {0};
        sprintf(errtxt, "parse error! response_str:%s", recv_buf);
        controller->SetFailed(errtxt);
        return;
    }
}

bool MprpcChannel::newConnect(const char* ip, uint16_t port, string* errMsg) {
    int clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == clientfd) {
        char errtxt[512] = {0};
        sprintf(errtxt, "create socket error! errno:%d", errno);
        m_clientFd = -1;
        *errMsg = errtxt;
        return false;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip);
    // 连接rpc服务节点
    if (-1 == connect(clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr))) {
        close(clientfd);
        char errtxt[512] = {0};
        sprintf(errtxt, "connect fail! errno:%d", errno);
        m_clientFd = -1;
        *errMsg = errtxt;
        return false;
    }
    m_clientFd = clientfd;
    return true;
}

MprpcChannel::MprpcChannel(string ip, short port, bool connectNow) : m_ip(ip), m_port(port), m_clientFd(-1) {
    // 使用tcp编程，完成rpc方法的远程调用，使用的是短连接，因此每次都要重新连接上去，待改成长连接。
    // 没有连接或者连接已经断开，那么就要重新连接呢,会一直不断地重试
    // 读取配置文件rpcserver的信息
    // std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
    // uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
    // rpc调用方想调用service_name的method_name服务，需要查询zk上该服务所在的host信息
    //  /UserServiceRpc/Login
    if (!connectNow) {
        return;
    }  // 可以允许延迟连接
    std::string errMsg;
    auto rt = newConnect(ip.c_str(), port, &errMsg);
    int tryCount = 3;
    while (!rt && tryCount--) {
        std::cout << errMsg << std::endl;
        rt = newConnect(ip.c_str(), port, &errMsg);
    }
}