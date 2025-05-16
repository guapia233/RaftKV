#include "rpcprovider.h"
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <fstream>
#include <string>
#include "rpcheader.pb.h"
#include "util.h"

/*
service_name =>  service描述
                        =》 service* 记录服务对象
                        method_name  =>  method方法对象
json   protobuf
*/
// 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
// 只是简单的把服务描述符和方法描述符全部保存在本地而已
// todo：要把本机开启的ip和端口写在文件里面
void RpcProvider::NotifyService(google::protobuf::Service *service) {
    ServiceInfo service_info;

    // 获取了服务对象的描述信息
    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
    // 获取服务的名字
    std::string service_name = pserviceDesc->name();
    // 获取服务对象service的方法的数量
    int methodCnt = pserviceDesc->method_count();

    std::cout << "service_name:" << service_name << std::endl;

    for (int i = 0; i < methodCnt; ++i) {
        // 获取了服务对象指定下标的服务方法的描述（抽象描述） UserService   Login
        const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
        std::string method_name = pmethodDesc->name();
        service_info.m_methodMap.insert({method_name, pmethodDesc});
    }
    service_info.m_service = service;
    m_serviceMap.insert({service_name, service_info});
}

// 启动rpc服务节点，开始提供rpc远程网络调用服务
void RpcProvider::Run(int nodeIndex, short port) {
    // 获取可用ip
    char *ipC;
    char hname[128];
    struct hostent *hent;
    gethostname(hname, sizeof(hname));
    hent = gethostbyname(hname);
    for (int i = 0; hent->h_addr_list[i]; i++) {
        ipC = inet_ntoa(*(struct in_addr *)(hent->h_addr_list[i]));  // IP地址
    }
    std::string ip = std::string(ipC);
    //    // 获取端口
    //    if(getReleasePort(port)) //在port的基础上获取一个可用的port，不知道为何没有效果
    //    {
    //        std::cout << "可用的端口号为：" << port << std::endl;
    //    }
    //    else
    //    {
    //        std::cout << "获取可用端口号失败！" << std::endl;
    //    }
    // 写入文件 "test.conf"
    std::string node = "node" + std::to_string(nodeIndex);
    std::ofstream outfile;
    outfile.open("test.conf", std::ios::app);  // 打开文件并追加写入
    if (!outfile.is_open()) {
        std::cout << "打开文件失败！" << std::endl;
        exit(EXIT_FAILURE);
    }
    outfile << node + "ip=" + ip << std::endl;
    outfile << node + "port=" + std::to_string(port) << std::endl;
    outfile.close();

    // 创建服务器
    muduo::net::InetAddress address(ip, port);

    // 创建TcpServer对象
    m_muduo_server = std::make_shared<muduo::net::TcpServer>(&m_eventLoop, address, "RpcProvider");

    // 绑定连接回调和消息读写回调方法  分离了网络代码和业务代码
    /*
    bind的作用：
    如果不使用std::bind将回调函数和TcpConnection对象绑定起来，那么在回调函数中就无法直接访问和修改TcpConnection对象的状态。因为回调函数是作为一个独立的函数被调用的，它没有当前对象的上下文信息（即this指针），也就无法直接访问当前对象的状态。
    如果要在回调函数中访问和修改TcpConnection对象的状态，需要通过参数的形式将当前对象的指针传递进去，并且保证回调函数在当前对象的上下文环境中被调用。这种方式比较复杂，容易出错，也不便于代码的编写和维护。因此，使用std::bind将回调函数和TcpConnection对象绑定起来，可以更加方便、直观地访问和修改对象的状态，同时也可以避免一些常见的错误。
    */
    m_muduo_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    m_muduo_server->setMessageCallback(
        std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

    // 设置muduo库的线程数量
    m_muduo_server->setThreadNum(4);

    // rpc服务端准备启动，打印信息
    std::cout << "RpcProvider start service at ip:" << ip << " port:" << port << std::endl;

    // 启动网络服务
    m_muduo_server->start();
    m_eventLoop.loop();
    /*
    这段代码是在启动网络服务和事件循环，其中server是一个TcpServer对象，m_eventLoop是一个EventLoop对象。

  首先调用server.start()函数启动网络服务。在Muduo库中，TcpServer类封装了底层网络操作，包括TCP连接的建立和关闭、接收客户端数据、发送数据给客户端等等。通过调用TcpServer对象的start函数，可以启动底层网络服务并监听客户端连接的到来。

  接下来调用m_eventLoop.loop()函数启动事件循环。在Muduo库中，EventLoop类封装了事件循环的核心逻辑，包括定时器、IO事件、信号等等。通过调用EventLoop对象的loop函数，可以启动事件循环，等待事件的到来并处理事件。

  在这段代码中，首先启动网络服务，然后进入事件循环阶段，等待并处理各种事件。网络服务和事件循环是两个相对独立的模块，它们的启动顺序和调用方式都是确定的。启动网络服务通常是在事件循环之前，因为网络服务是事件循环的基础。启动事件循环则是整个应用程序的核心，所有的事件都在事件循环中被处理。
    */
}

// 新的socket连接回调
void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn) {
    // 如果是新连接就什么都不干，即正常的接收连接即可
    if (!conn->connected()) {
        // 和rpc client的连接断开了
        conn->shutdown();
    }
}


// 如果远程有一个 RPC 服务的调用请求，那么 OnMessage 方法就会响应
// 因此本函数的作用是：解析请求，然后根据服务名、方法名和参数，来调用 service 的 callmethod 来调用本地的业务
void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buffer, muduo::Timestamp) {
    // 从网络缓冲区读取所有数据，包含：header_size + rpc_header + args
    std::string recv_buf = buffer->retrieveAllAsString();

    // 使用 protobuf 的 CodedInputStream 来解析数据流
    google::protobuf::io::ArrayInputStream array_input(recv_buf.data(), recv_buf.size());
    google::protobuf::io::CodedInputStream coded_input(&array_input);

    uint32_t header_size{};
    coded_input.ReadVarint32(&header_size);  // 解析出 header_size

    // 根据 header_size 读取数据头部的原始字节流，反序列化数据，得到 RPC 请求的详细信息
    std::string rpc_header_str;
    RPC::RpcHeader rpcHeader;
    std::string service_name;
    std::string method_name;

    // 设置读取限制，不必担心数据读多
    google::protobuf::io::CodedInputStream::Limit msg_limit = coded_input.PushLimit(header_size);
    
    coded_input.ReadString(&rpc_header_str, header_size); // 解析出 header

    // 恢复之前的限制，以便安全地继续读取其他数据
    coded_input.PopLimit(msg_limit);

    uint32_t args_size{};
    // 对数据头部进行反序列化，得到 service_name、method_name、args_size
    if (rpcHeader.ParseFromString(rpc_header_str)) {
        service_name = rpcHeader.service_name();
        method_name = rpcHeader.method_name();
        args_size = rpcHeader.args_size();
    } else {
        // 反序列化失败
        std::cout << "rpc_header_str:" << rpc_header_str << " parse error!" << std::endl;
        return;
    }

    // 解析调用方法所需要的实际参数部分 args
    std::string args_str; // 获取 RPC 方法参数的字符流数据

    // 直接读取 args_size 长度的字符串数据
    bool read_args_success = coded_input.ReadString(&args_str, args_size);

    // 处理错误：参数数据读取失败
    if (!read_args_success) {
        return;
    }

    // 打印调试信息
    //    std::cout << "============================================" << std::endl;
    //    std::cout << "header_size: " << header_size << std::endl;
    //    std::cout << "rpc_header_str: " << rpc_header_str << std::endl;
    //    std::cout << "service_name: " << service_name << std::endl;
    //    std::cout << "method_name: " << method_name << std::endl;
    //    std::cout << "args_str: " << args_str << std::endl;
    //    std::cout << "============================================" << std::endl;

    // 查找服务与方法
    // 框架预先注册了本地服务，这里查找出对应的方法对象 
    auto it = m_serviceMap.find(service_name);
    if (it == m_serviceMap.end()) {
        std::cout << "服务：" << service_name << " is not exist!" << std::endl;
        std::cout << "当前已经有的服务列表为:";
        for (auto item : m_serviceMap) {
            std::cout << item.first << " ";
        }
        std::cout << std::endl;
        return;
    }

    auto mit = it->second.m_methodMap.find(method_name);
    if (mit == it->second.m_methodMap.end()) {
        std::cout << service_name << ":" << method_name << " is not exist!" << std::endl;
        return;
    }

    google::protobuf::Service *service = it->second.m_service;       // 获取 service 对象   
    const google::protobuf::MethodDescriptor *method = mit->second;  // 获取 method 对象   

    // 生成 RPC 方法调用的请求 request 和响应 response  
    google::protobuf::Message *request = service->GetRequestPrototype(method).New();

    // 利用 request 对调用方法所需要的实际参数进行反序列化
    if (!request->ParseFromString(args_str)) {
        std::cout << "request parse error, content:" << args_str << std::endl;
        return;
    }
    google::protobuf::Message *response = service->GetResponsePrototype(method).New();

    /**
     * 给下面 method 方法的调用，绑定一个 Closure 回调函数，需要完成序列化和反向发送请求的操作
     * Closure 是 protobuf 提供的泛型回调接口（类似函数指针）
     * 当业务方法执行完时，会自动调用 SendRpcResponse，将 response 序列化并通过 TCP 发送回客户端
     * 这种回调机制的好处是：业务逻辑和网络通信解耦，业务只负责设置好 response，真正发送由框架自动处理
     */
    google::protobuf::Closure *done =
        google::protobuf::NewCallback<RpcProvider, const muduo::net::TcpConnectionPtr &, google::protobuf::Message *>(
            this, &RpcProvider::SendRpcResponse, conn, response);

    // 真正调用方法
    service->CallMethod(method, nullptr, request, response, done);
    
    /**
     * CallMethod 动态分发机制：为什么可以通过 service->CallMethod 来直接调用具体的业务方法？
     * 我们注册的 service 实例继承自 .proto 文件生成的 serviceRpc 类，而 ServiceRpc 又继承自 google::protobuf::Service
     * 在 ServiceRpc 中重写了 Service 的纯虚函数 CallMethod，该函数内部会根据传入的 method 描述符自动调用对应的业务方法（如 Login）
     * 而这些具体的业务方法（如 Login）又被我们用户自定义的 service 类重写了
     * 所以当调用 CallMethod 时，实际上最终会执行我们在用户自定义的 service 类中实现的具体业务逻辑
     */
}

// Closure的回调操作，用于序列化rpc的响应和网络发送,发送响应回去
void RpcProvider::SendRpcResponse(const muduo::net::TcpConnectionPtr &conn, google::protobuf::Message *response) {
    std::string response_str;
    if (response->SerializeToString(&response_str))  // response进行序列化
    {
        // 序列化成功后，通过网络把rpc方法执行的结果发送会rpc的调用方
        conn->send(response_str);
    } else {
        std::cout << "serialize response_str error!" << std::endl;
    }
    //    conn->shutdown(); // 模拟http的短链接服务，由rpcprovider主动断开连接  //改为长连接，不主动断开
}

RpcProvider::~RpcProvider() {
    std::cout << "[func - RpcProvider::~RpcProvider()]: ip和port信息：" << m_muduo_server->ipPort() << std::endl;
    m_eventLoop.quit();
    //    m_muduo_server.   怎么没有stop函数，奇奇怪怪，看csdn上面的教程也没有要停止，甚至上面那个都没有
}
