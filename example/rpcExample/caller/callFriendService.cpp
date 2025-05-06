#include <iostream>

// #include "mprpcapplication.h"
#include "rpcExample/friend.pb.h"

#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
int main(int argc, char **argv) {
    // 在一些 Linux 发行版会将.0.1保留给 localhost，将.1.1映射成主机名用于本机通信，其实二者没有本质区别
    std::string ip = "127.0.1.1"; // RPC server 监听在本地 127.0.1.1:7788 端口上
    short port = 7788;

    // 演示调用远程发布的 rpc 方法 Login：

    // stub 是客户端使用的代理对象，相当于把远程的方法本地化了
    fixbug::FiendServiceRpc_Stub stub(
        new MprpcChannel(ip, port, true));  // 注册进自己实现的 channel 类，channel 类用于自定义发送格式和负责序列化等操作
    // rpc 方法的请求参数
    fixbug::GetFriendsListRequest request;
    request.set_userid(1000);
    // rpc 方法的响应
    fixbug::GetFriendsListResponse response;
    // controller 用来监控一次 RPC 调用是否失败（比如网络断了） 
    MprpcController controller; 

    // 长连接测试 ，发送 10 次请求
    int count = 10;
    while (count--) {
        std::cout << " 倒数" << count << "次发起RPC请求" << std::endl;
        // 发起 rpc 方法的调用，实际内部会调用 channel 的 call_method 方法，该方法集中来做所有 rpc 调用的参数序列化和网络发送功能   
        stub.GetFriendsList(&controller, &request, &response, nullptr);
 
        // 一次 rpc 调用完成，读取调用的结果
        // rpc 调用是否失败由框架来决定，rpc调用失败 ！= 业务逻辑返回 false，rpc 和业务逻辑是隔离的
        if (controller.Failed()) { // 网络层或者 RPC 框架出错（比如 server 宕机，网络断开）
            std::cout << controller.ErrorText() << std::endl;
        } else {
            if (0 == response.result().errcode()) {
                std::cout << "rpc GetFriendsList response success!" << std::endl;
                int size = response.friends_size();
                for (int i = 0; i < size; i++) {
                    std::cout << "index:" << (i + 1) << " name:" << response.friends(i) << std::endl;
                }
            } else {
                // 这里不是 rpc 失败，而是业务逻辑的返回值是失败，两者要区分开
                std::cout << "rpc GetFriendsList response error : " << response.result().errmsg() << std::endl;
            }
        }
        sleep(5);  // sleep 5 seconds
    }
    return 0;
}
