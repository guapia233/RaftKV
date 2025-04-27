//
// Created by swx on 23-6-4.
//
#include <iostream>
#include "clerk.h"
#include "util.h"
int main() {
    Clerk client; // 创建一个 Clerk 客户端实例 
    client.Init("test.conf"); // 初始化客户端，传入配置文件
    auto start = now();
    int count = 500; // 备进行 500 次写+读测试
    int tmp = count;
    while (tmp--) {
        client.Put("x", std::to_string(tmp)); // 每次循环，把变量 tmp 转为字符串，写入 key "x" 

        std::string get1 = client.Get("x"); // 紧跟着每次写入后，马上读取同一个 key "x"
        std::printf("get return :{%s}\r\n", get1.c_str());
    }
    return 0;
}