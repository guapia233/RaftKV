#include "util.h"
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <iomanip>

void myAssert(bool condition, std::string message) {
    if (!condition) {
        std::cerr << "Error: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

std::chrono::_V2::system_clock::time_point now() { return std::chrono::high_resolution_clock::now(); }

// 计算随机选举超时时间
std::chrono::milliseconds getRandomizedElectionTimeout() {
    std::random_device rd; // 随机数种子

    // 使用 Mersenne Twister 算法构造一个高质量伪随机数生成器 rng，用上面的 rd() 初始化，mt19937 是 C++11 标准中的推荐引擎
    std::mt19937 rng(rd());
    // 创建一个均匀分布的整数生成器，用于在 [min, max] 区间之间随机采样 election timeout 
    std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime);

    return std::chrono::milliseconds(dist(rng));
}

void sleepNMilliseconds(int N) { std::this_thread::sleep_for(std::chrono::milliseconds(N)); };

bool getReleasePort(short &port) {
    short num = 0;
    while (!isReleasePort(port) && num < 30) {
        ++port;
        ++num;
    }
    if (num >= 30) {
        port = -1;
        return false;
    }
    return true;
}

bool isReleasePort(unsigned short usPort) {
    int s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(usPort);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int ret = ::bind(s, (sockaddr *)&addr, sizeof(addr));
    if (ret != 0) {
        close(s);
        return false;
    }
    close(s);
    return true;
}

void DPrintf(const char *format, ...) {
    if (Debug) {
        // 获取当前的日期，然后取日志信息，写入相应的日志文件当中 a+
        time_t now = time(nullptr);
        tm *nowtm = localtime(&now);
        va_list args;
        va_start(args, format);
        std::printf("[%d-%d-%d-%d-%d-%d] ", nowtm->tm_year + 1900, nowtm->tm_mon + 1, nowtm->tm_mday, nowtm->tm_hour,
                    nowtm->tm_min, nowtm->tm_sec);
        std::vprintf(format, args);
        std::printf("\n");
        va_end(args);
    }
}
