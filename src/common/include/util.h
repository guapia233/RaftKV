
#ifndef UTIL_H
#define UTIL_H

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <condition_variable>  // pthread_condition_t
#include <functional>
#include <iostream>
#include <mutex>  // pthread_mutex_t
#include <queue>
#include <random>
#include <sstream>
#include <thread>
#include "config.h"

template <class F>
class DeferClass {
   public:
    DeferClass(F&& f) : m_func(std::forward<F>(f)) {}
    DeferClass(const F& f) : m_func(f) {}
    ~DeferClass() { m_func(); }

    DeferClass(const DeferClass& e) = delete;
    DeferClass& operator=(const DeferClass& e) = delete;

   private:
    F m_func;
};

#define _CONCAT(a, b) a##b
#define _MAKE_DEFER_(line) DeferClass _CONCAT(defer_placeholder, line) = [&]()

#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__)

void DPrintf(const char* format, ...);

void myAssert(bool condition, std::string message = "Assertion failed!");

template <typename... Args>
std::string format(const char* format_str, Args... args) {
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1;  // "\0"
    if (size_s <= 0) {
        throw std::runtime_error("Error during formatting.");
    }
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1);  // remove '\0'
}

std::chrono::_V2::system_clock::time_point now();

std::chrono::milliseconds getRandomizedElectionTimeout();
void sleepNMilliseconds(int N);

// 异步写 阻塞读 的队列
// 多个线程可以并发地 Push 数据（写入队列）
// 一个或多个线程可以阻塞式 Pop 数据（读取队列）
template <typename T>
class LockQueue {
   public:
    // 多个 worker 线程都会写日志 queue
    void Push(const T& data) {
        std::lock_guard<std::mutex> lock(m_mutex);  // 使用 lock_gurad，即 RAII 的思想保证锁正确释放
        m_queue.push(data);
        m_condvariable.notify_one();
    }

    // 一个线程读日志 queue，写日志文件
    T Pop() {
        std::unique_lock<std::mutex> lock(m_mutex); // 这里用 unique_lock 是因为 lock_guard 不支持手动解锁 
        while (m_queue.empty()) {
            // 若日志队列为空，线程进入 wait 阻塞状态
            m_condvariable.wait(lock);  
        }
        T data = m_queue.front();
        m_queue.pop();
        return data;
    }

    // 带超时时间的 Pop 操作
    // 若在 timeout 毫秒内等不到数据，就返回 false；否则将队头元素赋值给 *ResData，返回 true
    bool timeOutPop(int timeout, T* ResData)  // 添加一个超时时间参数，默认为 50 毫秒
    {
        std::unique_lock<std::mutex> lock(m_mutex); // 比起 lock_guard，unique_lock 支持手动解锁和超时等待

        // 获取当前时间点，并计算出超时时刻
        auto now = std::chrono::system_clock::now();
        auto timeout_time = now + std::chrono::milliseconds(timeout);

        // 在超时之前，不断检查队列是否为空
        while (m_queue.empty()) {
            // 如果已经超时了，就返回一个空对象
            if (m_condvariable.wait_until(lock, timeout_time) == std::cv_status::timeout) {
                return false;
            } else {
                continue;
            }
        }

        T data = m_queue.front();
        m_queue.pop();
        *ResData = data;
        return true;
    }

   private:
    std::queue<T> m_queue;
    std::mutex m_mutex;
    std::condition_variable m_condvariable;
};
 

// 这个 Op 是 kvServer 传递给 raft集群 的 command
class Op {
   public:
    // Your definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    std::string Operation;  // "Get" "Put" "Append"
    std::string Key;
    std::string Value;
    std::string ClientId;  // 客户端号码
    int RequestId;         // 客户端号码请求的Request的序列号，为了保证线性一致性
                    //  IfDuplicate bool // Duplicate command can't be applied twice , but only for PUT and APPEND

   public:
    // todo
    // 为了协调raftRPC中的command只设置成了string,这个的限制就是正常字符中不能包含|
    // 当然后期可以换成更高级的序列化方法，比如protobuf
    std::string asString() const {
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);

        // write class instance to archive
        oa << *this;
        // close archive

        return ss.str();
    }

    bool parseFromString(std::string str) {
        std::stringstream iss(str);
        boost::archive::text_iarchive ia(iss);
        // read class state from archive
        ia >> *this;
        return true;  // todo : 解析失敗如何處理，要看一下boost庫了
    }

   public:
    friend std::ostream& operator<<(std::ostream& os, const Op& obj) {
        os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key + "},Value{" + obj.Value + "},ClientId{" +
                  obj.ClientId + "},RequestId{" + std::to_string(obj.RequestId) + "}";  // 在这里实现自定义的输出格式
        return os;
    }

   private:
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & Operation;
        ar & Key;
        ar & Value;
        ar & ClientId;
        ar & RequestId;
    }
};

///////////////////////////////////////////////kvserver reply err to clerk

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";

////////////////////////////////////获取可用端口

bool isReleasePort(unsigned short usPort);

bool getReleasePort(short& port);

// int main(int argc, char** argv)
//{
//     short port = 9060;
//     if(getReleasePort(port)) //在port的基础上获取一个可用的port
//     {
//         std::cout << "可用的端口号为：" << port << std::endl;
//     }
//     else
//     {
//         std::cout << "获取可用端口号失败！" << std::endl;
//     }
//     return 0;
// }

#endif  //  UTIL_H