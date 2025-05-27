//
// Created by swx on 23-12-23.
//

#ifndef CONFIG_H
#define CONFIG_H

const bool Debug = true;

// 时间单位：time.Millisecond
const int debugMul = 1; // 用于调试的倍率系数，不同网络环境 rpc 速度不同，因此需要乘以一个系数，
// 如果在本地调试速度很快，可以设置为 1；如果部署到网络延迟较高的环境，可能会设置为 10 或 100，把时间“放大”


const int HeartBeatTimeout = 25 * debugMul;  // 25ms，心跳时间一般要比选举超时小一个数量级
const int ApplyInterval = 10 * debugMul;     // 10ms，状态机应用日志的间隔

const int minRandomizedElectionTime = 300 * debugMul;  
const int maxRandomizedElectionTime = 500 * debugMul;  

const int CONSENSUS_TIMEOUT = 500 * debugMul;  

// 协程相关设置

const int FIBER_THREAD_NUM = 1;              // 协程库中线程池大小
const bool FIBER_USE_CALLER_THREAD = false;  // 是否使用caller_thread执行调度任务

#endif  // CONFIG_H
