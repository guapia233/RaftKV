#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"

// 接收 leader 发送的日志追加条目请求，检查当前日志是否匹配并同步 leader 的日志到本机
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
    std::lock_guard<std::mutex> locker(m_mtx);

    reply->set_appstate(AppNormal);  // 能接收到代表网络是正常的

    // Your code here (2A, 2B).
    // 不同的节点收到 AppendEntries 的反应是不同的，要注意无论什么时候收到 rpc 请求和响应都要检查 term
    if (args->term() < m_currentTerm) { // 如果 leader 的任期小于自己的
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(-100);  // 让 leader 可以及时更新自己
        DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me,
                args->leaderid(), args->term(), m_me, m_currentTerm);
        return;  // 注意从过期的 leader 收到消息不要重设定时器
    }
    // Defer ec1([this]() -> void { this->persist(); });
    
    // 确保持久化会在作用域结束时自动调用，即使在这个作用域中发生了异常或提前返回了
    DEFER { persist(); };  // 由于这个局部变量创建在锁之后，因此执行 persist 的时候应该也是拿到锁的

    if (args->term() > m_currentTerm) { // 如果 leader 的任期大于自己的
        // DPrintf("[func-AppendEntries-rf{%v} ] 变成follower且更新term 因为Leader{%v}的term{%v}> rf{%v}.term{%v}\n",
        // rf.me, args.LeaderId, args.Term, rf.me, rf.currentTerm)

        // 依旧三变
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1;  
        // 这里设置成 -1 是有意义的，如果突然宕机然后上线，理论上是可以投票的
        // 这里可不返回，应该改成让该节点尝试接收日志
        // 如果是 leader 或 candidate，突然转到 follower，好像也不用其他操作
        // 如果本来就是 follower，那么其 term 变化，相当于“不言自明”的换了追随的对象，因为原来的 leader 的 term 更小，是不会再接收其消息了
    }

    myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));

    // 如果发生网络分区，那么 candidate 可能会收到同一个 term 的 leader 的消息，要转变为 follower 
    m_status = Follower;  // 这里是有必要的，因为如果 candidate 收到同一个 term 的 leader 的 AE，需要变成 follower
    m_lastResetElectionTime = now(); // 重置定时器的作用是告诉当前分区已经有了 leader，避免重复选举
    //  DPrintf("[	AppendEntries-func-rf(%v)		] 重置了选举超时定时器\n", rf.me);

    // 不能无脑地从 prevlogIndex 开始截断日志，因为 RPC 可能会延迟，导致发过来的 log 是很久之前的
    // 那么就比较日志，日志有三种情况：
    if (args->prevlogindex() > getLastLogIndex()) {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        // updatenextindex 的意思是期望下次传送数据的 index 范围由接收节点自己判断
        reply->set_updatenextindex(getLastLogIndex() + 1);
        //  DPrintf("[func-AppendEntries-rf{%v}] 拒绝了节点{%v}，因为日志太新,args.PrevLogIndex{%v} >
        //  lastLogIndex{%v}，返回值：{%v}\n", rf.me, args.LeaderId, args.PrevLogIndex, rf.getLastLogIndex(), reply)
        return;
    } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) { 
    // 如果 prevlogIndex 小于本地快照的最后一个日志索引，说明 leader 发送的日志太旧
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);  // todo：如果想直接弄到最新好像不对，因为是从后慢慢往前匹配的，这里不匹配说明后面的都不匹配
        //  DPrintf("[func-AppendEntries-rf{%v}] 拒绝了节点{%v}，因为log太老，返回值：{%v}\n", rf.me, args.LeaderId,
        //  reply) return
    }
    // 本机日志有那么长，冲突(same index,different term),截断日志
    // 注意：这里目前当 args.PrevLogIndex == rf.lastSnapshotIncludeIndex 与不等的时候要分开考虑，可以看看能不能优化这块
    if (matchLog(args->prevlogindex(), args->prevlogterm())) {
        // todo：整理logs，不能直接截断，必须一个一个检查，因为发送来的 log 可能是之前的，直接截断可能导致“取回”已经在 follower 日志中的条目
        // 那意思是不是可能会有一段发来的 AE 中的 logs 中前半是匹配的，后半是不匹配的，这种应该：1.follower如何处理？ 2.如何给leader回复 3. leader如何处理

        // 这里判断的情况是 leader 发送的 index 大于本地快照 lastIndex，并且小于本地日志数组范围，不能无脑截断，直接截断可能会重复拿到已经
        // 存到 follower 当前的日志条目，需要一个一个遍历匹配，也就是向前纠错，检查到具体哪个日志条目出现问题
        for (int i = 0; i < args->entries_size(); i++) {
            auto log = args->entries(i);
            if (log.logindex() > getLastLogIndex()) {
                // 超过就直接添加日志
                m_logs.push_back(log);
            } else {
                // 没超过意味着 leader 发送的日志条目的 index 小于等于当前节点日志最后的 index
                // 没超过就判断是否匹配，不匹配就更新，而不是直接截断
                // todo：这里可以改进为比较对应 logIndex 位置的 term 是否相等，term 相等就代表匹配
                // todo：这个地方放出来会出问题，按理说 index 相同，term 相同，log 也应该相同才对
                // rf.logs[entry.Index-firstIndex].Term ?= entry.Term
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
                    m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
                    // 判断 leader 和该节点相同位置的 log，其 logTerm 相等，但是命令却不相同，不符合 raft 的前向匹配，异常！
                    myAssert(
                        false,
                        format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                               " {%d:%d}却不同！！\n",
                               m_me, log.logindex(), log.logterm(), m_me,
                               m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                               log.command()));
                }
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
                    // 如果 leader 和该节点相同位置的日志条目任期不一致，就更新为 leader 的
                    m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
                }
            }
        }

        // 错误写法like：  rf.shrinkLogsToIndex(args.PrevLogIndex)
        // rf.logs = append(rf.logs, args.Entries...)
        // 因为可能会收到过期的log！！！ 因此这里是大于等于
        myAssert(
            getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
            format(
                "[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
                m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
        // if len(args.Entries) > 0 {
        //	fmt.Printf("[func-AppendEntries  rf:{%v}] ] : args.term:%v, rf.term:%v  ,rf.logs的长度：%v\n", rf.me,
        //args.Term,
        // rf.currentTerm, len(rf.logs))
        // }

        // 这种情况是：当前任期没有提交但是 leader 之前的日志保存了已经提交的日志信息，所以 follower 不能无脑上 getLastLogIndex()
        if (args->leadercommit() > m_commitIndex) {
            m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
            // 这个地方不能无脑跟上 getLastLogIndex()，因为可能存在 args->leadercommit() 落后于 getLastLogIndex() 的情况
        }

        // leader 会一次发送完所有的日志
        myAssert(getLastLogIndex() >= m_commitIndex,
                 format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                        getLastLogIndex(), m_commitIndex));
        reply->set_success(true);
        reply->set_term(m_currentTerm);

        //        DPrintf("[func-AppendEntries-rf{%v}] 接收了来自节点{%v}的log，当前lastLogIndex{%v}，返回值：{%v}\n",
        //        rf.me,
        //                args.LeaderId, rf.getLastLogIndex(), reply)

        return;
    } else { // 这里的情况是：leader 认为我们应该同步的 index 和任期在 follower 日志中不一致，因此需要发送期望 leader 发送日志的 index 值，这个由接收者去设置期望的 index
        // 优化
        // PrevLogIndex 长度合适，但是不匹配，因此往前寻找 矛盾的term的第一个元素
        // 为什么该term的日志都是矛盾的呢？也不一定都是矛盾的，只是这么优化减少rpc而已
        // ？什么时候term会矛盾呢？很多情况，比如leader接收了日志之后马上就崩溃等等
        reply->set_updatenextindex(args->prevlogindex());

        for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
            if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
                reply->set_updatenextindex(index + 1);
                break;
            }
        }
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        // 对UpdateNextIndex待优化  todo  找到符合的term的最后一个
        //        DPrintf("[func-AppendEntries-rf{%v}]
        //        拒绝了节点{%v}，因为prevLodIndex{%v}的args.term{%v}不匹配当前节点的logterm{%v}，返回值：{%v}\n",
        //                rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
        //                rf.logs[rf.getSlicesIndexFromLogIndex(args.PrevLogIndex)].LogTerm, reply)
        //        DPrintf("[func-AppendEntries-rf{%v}] 返回值: reply.UpdateNextIndex从{%v}优化到{%v}，优化了{%v}\n",
        //        rf.me,
        //                args.PrevLogIndex, reply.UpdateNextIndex, args.PrevLogIndex - reply.UpdateNextIndex) //
        //                很多都是优化了0
        return;
    }

    // fmt.Printf("[func-AppendEntries,rf{%v}]:len(rf.logs):%v, rf.commitIndex:%v\n", rf.me, len(rf.logs),
    // rf.commitIndex)
}

void Raft::applierTicker() {
    while (true) {
        m_mtx.lock();
        if (m_status == Leader) {
            DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
                    m_commitIndex);
        }
        auto applyMsgs = getApplyLogs();
        m_mtx.unlock();
        // 使用匿名函数是因为传递管道的时候不用拿锁
        //  todo:好像必须拿锁，因为不拿锁的话如果调用多次applyLog函数，可能会导致应用的顺序不一样
        if (!applyMsgs.empty()) {
            DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver報告的applyMsgs長度爲：{%d}", m_me,
                    applyMsgs.size());
        }
        for (auto& message : applyMsgs) {
            applyChan->Push(message);
        }
        // usleep(1000 * ApplyInterval);
        sleepNMilliseconds(ApplyInterval);
    }
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
    return true;
    //// Your code here (2D).
    // rf.mu.Lock()
    // defer rf.mu.Unlock()
    // DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex {%v} to check
    // whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)
    //// outdated snapshot
    // if lastIncludedIndex <= rf.commitIndex {
    //	return false
    // }
    //
    // lastLogIndex, _ := rf.getLastLogIndexAndTerm()
    // if lastIncludedIndex > lastLogIndex {
    //	rf.logs = make([]LogEntry, 0)
    // } else {
    //	rf.logs = rf.logs[rf.getSlicesIndexFromLogIndex(lastIncludedIndex)+1:]
    // }
    //// update dummy entry with lastIncludedTerm and lastIncludedIndex
    // rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
    //
    // rf.persister.Save(rf.persistData(), snapshot)
    // return true
}

// 当选举定时器超时后调用该函数，目的是发起一次选举，希望自己成为新 leader
void Raft::doElection() {
    std::lock_guard<std::mutex> g(m_mtx);

    if (m_status == Leader) {
        // fmt.Printf("[       ticker-func-rf(%v)              ] is a Leader,wait the  lock\n", rf.me)
    }
    // fmt.Printf("[       ticker-func-rf(%v)              ] get the  lock\n", rf.me)

    if (m_status != Leader) {
        DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader，开始选举 \n", m_me);
        /**
         * 开始新一轮选举：
         * 1.状态由 follower 转换为 candidate
         * 2.任期 +1
         * 3.给自己投票
         * 4.征求其他节点的票
         * ps：当选举的时候定时器超时就必须重新选举，不然会因为没有选票而卡住，重新竞选又超时，term 也要增加
         */
        m_status = Candidate;
        m_currentTerm += 1;
        m_votedFor = m_me;  // 给自己投票
        persist();          // 对以上数据进行持久化

        // 亮点：使用 make_shared 初始化票数，好处是将构建对象和内存分配一起进行，避免内存分配失败
        std::shared_ptr<int> votedNum = std::make_shared<int>(1);
        // 重新设置选举定时器
        m_lastResetElectionTime = now(); 

        // 请求其他节点投票，发送 RequestVote RPC
        // 设置请求参数和响应参数创建工作线程调用 sendRequestVote 发送给其他 raft 节点
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                continue;
            }
            int lastLogIndex = -1, lastLogTerm = -1;
            getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  // 获取最后一个日志的索引和任期

            std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
                std::make_shared<raftRpcProctoc::RequestVoteArgs>();
            requestVoteArgs->set_term(m_currentTerm);
            requestVoteArgs->set_candidateid(m_me);
            requestVoteArgs->set_lastlogindex(lastLogIndex);
            requestVoteArgs->set_lastlogterm(lastLogTerm);
            auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

            // 创建新线程并执行 sendRequestVote 函数，使用匿名函数避免其拿到锁
            std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply, votedNum);
            // 分离线程，不关注其回收
            t.detach();
        }
    }
}

// 向每个 Follower 发送心跳（空的 AppendEntries RPC）或日志条目（正常同步日志）或启用新线程发送快照（Follower 落后太多）
// 并更新 m_lastResetHearBeatTime，用于心跳节奏控制
void Raft::doHeartBeat() {
    std::lock_guard<std::mutex> g(m_mtx);

    if (m_status == Leader) {
        DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
        auto appendNums = std::make_shared<int>(1);  // 表示成功接收心跳或日志复制的 follower 的数量

        // 对所有 follower 发送心跳和日志复制，需要注意接下来日志复制是发送 preindex+1 还是发送快照
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) { // 不用给作为 leader 的自己发
                continue;
            }
            DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
            myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
            
            // 判断是发送快照还是发送 AE，如果需要发送给 follower 的下一个条目小于当前快照，就创建线程发送快照
            if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
                //                        DPrintf("[func-ticker()-rf{%v}]rf.nextIndex[%v] {%v} <=
                //                        rf.lastSnapshotIncludeIndex{%v},so leaderSendSnapShot", rf.me, i,
                //                        rf.nextIndex[i], rf.lastSnapshotIncludeIndex)
                std::thread t(&Raft::leaderSendSnapShot, this, i);  // 创建新线程并执行函数 
                t.detach(); // 分离线程，就无需关注回收
                continue;
            }

            // 发送 AppendEntries
            // 构造请求参数
            int preLogIndex = -1;
            int PrevLogTerm = -1;
            // 获取第 i 个 follower 最后匹配的日志条目的索引和任期
            getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);
            std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
                std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
            appendEntriesArgs->set_term(m_currentTerm);
            appendEntriesArgs->set_leaderid(m_me);
            appendEntriesArgs->set_prevlogindex(preLogIndex);
            appendEntriesArgs->set_prevlogterm(PrevLogTerm);
            appendEntriesArgs->set_leadercommit(m_commitIndex);
            appendEntriesArgs->clear_entries(); // 清空可能复用的 protobuf 结构体里的残留日志，防止有残留的日志条目信息和新的一起发送出去
           

            // 说明该 follower 的日志不是快照中的最后一条日志，因此直接从日志数组中提取日志条目，从 preLogIndex+1 开始发，直到 m_logs 的末尾
            if (preLogIndex != m_lastSnapshotIncludeIndex) {
                for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
                    raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries(); // 添加了新的日志条目和指向它的指针
                    *sendEntryPtr = m_logs[j];  // 将日志数据赋值给新添加的日志条目
                }
            } else {
            // follower 同步的 preindex 和 leader 记录的最新快照 index 一致，所以从快照往后发 
                for (const auto& item : m_logs) {
                    raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = item;  // =是可以点进去的，可以点进去看下 protobuf 是如何重写这个的
                }
            }

            int lastLogIndex = getLastLogIndex();
            // leader 对每个节点发送的日志长短不一，但是都保证从 prevIndex 发送直到最后
            myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
                     format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                            appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
            // 构造返回值
            const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
                std::make_shared<raftRpcProctoc::AppendEntriesReply>();
            appendEntriesReply->set_appstate(Disconnected); // 未与其他节点建立连接的状态

            std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply,
                          appendNums);  // 创建新线程并传递参数执行函数
            t.detach();
        }
        m_lastResetHearBeatTime = now();  // leader 发送心跳，重置心跳时间
    }
}

// 负责检查是否应该发起选举，如果需要就调用 doElection() 发起选举
void Raft::electionTimeOutTicker() {
    while (true) {
        /**
         * 如果不睡眠，那么对于 leader，这个函数会一直空转，浪费 cpu
         * 且加入协程之后，空转会导致其他协程无法运行，对于时间敏感的 AE，会导致心跳无法正常发送导致异常
         */
        while (m_status == Leader) {
            // 所以要让其睡眠心跳超时时间，因为心跳超时时间一般比选举超时时间小一个数量级
            usleep(HeartBeatTimeout);
        }
        std::chrono::duration<signed long int, std::ratio<1, 1000000000>>
            suitableSleepTime{};                           // 初始化一个纳秒级别的时间间隔对象
        std::chrono::system_clock::time_point wakeTime{};  // 记录时间节点
        {
            m_mtx.lock();
            wakeTime = now();
            suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
            m_mtx.unlock();
        }

        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();
            // 因为没有超过睡眠时间继续睡眠
            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
            // std::this_thread::sleep_for(suitableSleepTime);

            // 获取函数运行结束后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用ANSI控制序列将输出颜色修改为紫色
            std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count()
                      << " 毫秒\033[0m" << std::endl;
            std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                      << std::endl;
        }

        if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
            // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
            continue;
        }
        doElection();
    }
}

std::vector<ApplyMsg> Raft::getApplyLogs() {
    std::vector<ApplyMsg> applyMsgs;
    myAssert(m_commitIndex <= getLastLogIndex(),
             format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}", m_me, m_commitIndex,
                    getLastLogIndex()));

    while (m_lastApplied < m_commitIndex) {
        m_lastApplied++;
        myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
                 format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                        m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
        ApplyMsg applyMsg;
        applyMsg.CommandValid = true;
        applyMsg.SnapshotValid = false;
        applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
        applyMsg.CommandIndex = m_lastApplied;
        applyMsgs.emplace_back(applyMsg);
        //        DPrintf("[	applyLog func-rf{%v}	] apply Log,logIndex:%v  ，logTerm：{%v},command：{%v}\n",
        //        rf.me, rf.lastApplied, rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogTerm,
        //        rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].Command)
    }
    return applyMsgs;
}

// 获取新命令应该分配的Index
int Raft::getNewCommandIndex() {
    //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
    auto lastLogIndex = getLastLogIndex();
    return lastLogIndex + 1;
}

// getPrevLogInfo
// leader调用，传入：服务器index，传出：发送的AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
    // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
    if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
        // 要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
        *preIndex = m_lastSnapshotIncludeIndex;
        *preTerm = m_lastSnapshotIncludeTerm;
        return;
    }
    auto nextIndex = m_nextIndex[server];
    *preIndex = nextIndex - 1;
    *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

// GetState return currentTerm and whether this server
// believes it is the Leader.
void Raft::GetState(int* term, bool* isLeader) {
    m_mtx.lock();
    DEFER {
        // todo 暂时不清楚会不会导致死锁
        m_mtx.unlock();
    };

    // Your code here (2A).
    *term = m_currentTerm;
    *isLeader = (m_status == Leader);
}

// 接收 leader 发送的快照请求，检同步快照到本机
void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
    m_mtx.lock();
    DEFER { m_mtx.unlock(); };
    if (args->term() < m_currentTerm) {
        reply->set_term(m_currentTerm);
        //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.term{%v}<rf{%v}.term{%v} ", rf.me, args.LeaderId,
        //        args.Term, rf.me, rf.currentTerm)

        return;
    }
    if (args->term() > m_currentTerm) {
        // 后面两种情况都要接收日志
        m_currentTerm = args->term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
    }
    m_status = Follower;
    m_lastResetElectionTime = now();
    // outdated snapshot
    if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
        //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.LastSnapShotIncludeIndex{%v} <=
        //        rf{%v}.lastSnapshotIncludeIndex{%v} ", rf.me, args.LeaderId, args.LastSnapShotIncludeIndex, rf.me,
        //        rf.lastSnapshotIncludeIndex)
        return;
    }
    // 截断日志，修改commitIndex和lastApplied
    // 截断日志包括：日志长了，截断一部分，日志短了，全部清空，其实两个是一种情况
    // 但是由于现在getSlicesIndexFromLogIndex的实现，不能传入不存在logIndex，否则会panic
    auto lastLogIndex = getLastLogIndex();

    if (lastLogIndex > args->lastsnapshotincludeindex()) {
        m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
    } else {
        m_logs.clear();
    }
    m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
    m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
    m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
    m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

    reply->set_term(m_currentTerm);
    ApplyMsg msg;
    msg.SnapshotValid = true;
    msg.Snapshot = args->data();
    msg.SnapshotTerm = args->lastsnapshotincludeterm();
    msg.SnapshotIndex = args->lastsnapshotincludeindex();

    std::thread t(&Raft::pushMsgToKvServer, this, msg);  // 创建新线程并执行b函数，并传递参数
    t.detach();
    // 看下这里能不能再优化
    //     DPrintf("[func-InstallSnapshot-rf{%v}] receive snapshot from {%v} ,LastSnapShotIncludeIndex ={%v} ", rf.me,
    //     args.LeaderId, args.LastSnapShotIncludeIndex)
    // 持久化
    m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

// 负责检查是否应该发送心跳，如果应该就调用 doHeartBeat()
void Raft::leaderHearBeatTicker() {
    while (true) {
        // 如果自己不是 leader 就没有必要进行后续操作，况且还要拿锁，很影响性能，目前是睡眠，后面再优化优化
        while (m_status != Leader) {
            usleep(1000 * HeartBeatTimeout);
            // std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
        }
        static std::atomic<int32_t> atomicCount = 0;
        // 表示当前线程需要睡眠的时间，计算方式基于心跳超时时间和上一次心跳重置时间
        // 目的：用于动态调整睡眠时间，避免线程频繁检查状态导致 cpu 空转
        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
        std::chrono::system_clock::time_point wakeTime{};
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            wakeTime = now();
            // 睡眠时间计算公式 = 上一次心跳时间点 + 心跳周期 - 当前时间
            suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
        }

        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
            std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count()
                      << " 毫秒\033[0m" << std::endl;
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();

            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
            // std::this_thread::sleep_for(suitableSleepTime);

            // 获取函数运行结束后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用 ANSI 控制序列将输出颜色修改为紫色
            std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count()
                      << " 毫秒\033[0m" << std::endl;
            ++atomicCount;
        }

        // 如果睡眠的这段时间重置了定时器，没有超时，就再次睡眠
        if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0) {
            continue;
        }
        // DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了\n", m_me);

        doHeartBeat(); // 执行实际发送心跳的函数
    }
}

// 负责发送快照的 RPC，在发送完 RPC 后还要负责接收并处理对端发送回来的响应
void Raft::leaderSendSnapShot(int server) {
    m_mtx.lock();
    raftRpcProctoc::InstallSnapshotRequest args;
    args.set_leaderid(m_me);
    args.set_term(m_currentTerm);
    args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
    args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
    args.set_data(m_persister->ReadSnapshot());

    raftRpcProctoc::InstallSnapshotResponse reply;
    m_mtx.unlock();
    bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
    m_mtx.lock();
    DEFER { m_mtx.unlock(); };
    if (!ok) {
        return;
    }
    if (m_status != Leader || m_currentTerm != args.term()) {
        return;  // 中间释放过锁，可能状态已经改变了
    }
    //	无论什么时候都要判断term
    if (reply.term() > m_currentTerm) {
        // 三变
        m_currentTerm = reply.term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
        m_lastResetElectionTime = now();
        return;
    }
    m_matchIndex[server] = args.lastsnapshotincludeindex();
    m_nextIndex[server] = m_matchIndex[server] + 1;
}

void Raft::leaderUpdateCommitIndex() {
    m_commitIndex = m_lastSnapshotIncludeIndex;
    // for index := rf.commitIndex+1;index < len(rf.log);index++ {
    // for index := rf.getLastIndex();index>=rf.commitIndex+1;index--{
    for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--) {
        int sum = 0;
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                sum += 1;
                continue;
            }
            if (m_matchIndex[i] >= index) {
                sum += 1;
            }
        }

        //        !!!只有当前term有新提交的，才会更新commitIndex！！！！
        // log.Printf("lastSSP:%d, index: %d, commitIndex: %d, lastIndex: %d",rf.lastSSPointIndex, index,
        // rf.commitIndex, rf.getLastIndex())
        if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
            m_commitIndex = index;
            break;
        }
    }
    //    DPrintf("[func-leaderUpdateCommitIndex()-rf{%v}] Leader %d(term%d) commitIndex
    //    %d",rf.me,rf.me,rf.currentTerm,rf.commitIndex)
}

// 进来前要保证logIndex是存在的，即≥rf.lastSnapshotIncludeIndex	，而且小于等于rf.getLastLogIndex()
bool Raft::matchLog(int logIndex, int logTerm) {
    myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
             format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                    logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
    return logTerm == getLogTermFromLogIndex(logIndex);
    // if logIndex == rf.lastSnapshotIncludeIndex {
    // 	return logTerm == rf.lastSnapshotIncludeTerm
    // } else {
    // 	return logTerm == rf.logs[rf.getSlicesIndexFromLogIndex(logIndex)].LogTerm
    // }
}

void Raft::persist() {
    // Your code here (2C).
    auto data = persistData();
    m_persister->SaveRaftState(data);
    // fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm,
    // rf.votedFor, rf.logs) fmt.Printf("%v\n", string(data))
}

// 响应 sendRequestVote()，即 Candidate 节点发来的 RequestVote RPC，判断是否投票给他让其成为 Leader
void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
    std::lock_guard<std::mutex> lg(m_mtx); // 加锁，防止竞态

    DEFER { // 确保即使提前 return，也会在退出前调用 persist()
        persist(); // 应该先持久化，再撤销 lock，要在锁释放前更新状态避免在锁释放后才更新，导致状态被别人更新
    };

    // 任何情况下都应该检查任期，对 Candidate 的任期与当前任期对比的三种情况分别进行处理：

    // 情况1：Candidate 的任期小于自己的任期，说明出现了网络分区，该候选者已经 OutOfDate（过时），因此拒绝给他投票，然后直接返回
    if (args->term() < m_currentTerm) {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Expire);
        reply->set_votegranted(false); // 投票失败
        return;
    }
    // 情况2：Candidate 的任期大于自己的任期。如果自己也是候选者，那么应该更新 term，并主动退让为 Follower
    if (args->term() > m_currentTerm) {
        //        DPrintf("[	    func-RequestVote-rf(%v)		] : 变成follower且更新term
        //        因为candidate{%v}的term{%v}> rf{%v}.term{%v}\n ", rf.me, args.CandidateId, args.Term, rf.me,
        //        rf.currentTerm)
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1; // 期望它投票继续进行日志复制

        // 重置定时器：收到 leader 的 ae，开始选举，投出票
        // 这时更新了 term 之后，votedFor 也要置为 -1
    }
    myAssert(args->term() == m_currentTerm,
             format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));

    // 情况3：两节点的任期都是相同的，还需要检查 log 的 term 和 index 是不是匹配的
    int lastLogTerm = getLastLogTerm();
    
    // UpToDate 函数用于比较候选者的日志是否比自己的新，只有候选者的日志至少与当前节点一样新时，才会考虑投给它
    if (!UpToDate(args->lastlogindex(), args->lastlogterm())) { 
        // args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
        // 日志太旧了
        if (args->lastlogterm() < lastLogTerm) {
            //                    DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
            //                    candidate_lastlog_term{%v} < lastlog_term{%v}\n", rf.me, args.CandidateId,
            //                    args.LastLogTerm, lastLogTerm)
        } else {
            //            DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
            //            candidate_log_index{%v} < log_index{%v}\n", rf.me, args.CandidateId, args.LastLogIndex,
            //            rf.getLastLogIndex())
        }

        // 候选者日志的最后一个任期或者索引和请求参数的不匹配导致无法投票
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);

        return;
    }

    // 处理由于网络不好导致响应丢失重发的情况
    if (m_votedFor != -1 && m_votedFor != args->candidateid()) { // 该节点已经投过票但不是投给的该候选者
        //        DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because has voted\n",
        //        rf.me, args.CandidateId)
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);

        return;
    } else { // 该节点已经投过票且投给的是该候选者
        m_votedFor = args->candidateid();
        m_lastResetElectionTime = now();  // 必须要在投出票的时候才重置定时器 
        //        DPrintf("[	    func-RequestVote-rf(%v)		] : voted rf[%v]\n", rf.me, rf.votedFor)
        reply->set_term(m_currentTerm);
        reply->set_votestate(Normal);
        reply->set_votegranted(true);

        return;
    }
}

bool Raft::UpToDate(int index, int term) {
    // lastEntry := rf.log[len(rf.log)-1]

    int lastIndex = -1;
    int lastTerm = -1;
    getLastLogIndexAndTerm(&lastIndex, &lastTerm);
    return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
    if (m_logs.empty()) {
        *lastLogIndex = m_lastSnapshotIncludeIndex;
        *lastLogTerm = m_lastSnapshotIncludeTerm;
        return;
    } else {
        *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
        *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
        return;
    }
}
/**
 *
 * @return 最新的log的logindex，即log的逻辑index。区别于log在m_logs中的物理index
 * 可见：getLastLogIndexAndTerm()
 */
int Raft::getLastLogIndex() {
    int lastLogIndex = -1;
    int _ = -1;
    getLastLogIndexAndTerm(&lastLogIndex, &_);
    return lastLogIndex;
}

int Raft::getLastLogTerm() {
    int _ = -1;
    int lastLogTerm = -1;
    getLastLogIndexAndTerm(&_, &lastLogTerm);
    return lastLogTerm;
}

/**
 *
 * @param logIndex log的逻辑index。注意区别于m_logs的物理index
 * @return
 */
int Raft::getLogTermFromLogIndex(int logIndex) {
    myAssert(logIndex >= m_lastSnapshotIncludeIndex,
             format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                    logIndex, m_lastSnapshotIncludeIndex));

    int lastLogIndex = getLastLogIndex();

    myAssert(logIndex <= lastLogIndex,
             format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}", m_me, logIndex,
                    lastLogIndex));

    if (logIndex == m_lastSnapshotIncludeIndex) {
        return m_lastSnapshotIncludeTerm;
    } else {
        return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
    }
}

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

// 找到index对应的真实下标位置！！！
// 限制，输入的logIndex必须保存在当前的logs里面（不包含snapshot）
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
    myAssert(logIndex > m_lastSnapshotIncludeIndex,
             format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                    logIndex, m_lastSnapshotIncludeIndex));
    int lastLogIndex = getLastLogIndex();
    myAssert(logIndex <= lastLogIndex,
             format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}", m_me, logIndex,
                    lastLogIndex));
    int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
    return SliceIndex;
}

// 用于 candidate 向其他节点发送 RequestVote RPC 请求，以争取选票成为集群的 leader
bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
    auto start = now();
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 開始", m_me, m_currentTerm, getLastLogIndex());
    bool ok = m_peers[server]->RequestVote(args.get(), reply.get());  // 接收其他 raft 节点的返回结果
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 完畢，耗時:{%d} ms", m_me, m_currentTerm,
            getLastLogIndex(), now() - start);

    // 这个 ok 表示网络是否能正常通信，而不是 是否投票
    if (!ok) {
        return ok;  // RPC 通信失败就立刻返回，避免浪费资源
    }
    // for !ok {
    //
    //	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    //	//if ok {
    //	//	break
    //	//}
    // } //这里是发送出去了，但是不能保证他一定到达

    // 已经发送过去了，现在对回应进行处理，无论什么时候收到回复都要检查 term
    std::lock_guard<std::mutex> lg(m_mtx);
    // 如果别的节点回复的 term 比自己的大，说明自己落后了，就退回 follower 状态并把 term 更新为较大的那个
    if (reply->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;  // 重置投票
        persist();        // 持久化
        return true;      // 表示 RPC 顺利发送并响应回来
    } else if (reply->term() < m_currentTerm) {
        return true;  // 表示 RPC 顺利发送并响应回来
    }
    // 判断响应节点的 term 是否和自己一致
    myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));

    // 如果这个节点由于某些原因没给本节点投票，结束该函数
    if (!reply->votegranted()) {
        return true;
    }

    *votedNum = *votedNum + 1; // 注意自己会给自己投一票

    // 得到超半数票就当选 leader
    if (*votedNum >= m_peers.size() / 2 + 1) {
        *votedNum = 0;
        if (m_status == Leader) {
            // 如果之前已经是 leader 了，就不会进行下一步处理了
            myAssert(false, format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me,
                                   m_currentTerm));
        }
        // 刚刚变成 leader，初始化状态和 nextIndex、matchIndex
        m_status = Leader;

        DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me,
                m_currentTerm, getLastLogIndex());

        int lastLogIndex = getLastLogIndex();
        for (int i = 0; i < m_nextIndex.size(); i++) {
            m_nextIndex[i] = lastLogIndex + 1;  // 有效下标从1开始，因此要+1
            m_matchIndex[i] = 0;                // 表示已经接收到领导日志的 index 下标，每换一个领导都要从0开始 
        }
        std::thread t(&Raft::doHeartBeat, this);  // 发起心跳向其他节点宣告自己是 leader
        t.detach();

        persist();
    }
    return true;
}

// 负责发送日志追加条目的 RPC，在发送完 RPC 后还要负责接收并处理对端发送回来的响应
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums) {
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc开始 ， args->entries_size():{%d}",
            m_me, server, args->entries_size());
    
    // 调用 RPC 发送 AppendEntries 请求并等待响应，ok 表示 RPC 调用成功，而不是请求的逻辑处理结果
    bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

    if (!ok) { // 如果 RPC 调用失败（比如网络问题）
        DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失败", m_me, server);
        return ok;
    }
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);

    if (reply->appstate() == Disconnected) { // RPC 调用成功，但 follower 由于网络分区或其他原因未能处理请求
        return ok;
    }

    std::lock_guard<std::mutex> lg1(m_mtx);
    // 对 reply 进行处理，无论什么时候都要检查 term
    if (reply->term() > m_currentTerm) { // 说明自己的 term 过时，降级为 follower，并更新 term 和 votedFor
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;
        persist(); // 源代码没有这句，但文档里有
        return ok;
    } else if (reply->term() < m_currentTerm) {
        // 忽略 term 过时的 follower 的响应
        DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server,
                reply->term(), m_me, m_currentTerm);
        return ok;
    }

    if (m_status != Leader) {
        // 如果当前节点不是 leader，那么就不要对返回的情况进行处理了
        return ok;
    }
    
    // 确保响应返回的 term 和自己的一致
    myAssert(reply->term() == m_currentTerm,
             format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));

    if (!reply->success()) { // 日志不匹配，调整 nextIndex 并重试
        // 正常来说就是 index 要不断往前 -1 遍历直至匹配，既然能走到这里，第一个日志（idnex = 1）发送后肯定是匹配的，因此不用考虑会变成负数 
        // 因为真正的环境不会知道是服务器宕机还是发生网络分区了
        if (reply->updatenextindex() != -100) { // -100 是特殊标记，用于优化 leader 的回退逻辑
            // todo:待总结，就算term匹配，失败的时候nextIndex也不是照单全收的，因为如果发生rpc延迟，leader的term可能从不符合term要求
            // 变得符合term要求
            // 但是不能直接赋值reply.UpdateNextIndex
            DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n",
                    m_me, server, reply->updatenextindex());
            m_nextIndex[server] = reply->updatenextindex();  // 使用 follower 提供的 nextIndex，减少不必要的重试
        }
    } else { // 日志匹配，更新 appendNums 和相关索引
        *appendNums = *appendNums + 1; // 表示有一个 follower 接收了日志或心跳
        DPrintf("---------------------------tmp------------------------- 节点{%d}返回true,当前*appendNums{%d}", server,
                *appendNums);
        // rf.matchIndex[server] = len(args.Entries) //只要返回一个响应就对其matchIndex应该对其做出反应，
        // 但是这么修改是有问题的，如果对某个消息发送了多遍（心跳时就会再发送），那么一条消息会导致n次上涨

        // 更新 matchIndex 和 nextIndex
        m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
        m_nextIndex[server] = m_matchIndex[server] + 1;
        int lastLogIndex = getLastLogIndex();

        myAssert(m_nextIndex[server] <= lastLogIndex + 1,
                 format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d",
                        server, m_logs.size(), server, lastLogIndex));
        
        // 检查是否有日志可以 commit
        if (*appendNums >= 1 + m_peers.size() / 2) { // 超过半数节点已复制该日志条目就可以commit了
            // 两种方法保证幂等性：1.赋值为0 	2.上面 >=  改为 ==
            *appendNums = 0; // 避免重复提交
            // todo https://578223592-laughing-halibut-wxvpggvw69qh99q4.github.dev/ 不断遍历来统计rf.commitIndex
            // 改了好久！！！！！
            // leader只有在当前term有日志提交的时候才更新commitIndex，因为raft无法保证之前term的Index是否提交
            // 只有当前term有日志提交，之前term的log才可以被提交，只有这样才能保证“领导人完备性{当选领导人的节点拥有之前被提交的所有log，当然也可能有一些没有被提交的}”
            // rf.leaderUpdateCommitIndex()
            if (args->entries_size() > 0) {
                DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}",
                        args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
            }
            // 只有当前 term 的日志被大多数 follower 同步后，才能 commit
            if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
                DPrintf(
                    "---------------------------tmp------------------------- "
                    "当前term有log成功提交，更新leader的m_commitIndex "
                    "from{%d} to{%d}",
                    m_commitIndex, args->prevlogindex() + args->entries_size());

                m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
            }
            myAssert(m_commitIndex <= lastLogIndex,
                     format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, lastLogIndex,
                            m_commitIndex));
            // fmt.Printf("[func-sendAppendEntries,rf{%v}] len(rf.logs):%v  rf.commitIndex:%v\n", rf.me, len(rf.logs),
            // rf.commitIndex)
        }
    }
    return ok;
}

void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) {
    AppendEntries1(request, response);
    done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController* controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest* request,
                           ::raftRpcProctoc::InstallSnapshotResponse* response, ::google::protobuf::Closure* done) {
    InstallSnapshot(request, response);

    done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) {
    RequestVote(request, response);
    done->Run();
}

// 启动 Raft 日志复制流程
void Raft::Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader) {
    std::lock_guard<std::mutex> lg1(m_mtx);
  
    if (m_status != Leader) {
        DPrintf("[func-Start-rf{%d}]  is not leader");
        *newLogIndex = -1;
        *newLogTerm = -1;
        *isLeader = false;
        return;
    }

    raftRpcProctoc::LogEntry newLogEntry;
    newLogEntry.set_command(command.asString());
    newLogEntry.set_logterm(m_currentTerm);
    newLogEntry.set_logindex(getNewCommandIndex());
    m_logs.emplace_back(newLogEntry);

    int lastLogIndex = getLastLogIndex();

    // leader应该不停的向各个Follower发送AE来维护心跳和保持日志同步，目前的做法是新的命令来了不会直接执行，而是等待leader的心跳触发
    DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
    // rf.timer.Reset(10) //接收到命令后马上给follower发送,改成这样不知为何会出现问题，待修正 todo
    persist();
    *newLogIndex = newLogEntry.logindex();
    *newLogTerm = newLogEntry.logterm();
    *isLeader = true;
}

/**
 * 初始化一个 Raft 节点
 * 所有 Raft 节点的通信端口都存在 peers[] 中，本节点的端口是 peers[me]，所有节点的 peers[] 顺序一致，用于统一编号和通信
 * 持久化器 persister 用于保存当前节点的 Raft 状态，如：term、log、快照，同时也包含重启时恢复用的状态
 * applyCh 是服务用于接收 Raft 发来的 ApplyMsg 的通道
 * init() 必须快速返回，因此长时间运行的任务应另起线程或协程
 */
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
    /**
     * 参数：
     * peers: 所有节点的 RPC 通信对象数组
     * me: 当前节点在 peers 中的下标
     * persister: 用于存储 Raft 状态
     * applyCh: 与上层服务通信的消息通道
     */
    m_peers = peers;           
    m_persister = persister;   
    m_me = me; // 标记自己，不能给自己发送 rpc
   
    m_mtx.lock(); // 线程安全，防止并发初始化
    
    /**
     * 核心状态初始化（2A、2B、2C）
     * 在 MIT 6.824 分布式系统课程中，2A、2B、2C 通常指的是实验的不同阶段，每一阶段要求实现 Raft 协议的核心功能
     * 2A：Leader 选举
     * 2B：日志复制 
     * 2C：日志持久化和日志应用
     */

    this->applyChan = applyCh;  // 与 kvServer 交互

    m_currentTerm = 0;    // 任期初始化为0
    m_status = Follower;  // 初始化身份为 Follower
    m_commitIndex = 0;    // 初始化日志提交索引
    m_lastApplied = 0;    // 初始化应用到状态机的日志索引
    m_logs.clear(); // 清空日志

    // matchIndex[]: Leader 记录每个 Follower 日志匹配到哪一条
    // nextIndex[]: Leader 记录向每个 Follower 发送下一条日志的 index
    for (int i = 0; i < m_peers.size(); i++) {
        m_matchIndex.push_back(0);   
        m_nextIndex.push_back(0);
    }

    m_votedFor = -1;  // 表示还没投过票

    // 快照字段初始化
    m_lastSnapshotIncludeIndex = 0;
    m_lastSnapshotIncludeTerm = 0;
    m_lastResetElectionTime = now();
    m_lastResetHearBeatTime = now();

    // 如果之前出现故障，从持久化中恢复
    readPersist(m_persister->ReadRaftState());  // 从持久化存储中恢复 raft 节点状态
    // 如果当前状态是基于快照恢复的（即有快照），则将状态机已经应用的日志索引 m_lastApplied 设置为快照中最后一个包含的日志索引
    // 避免状态机重复应用快照之前的日志，保证状态机状态与快照保持一致
    if (m_lastSnapshotIncludeIndex > 0) {
        m_lastApplied = m_lastSnapshotIncludeIndex;
        // rf.commitIndex = rf.lastSnapshotIncludeIndex   todo：崩溃恢复为何不能读取 commitIndex
    }

    DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
            m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

    m_mtx.unlock();  // 完成初始化后解锁，以便其他线程或协程可以访问共享数据


    /**
     * 启动三个循环定时器：
     *     -leaderHeartBeatTicker：Leader 周期性发送心跳
     *     -electionTimeOutTicker：Follower 检测选举超时
     *     -applierTicker：定期将日志应用到状态机中
     * 原本是启动了三个线程，现在改为前两个使用协程，另一个还是线程
     * 因为 leaderHearBeatTicker() 和 electionTimeOutTicker()的执行时间恒定，没有 IO 阻塞，逻辑轻，适合用协程
     * 而 applierTicker() 时间受到数据库响应延迟和两次 apply 之间请求数量的影响，这个随着数据量增多性能可能会降低，最好还是启用一个线程
     */
    m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

    m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });
    m_ioManager->scheduler([this]() -> void { this->electionTimeOutTicker(); });

    std::thread t3(&Raft::applierTicker, this);
    t3.detach();

    // std::thread t(&Raft::leaderHearBeatTicker, this);
    // t.detach();
    //
    // std::thread t2(&Raft::electionTimeOutTicker, this);
    // t2.detach();
    //
    // std::thread t3(&Raft::applierTicker, this);
    // t3.detach();
}

std::string Raft::persistData() {
    BoostPersistRaftNode boostPersistRaftNode;
    boostPersistRaftNode.m_currentTerm = m_currentTerm;
    boostPersistRaftNode.m_votedFor = m_votedFor;
    boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
    boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
    for (auto& item : m_logs) {
        boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
    }
    
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << boostPersistRaftNode;
    return ss.str();
}

void Raft::readPersist(std::string data) {
    if (data.empty()) {
        return;
    }
    std::stringstream iss(data);
    boost::archive::text_iarchive ia(iss);
    // read class state from archive
    BoostPersistRaftNode boostPersistRaftNode;
    ia >> boostPersistRaftNode;

    m_currentTerm = boostPersistRaftNode.m_currentTerm;
    m_votedFor = boostPersistRaftNode.m_votedFor;
    m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
    m_logs.clear();
    for (auto& item : boostPersistRaftNode.m_logs) {
        raftRpcProctoc::LogEntry logEntry;
        logEntry.ParseFromString(item);
        m_logs.emplace_back(logEntry);
    }
}

void Raft::Snapshot(int index, std::string snapshot) {
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
        DPrintf(
            "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger "
            "or "
            "smaller ",
            m_me, index, m_lastSnapshotIncludeIndex);
        return;
    }
    auto lastLogIndex = getLastLogIndex();  // 为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

    // 制造完此快照后剩余的所有日志
    int newLastSnapshotIncludeIndex = index;
    int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
    std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
    // todo :这种写法有点笨，待改进，而且有内存泄漏的风险
    for (int i = index + 1; i <= getLastLogIndex(); i++) {
        // 注意有=，因为要拿到最后一个日志
        trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
    }
    m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
    m_logs = trunckedLogs;
    m_commitIndex = std::max(m_commitIndex, index);
    m_lastApplied = std::max(m_lastApplied, index);

    // rf.lastApplied = index //lastApplied 和 commit应不应该改变呢？？？ 为什么  不应该改变吧
    m_persister->Save(persistData(), snapshot);

    DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
            m_lastSnapshotIncludeTerm, m_logs.size());
    myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
             format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                    m_lastSnapshotIncludeIndex, lastLogIndex));
}