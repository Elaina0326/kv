#pragma once

#include <sys/epoll.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace raftkv
{

    class EventLoop
    {
    public:
        using Handler = std::function<void(uint32_t events)>;

        EventLoop();
        ~EventLoop();

        EventLoop(const EventLoop &) = delete;
        EventLoop &operator=(const EventLoop &) = delete;

        bool Init(std::string *err);

        void Loop();
        void Quit();


        void RunInLoop(std::function<void()> fn);

        bool AddHandler(int fd, uint32_t events, Handler handler);
        bool UpdateEvents(int fd, uint32_t events);
        void RemoveHandler(int fd);


        bool WaitStopped(int timeout_ms);

        bool IsInLoopThread() const { return std::this_thread::get_id() == thread_id_; }
        bool running() const { return running_.load(); }

    private:
        void HandleWakeup();
        void DoPendingFunctors();
        void Wakeup();

        int epfd_ = -1;
        int wakeup_fd_ = -1;
        std::unordered_map<int, uint32_t> events_;
        std::unordered_map<int, Handler> handlers_;
        std::vector<epoll_event> revents_;

        std::mutex mu_;
        std::vector<std::function<void()>> pending_;

        std::atomic<bool> running_{false};
        std::atomic<bool> quit_{false};
        std::thread::id thread_id_;

        std::mutex stop_mu_;
        std::condition_variable stop_cv_;
    };

} 
