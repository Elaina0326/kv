#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace raftkv
{


    class ThreadPool
    {
    public:
        explicit ThreadPool(size_t threads);
        ~ThreadPool();

        ThreadPool(const ThreadPool &) = delete;
        ThreadPool &operator=(const ThreadPool &) = delete;

        void Submit(std::function<void()> task);
        void Stop();

    private:
        void WorkerLoop();

        std::vector<std::thread> workers_;
        std::queue<std::function<void()>> tasks_;
        std::mutex mu_;
        std::condition_variable cv_;
        bool stopped_ = false;
    };

} 
