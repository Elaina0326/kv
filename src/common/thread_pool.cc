#include "common/thread_pool.h"
#include "common/log.h"

namespace raftkv
{

    ThreadPool::ThreadPool(size_t threads)
    {
        for (size_t i = 0; i < threads; ++i)
        {
            workers_.emplace_back([this]
                                  { WorkerLoop(); });
        }
    }

    ThreadPool::~ThreadPool() { Stop(); }

    void ThreadPool::Submit(std::function<void()> task)
    {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopped_)
                return;
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
    }

    void ThreadPool::Stop()
    {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopped_)
                return;
            stopped_ = true;
        }
        cv_.notify_all();
        for (auto &t : workers_)
        {
            if (t.joinable())
                t.join();
        }
        workers_.clear();
    }

    void ThreadPool::WorkerLoop()
    {
        while (true)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mu_);
                cv_.wait(lock, [this]
                         { return stopped_ || !tasks_.empty(); });
                if (stopped_ && tasks_.empty())
                    return;
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            try
            {
                task();
            }
            catch (const std::exception &e)
            {
                LOG_ERROR("thread pool task threw: %s", e.what());
            }
        }
    }

} 
