#include "net/event_loop.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>

#include "common/log.h"
#include "common/util.h"

namespace raftkv
{

    EventLoop::EventLoop() : revents_(256) { thread_id_ = std::this_thread::get_id(); }

    EventLoop::~EventLoop()
    {
        if (wakeup_fd_ >= 0)
            ::close(wakeup_fd_);
        if (epfd_ >= 0)
            ::close(epfd_);
    }

    bool EventLoop::Init(std::string *err)
    {
        epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epfd_ < 0)
        {
            *err = "epoll_create1 failed: " + ErrnoString();
            return false;
        }
        wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wakeup_fd_ < 0)
        {
            *err = "eventfd failed: " + ErrnoString();
            return false;
        }
        if (!AddHandler(wakeup_fd_, EPOLLIN, [this](uint32_t)
                        { HandleWakeup(); }))
        {
            *err = "add wakeup fd failed: " + ErrnoString();
            return false;
        }
        return true;
    }

    void EventLoop::Loop()
    {
        thread_id_ = std::this_thread::get_id();
        running_.store(true);
        LOG_INFO("event loop running, epfd=%d", epfd_);

        while (!quit_.load())
        {
            int n = ::epoll_wait(epfd_, revents_.data(), static_cast<int>(revents_.size()), 100);
            if (n < 0)
            {
                if (errno == EINTR)
                    continue;
                LOG_ERROR("epoll_wait failed: %s", ErrnoString().c_str());
                break;
            }
            for (int i = 0; i < n; ++i)
            {
                const int fd = revents_[i].data.fd;
                const uint32_t ev = revents_[i].events;
                auto it = handlers_.find(fd);
                if (it == handlers_.end())
                    continue;
        
                Handler handler = it->second;
                handler(ev);
            }
            DoPendingFunctors();
        }

        {
            std::lock_guard<std::mutex> lock(stop_mu_);
            running_.store(false);
        }
        stop_cv_.notify_all();
        LOG_INFO("event loop stopped");
    }

    
    bool EventLoop::WaitStopped(int timeout_ms)
    {
        std::unique_lock<std::mutex> lock(stop_mu_);
        return stop_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                                 [this]
                                 { return !running_.load(); });
    }

    void EventLoop::Quit()
    {
        quit_.store(true);
        Wakeup();
    }

    void EventLoop::RunInLoop(std::function<void()> fn)
    {
        if (IsInLoopThread())
        {
            fn();
            return;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            pending_.push_back(std::move(fn));
        }
        Wakeup();
    }

    bool EventLoop::AddHandler(int fd, uint32_t events, Handler handler)
    {
        struct epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0)
            return false;
        events_[fd] = events;
        handlers_[fd] = std::move(handler);
        return true;
    }

    bool EventLoop::UpdateEvents(int fd, uint32_t events)
    {
        auto it = events_.find(fd);
        if (it == events_.end())
            return false;
        if (it->second == events)
            return true;
        struct epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) != 0)
            return false;
        it->second = events;
        return true;
    }

    void EventLoop::RemoveHandler(int fd)
    {
        if (fd == wakeup_fd_)
            return;
        if (events_.erase(fd) == 0)
            return;
        ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
        handlers_.erase(fd);
    }

    void EventLoop::HandleWakeup()
    {
        uint64_t value = 0;
        while (::read(wakeup_fd_, &value, sizeof(value)) > 0)
        {
        }
    }

    void EventLoop::Wakeup()
    {
        uint64_t one = 1;
        ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));
        (void)n;
    }

    void EventLoop::DoPendingFunctors()
    {
        std::vector<std::function<void()>> functors;
        {
            std::lock_guard<std::mutex> lock(mu_);
            functors.swap(pending_);
        }
        for (auto &fn : functors)
        {
            try
            {
                fn();
            }
            catch (const std::exception &e)
            {
                LOG_ERROR("loop functor threw: %s", e.what());
            }
        }
    }

} 
