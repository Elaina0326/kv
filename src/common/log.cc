#include "common/log.h"

#include <cstdlib>
#include <ctime>
#include <mutex>

namespace raftkv
{
    namespace
    {
        std::atomic<int> g_level{static_cast<int>(LogLevel::kInfo)};
        std::mutex g_mu;
    }

    LogLevel ParseLogLevel(const char *s)
    {
        if (s == nullptr)
            return LogLevel::kInfo;
        std::string v(s);
        if (v == "debug")
            return LogLevel::kDebug;
        if (v == "warn")
            return LogLevel::kWarn;
        if (v == "error")
            return LogLevel::kError;
        return LogLevel::kInfo;
    }

    void SetLogLevel(LogLevel level) { g_level.store(static_cast<int>(level)); }

    LogLevel GetLogLevel() { return static_cast<LogLevel>(g_level.load(std::memory_order_relaxed)); }

    void InitLogFromEnv()
    {
        const char *env = std::getenv("RAFTKV_LOG");
        if (env != nullptr)
            SetLogLevel(ParseLogLevel(env));
    }

    void LogWrite(LogLevel level, const char *file, int line, const char *fmt, ...)
    {
        char msg[2048];
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(msg, sizeof(msg), fmt, ap);
        va_end(ap);

        const char *base = std::strrchr(file, '/');
        base = (base == nullptr) ? file : base + 1;

        struct timespec ts{};
        clock_gettime(CLOCK_REALTIME, &ts);
        struct tm tm_time{};
        localtime_r(&ts.tv_sec, &tm_time);

        char head[64];
        snprintf(head, sizeof(head), "%02d:%02d:%02d.%03ld", tm_time.tm_hour, tm_time.tm_min,
                 tm_time.tm_sec, ts.tv_nsec / 1000000);

        static thread_local int tls_tid = -1;
        if (tls_tid < 0)
        {
            static std::atomic<int> next_tid{0};
            tls_tid = next_tid.fetch_add(1);
        }

        std::lock_guard<std::mutex> lock(g_mu);
        fprintf(stderr, "[%s %-5s t%d %s:%d] %s\n", head, LogLevelName(level), tls_tid, base, line, msg);
        fflush(stderr);
    }

} 
