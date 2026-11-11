#pragma once

#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

namespace raftkv
{

    enum class LogLevel : int
    {
        kDebug = 0,
        kInfo = 1,
        kWarn = 2,
        kError = 3
    };

    LogLevel ParseLogLevel(const char *s);
    void SetLogLevel(LogLevel level);
    LogLevel GetLogLevel();

    void InitLogFromEnv();

    void LogWrite(LogLevel level, const char *file, int line, const char *fmt, ...)
        __attribute__((format(printf, 4, 5)));

    inline const char *LogLevelName(LogLevel level)
    {
        switch (level)
        {
        case LogLevel::kDebug:
            return "DEBUG";
        case LogLevel::kInfo:
            return "INFO";
        case LogLevel::kWarn:
            return "WARN";
        case LogLevel::kError:
            return "ERROR";
        }
        return "?";
    }

} 

#define RKV_LOG(level, ...)                                                       \
    do                                                                            \
    {                                                                             \
        if (static_cast<int>(level) >= static_cast<int>(::raftkv::GetLogLevel())) \
            ::raftkv::LogWrite(level, __FILE__, __LINE__, __VA_ARGS__);           \
    } while (0)

#define LOG_DEBUG(...) RKV_LOG(::raftkv::LogLevel::kDebug, __VA_ARGS__)
#define LOG_INFO(...) RKV_LOG(::raftkv::LogLevel::kInfo, __VA_ARGS__)
#define LOG_WARN(...) RKV_LOG(::raftkv::LogLevel::kWarn, __VA_ARGS__)
#define LOG_ERROR(...) RKV_LOG(::raftkv::LogLevel::kError, __VA_ARGS__)
