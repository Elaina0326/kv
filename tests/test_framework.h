#pragma once

#include <functional>
#include <string>
#include <type_traits>
#include <vector>

#include "common/types.h"

namespace testing
{

    struct TestCase
    {
        std::string name;
        std::function<void()> fn;
    };

    std::vector<TestCase> &Registry();
    void ReportFailure(const char *file, int line, const std::string &message);
    int RunAll();

    struct Registrar
    {
        Registrar(const char *name, std::function<void()> fn);
    };

    // 断言里最常出现的就是 ErrorCode，单独给它可读输出（否则失败信息是「期望 <value>」）
    inline std::string Show(raftkv::ErrorCode code) { return raftkv::ErrorCodeName(code); }

    template <typename T>
    std::string Show(const T &value)
    {
        if constexpr (std::is_arithmetic<T>::value)
        {
            return std::to_string(value);
        }
        else if constexpr (std::is_convertible<T, std::string>::value)
        {
            return std::string(value);
        }
        else if constexpr (std::is_enum<T>::value)
        {
            return std::to_string(static_cast<long long>(value));
        }
        else
        {
            return "<value>";
        }
    }

} // namespace testing

#define TEST_CASE(name)                                        \
    static void name();                                        \
    static ::testing::Registrar registrar_##name(#name, name); \
    static void name()

#define EXPECT_TRUE(cond)                                                           \
    do                                                                              \
    {                                                                               \
        if (!(cond))                                                                \
        {                                                                           \
            ::testing::ReportFailure(__FILE__, __LINE__, "EXPECT_TRUE(" #cond ")"); \
        }                                                                           \
    } while (0)

#define EXPECT_FALSE(cond) EXPECT_TRUE(!(cond))

#define EXPECT_EQ(expected, actual)                                                                \
    do                                                                                             \
    {                                                                                              \
        const auto &expect_value = (expected);                                                     \
        const auto &actual_value = (actual);                                                       \
        if (!(expect_value == actual_value))                                                       \
        {                                                                                          \
            ::testing::ReportFailure(__FILE__, __LINE__,                                           \
                                     std::string("EXPECT_EQ(" #expected ", " #actual ")：期望 ") + \
                                         ::testing::Show(expect_value) + "，实际 " +               \
                                         ::testing::Show(actual_value));                           \
        }                                                                                          \
    } while (0)

#define EXPECT_NE(left, right)                                                                    \
    do                                                                                            \
    {                                                                                             \
        const auto &left_value = (left);                                                          \
        const auto &right_value = (right);                                                        \
        if (left_value == right_value)                                                            \
        {                                                                                         \
            ::testing::ReportFailure(__FILE__, __LINE__,                                          \
                                     std::string("EXPECT_NE(" #left ", " #right ")：两者都是 ") + \
                                         ::testing::Show(left_value));                            \
        }                                                                                         \
    } while (0)

#define ASSERT_TRUE(cond)                                                           \
    do                                                                              \
    {                                                                               \
        if (!(cond))                                                                \
        {                                                                           \
            ::testing::ReportFailure(__FILE__, __LINE__, "ASSERT_TRUE(" #cond ")"); \
            return;                                                                 \
        }                                                                           \
    } while (0)
