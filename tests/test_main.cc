#include "test_framework.h"

#include <chrono>
#include <cstdio>
#include <exception>

namespace testing
{
    namespace
    {

        bool g_current_failed = false;

    } 

    std::vector<TestCase> &Registry()
    {
        static std::vector<TestCase> registry;
        return registry;
    }

    Registrar::Registrar(const char *name, std::function<void()> fn)
    {
        Registry().push_back(TestCase{name, std::move(fn)});
    }

    void ReportFailure(const char *file, int line, const std::string &message)
    {
        g_current_failed = true;
        std::printf("      %s:%d: %s\n", file, line, message.c_str());
    }

    int RunAll()
    {
        int failed_cases = 0;
        int passed_cases = 0;
        const auto begin = std::chrono::steady_clock::now();
        for (auto &test_case : Registry())
        {
            g_current_failed = false;
            std::printf("[ RUN      ] %s\n", test_case.name.c_str());
            try
            {
                test_case.fn();
            }
            catch (const std::exception &e)
            {
                g_current_failed = true;
                std::printf("      未捕获异常: %s\n", e.what());
            }
            catch (...)
            {
                g_current_failed = true;
                std::printf("      未捕获的未知异常\n");
            }
            if (g_current_failed)
            {
                ++failed_cases;
                std::printf("[  FAILED  ] %s\n", test_case.name.c_str());
            }
            else
            {
                ++passed_cases;
                std::printf("[       OK ] %s\n", test_case.name.c_str());
            }
        }
        const double elapsed_ms = std::chrono::duration<double, std::milli>(
                                      std::chrono::steady_clock::now() - begin)
                                      .count();
        std::printf("\n%d 个用例通过，%d 个失败，耗时 %.1f ms\n", passed_cases, failed_cases, elapsed_ms);
        return failed_cases == 0 ? 0 : 1;
    }

} 

int main() { return testing::RunAll(); }
