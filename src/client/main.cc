#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "client/kv_client.h"
#include "common/log.h"
#include "common/util.h"

namespace
{

    void PrintUsage()
    {
        std::printf(
            "raftkv-cli —— 分布式 KV 命令行客户端\n"
            "\n"
            "全局选项:\n"
            "  --addrs <list>   节点地址，按 node id 顺序给出，默认 127.0.0.1:9201\n"
            "  --timeout <ms>   单次 RPC 超时，默认 2000\n"
            "  --ttl <ms>       put 时设置过期时间（默认永久）\n"
            "\n"
            "命令:\n"
            "  put <key> <value>        写入\n"
            "  get <key>                读取\n"
            "  del <key>                删除\n"
            "  scan <start> <end> [n]   范围扫描（左闭右开）\n"
            "  status [node_index]      查看各节点角色/任期/进度\n"
            "  bench                    压测\n"
            "\n"
            "bench 选项:\n"
            "  --keys <n>          预置 key 数量，默认 1000\n"
            "  --ops <n>           总操作数，默认 20000\n"
            "  --concurrency <n>   并发客户端数，默认 8\n"
            "  --read-ratio <f>    读比例 0~1，默认 0.5\n"
            "  --value-size <n>    value 字节数，默认 64\n");
    }

    struct BenchOptions
    {
        int keys = 1000;
        int ops = 20000;
        int concurrency = 8;
        double read_ratio = 0.5;
        int value_size = 64;
        uint64_t ttl_ms = 0;
    };

    struct BenchResult
    {
        uint64_t ok = 0;
        uint64_t fail = 0;
        std::vector<uint32_t> lat_us;
    };

    const char *RoleLabel(uint8_t role)
    {
        switch (role)
        {
        case 0:
            return "follower";
        case 1:
            return "candidate";
        case 2:
            return "leader";
        default:
            return "?";
        }
    }

    double Percentile(const std::vector<uint32_t> &sorted, double p)
    {
        if (sorted.empty())
            return 0.0;
        const size_t idx = static_cast<size_t>(p * static_cast<double>(sorted.size() - 1));
        return static_cast<double>(sorted[idx]) / 1000.0; // ms
    }

    int RunBench(const std::vector<std::string> &addrs, const BenchOptions &opt)
    {
        const int threads = std::max(1, opt.concurrency);
        const std::string value(static_cast<size_t>(std::max(1, opt.value_size)), 'v');

        std::printf("[bench] 节点 %zu 个, 预置 %d 个 key (并发 %d)\n", addrs.size(), opt.keys, threads);
        std::atomic<uint64_t> preload_fail{0};
        std::vector<std::thread> workers;
        for (int t = 0; t < threads; ++t)
        {
            workers.emplace_back([&, t]
                                 {
      raftkv::KVClient client(addrs, 3000);
      for (int i = t; i < opt.keys; i += threads) {
        char key[32];
        std::snprintf(key, sizeof(key), "key_%06d", i);
        if (client.Put(key, value, opt.ttl_ms) != raftkv::ErrorCode::kOk) ++preload_fail;
      } });
        }
        for (auto &worker : workers)
            worker.join();
        workers.clear();
        if (preload_fail.load() > 0)
        {
            std::printf("[bench] 预置阶段有 %llu 次失败，继续压测（可能是选主/超时）\n",
                        static_cast<unsigned long long>(preload_fail.load()));
        }

        std::printf("[bench] 开始压测: %d ops, 读比例 %.2f, 并发 %d\n", opt.ops, opt.read_ratio, threads);
        std::vector<BenchResult> results(static_cast<size_t>(threads));
        const auto begin = std::chrono::steady_clock::now();

        for (int t = 0; t < threads; ++t)
        {
            workers.emplace_back([&, t]
                                 {
      raftkv::KVClient client(addrs, 3000);
      BenchResult& result = results[static_cast<size_t>(t)];

      const int ops_per_thread = opt.ops / threads + (t < opt.ops % threads ? 1 : 0);
      result.lat_us.reserve(static_cast<size_t>(ops_per_thread));
      uint64_t rng = 0x9E3779B97F4A7C15ULL * static_cast<uint64_t>(t + 1);
      auto next = [&rng] {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        return rng;
      };
      const uint64_t read_threshold = static_cast<uint64_t>(opt.read_ratio * 1000.0);
      for (int i = 0; i < ops_per_thread; ++i) {
        const bool is_read = (next() % 1000) < read_threshold;
        char key[32];
        std::snprintf(key, sizeof(key), "key_%06d",
                      static_cast<int>(next() % static_cast<uint64_t>(opt.keys)));
        const int64_t t0 = raftkv::NowUs();
        raftkv::ErrorCode code;
        if (is_read) {
          std::string got;
          code = client.Get(key, &got);
        } else {
          code = client.Put(key, value, opt.ttl_ms);
        }
        const int64_t elapsed_us = raftkv::NowUs() - t0;
        if (code == raftkv::ErrorCode::kOk) {
          ++result.ok;
          result.lat_us.push_back(static_cast<uint32_t>(std::min<int64_t>(elapsed_us, 60'000'000)));
        } else {
          ++result.fail;
        }
      } });
        }
        for (auto &worker : workers)
            worker.join();

        const double elapsed_s =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
        uint64_t ok = 0;
        uint64_t fail = 0;
        std::vector<uint32_t> latencies;
        for (const auto &r : results)
        {
            ok += r.ok;
            fail += r.fail;
            latencies.insert(latencies.end(), r.lat_us.begin(), r.lat_us.end());
        }
        std::sort(latencies.begin(), latencies.end());
        const uint64_t total = ok + fail;
        std::printf("\n=== 压测结果 ===\n");
        std::printf("总操作   : %llu (成功 %llu, 失败 %llu)\n", static_cast<unsigned long long>(total),
                    static_cast<unsigned long long>(ok), static_cast<unsigned long long>(fail));
        std::printf("耗时     : %.2f s\n", elapsed_s);
        std::printf("吞吐     : %.0f ops/s\n", elapsed_s > 0 ? static_cast<double>(total) / elapsed_s : 0.0);
        std::printf("延迟(ms) : p50 %.2f | p90 %.2f | p99 %.2f | max %.2f\n",
                    Percentile(latencies, 0.50), Percentile(latencies, 0.90),
                    Percentile(latencies, 0.99), Percentile(latencies, 1.0));

        if (total != static_cast<uint64_t>(opt.ops))
        {
            std::fprintf(stderr, "[bench] 计数错误：--ops %d，实际 %llu 次（成功 %llu / 失败 %llu）\n",
                         opt.ops, static_cast<unsigned long long>(total),
                         static_cast<unsigned long long>(ok), static_cast<unsigned long long>(fail));
            return 2;
        }
        return fail == 0 ? 0 : 1;
    }

    int PrintStatus(const std::vector<std::string> &addrs, int only_index)
    {
        raftkv::KVClient client(addrs, 1000);
        std::printf("%-6s %-10s %-8s %-12s %-12s %-12s %-8s %-8s\n", "node", "role", "term", "commit",
                    "applied", "last_log", "leader", "keys");
        int failures = 0;
        for (size_t i = 0; i < addrs.size(); ++i)
        {
            if (only_index >= 0 && static_cast<int>(i) != only_index)
                continue;
            raftkv::NodeStatus status;
            const raftkv::ErrorCode code = client.Status(&status, static_cast<int>(i));
            if (code != raftkv::ErrorCode::kOk)
            {
                std::printf("%-6zu %-10s (unreachable: %s)\n", i + 1, "-", raftkv::ErrorCodeName(code));
                ++failures;
                continue;
            }
            std::printf("%-6d %-10s %-8llu %-12llu %-12llu %-12llu %-8d %-8llu\n", status.node_id,
                        RoleLabel(status.role), static_cast<unsigned long long>(status.term),
                        static_cast<unsigned long long>(status.commit_index),
                        static_cast<unsigned long long>(status.last_applied),
                        static_cast<unsigned long long>(status.last_log_index), status.leader_id,
                        static_cast<unsigned long long>(status.key_count));
        }
        return failures > 0 ? 1 : 0;
    }

} 

int main(int argc, char **argv)
{
    raftkv::InitLogFromEnv();
    std::vector<std::string> addrs;
    int timeout_ms = 2000;
    uint64_t ttl_ms = 0;
    BenchOptions bench;
    std::vector<std::string> positional;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        const auto next = [&](const char *name) -> std::string
        {
            if (i + 1 >= argc)
            {
                std::fprintf(stderr, "missing value for %s\n", name);
                std::exit(2);
            }
            return argv[++i];
        };
        if (arg == "--addrs" || arg == "-a")
        {
            for (const auto &addr : raftkv::SplitString(next("--addrs"), ','))
            {
                if (!addr.empty())
                    addrs.push_back(addr);
            }
        }
        else if (arg == "--timeout")
        {
            timeout_ms = std::atoi(next("--timeout").c_str());
        }
        else if (arg == "--ttl")
        {
            ttl_ms = std::strtoull(next("--ttl").c_str(), nullptr, 10);
        }
        else if (arg == "--keys")
        {
            bench.keys = std::atoi(next("--keys").c_str());
        }
        else if (arg == "--ops")
        {
            bench.ops = std::atoi(next("--ops").c_str());
        }
        else if (arg == "--concurrency")
        {
            bench.concurrency = std::atoi(next("--concurrency").c_str());
        }
        else if (arg == "--read-ratio")
        {
            bench.read_ratio = std::atof(next("--read-ratio").c_str());
        }
        else if (arg == "--value-size")
        {
            bench.value_size = std::atoi(next("--value-size").c_str());
        }
        else if (arg == "--help" || arg == "-h")
        {
            PrintUsage();
            return 0;
        }
        else
        {
            positional.push_back(arg);
        }
    }
    bench.ttl_ms = ttl_ms;
  
    if (bench.keys < 1)
    {
        std::fprintf(stderr, "--keys 必须 >= 1（当前 %d）\n", bench.keys);
        return 2;
    }
    if (bench.ops < 1)
    {
        std::fprintf(stderr, "--ops 必须 >= 1（当前 %d）\n", bench.ops);
        return 2;
    }
    if (bench.concurrency < 1)
    {
        std::fprintf(stderr, "--concurrency 必须 >= 1（当前 %d）\n", bench.concurrency);
        return 2;
    }
    if (bench.read_ratio < 0.0 || bench.read_ratio > 1.0)
    {
        std::fprintf(stderr, "--read-ratio 必须在 [0,1] 之间（当前 %.2f）\n", bench.read_ratio);
        return 2;
    }
    if (bench.value_size < 1)
    {
        std::fprintf(stderr, "--value-size 必须 >= 1（当前 %d）\n", bench.value_size);
        return 2;
    }
    if (addrs.empty())
        addrs.push_back("127.0.0.1:9201");
    if (positional.empty())
    {
        PrintUsage();
        return 2;
    }

    raftkv::KVClient client(addrs, timeout_ms);
    const std::string &cmd = positional[0];

    if (cmd == "put")
    {
        if (positional.size() < 3)
        {
            std::fprintf(stderr, "usage: put <key> <value>\n");
            return 2;
        }
        const auto code = client.Put(positional[1], positional[2], ttl_ms);
        std::printf("%s\n", raftkv::ErrorCodeName(code));
        return code == raftkv::ErrorCode::kOk ? 0 : 1;
    }

    if (cmd == "get")
    {
        if (positional.size() < 2)
        {
            std::fprintf(stderr, "usage: get <key>\n");
            return 2;
        }
        std::string value;
        const auto code = client.Get(positional[1], &value);
        if (code == raftkv::ErrorCode::kOk)
        {
            std::printf("%s\n", value.c_str());
            return 0;
        }
        if (code == raftkv::ErrorCode::kNotFound)
        {
            std::printf("(nil)\n");
            return 1;
        }
        std::fprintf(stderr, "%s\n", raftkv::ErrorCodeName(code));
        return 1;
    }

    if (cmd == "del" || cmd == "delete")
    {
        if (positional.size() < 2)
        {
            std::fprintf(stderr, "usage: del <key>\n");
            return 2;
        }
        const auto code = client.Delete(positional[1]);
        std::printf("%s\n", raftkv::ErrorCodeName(code));
        return code == raftkv::ErrorCode::kOk ? 0 : 1;
    }

    if (cmd == "scan")
    {
        if (positional.size() < 3)
        {
            std::fprintf(stderr, "usage: scan <start> <end> [limit]\n");
            return 2;
        }
        const uint32_t limit =
            positional.size() >= 4 ? static_cast<uint32_t>(std::atoi(positional[3].c_str())) : 100;
        std::vector<std::pair<std::string, std::string>> kvs;
        const auto code = client.Scan(positional[1], positional[2], limit, &kvs);
        if (code != raftkv::ErrorCode::kOk)
        {
            std::fprintf(stderr, "%s\n", raftkv::ErrorCodeName(code));
            return 1;
        }
        for (const auto &kv : kvs)
            std::printf("%s = %s\n", kv.first.c_str(), kv.second.c_str());
        std::printf("(%zu keys)\n", kvs.size());
        return 0;
    }

    if (cmd == "status")
    {
        const int only = positional.size() >= 2 ? std::atoi(positional[1].c_str()) - 1 : -1;
        return PrintStatus(addrs, only);
    }

    if (cmd == "bench")
        return RunBench(addrs, bench);

    std::fprintf(stderr, "unknown command: %s (try --help)\n", cmd.c_str());
    return 2;
}
