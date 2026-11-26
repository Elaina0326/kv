#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "common/log.h"
#include "common/util.h"
#include "server/kv_server.h"

namespace
{

    void PrintUsage()
    {
        std::printf(
            "raftkv-server —— 基于 Raft 的分布式 KV 存储（单组分片）\n"
            "\n"
            "用法:\n"
            "  raftkv-server --id <n> --peers <id@host:port,...> --data-dir <dir> [options]\n"
            "\n"
            "必选:\n"
            "  --id <n>              本节点 id（集群内唯一，从 1 开始）\n"
            "  --peers <list>        全部节点，格式 1@127.0.0.1:9201,2@127.0.0.1:9202,...\n"
            "  --data-dir <dir>      WAL 与数据目录（每个节点必须不同）\n"
            "\n"
            "可选:\n"
            "  --listen <ip:port>    监听地址，缺省取 --peers 里本节点的地址\n"
            "  --threads <n>         处理客户端请求的线程数，默认 8\n"
            "  --raft-threads <n>    处理选举/复制 RPC 的线程数，默认 2（与客户端请求隔离）\n"
            "  --request-timeout <ms> 单请求等待复制+apply 的预算，默认 1500\n"
            "  --election-timeout <ms> 选举超时基准值，默认 300（实际取 1~2 倍随机）\n"
            "  --heartbeat <ms>      心跳间隔，默认 100\n"
            "  --sync-wal            每条日志记录 fdatasync（默认关闭：掉电可能丢最后几条已应答写；\n"
            "                        进程崩溃不丢。term/voted_for 无论如何都强制 fsync）\n"
            "  --log-level <level>   debug|info|warn|error，默认 info\n"
            "  --help                显示本帮助\n");
    }

    bool ParsePeers(const std::string &spec, std::vector<raftkv::PeerInfo> *peers)
    {
        for (const auto &item : raftkv::SplitString(spec, ','))
        {
            const auto at = item.find('@');
            if (at == std::string::npos)
            {
                std::fprintf(stderr, "invalid peer spec: %s\n", item.c_str());
                return false;
            }
            raftkv::PeerInfo peer;
            peer.id = std::atoi(item.substr(0, at).c_str());
            peer.addr = item.substr(at + 1);
            if (peer.id <= 0 || peer.addr.empty())
            {
                std::fprintf(stderr, "invalid peer spec: %s\n", item.c_str());
                return false;
            }
            peers->push_back(peer);
        }
        return !peers->empty();
    }

} 

int main(int argc, char **argv)
{
    raftkv::InitLogFromEnv();
    raftkv::ServerOptions opts;
    bool have_id = false;
    bool have_peers = false;
    std::string listen;

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
        if (arg == "--help" || arg == "-h")
        {
            PrintUsage();
            return 0;
        }
        else if (arg == "--id")
        {
            opts.raft.node_id = std::atoi(next("--id").c_str());
            have_id = true;
        }
        else if (arg == "--listen")
        {
            listen = next("--listen");
        }
        else if (arg == "--peers")
        {
            if (!ParsePeers(next("--peers"), &opts.raft.peers))
                return 2;
            have_peers = true;
        }
        else if (arg == "--data-dir")
        {
            opts.raft.data_dir = next("--data-dir");
        }
        else if (arg == "--threads")
        {
            opts.rpc_threads = std::atoi(next("--threads").c_str());
        }
        else if (arg == "--raft-threads")
        {
            opts.raft_rpc_threads = std::atoi(next("--raft-threads").c_str());
        }
        else if (arg == "--request-timeout")
        {
            opts.request_timeout_ms = std::atoi(next("--request-timeout").c_str());
        }
        else if (arg == "--election-timeout")
        {
            opts.raft.election_timeout_ms = std::atoi(next("--election-timeout").c_str());
        }
        else if (arg == "--heartbeat")
        {
            opts.raft.heartbeat_interval_ms = std::atoi(next("--heartbeat").c_str());
        }
        else if (arg == "--sync-wal")
        {
            opts.raft.sync_wal = true;
        }
        else if (arg == "--log-level")
        {
            raftkv::SetLogLevel(raftkv::ParseLogLevel(next("--log-level").c_str()));
        }
        else
        {
            std::fprintf(stderr, "unknown argument: %s (try --help)\n", arg.c_str());
            return 2;
        }
    }

    if (!have_id || !have_peers || opts.raft.data_dir.empty())
    {
        PrintUsage();
        return 2;
    }

  
    std::string host = "0.0.0.0";
    int port = 0;
    if (!listen.empty())
    {
        if (!raftkv::SplitHostPort(listen, &host, &port))
        {
            std::fprintf(stderr, "invalid --listen: %s\n", listen.c_str());
            return 2;
        }
    }
    else
    {
        for (const auto &peer : opts.raft.peers)
        {
            if (peer.id != opts.raft.node_id)
                continue;
            std::string peer_host;
            if (!raftkv::SplitHostPort(peer.addr, &peer_host, &port))
            {
                std::fprintf(stderr, "invalid peer addr: %s\n", peer.addr.c_str());
                return 2;
            }
            if (peer_host != "0.0.0.0")
                host = peer_host;
            break;
        }
        if (port == 0)
        {
            std::fprintf(stderr, "node id %d not found in --peers\n", opts.raft.node_id);
            return 2;
        }
    }
    opts.listen_ip = host;
    opts.listen_port = port;

  
    std::signal(SIGPIPE, SIG_IGN);


    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    raftkv::KVServer server(std::move(opts));
    std::string err;
    if (!server.Start(&err))
    {
        std::fprintf(stderr, "start failed: %s\n", err.c_str());
        return 1;
    }

    std::thread runner([&server]
                       { server.Run(); });
    LOG_INFO("raftkv-server ready, press Ctrl-C to stop");

    int received = 0;
    sigwait(&signals, &received);
    LOG_INFO("signal %d received, shutting down", received);
    server.Stop();
    runner.join();
    return 0;
}
