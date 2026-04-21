#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <unordered_map>
#include <chrono>
#include <vector>
#include <set>

struct RedisConfig {
    std::string dir;
    std::string dbfilename;
    int port = 6379;

    bool is_replica = false;
    std::string master_host;
    int master_port;

    std::string master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    int master_repl_offset = 0;
    long long processed_bytes = 0;
};
extern RedisConfig g_config;

struct WaitingClient {
    int fd;
    int target_count;
    long long target_offset;
    std::chrono::steady_clock::time_point deadline;
};

extern std::vector<WaitingClient> g_waiting_clients;
extern std::unordered_map<int, long long> g_replica_offsets;

void load_rdb();

int initiate_replica_handshake();
extern std::vector<int> g_replicas;
extern int g_master_fd;

enum class ValueType {STRING, LIST, ZSET};

struct ZSetMember {
    std::string member;
    double score;
    bool operator<(const ZSetMember& other) const {
        if (score != other.score) return score < other.score;
        return member < other.member;
    }
};

struct ValueEntry {
    ValueType type = ValueType::STRING;
    std::string value;
    std::vector<std::string> list_val;
    std::set<ZSetMember> zset_val;
    std::chrono::time_point<std::chrono::steady_clock> expiry_time;
    bool has_expiry = false;
};

struct BlockedClient {
    int fd;
    std::string key;
    std::chrono::time_point<std::chrono::steady_clock> deadline;
    bool has_timeout = false;
};

extern std::vector<BlockedClient> g_blocked_clients_list;
extern std::unordered_map<std::string, ValueEntry> g_kv_store;
std::vector<std::string> split_resp(const std::string& s);

#endif