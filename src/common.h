#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <unordered_map>
#include <chrono>
#include <vector>
#include <set>

struct RedisConfig {
    int port = 6379;
};
extern RedisConfig g_config;

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

extern std::unordered_map<std::string, ValueEntry> g_kv_store;
std::vector<std::string> split_resp(const std::string& s);

#endif