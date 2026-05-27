#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <deque>
#include <set>

enum class ValueType {STRING, LIST, ZSET};

struct ZSetMember {
    std::string member;
    double score;
    bool operator<(const ZSetMember& other) const {
        if (score != other.score) {
            return score < other.score;
        }
        return member < other.member;
    }
};

struct ValueEntry {
    ValueType type = ValueType::STRING;
    std::string value;
    std::deque<std::string> list_val;
    std::set<ZSetMember> zset_val;
};

extern std::unordered_map<std::string, ValueEntry> g_kv_store;

// string operations
std::string db_set(const std::vector<std::string>& parts);
std::string db_get(const std::vector<std::string>& parts);
std::string db_incr(const std::vector<std::string>& parts);
std::string db_type(const std::vector<std::string>& parts);

// list operations
std::string db_rpush(const std::vector<std::string>& parts);
std::string db_lpush(const std::vector<std::string>& parts);
std::string db_lpop(const std::vector<std::string>& parts);
std::string db_rpop(const std::vector<std::string>& parts);
std::string db_llen(const std::vector<std::string>& parts);
std::string db_lrange(const std::vector<std::string>& parts);
std::string db_lindex(const std::vector<std::string>& parts);

// sorted set operations
std::string db_zadd(const std::vector<std::string>& parts);
std::string db_zcard(const std::vector<std::string>& parts);
std::string db_zrank(const std::vector<std::string>& parts);
std::string db_zrange(const std::vector<std::string>& parts);
std::string db_zscore(const std::vector<std::string>& parts);
std::string db_zrem(const std::vector<std::string>& parts);
