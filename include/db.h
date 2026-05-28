#pragma once
#include <string>
#include <vector>
#include <deque>
#include <unordered_set>
#include <unordered_map>
#include <variant>

using ListType = std::deque<std::string>;
using SetType = std::unordered_set<std::string>;
using HashType = std::unordered_map<std::string, std::string>;

struct ValueEntry {
    std::variant<std::string, ListType, SetType, HashType> data;
};

// string operations
std::string db_set(const std::vector<std::string>& parts);
std::string db_get(const std::vector<std::string>& parts);
std::string db_exists(const std::vector<std::string>& parts);
std::string db_del(const std::vector<std::string>& parts);
std::string db_incr(const std::vector<std::string>& parts);
std::string db_decr(const std::vector<std::string>& parts);
std::string db_incrby(const std::vector<std::string>& parts);
std::string db_decrby(const std::vector<std::string>& parts);
std::string db_type(const std::vector<std::string>& parts);

// list operations
std::string db_rpush(const std::vector<std::string>& parts);
std::string db_lpush(const std::vector<std::string>& parts);
std::string db_lpop(const std::vector<std::string>& parts);
std::string db_rpop(const std::vector<std::string>& parts);
std::string db_llen(const std::vector<std::string>& parts);
std::string db_lrange(const std::vector<std::string>& parts);
std::string db_lindex(const std::vector<std::string>& parts);

// hash operations
std::string db_hset(const std::vector<std::string>& parts);
std::string db_hget(const std::vector<std::string>& parts);
std::string db_hdel(const std::vector<std::string>& parts);
std::string db_hkeys(const std::vector<std::string>& parts);
std::string db_hvals(const std::vector<std::string>& parts);
std::string db_hgetall(const std::vector<std::string>& parts);
std::string db_hexists(const std::vector<std::string>& parts);
std::string db_hlen(const std::vector<std::string>& parts);

// set operations
std::string db_sadd(const std::vector<std::string>& parts);
std::string db_srem(const std::vector<std::string>& parts);
std::string db_scard(const std::vector<std::string>& parts);
std::string db_smembers(const std::vector<std::string>& parts);
std::string db_sismember(const std::vector<std::string>& parts);