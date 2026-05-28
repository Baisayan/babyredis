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

struct DB {
    std::unordered_map<std::string, ValueEntry> kvstore;
};

// string operations
std::string db_set(DB& db, const std::vector<std::string>& parts);
std::string db_get(DB& db, const std::vector<std::string>& parts);
std::string db_exists(DB& db, const std::vector<std::string>& parts);
std::string db_del(DB& db, const std::vector<std::string>& parts);
std::string db_incr(DB& db, const std::vector<std::string>& parts);
std::string db_decr(DB& db, const std::vector<std::string>& parts);
std::string db_incrby(DB& db, const std::vector<std::string>& parts);
std::string db_decrby(DB& db, const std::vector<std::string>& parts);
std::string db_type(DB& db, const std::vector<std::string>& parts);

// list operations
std::string db_rpush(DB& db, const std::vector<std::string>& parts);
std::string db_lpush(DB& db, const std::vector<std::string>& parts);
std::string db_lpop(DB& db, const std::vector<std::string>& parts);
std::string db_rpop(DB& db, const std::vector<std::string>& parts);
std::string db_llen(DB& db, const std::vector<std::string>& parts);
std::string db_lrange(DB& db, const std::vector<std::string>& parts);
std::string db_lindex(DB& db, const std::vector<std::string>& parts);

// hash operations
std::string db_hset(DB& db, const std::vector<std::string>& parts);
std::string db_hget(DB& db, const std::vector<std::string>& parts);
std::string db_hdel(DB& db, const std::vector<std::string>& parts);
std::string db_hkeys(DB& db, const std::vector<std::string>& parts);
std::string db_hvals(DB& db, const std::vector<std::string>& parts);
std::string db_hgetall(DB& db, const std::vector<std::string>& parts);
std::string db_hexists(DB& db, const std::vector<std::string>& parts);
std::string db_hlen(DB& db, const std::vector<std::string>& parts);

// set operations
std::string db_sadd(DB& db, const std::vector<std::string>& parts);
std::string db_srem(DB& db, const std::vector<std::string>& parts);
std::string db_scard(DB& db, const std::vector<std::string>& parts);
std::string db_smembers(DB& db, const std::vector<std::string>& parts);
std::string db_sismember(DB& db, const std::vector<std::string>& parts);