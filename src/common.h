#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <unordered_map>
#include <chrono>
#include <vector>
#include <set>
#include <functional>
#include <cstddef>

struct RedisConfig {
    int port = 6379;
};

extern RedisConfig g_config;

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
    std::vector<std::string> list_val;
    std::set<ZSetMember> zset_val;
    std::chrono::time_point<std::chrono::steady_clock> expiry_time;
    bool has_expiry = false;
};

extern std::unordered_map<std::string, ValueEntry> g_kv_store;

enum class ParseResultType {COMPLETE, INCOMPLETE, ERROR};

struct ParseResult {
    ParseResultType type;
    std::vector<std::string> command;
    std::string error;
};

struct RespParser {
    size_t pos = 0;
    int expected_args = -1;
    std::vector<std::string> args;
};

struct Client {
    int fd;
    std::string input_buffer;
    std::string output_buffer;
    bool closed = false;
    RespParser parser;
};

std::string resp_simple_string(const std::string& value);
std::string resp_error(const std::string& value);
std::string resp_bulk_string(const std::string& value);
std::string resp_null();
std::string resp_integer(long long value);
std::string resp_array(const std::vector<std::string>& values);

using CommandHandler = std::function<std::string(const std::vector<std::string>&)>;
std::string dispatch_command(const std::vector<std::string>& parts);

void handle_read(Client& client);
void handle_write(Client& client);
ParseResult parse_resp(Client& client);

std::string db_set(
    const std::vector<std::string>& parts
);

std::string db_get(
    const std::vector<std::string>& parts
);

std::string db_rpush(
    const std::vector<std::string>& parts
);

std::string db_lpush(
    const std::vector<std::string>& parts
);

std::string db_lpop(
    const std::vector<std::string>& parts
);

std::string db_llen(
    const std::vector<std::string>& parts
);

std::string db_lrange(
    const std::vector<std::string>& parts
);

std::string db_incr(
    const std::vector<std::string>& parts
);

std::string db_zadd(
    const std::vector<std::string>& parts
);

std::string db_zcard(
    const std::vector<std::string>& parts
);

std::string db_zrank(
    const std::vector<std::string>& parts
);

std::string db_zrange(
    const std::vector<std::string>& parts
);

std::string db_zscore(
    const std::vector<std::string>& parts
);

std::string db_zrem(
    const std::vector<std::string>& parts
);

std::string db_type(
    const std::vector<std::string>& parts
);

#endif