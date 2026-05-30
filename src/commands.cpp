#include "commands.h"
#include "resp.h"

static std::string handle_ping(DB& , const std::vector<std::string>& parts) {
    if (parts.size() == 1) return resp_simple_string("PONG");
    if (parts.size() == 2) return resp_bulk_string(parts[1]);
    return resp_error("wrong number of arguments");
}

static std::string handle_echo(DB& , const std::vector<std::string>& parts) {
    if (parts.size() != 2) return resp_error("wrong number of arguments");
    return resp_bulk_string(parts[1]);
}

static const std::unordered_map<std::string, CommandHandler> command_registry = {
    {"PING", handle_ping},
    {"ECHO", handle_echo},

    {"SET", db_set},
    {"GET", db_get},
    {"EXISTS", db_exists},
    {"DEL", db_del},
    {"INCR", db_incr},
    {"DECR", db_decr},
    {"INCRBY", db_incrby},
    {"DECRBY", db_decrby},    
    {"TYPE", db_type},

    {"RPUSH", db_rpush},
    {"LPUSH", db_lpush},
    {"RPOP", db_rpop},
    {"LPOP", db_lpop},
    {"LLEN", db_llen},
    {"LRANGE", db_lrange},
    {"LINDEX", db_lindex},

    {"HSET", db_hset},
    {"HGET", db_hget},
    {"HDEL", db_hdel},
    {"HKEYS", db_hkeys},
    {"HVALS", db_hvals},
    {"HEXISTS", db_hexists},
    {"HLEN", db_hlen},
    {"HGETALL", db_hgetall},

    {"SADD", db_sadd},
    {"SREM", db_srem},
    {"SCARD", db_scard},
    {"SMEMBERS", db_smembers},
    {"SISMEMBER", db_sismember},
};

bool is_write_command(const std::string& command) {
    static const std::unordered_set<std::string> write_commands = {
        "SET", "DEL", "INCR", "DECR", "INCRBY", "DECRBY",
        "RPUSH", "LPUSH", "RPOP", "LPOP",
        "HSET", "HDEL", "SADD", "SREM"
    };

    return write_commands.find(command) != write_commands.end();
}

std::string dispatch_command(DB& db, const std::vector<std::string>& parts) {
    if (parts.empty()) return resp_error("empty command");
    std::string command = parts[0];
    for (char& c : command) c = toupper(c);
    
    auto it = command_registry.find(command);
    if (it == command_registry.end()) return resp_error("unknown command");
    return it->second(db, parts);
}