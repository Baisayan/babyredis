#include "commands.h"
#include "resp.h"
#include "db.h"

static std::string handle_ping(const std::vector<std::string>& parts) {
    if (parts.size() == 1) {
        return resp_simple_string("PONG");
    }

    if (parts.size() == 2) {
        return resp_bulk_string(parts[1]);
    }

    return resp_error("wrong number of arguments");
}

static std::string handle_echo(const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

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

    {"ZADD", db_zadd},
    {"ZCARD", db_zcard},
    {"ZRANK", db_zrank},
    {"ZRANGE", db_zrange},
    {"ZSCORE", db_zscore},
    {"ZREM", db_zrem}
};

std::string dispatch_command(const std::vector<std::string>& parts) {
    if (parts.empty()) {
        return resp_error("empty command");
    }

    std::string command = parts[0];
    for (char& c : command) {
        c = toupper(c);
    }

    auto it = command_registry.find(command);
    if (it == command_registry.end()) {
        return resp_error("unknown command");
    }

    return it->second(parts);
}