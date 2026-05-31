#pragma once
#include "aof.h"
#include "pubsub.h" 
#include <string>
#include <unordered_map>
#include <vector>
#include <cstddef>

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
    size_t offset = 0;
    bool closed = false;
    RespParser parser;
    bool is_subscribed = false;
    size_t subscribed_count = 0;
};

std::string resp_simple_string(const std::string& value);
std::string resp_error(const std::string& value);
std::string resp_bulk_string(const std::string& value);
std::string resp_null();
std::string resp_integer(long long value);
std::string resp_array(const std::vector<std::string>& values);

struct DB;
struct Aof;
ParseResult parse_resp(Client& client);
void handle_read(DB& db, Aof& aof, Client& client, PubSub& pubsub, std::unordered_map<int, Client>& clients);
void handle_write(Client& client);
