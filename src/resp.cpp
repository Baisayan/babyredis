#include <sys/socket.h>
#include <cerrno>
#include <iostream>

#include "resp.h"
#include "commands.h"
#include "pubsub.h"

std::string resp_simple_string(const std::string& value) {
    return "+" + value + "\r\n";
}

std::string resp_error(const std::string& value) {
    return "-ERR " + value + "\r\n";
}

std::string resp_bulk_string(const std::string& value) {
    return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

std::string resp_null() {
    return "$-1\r\n";
}

std::string resp_integer(long long value) {
    return ":" + std::to_string(value) + "\r\n";
}

std::string resp_array(const std::vector<std::string>& values) {
    std::string result = "*" + std::to_string(values.size()) + "\r\n";
    for (const auto& value : values) {
        result += resp_bulk_string(value);
    }
    return result;
}

static bool read_line(const std::string& buffer, size_t start, size_t& line_end) {
    size_t pos = buffer.find("\r\n", start);
    if (pos == std::string::npos) return false;
    line_end = pos;
    return true;
}

ParseResult parse_resp(Client& client) {
    RespParser& parser = client.parser;
    std::string& buffer = client.input_buffer;
    size_t& pos = parser.pos;

    if (parser.expected_args == -1) {
        if (pos >= buffer.size()) {
            return {ParseResultType::INCOMPLETE, {}, ""};
        }
        if (buffer[pos] != '*') {
            return {ParseResultType::ERROR, {}, "Protocol error: expected '*'"};
        }
        size_t line_end;
        if (!read_line(buffer, pos, line_end)) {
            return {ParseResultType::INCOMPLETE, {}, ""};
        }
        std::string count_str = buffer.substr(pos + 1, line_end - pos - 1);
        try {
            parser.expected_args = std::stoi(count_str);
        }
        catch (...) {
            return {ParseResultType::ERROR, {}, "Protocol error: invalid multibulk length"};
        }
        pos = line_end + 2;
    }

    while (static_cast<int>(parser.args.size()) < parser.expected_args) {
        if (pos >= buffer.size()) {
            return {ParseResultType::INCOMPLETE, {}, ""};
        }
        if (buffer[pos] != '$') {
            return {ParseResultType::ERROR, {}, "Protocol error: expected '$'"};
        }
        size_t line_end;
        if (!read_line(buffer, pos, line_end)) {
            return {ParseResultType::INCOMPLETE, {}, ""};
        }
        int bulk_len = 0;
        try {
            std::string len_str = buffer.substr(pos + 1, line_end - pos - 1);
            bulk_len = std::stoi(len_str);
        }
        catch (...) {
            return {ParseResultType::ERROR, {}, "Protocol error: invalid bulk length"};
        }
        pos = line_end + 2;
        if (pos + bulk_len + 2 > buffer.size()) {
            return {ParseResultType::INCOMPLETE, {}, ""};
        }
        parser.args.emplace_back(buffer.data() + pos, bulk_len);
        pos += bulk_len;
        if (buffer[pos] != '\r' || buffer[pos + 1] != '\n') {
            return {ParseResultType::ERROR, {}, "Protocol error: invalid bulk termination"};
        }
        pos += 2;
    }

    ParseResult result{ParseResultType::COMPLETE, parser.args, ""};
    if (pos >= buffer.size()) {
        buffer.clear();
        pos = 0;
    }
    parser.expected_args = -1;
    parser.args.clear();
    return result;
}

void handle_read(DB& db, Aof& aof, Client& client, PubSub& pubsub, std::unordered_map<int, Client>& clients) {
    char buffer[4096];
    while (true) {
        ssize_t bytes_read = recv(client.fd, buffer, sizeof(buffer), 0);
        if (bytes_read > 0) {
            client.input_buffer.append(buffer, bytes_read);
        }
        else if (bytes_read == 0) {
            client.closed = true;
            return;
        }
        else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            client.closed = true;
            return;
        }
    }

    while (true) {
        ParseResult result = parse_resp(client);
        if (result.type == ParseResultType::INCOMPLETE) break;
        if (result.type == ParseResultType::ERROR) {
            client.output_buffer += resp_error(result.error);
            client.closed = true;
            return;
        }
        if (result.command.empty()) continue;
        
        std::string cmd = result.command[0];
        for (char& c : cmd) c = toupper(c);
        
        if (cmd == "QUIT") {
            client.output_buffer += resp_simple_string("OK");
            client.closed = true;
            return;
        }
        
        bool is_pubsub_cmd = (cmd == "SUBSCRIBE" || cmd == "UNSUBSCRIBE" || cmd == "PUBLISH");
        bool is_allowed_in_sub = is_pubsub_cmd || cmd == "PING" || cmd == "QUIT";

        if (client.is_subscribed && !is_allowed_in_sub) {
            client.output_buffer += resp_error("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
            continue;
        }

        if (is_pubsub_cmd) {
            handle_pubsub(client, result.command, pubsub, clients);
        } else if (client.is_subscribed && cmd == "PING") {
            std::vector<std::string> ping_resp = {"pong"};
            ping_resp.push_back(result.command.size() > 1 ? result.command[1] : "");
            client.output_buffer += resp_array(ping_resp);
        } else {
            std::string response = dispatch(db, result.command);
            if (!response.empty()) {
                client.output_buffer += response;
                if (response[0] != '-' && !result.command.empty() && is_write_command(cmd)) {
                    std::string aof_error;
                    if (!aof_append(aof, result.command, aof_error)) {
                        std::cerr << "[FATAL] " << aof_error << "\n";
                        exit(1);
                    }
                }
            }
        }
    }
}

void handle_write(Client& client) {
    while (client.offset < client.output_buffer.size()) {
        ssize_t bytes_sent = send(
            client.fd,
            client.output_buffer.data() + client.offset,
            client.output_buffer.size() - client.offset,
            0
        );
        if (bytes_sent > 0) {
            client.offset += bytes_sent;
        }
        else if (bytes_sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            client.closed = true;
            return;
        }
    }
    if (client.offset >= client.output_buffer.size()) {
        client.output_buffer.clear();
        client.offset = 0;
    }
}