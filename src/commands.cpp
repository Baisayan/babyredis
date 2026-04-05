#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include "common.h"

// Init global store defined in common.h
std::unordered_map<std::string, ValueEntry> g_kv_store;

void handle_client(int client_fd) {
    char buffer[1024];
    int bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

    if (bytes_read <= 0) {
        close(client_fd);
        return;
    }

    buffer[bytes_read] = '\0';
    std::vector<std::string> parts = split_resp(std::string(buffer));
    
    if (parts.size() < 3) return;

    std::string command = parts[2];
    for (auto &c : command) c = toupper(c);

    if (command == "PING") {
        send(client_fd, "+PONG\r\n", 7, 0);
    }
    else if (command == "ECHO") {
        std::string msg = parts[4];
        std::string resp = "$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n";
        send(client_fd, resp.c_str(), resp.length(), 0);
    }
    else if (command == "SET") {
        std::string key = parts[4];
        std::string value = parts[6];
        ValueEntry entry;
        entry.type = ValueType::STRING;
        entry.value = value;

        if (parts.size() >= 10) {
            std::string option = parts[8];
            for (auto &c : option) c = toupper(c);
            if (option == "PX") {
                long long ms = std::stoll(parts[10]);
                entry.has_expiry = true;
                entry.expiry_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
            }
        }
        g_kv_store[key] = entry;
        send(client_fd, "+OK\r\n", 5, 0);
    }
    else if (command == "GET") {
        std::string key = parts[4];
        if (g_kv_store.count(key)) {
            ValueEntry &entry = g_kv_store[key];

            // expiry check
            if (entry.has_expiry && std::chrono::steady_clock::now() >= entry.expiry_time) {
                g_kv_store.erase(key);
                send(client_fd, "$-1\r\n", 5, 0);
            } else if (entry.type != ValueType::STRING) {
                send(client_fd, "-WRONGTYPE Operation - Key holding wrong value\r\n", 67, 0);
            } else {
                std::string resp = "$" + std::to_string(entry.value.length()) + "\r\n" + entry.value + "\r\n";
                send(client_fd, resp.c_str(), resp.length(), 0);
            }
        } else {
            send(client_fd, "$-1\r\n", 5, 0); // null bulk string
        }
    }
    else if (command == "RPUSH") {
        if (parts.size() < 7) return;
        std::string key = parts[4];

        // Init list if key doesn't exist
        if (g_kv_store.find(key) == g_kv_store.end()) {
            ValueEntry entry;
            entry.type = ValueType::LIST;
            g_kv_store[key] = entry;
        }

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::LIST) {
            send(client_fd, "-WRONGTYPE Operation - Key holding wrong value\r\n", 67, 0);
            return;
        }
        for (size_t i = 6; i < parts.size(); i += 2) {
            entry.list_val.push_back(parts[i]);
        }
        std::string resp = ":" + std::to_string(entry.list_val.size()) + "\r\n";
        send(client_fd, resp.c_str(), resp.length(), 0);
    }
    else if (command == "LRANGE") {
        if (parts.size() < 9) return; // LRANGE, key, start, stop

        std::string key = parts[4];
        long long start = std::stoll(parts[6]);
        long long stop = std::stoll(parts[8]);

        // if key doesn't exist, return empty array
        if (g_kv_store.find(key) == g_kv_store.end()) {
            send(client_fd, "*0\r\n", 4, 0);
            return;
        }

        ValueEntry &entry = g_kv_store[key];

        // 2. Type Check
        if (entry.type != ValueType::LIST) {
            send(client_fd, "-WRONGTYPE Operation - Key holding wrong value\r\n", 67, 0);
            return;
        }

        size_t list_len = entry.list_val.size();

        if (start < 0) start = 0;
        if (start >= (long long)list_len || start > stop) {
            send(client_fd, "*0\r\n", 4, 0);
            return;
        }
        if (stop >= (long long)list_len) {
            stop = list_len - 1;
        }
        size_t num_elements = (size_t)(stop - start + 1);

        // construct RESP array response n send it back
        std::string resp = "*" + std::to_string(num_elements) + "\r\n";
        for (size_t i = (size_t)start; i <= (size_t)stop; ++i) {
            std::string val = entry.list_val[i];
            resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
        }
        send(client_fd, resp.c_str(), resp.length(), 0);
    }
}