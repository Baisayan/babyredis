#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include "common.h"

std::unordered_map<std::string, ValueEntry> g_kv_store;
std::vector<BlockedClient> g_blocked_clients_list;
std::unordered_map<int, ClientState> g_client_states;

// notify a blocked client if key becomes available
void handle_blocked_clients(int client_fd, const std::string& key, const std::string& value) {
    std::string resp = "*2\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
    resp += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
    send(client_fd, resp.c_str(), resp.length(), 0);
}

void handle_client(int client_fd) {
    char buffer[1024];
    int bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read <= 0) {
        g_client_states.erase(client_fd);
        close(client_fd);
        return;
    }

    buffer[bytes_read] = '\0';
    std::vector<std::string> parts = split_resp(std::string(buffer));
    if (parts.size() < 3) return;

    std::string command = parts[2];
    for (auto &c : command) c = toupper(c);

    if (g_client_states.find(client_fd) == g_client_states.end()) {
        g_client_states[client_fd] = {false, {}};
    }
    ClientState &state = g_client_states[client_fd];

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

    else if (command == "RPUSH" || command == "LPUSH") {
        if (parts.size() < 7) return;
        std::string key = parts[4];

        // Init list if key doesn't exist
        if (g_kv_store.find(key) == g_kv_store.end()) {
            ValueEntry entry;
            entry.type = ValueType::LIST;
            g_kv_store[key] = entry;
        }
        ValueEntry &entry = g_kv_store[key];

        for (size_t i = 6; i < parts.size(); i += 2) {
            if (command == "RPUSH") entry.list_val.push_back(parts[i]);
            else entry.list_val.insert(entry.list_val.begin(), parts[i]);
        }

        int final_length = entry.list_val.size();
        
        while (!entry.list_val.empty()) {
            auto it = std::find_if(
                g_blocked_clients_list.begin(),
                g_blocked_clients_list.end(),
                [&](const BlockedClient& bc) { return bc.key == key; }
            );
            
            if (it == g_blocked_clients_list.end()) break;

            // Pop the first element
            std::string val = entry.list_val.front();
            entry.list_val.erase(entry.list_val.begin());

            handle_blocked_clients(it->fd, key, val);
            g_blocked_clients_list.erase(it);
        }

        std::string resp = ":" + std::to_string(final_length) + "\r\n";
        send(client_fd, resp.c_str(), resp.length(), 0);
    }

    else if (command == "BLPOP") {
        if (parts.size() < 7) return;
        std::string key = parts[4];
        double timeout_sec = std::stod(parts[6]);

        // list exists n not empty, behave like LPOP
        if (g_kv_store.count(key) && !g_kv_store[key].list_val.empty()) {
            ValueEntry &entry = g_kv_store[key];
            std::string val = entry.list_val.front();
            entry.list_val.erase(entry.list_val.begin());
            handle_blocked_clients(client_fd, key, val);
            return;
        }

        // otherwise, block client
        BlockedClient bc;
        bc.fd = client_fd;
        bc.key = key;
        if (timeout_sec > 0) {
            bc.has_timeout = true;
            bc.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds((long long)(timeout_sec * 1000));
        }

        g_blocked_clients_list.push_back(bc);
        return;
    }

    else if (command == "LPOP") {
        if (parts.size() < 5) return;
        std::string key = parts[4];

        // if key doesn't exist, return Null Bulk String
        if (g_kv_store.find(key) == g_kv_store.end()) {
            send(client_fd, "$-1\r\n", 5, 0);
            return;
        }

        ValueEntry &entry = g_kv_store[key]; // type check
        if (entry.type != ValueType::LIST) {
            send(client_fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", 67, 0);
            return;
        }

        if (entry.list_val.empty()) {
            send(client_fd, "$-1\r\n", 5, 0);
            return;
        }

        if (parts.size() >= 7) {
            long long count = std::stoll(parts[6]);
            if (count < 0) {
                send(client_fd, "-ERR value is out of range, must be positive\r\n", 46, 0);
                return;
            }

            size_t num_to_pop = std::min((size_t)count, entry.list_val.size());
            std::string resp = "*" + std::to_string(num_to_pop) + "\r\n";

            for (size_t i = 0; i < num_to_pop; ++i) {
                std::string val = entry.list_val.front();
                entry.list_val.erase(entry.list_val.begin());
                resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
            }
            send(client_fd, resp.c_str(), resp.length(), 0);
        } else {
            // normal single pop
            std::string val = entry.list_val.front();
            entry.list_val.erase(entry.list_val.begin());
            std::string resp = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
            send(client_fd, resp.c_str(), resp.length(), 0);
        }
    }

    else if (command == "LLEN") {
        if (parts.size() < 5) return; // LLEN, key
        std::string key = parts[4];

        if (g_kv_store.find(key) == g_kv_store.end()) {
            send(client_fd, ":0\r\n", 4, 0);
            return;
        }

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::LIST) {
            send(client_fd, "-WRONGTYPE Operation - Key holding wrong value\r\n", 67, 0);
            return;
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
        if (entry.type != ValueType::LIST) {
            send(client_fd, "-WRONGTYPE Operation - Key holding wrong value\r\n", 67, 0);
            return;
        }

        long long list_len = (long long)entry.list_val.size();

        if (start < 0) start = list_len + start;
        if (start < 0) start = 0;
        if (stop < 0) stop = list_len + stop;

        if (start >= list_len || start > stop) {
            send(client_fd, "*0\r\n", 4, 0);
            return;
        }
        if (stop >= list_len) stop = list_len - 1;

        // construct RESP array response n send it back
        size_t num_elements = (size_t)(stop - start + 1);
        std::string resp = "*" + std::to_string(num_elements) + "\r\n";
        for (long long i = start; i <= stop; ++i) {
            std::string val = entry.list_val[(size_t)i];
            resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
        }
        send(client_fd, resp.c_str(), resp.length(), 0);
    }

    else if (command == "INCR") {
        if (parts.size() < 5) return; // INCR, key
        std::string key = parts[4];

        if (g_kv_store.find(key) == g_kv_store.end()) {
            ValueEntry entry;
            entry.type = ValueType::STRING;
            entry.value = "1";
            g_kv_store[key] = entry;

            send(client_fd, ":1\r\n", 4, 0);
        } else {
            ValueEntry &entry = g_kv_store[key];

            try {
                long long val = std::stoll(entry.value);
                val++;
                entry.value = std::to_string(val);
                
                std::string resp = ":" + entry.value + "\r\n";
                send(client_fd, resp.c_str(), resp.length(), 0);
            } catch (const std::exception& e) {
                send(client_fd, "-ERR value is not an integer or out of range\r\n", 46, 0);
            }
        }
    }

    else if (command == "MULTI") {
        state.in_transaction = true;
        state.transaction_queue.clear();
        send(client_fd, "+OK\r\n", 5, 0);
        return;
    }

    else if (command == "EXEC") {
        if (!state.in_transaction) {
            send(client_fd, "-ERR EXEC without MULTI\r\n", 25, 0);
            return;
        }
        if (state.transaction_queue.empty()) {
            send(client_fd, "*0\r\n", 4, 0);
        } else {
            send(client_fd, "*0\r\n", 4, 0); 
        }

        state.in_transaction = false;
        state.transaction_queue.clear();
        return;
    }
}