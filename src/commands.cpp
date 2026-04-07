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

std::string dispatch_command(int client_fd, const std::vector<std::string>& parts, bool is_from_exec = false) {
    if (parts.size() < 3) return "";
    std::string command = parts[2];
    for (auto &c : command) c = toupper(c);

    if (command == "PING") {
        return "+PONG\r\n";
    }

    else if (command == "ECHO") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string msg = parts[4];
        return "$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n";
    }

    else if (command == "SET") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
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
        return "+OK\r\n";
    }

    else if (command == "GET") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];

        if (g_kv_store.count(key)) {
            ValueEntry &entry = g_kv_store[key];
            if (entry.has_expiry && std::chrono::steady_clock::now() >= entry.expiry_time) {
                g_kv_store.erase(key);
                return "$-1\r\n";
            } else if (entry.type != ValueType::STRING) {
                return "-WRONGTYPE Operation\r\n";
            } else {
                return "$" + std::to_string(entry.value.length()) + "\r\n" + entry.value + "\r\n";
            }
        }
        
        return "$-1\r\n";
    }

    else if (command == "RPUSH" || command == "LPUSH") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
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

        return ":" + std::to_string(final_length) + "\r\n";
    }

    else if (command == "BLPOP") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        double timeout_sec = std::stod(parts[6]);

        // list exists n not empty, behave like LPOP
        if (g_kv_store.count(key) && !g_kv_store[key].list_val.empty()) {
            ValueEntry &entry = g_kv_store[key];
            std::string val = entry.list_val.front();
            entry.list_val.erase(entry.list_val.begin());
            handle_blocked_clients(client_fd, key, val);
            return "";
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
        return "";
    }

    else if (command == "LPOP") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];

        if (g_kv_store.find(key) == g_kv_store.end()) return "$-1\r\n";

        ValueEntry &entry = g_kv_store[key]; // type check
        if (entry.type != ValueType::LIST) return "-WRONGTYPE Operation\r\n";

        if (entry.list_val.empty()) return "$-1\r\n";

        if (parts.size() >= 7) {
            long long count = std::stoll(parts[6]);
            if (count < 0) return "-ERR value is out of range\r\n";

            size_t num_to_pop = std::min((size_t)count, entry.list_val.size());
            std::string resp = "*" + std::to_string(num_to_pop) + "\r\n";

            for (size_t i = 0; i < num_to_pop; ++i) {
                std::string val = entry.list_val.front();
                entry.list_val.erase(entry.list_val.begin());
                resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
            }
            return resp;
        } else {
            std::string val = entry.list_val.front();
            entry.list_val.erase(entry.list_val.begin());
            return "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
        }
    }

    else if (command == "LLEN") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        if (g_kv_store.find(key) == g_kv_store.end()) return ":0\r\n";

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::LIST) return "-WRONGTYPE Operation\r\n";
        return ":" + std::to_string(entry.list_val.size()) + "\r\n";
    }

    else if (command == "LRANGE") {
        if (parts.size() < 9) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        long long start = std::stoll(parts[6]);
        long long stop = std::stoll(parts[8]);

        if (g_kv_store.find(key) == g_kv_store.end()) return "*0\r\n";

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::LIST) return "-WRONGTYPE Operation\r\n";

        long long list_len = (long long)entry.list_val.size();
        if (start < 0) start = list_len + start;
        if (start < 0) start = 0;
        if (stop < 0) stop = list_len + stop;
        if (start >= list_len || start > stop) return "*0\r\n";
        if (stop >= list_len) stop = list_len - 1;

        size_t num_elements = (size_t)(stop - start + 1);
        std::string resp = "*" + std::to_string(num_elements) + "\r\n";
        for (long long i = start; i <= stop; ++i) {
            std::string val = entry.list_val[(size_t)i];
            resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
        }
        return resp;
    }

    else if (command == "INCR") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];

        if (g_kv_store.find(key) == g_kv_store.end()) {
            g_kv_store[key] = {ValueType::STRING, "1"};
            return ":1\r\n";
        } else {
            ValueEntry &entry = g_kv_store[key];
            try {
                long long val = std::stoll(entry.value);
                val++;
                entry.value = std::to_string(val); 
                return ":" + entry.value + "\r\n";
            } catch (const std::exception& e) {
                return "-ERR value is not an integer or out of range\r\n";
            }
        }
    }
    
    else if (command == "CONFIG") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        
        std::string sub_command = parts[4];
        for (auto &c : sub_command) c = toupper(c);

        if (sub_command == "GET") {
            std::string param = parts[6];
            std::string value = "";

            if (param == "dir") {
                value = g_config.dir;
            } else if (param == "dbfilename") {
                value = g_config.dbfilename;
            }

            std::string resp = "*2\r\n";
            resp += "$" + std::to_string(param.length()) + "\r\n" + param + "\r\n";
            resp += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
            return resp;
        }
    }

    else if (command == "KEYS") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string pattern = parts[4];

        if (pattern == "*") {
            std::string resp = "*" + std::to_string(g_kv_store.size()) + "\r\n";
            for (auto const& [key, val] : g_kv_store) {
                resp += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
            }
            return resp;
        }
        return "*0\r\n";
    }

    else if (command == "INFO") {
        std::string section = "";
        if (parts.size() >= 5) {
            section = parts[4];
            for (auto &c : section) c = toupper(c);
        }

        std::string role = g_config.is_replica ? "slave" : "master";
        std::string info_content = "role:" + role + "\n";

        info_content += "master_replid:" + g_config.master_replid + "\n";
        info_content += "master_repl_offset:" + std::to_string(g_config.master_repl_offset) + "\n";

        std::string resp = "$" + std::to_string(info_content.length()) + "\r\n" + info_content + "\r\n";
        return resp;
    }

    else if (command == "REPLCONF") {
        return "+OK\r\n";
    }

    else if (command == "PSYNC") {
        std::string resp = "+FULLRESYNC " + g_config.master_replid + " 0\r\n";
        return resp;
    }

    return "-ERR unknown command\r\n";
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

    // Handle transaction commands which cant be queued
    if (command == "MULTI") {
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
        
        std::string final_resp = "*" + std::to_string(state.transaction_queue.size()) + "\r\n";
        for (const auto& queued_parts : state.transaction_queue) {
            final_resp += dispatch_command(client_fd, queued_parts, true);
        }
        send(client_fd, final_resp.c_str(), final_resp.length(), 0);

        state.in_transaction = false;
        state.transaction_queue.clear();
        return;
    }

    else if (command == "DISCARD") {
        if (!state.in_transaction) {
            send(client_fd, "-ERR DISCARD without MULTI\r\n", 28, 0);
            return;
        }

        state.transaction_queue.clear();
        state.in_transaction = false;
        send(client_fd, "+OK\r\n", 5, 0);
        return;
    }

    // if client in transaction, queue any other command
    else if (state.in_transaction) {
        state.transaction_queue.push_back(parts);
        send(client_fd, "+QUEUED\r\n", 9, 0);
        return;
    }

    // Normal execution - Handle non-transaction commands
    std::string result = dispatch_command(client_fd, parts);
    if (!result.empty()) {
        send(client_fd, result.c_str(), result.length(), 0);
    }
}