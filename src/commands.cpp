#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include "common.h"

std::unordered_map<std::string, ValueEntry> g_kv_store;
std::vector<BlockedClient> g_blocked_clients_list;
std::unordered_map<int, ClientState> g_client_states;
std::vector<int> g_replicas;
std::unordered_map<std::string, std::vector<int>> g_key_watchers;

void propagate_to_replicas(const std::vector<std::string>& parts) {
    if (parts.empty()) return;
    std::vector<std::string> values;
    for (size_t i = 2; i < parts.size(); i += 2) values.push_back(parts[i]);

    std::string resp = "*" + std::to_string(values.size()) + "\r\n";
    for (const auto& val : values) {
        resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
    }

    g_config.master_repl_offset += resp.length();
    for (int replica_fd : g_replicas) {
        send(replica_fd, resp.c_str(), resp.length(), 0);
    }
}

void touch_key(const std::string& key) {
    if (g_key_watchers.count(key)) {
        for (int client_fd : g_key_watchers[key]) {
            if (g_client_states.count(client_fd)) {
                g_client_states[client_fd].is_dirty = true;
            }
        }
        g_key_watchers.erase(key);
    }
}

// notify a blocked client if key becomes available
void handle_blocked_clients(int client_fd, const std::string& key, const std::string& value) {
    std::string resp = "*2\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
    resp += "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
    send(client_fd, resp.c_str(), resp.length(), 0);
}

std::string dispatch_command(int client_fd, const std::vector<std::string>& parts, bool is_from_exec = false) {
    if (parts.size() < 3) return "";

    std::string original_cmd= parts[2];
    std::string command = original_cmd;
    for (auto &c : command) c = toupper(c);

    ClientState &state = g_client_states[client_fd];

    bool is_subscribed = !state.subscribed_channels.empty();

    if (is_subscribed) {
        if (command != "SUBSCRIBE" && command != "UNSUBSCRIBE" && command != "PSUBSCRIBE" && 
            command != "PUNSUBSCRIBE" && command != "PING" && command != "QUIT") {
            return "-ERR Can't execute '" + original_cmd + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n";
        }
    }

    if (state.in_transaction && !is_from_exec && command != "EXEC" && command != "DISCARD" && command != "MULTI" && command != "QUIT") {
        if (command == "WATCH") return "-ERR WATCH inside MULTI is not allowed\r\n";
        state.transaction_queue.push_back(parts);
        return "+QUEUED\r\n";
    }

    if (command == "PING") {
        if (is_subscribed) return "*2\r\n$4\r\npong\r\n$0\r\n\r\n";
        return "+PONG\r\n";
    }

    else if (command == "MULTI") {
        if (state.in_transaction) return "-ERR MULTI calls can not be nested\r\n";
        state.in_transaction = true;
        state.transaction_queue.clear();
        return "+OK\r\n";
    }

    else if (command == "EXEC") {
        if (!state.in_transaction) return "-ERR EXEC without MULTI\r\n";

        if (state.is_dirty) {
            state.in_transaction = false;
            state.transaction_queue.clear();
            state.watched_keys.clear();
            state.is_dirty = false;
            return "*-1\r\n"; // RESP Null Array
        }

        std::string final_resp = "*" + std::to_string(state.transaction_queue.size()) + "\r\n";
        for (const auto& queued_parts : state.transaction_queue) {
            final_resp += dispatch_command(client_fd, queued_parts, true);
        }

        state.in_transaction = false;
        state.transaction_queue.clear();
        state.watched_keys.clear();
        state.is_dirty = false;
        return final_resp;
    }

    else if (command == "DISCARD") {
        if (!state.in_transaction) return "-ERR DISCARD without MULTI\r\n";
        state.transaction_queue.clear();
        state.in_transaction = false;
        state.watched_keys.clear();
        state.is_dirty = false;
        return "+OK\r\n";
    }

    else if (command == "WATCH") {
        if (state.in_transaction) return "-ERR WATCH inside MULTI is not allowed\r\n";
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        for (size_t i = 4; i < parts.size(); i += 2) {
            std::string key = parts[i];
            if (std::find(state.watched_keys.begin(), 
                          state.watched_keys.end(), 
                          key) == state.watched_keys.end()) {
                state.watched_keys.push_back(key);
                g_key_watchers[key].push_back(client_fd);
            }
        }
        return "+OK\r\n";
    }

    else if (command == "UNWATCH") {
        for (const std::string& key : state.watched_keys) {
            if (g_key_watchers.count(key)) {
                auto& watchers = g_key_watchers[key];
                watchers.erase(
                    std::remove(watchers.begin(), watchers.end(), client_fd),
                    watchers.end()
                );
                if (watchers.empty()) {
                    g_key_watchers.erase(key);
                }
            }
        }
        state.watched_keys.clear();
        state.is_dirty = false;
        return "+OK\r\n";
    }

    else if (command == "ECHO") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string msg = parts[4];
        return "$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n";
    }

    else if (command == "SET") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4], value = parts[6];
        ValueEntry entry;
        entry.type = ValueType::STRING;
        entry.value = value;

        if (parts.size() >= 10) {
            std::string option = parts[8];
            for (auto &c : option) c = toupper(c);
            if (option == "PX") {
                entry.has_expiry = true;
                entry.expiry_time = std::chrono::steady_clock::now() +
                                    std::chrono::milliseconds(std::stoll(parts[10]));
            }
        }
        g_kv_store[key] = entry;
        touch_key(key);
        if (!is_from_exec) propagate_to_replicas(parts);
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
            }
            return "$" + std::to_string(entry.value.length()) + "\r\n" + entry.value + "\r\n";
        }     
        return "$-1\r\n";
    }

    else if (command == "RPUSH" || command == "LPUSH") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];

        // Init list if key doesn't exist
        if (g_kv_store.find(key) == g_kv_store.end()) {
            ValueEntry entry; entry.type = ValueType::LIST;
            g_kv_store[key] = entry;
        }
        ValueEntry &entry = g_kv_store[key];

        for (size_t i = 6; i < parts.size(); i += 2) {
            if (command == "RPUSH") entry.list_val.push_back(parts[i]);
            else entry.list_val.insert(entry.list_val.begin(), parts[i]);
        }

        touch_key(key);
        if (!is_from_exec) propagate_to_replicas(parts);

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

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::LIST) return "-WRONGTYPE Operation\r\n";
        if (entry.list_val.empty()) return "$-1\r\n";

        touch_key(key);
        if (!is_from_exec) propagate_to_replicas(parts);

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
            touch_key(key);
            if (!is_from_exec) propagate_to_replicas(parts);
            return ":1\r\n";
        } else {
            ValueEntry &entry = g_kv_store[key];
            try {
                long long val = std::stoll(entry.value);
                val++;
                entry.value = std::to_string(val);
                touch_key(key);
                if (!is_from_exec) propagate_to_replicas(parts);
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
        std::string role = g_config.is_replica ? "slave" : "master";
        std::string info_content = "role:" + role + "\n";

        info_content += "master_replid:" + g_config.master_replid + "\n";
        info_content += "master_repl_offset:" + std::to_string(g_config.master_repl_offset) + "\n";

        return "$" + std::to_string(info_content.length()) + "\r\n" + info_content + "\r\n";
    }

    else if (command == "REPLCONF") {
        if (parts.size() >= 7) {
            std::string sub_command = parts[4];
            for (auto &c : sub_command) c = toupper(c);
            if (sub_command == "GETACK") {
                std::string offset_str = std::to_string(g_config.processed_bytes);
                return "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + 
                       std::to_string(offset_str.length()) + "\r\n" + offset_str + "\r\n";
            }
            if (sub_command == "ACK") {
                g_replica_offsets[client_fd] = std::stoll(parts[6]);
                return "";
            }
        }
        return "+OK\r\n";
    }

    else if (command == "PSYNC") {
        std::string full_resync = "+FULLRESYNC " + g_config.master_replid + " 0\r\n";
        send(client_fd, full_resync.c_str(), full_resync.length(), 0);

        std::string hex_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000ff10aa32556c03d1ad";
        
        std::vector<char> rdb_binary;
        for (size_t i = 0; i < hex_rdb.length(); i += 2) {
            std::string byteString = hex_rdb.substr(i, 2);
            char byte = (char)strtol(byteString.c_str(), NULL, 16);
            rdb_binary.push_back(byte);
        }

        // send the length header
        std::string header = "$" + std::to_string(rdb_binary.size()) + "\r\n";
        send(client_fd, header.c_str(), header.length(), 0);
        send(client_fd, rdb_binary.data(), rdb_binary.size(), 0);

        g_replicas.push_back(client_fd);
        g_replica_offsets[client_fd] = 0;
        return "";
    }

    else if (command == "WAIT") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        int num_replicas_requested = std::stoi(parts[4]);
        int timeout_ms = std::stoi(parts[6]);

        int in_sync = 0;
        for (int replica_fd : g_replicas) {
            if (g_replica_offsets[replica_fd] >= g_config.master_repl_offset) {
                in_sync++;
            }
        }

        if (in_sync >= num_replicas_requested || g_config.master_repl_offset == 0) {
            return ":" + std::to_string(in_sync) + "\r\n";
        }

        std::string getack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        for (int replica_fd : g_replicas) {
            send(replica_fd, getack.c_str(), getack.length(), 0);
        }

        WaitingClient wc;
        wc.fd = client_fd;
        wc.target_count = num_replicas_requested;
        wc.target_offset = g_config.master_repl_offset;
        wc.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        g_waiting_clients.push_back(wc);

        return "";
    }

    else if (command == "PUBLISH") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        std::string channel = parts[4], message = parts[6];
        std::string resp = "*3\r\n$7\r\nmessage\r\n$" + std::to_string(channel.length()) + "\r\n" + channel + "\r\n$" + std::to_string(message.length()) + "\r\n" + message + "\r\n";
        int count = 0;
        for (auto const& [fd, state] : g_client_states) {
            if (std::find(state.subscribed_channels.begin(), state.subscribed_channels.end(), channel) != state.subscribed_channels.end()) {
                send(fd, resp.c_str(), resp.length(), 0);
                count++;
            }
        }
        return ":" + std::to_string(count) + "\r\n";
    }

    else if (command == "SUBSCRIBE") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string full_resp = "";
        for (size_t i = 4; i < parts.size(); i += 2) {
            std::string channel = parts[i];
            if (std::find(state.subscribed_channels.begin(), state.subscribed_channels.end(), channel) == state.subscribed_channels.end()) {
                state.subscribed_channels.push_back(channel);
            }
            full_resp += "*3\r\n$9\r\nsubscribe\r\n$" + std::to_string(channel.length()) + "\r\n" + channel + "\r\n";
            full_resp += ":" + std::to_string(state.subscribed_channels.size()) + "\r\n";
        }
        return full_resp;
    }

    else if (command == "UNSUBSCRIBE") {
        std::string full_resp = "";

        std::vector<std::string> targets;
        if (parts.size() < 5) targets = state.subscribed_channels;
        else {
            for(size_t i = 4; i < parts.size(); i += 2) targets.push_back(parts[i]);
        }

        if (targets.empty()) return "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n";

        for (const auto& channel : targets) {
            auto it = std::find(state.subscribed_channels.begin(), state.subscribed_channels.end(), channel);
            if (it != state.subscribed_channels.end()) {
                state.subscribed_channels.erase(it);
            }
            full_resp += "*3\r\n$11\r\nunsubscribe\r\n$" + std::to_string(channel.length()) + 
                         "\r\n" + channel + "\r\n:" + std::to_string(state.subscribed_channels.size()) + "\r\n";
        }
        
        return full_resp;
    }

    else if (command == "ZADD") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        double score = std::stod(parts[6]);
        std::string member = parts[8];

        if (g_kv_store.find(key) == g_kv_store.end()) {
            ValueEntry entry;
            entry.type = ValueType::ZSET;
            g_kv_store[key] = entry;
        }

        ValueEntry &entry = g_kv_store[key];     
        if (entry.type != ValueType::ZSET) return "-WRONGTYPE Operation against Key\r\n";

        bool exists = false;
        auto it = std::find_if(entry.zset_val.begin(), entry.zset_val.end(),
                               [&member](const ZSetMember& m) { return m.member == member; });

        if (it != entry.zset_val.end()) {
            exists = true;
            if (it->score != score) {
                entry.zset_val.erase(it);
                entry.zset_val.insert({member, score});
            }
        } else { entry.zset_val.insert({member, score}); }

        touch_key(key);
        if (!is_from_exec) propagate_to_replicas(parts);
        return exists ? ":0\r\n" : ":1\r\n";;
    }

    else if (command == "ZRANK") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        std::string target_member = parts[6];
        if (g_kv_store.find(key) == g_kv_store.end()) return "$-1\r\n";

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::ZSET) return "-WRONGTYPE Operation against Key\r\n";

        int rank = 0;
        bool found = false;
        for (const auto& m : entry.zset_val) {
            if (m.member == target_member) {
                found = true; break;
            } rank++;
        }

        if (found) {
            return ":" + std::to_string(rank) + "\r\n";
        } else { return "$-1\r\n"; }
    }

    else if (command == "ZRANGE") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        long long start = std::stoll(parts[6]);
        long long stop = std::stoll(parts[8]);

        if (g_kv_store.find(key) == g_kv_store.end()) return "*0\r\n";
        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::ZSET) return "-WRONGTYPE Operation against Key\r\n";

        // handle index boundary logic
        long long set_size = (long long)entry.zset_val.size();
        if (start < 0) {
            start = set_size + start;
            if (start < 0) start = 0;
        }
        if (stop < 0) stop = set_size + stop;
        if (start >= set_size || start > stop) return "*0\r\n";
        if (stop >= set_size) stop = set_size - 1;

        std::vector<std::string> result_members;
        auto it = entry.zset_val.begin();
        std::advance(it, (size_t)start);
        for (long long i = start; i <= stop && it != entry.zset_val.end(); ++i) {
            result_members.push_back(it->member);
            ++it;
        }
        std::string resp = "*" + std::to_string(result_members.size()) + "\r\n";
        for (const auto& member : result_members) {
            resp += "$" + std::to_string(member.length()) + "\r\n" + member + "\r\n";
        }
        return resp;
    }

    return "-ERR unknown command\r\n";
}

void handle_client(int client_fd) {
    char buffer[4096];
    int bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read <= 0) return;
    buffer[bytes_read] = '\0';

    std::string data(buffer, bytes_read);
    std::vector<std::string> all_parts = split_resp(data);

    size_t i = 0;
    while (i < all_parts.size()) {
        if (all_parts[i][0] != '*') { i++; continue; }

        int num_elements = std::stoi(all_parts[i].substr(1));
        std::vector<std::string> parts;
        size_t elements_to_capture = 1 + (num_elements * 2);
        long long current_command_size = 0;

        for (size_t j = 0; j < elements_to_capture && i < all_parts.size(); ++j) {
            current_command_size += all_parts[i].length() + 2;
            parts.push_back(all_parts[i++]);
        }

        if (parts.size() < 3) continue;

        if (g_client_states.find(client_fd) == g_client_states.end()) {
            g_client_states[client_fd] = {false, {}, {}, {}};
        }

        std::string result = dispatch_command(client_fd, parts);
        if (!result.empty()) {
            bool is_getack_reply = (parts[2] == "REPLCONF" &&
                parts.size() >= 7 && (parts[4] == "GETACK"));

            if (client_fd != g_master_fd || is_getack_reply) {
                send(client_fd, result.c_str(), result.length(), 0);
            }
        }

        if (client_fd == g_master_fd) {
            g_config.processed_bytes += current_command_size;
        }
    }
}