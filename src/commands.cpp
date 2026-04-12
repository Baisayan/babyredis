#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <iomanip>
#include <sstream>
#include <climits>
#include "common.h"

std::unordered_map<std::string, ValueEntry> g_kv_store;
std::vector<BlockedClient> g_blocked_clients_list;
std::unordered_map<int, ClientState> g_client_states;
std::vector<int> g_replicas;
std::unordered_map<std::string, std::vector<int>> g_key_watchers;

std::pair<long long, long long> parse_range_id(const std::string& id, bool is_start) {
    if (id == "-") return {0, 0};
    if (id == "+") return {LLONG_MAX, LLONG_MAX};
    size_t dash = id.find('-');
    if (dash == std::string::npos) {
        long long ms = std::stoll(id);
        return {ms, is_start ? 0 : -1};
    }
    long long ms = std::stoll(id.substr(0, dash));
    long long seq = std::stoll(id.substr(dash + 1));
    return {ms, seq};
}

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

std::pair<long long, long long> parse_stream_id(const std::string& id) {
    size_t dash = id.find('-');
    if (dash == std::string::npos) return {0, 0};
    long long ms = std::stoll(id.substr(0, dash));
    long long seq = std::stoll(id.substr(dash + 1));
    return {ms, seq};
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

    else if (command == "ZCARD") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];

        if (g_kv_store.find(key) == g_kv_store.end()) return ":0\r\n";
        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::ZSET) return "-WRONGTYPE Operation against Key\r\n";
        return ":" + std::to_string(entry.zset_val.size()) + "\r\n";
    }

    else if (command == "ZSCORE") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        std::string target_member = parts[6];
        if (g_kv_store.find(key) == g_kv_store.end()) return "$-1\r\n";

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::ZSET) return "-WRONGTYPE Operation against Key\r\n";

        auto it = std::find_if(entry.zset_val.begin(), entry.zset_val.end(),
                               [&target_member](const ZSetMember& m) {
                                   return m.member == target_member;
                               });

        if (it != entry.zset_val.end()) {
            std::ostringstream oss;
            oss << std::setprecision(17) << it->score;
            std::string score_str = oss.str();
            return "$" + std::to_string(score_str.length()) + "\r\n" + score_str + "\r\n";
        } else { return "$-1\r\n"; }
    }

    else if (command == "ZREM") {
        if (parts.size() < 7) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        std::string target_member = parts[6];
        if (g_kv_store.find(key) == g_kv_store.end()) return ":0\r\n";

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::ZSET) return "-WRONGTYPE Operation against Key\r\n";

        auto it = std::find_if(entry.zset_val.begin(), entry.zset_val.end(),
                               [&target_member](const ZSetMember& m) {
                                   return m.member == target_member;
                               });

        if (it != entry.zset_val.end()) {
            entry.zset_val.erase(it);
            touch_key(key);
            if (!is_from_exec) propagate_to_replicas(parts);
            return ":1\r\n";
        }
        return ":0\r\n";
    }

    else if (command == "TYPE") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        if (g_kv_store.find(key) == g_kv_store.end()) return "+none\r\n";
        ValueEntry &entry = g_kv_store[key];

        switch (entry.type) {
            case ValueType::STRING:
                return "+string\r\n";
            case ValueType::LIST:
                return "+list\r\n";
            case ValueType::ZSET:
                return "+zset\r\n";
            case ValueType::STREAM:
                return "+stream\r\n";
            default:
                return "+none\r\n";
        }
    }

    else if (command == "XADD") {
        if (parts.size() < 9) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        std::string id = parts[6];

        if (g_kv_store.find(key) == g_kv_store.end()) {
            ValueEntry entry;
            entry.type = ValueType::STREAM;
            g_kv_store[key] = entry;
        }

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::STREAM) return "-WRONGTYPE Operation against Key\r\n";

        std::string final_id = id;

        if (id == "*") {
            auto now = std::chrono::system_clock::now();
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               now.time_since_epoch()).count();
            long long seq = 0;

            if (!entry.stream_val.empty()) {
                auto last_id = parse_stream_id(entry.stream_val.back().id);
                if (ms == last_id.first) {
                    seq = last_id.second + 1;
                } else if (ms < last_id.first) {
                    ms = last_id.first;
                    seq = last_id.second + 1;
                } else {
                    seq = 0;
                }
            } else { seq = (ms == 0) ? 1 : 0; }
            final_id = std::to_string(ms) + "-" + std::to_string(seq);
        }

        else {
            size_t dash_pos = id.find('-');
            if (dash_pos != std::string::npos && id.substr(dash_pos + 1) == "*") {
                long long ms = std::stoll(id.substr(0, dash_pos));
                long long seq = 0;

                if (entry.stream_val.empty()) {
                    seq = (ms == 0) ? 1 : 0;
                } else {
                    auto last_id = parse_stream_id(entry.stream_val.back().id);
                    if (ms == last_id.first) {
                        seq = last_id.second + 1;
                    } else if (ms > last_id.first) {
                        seq = 0;
                    } else {
                        return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    }
                }
                final_id = std::to_string(ms) + "-" + std::to_string(seq);
            }
        }

        auto new_id = parse_stream_id(final_id);
        if (new_id.first == 0 && new_id.second == 0) {
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        }

        if (!entry.stream_val.empty()) {
            auto last_id = parse_stream_id(entry.stream_val.back().id);
            if (new_id.first < last_id.first || 
               (new_id.first == last_id.first && new_id.second <= last_id.second)) {
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            }
        }

        StreamEntry new_entry;
        new_entry.id = final_id;
        for (size_t i = 8; i < parts.size(); i += 4) {
            new_entry.kv_pairs.push_back({parts[i], parts[i+2]});
        }
        entry.stream_val.push_back(new_entry);

        auto it = g_blocked_streams.begin();
        while (it != g_blocked_streams.end()) {
            bool unblocked = false;
            for (size_t k = 0; k < it->keys.size(); ++k) {
                if (it->keys[k] == key) {
                    auto blocked_id = parse_stream_id(it->ids[k]);
                    if (new_id.first > blocked_id.first || 
                       (new_id.first == blocked_id.first && new_id.second > blocked_id.second)) {
                        std::vector<std::string> xread_parts = {
                            "*4", "$5", "XREAD", "$7", "STREAMS", 
                            "$" + std::to_string(key.length()), key, 
                            "$" + std::to_string(it->ids[k].length()), it->ids[k]
                        };  
                        std::string res = dispatch_command(it->fd, xread_parts, true);
                        send(it->fd, res.c_str(), res.length(), 0);
                        unblocked = true;
                        break;
                    }
                }
            }
            if (unblocked) it = g_blocked_streams.erase(it);
            else ++it;
        }
        touch_key(key);
        if (!is_from_exec) propagate_to_replicas(parts);
        return "$" + std::to_string(final_id.length()) + "\r\n" + final_id + "\r\n";
    }

    else if (command == "XRANGE") {
        if (parts.size() < 9) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];
        std::string start_id_str = parts[6];
        std::string end_id_str = parts[8];

        if (g_kv_store.find(key) == g_kv_store.end()) return "*0\r\n";

        ValueEntry &entry = g_kv_store[key];
        if (entry.type != ValueType::STREAM) {
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }

        auto start_limit = parse_range_id(start_id_str, true);
        auto end_limit = parse_range_id(end_id_str, false);
        std::string final_resp = "";
        int match_count = 0;

        for (const auto& s_entry : entry.stream_val) {
            auto current_id = parse_stream_id(s_entry.id);

            bool after_start = (current_id.first > start_limit.first) || 
                               (current_id.first == start_limit.first && current_id.second >= start_limit.second);

            bool before_end = true;
            if (end_limit.second == -1) {
                before_end = (current_id.first <= end_limit.first);
            } else {
                before_end = (current_id.first < end_limit.first) || 
                             (current_id.first == end_limit.first && current_id.second <= end_limit.second);
            }

            if (after_start && before_end) {
                match_count++;
                std::string entry_resp = "*2\r\n";
                entry_resp += "$" + std::to_string(s_entry.id.length()) + "\r\n" + s_entry.id + "\r\n";
                
                entry_resp += "*" + std::to_string(s_entry.kv_pairs.size() * 2) + "\r\n";
                for (const auto& pair : s_entry.kv_pairs) {
                    entry_resp += "$" + std::to_string(pair.first.length()) + "\r\n" + pair.first + "\r\n";
                    entry_resp += "$" + std::to_string(pair.second.length()) + "\r\n" + pair.second + "\r\n";
                }
                final_resp += entry_resp;
            }
        }
        return "*" + std::to_string(match_count) + "\r\n" + final_resp;
    }

    else if (command == "XREAD") {
        size_t block_idx = 0;
        long long block_ms = -1;
        size_t streams_idx = 0;

        for (size_t i = 0; i < parts.size(); ++i) {
            std::string p = parts[i];
            for (auto &c : p) c = toupper(c);
            if (p == "STREAMS") streams_idx = i;
            if (p == "BLOCK") block_idx = i;
        }

        if (block_idx > 0) block_ms = std::stoll(parts[block_idx + 2]);
        int total_args = (parts.size() - (streams_idx + 1)) / 2;
        int num_streams = total_args / 2;
        std::vector<std::string> keys, ids;

        for (int i = 0; i < num_streams; ++i) {
            keys.push_back(parts[streams_idx + 2 + (i * 2)]);
            ids.push_back(parts[streams_idx + 2 + (num_streams * 2) + (i * 2)]);
        }

        std::string resp = "";
        int total_matches = 0;
        for (int i = 0; i < num_streams; ++i) {
            std::string key = keys[i];
            auto limit_id = parse_stream_id(ids[i]);
            std::string stream_entries_resp = "";
            int match_count = 0;

            if (g_kv_store.count(key) && g_kv_store[key].type == ValueType::STREAM) {
                for (const auto& s_entry : g_kv_store[key].stream_val) {
                    auto current_id = parse_stream_id(s_entry.id);     
                    if ((current_id.first > limit_id.first) || 
                        (current_id.first == limit_id.first && current_id.second > limit_id.second)) {         
                        match_count++;
                        total_matches++;
                        std::string entry_data = "*2\r\n";

                        entry_data += "$" + std::to_string(s_entry.id.length()) + "\r\n" + s_entry.id + "\r\n";
                        entry_data += "*" + std::to_string(s_entry.kv_pairs.size() * 2) + "\r\n";
                        for (const auto& pair : s_entry.kv_pairs) {
                            entry_data += "$" + std::to_string(pair.first.length()) + "\r\n" + pair.first + "\r\n";
                            entry_data += "$" + std::to_string(pair.second.length()) + "\r\n" + pair.second + "\r\n";
                        }
                        stream_entries_resp += entry_data;
                    }
                }
            }
            if (match_count > 0) {
                resp += "*2\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
                resp += "*" + std::to_string(match_count) + "\r\n" + stream_entries_resp;
            }
        }
        if (total_matches > 0) {
            return "*" + std::to_string(num_streams) + "\r\n" + resp;
        } else if (block_ms >= 0) {
            BlockedStreamClient bsc;
            bsc.fd = client_fd;
            bsc.keys = keys;
            bsc.ids = ids;
            if (block_ms > 0) {
                bsc.has_timeout = true;
                bsc.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(block_ms);
            }
            g_blocked_streams.push_back(bsc);
            return "";
        }
        return "*-1\r\n";
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