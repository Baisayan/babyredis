#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <iomanip>
#include <sstream>
#include "common.h"

std::unordered_map<std::string, ValueEntry> g_kv_store;

std::string dispatch_command(int client_fd, const std::vector<std::string>& parts) {
    if (parts.size() < 3) return "";

    std::string command= parts[2];
    for (auto &c : command) c = toupper(c);

    if (command == "PING") {
        return "+PONG\r\n";
    }

    else if (command == "ECHO") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        return "$" + std::to_string(parts[4].length()) + "\r\n" + parts[4] + "\r\n";
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
            ValueEntry entry;
            entry.type = ValueType::LIST;
            g_kv_store[key] = entry;
        }
        ValueEntry &entry = g_kv_store[key];

        for (size_t i = 6; i < parts.size(); i += 2) {
            if (command == "RPUSH") entry.list_val.push_back(parts[i]);
            else entry.list_val.insert(entry.list_val.begin(), parts[i]);
        }

        return ":" + std::to_string(entry.list_val.size()) + "\r\n";
    }

    else if (command == "LPOP") {
        if (parts.size() < 5) return "-ERR wrong number of arguments\r\n";
        std::string key = parts[4];

        if (g_kv_store.find(key) == g_kv_store.end()) return "$-1\r\n";

        ValueEntry &entry = g_kv_store[key];
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
            default:
                return "+none\r\n";
        }
    }

    return "-ERR unknown command\r\n";
}

void handle_client(int client_fd) {
    char buffer[4096];
    int bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read <= 0) return;

    std::string data(buffer, bytes_read);
    std::vector<std::string> all_parts = split_resp(data);

    size_t i = 0;
    while (i < all_parts.size()) {
        if (all_parts[i][0] != '*') { i++; continue; }

        int num_elements = std::stoi(all_parts[i].substr(1));
        std::vector<std::string> parts;
        size_t elements_to_capture = 1 + (num_elements * 2);

        for (size_t j = 0; j < elements_to_capture && i < all_parts.size(); ++j) {
            parts.push_back(all_parts[i++]);
        }

        if (parts.size() < 3) continue;
        std::string result = dispatch_command(client_fd, parts);

        if (!result.empty()) {
            send(client_fd, result.c_str(), result.length(), 0);
        }
    }
}