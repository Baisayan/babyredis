#include <algorithm>
#include <iomanip>
#include <sstream>

#include "common.h"

std::unordered_map<std::string, ValueEntry> g_kv_store;

static inline std::string wrong_type() {
    return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
}

std::string db_set(const std::vector<std::string>& parts) {
    if (parts.size() < 3) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    const std::string& value = parts[2];
    ValueEntry entry;
    entry.type = ValueType::STRING;
    entry.value = value;

    if (parts.size() == 5) {
        std::string option = parts[3];
        for (char& c : option) c = toupper(c);

        if (option != "PX") {
            return resp_error("syntax error");
        }

        try {
            long long ttl_ms = std::stoll(parts[4]);
            if (ttl_ms <= 0) {
                return resp_error("invalid expire time");
            }

            entry.has_expiry = true;
            entry.expiry_time =
                std::chrono::steady_clock::now() +
                std::chrono::milliseconds(ttl_ms);
        }
        catch (...) {
            return resp_error("value is not an integer or out of range");
        }
    }

    else if (parts.size() != 3) {
        return resp_error("syntax error");
    }

    g_kv_store[key] = std::move(entry);
    return resp_simple_string("OK");
}

std::string db_get(const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_null();
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::STRING) {
        return wrong_type();
    }

    if (entry.has_expiry && std::chrono::steady_clock::now() >= entry.expiry_time) {
        g_kv_store.erase(it);
        return resp_null();
    }

    return resp_bulk_string(entry.value);
}

std::string db_incr(const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        ValueEntry entry;
        entry.type = ValueType::STRING;
        entry.value = "1";
        g_kv_store[key] = std::move(entry);
        return resp_integer(1);
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::STRING) {
        return wrong_type();
    }

    try {
        long long value = std::stoll(entry.value);
        ++value;
        entry.value = std::to_string(value);
        return resp_integer(value);
    }
    catch (...) {
        return resp_error("value is not an integer or out of range");
    }
}

std::string db_rpush(const std::vector<std::string>& parts) {
    if (parts.size() < 3) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        ValueEntry entry;
        entry.type = ValueType::LIST;
        auto result = g_kv_store.emplace(key, std::move(entry));
        it = result.first;
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::LIST) {
        return wrong_type();
    }

    for (size_t i = 2; i < parts.size(); ++i) {
        entry.list_val.push_back(parts[i]);
    }

    return resp_integer(static_cast<long long>(entry.list_val.size()));
}

std::string db_lpush(const std::vector<std::string>& parts) {
    if (parts.size() < 3) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        ValueEntry entry;
        entry.type = ValueType::LIST;
        auto result = g_kv_store.emplace(key, std::move(entry));
        it = result.first;
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::LIST) {
        return wrong_type();
    }

    for (size_t i = 2; i < parts.size(); ++i) {
        entry.list_val.insert(entry.list_val.begin(), parts[i]);
    }

    return resp_integer(static_cast<long long>(entry.list_val.size()));
}

std::string db_lpop(const std::vector<std::string>& parts) {
    if (parts.size() != 2 && parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_null();
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::LIST) {
        return wrong_type();
    }

    if (entry.list_val.empty()) {
        return resp_null();
    }

    if (parts.size() == 2) {
        std::string value = std::move(entry.list_val.front());
        entry.list_val.erase(entry.list_val.begin());
        return resp_bulk_string(value);
    }

    long long count = 0;
    try {
        count = std::stoll(parts[2]);
        if (count < 0) {
            return resp_error("value is out of range");
        }
    }
    catch (...) {
        return resp_error("value is not an integer or out of range");
    }

    size_t pop_count = std::min(static_cast<size_t>(count), entry.list_val.size());
    std::vector<std::string> values;

    for (size_t i = 0; i < pop_count; ++i) {
        values.push_back(std::move(entry.list_val.front()));
        entry.list_val.erase(entry.list_val.begin());
    }

    return resp_array(values);
}

std::string db_llen(const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_integer(0);
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::LIST) {
        return wrong_type();
    }

    return resp_integer(static_cast<long long>(entry.list_val.size()));
}

std::string db_lrange(const std::vector<std::string>& parts) {
    if (parts.size() != 4) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_array({});
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::LIST) {
        return wrong_type();
    }

    long long start;
    long long stop;
    try {
        start = std::stoll(parts[2]);
        stop = std::stoll(parts[3]);
    }
    catch (...) {
        return resp_error("value is not an integer or out of range");
    }

    long long list_size = static_cast<long long>(entry.list_val.size());
    if (start < 0) {
        start = list_size + start;
    }
    if (stop < 0) {
        stop = list_size + stop;
    }
    if (start < 0) {
        start = 0;
    }
    if (stop >= list_size) {
        stop = list_size - 1;
    }
    if (start > stop || start >= list_size) {
        return resp_array({});
    }

    std::vector<std::string> values;
    for (long long i = start; i <= stop; ++i) {
        values.push_back(entry.list_val[i]);
    }

    return resp_array(values);
}

std::string db_zadd(const std::vector<std::string>& parts) {
    if (parts.size() != 4) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    double score = 0;
    try {
        score = std::stod(parts[2]);
    }
    catch (...) {
        return resp_error("value is not a valid float");
    }

    const std::string& member = parts[3];
    auto it = g_kv_store.find(key);

    if (it == g_kv_store.end()) {
        ValueEntry entry;
        entry.type = ValueType::ZSET;
        auto result = g_kv_store.emplace(key, std::move(entry));
        it = result.first;
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::ZSET) {
        return wrong_type();
    }

    bool exists = false;
    auto existing =
        std::find_if(
            entry.zset_val.begin(),
            entry.zset_val.end(),
            [&member](const ZSetMember& m) {
                return m.member == member;
            }
        );

    if (existing != entry.zset_val.end()) {
        exists = true;
        if (existing->score != score) {
            entry.zset_val.erase(existing);
            entry.zset_val.insert({member, score});
        }
    }
    else {
        entry.zset_val.insert({member, score});
    }
    return resp_integer(exists ? 0 : 1);
}

std::string db_zcard(const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_integer(0);
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::ZSET) {
        return wrong_type();
    }

    return resp_integer(static_cast<long long>(entry.zset_val.size()));
}

std::string db_zrank(const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    const std::string& target_member = parts[2];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_null();
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::ZSET) {
        return wrong_type();
    }

    long long rank = 0;
    for (const auto& member : entry.zset_val) {
        if (member.member == target_member) {
            return resp_integer(rank);
        }
        ++rank;
    }

    return resp_null();
}

std::string db_zrange(const std::vector<std::string>& parts) {
    if (parts.size() != 4) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_array({});
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::ZSET) {
        return wrong_type();
    }

    long long start;
    long long stop;
    try {
        start = std::stoll(parts[2]);
        stop = std::stoll(parts[3]);
    }
    catch (...) {
        return resp_error("value is not an integer or out of range");
    }

    long long set_size = static_cast<long long>(entry.zset_val.size());
    if (start < 0) {
        start = set_size + start;
    }
    if (stop < 0) {
        stop = set_size + stop;
    }
    if (start < 0) {
        start = 0;
    }
    if (stop >= set_size) {
        stop = set_size - 1;
    }
    if (start > stop || start >= set_size) {
        return resp_array({});
    }

    std::vector<std::string> values;
    auto zset_it = entry.zset_val.begin();
    std::advance(zset_it, static_cast<size_t>(start));

    for (
        long long i = start;
        i <= stop && zset_it != entry.zset_val.end();
        ++i, ++zset_it
    ) {
        values.push_back(zset_it->member);
    }

    return resp_array(values);
}

std::string db_zscore(const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    const std::string& target_member = parts[2];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_null();
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::ZSET) {
        return wrong_type();
    }

    auto member_it =
        std::find_if(
            entry.zset_val.begin(),
            entry.zset_val.end(),
            [&target_member](const ZSetMember& m) {
                return m.member == target_member;
            }
        );

    if (member_it == entry.zset_val.end()) {
        return resp_null();
    }

    std::ostringstream oss;
    oss << std::setprecision(17) << member_it->score;
    return resp_bulk_string(oss.str());
}

std::string db_zrem(const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    const std::string& target_member = parts[2];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_integer(0);
    }

    ValueEntry& entry = it->second;
    if (entry.type != ValueType::ZSET) {
        return wrong_type();
    }

    auto member_it = std::find_if(
            entry.zset_val.begin(),
            entry.zset_val.end(),
            [&target_member](const ZSetMember& m) {
                return m.member == target_member;
            }
        );

    if (member_it == entry.zset_val.end()) {
        return resp_integer(0);
    }

    entry.zset_val.erase(member_it);
    return resp_integer(1);
}

std::string db_type(const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    const std::string& key = parts[1];
    auto it = g_kv_store.find(key);
    if (it == g_kv_store.end()) {
        return resp_simple_string("none");
    }

    switch (it->second.type) {
        case ValueType::STRING:
            return resp_simple_string("string");
        case ValueType::LIST:
            return resp_simple_string("list");
        case ValueType::ZSET:
            return resp_simple_string("zset");
        default:
            return resp_simple_string("none");
    }
}