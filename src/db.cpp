#include "db.h"
#include "resp.h"

static inline std::string wrong_type() {
    return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
}

std::string db_set(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    db.kvstore[parts[1]] = ValueEntry{parts[2]};
    return resp_simple_string("OK");
}

std::string db_get(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        return resp_null();
    }

    if (auto* str_val = std::get_if<std::string>(&it->second.data)) {
        return resp_bulk_string(*str_val);
    }

    return wrong_type();
}

std::string db_incr(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        db.kvstore[parts[1]] = ValueEntry{std::string("1")};
        return resp_integer(1);
    }

    if (auto* str_val = std::get_if<std::string>(&it->second.data)) {
            try {
                long long value = std::stoll(*str_val);
                ++value;
                *str_val = std::to_string(value);
                return resp_integer(value);
            }
            catch (...) {
                return resp_error("value is not an integer or out of range");
            }
        }
    return wrong_type();
}

std::string db_decr(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        db.kvstore[parts[1]] = ValueEntry{std::string("-1")};
        return resp_integer(-1);
    }

    if (auto* str_val = std::get_if<std::string>(&it->second.data)) {
        try {
            long long value = std::stoll(*str_val);
            --value;
            *str_val = std::to_string(value);
            return resp_integer(value);
        }
        catch (...) {
            return resp_error("value is not an integer or out of range");
        }
    }
    return wrong_type();
}

std::string db_incrby(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }
    
    long long increment = 0;
    try { increment = std::stoll(parts[2]); }
    catch (...) {
        return resp_error("value is not an integer or out of range");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        db.kvstore[parts[1]] = ValueEntry{std::to_string(increment)};
        return resp_integer(increment);
    }

    if (auto* str_val = std::get_if<std::string>(&it->second.data)) {
        try {
            long long value = std::stoll(*str_val);
            value += increment;
            *str_val = std::to_string(value);
            return resp_integer(value);
        }
        catch (...) {
            return resp_error("value is not an integer or out of range");
        }
    }
    return wrong_type();
}

std::string db_decrby(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    long long decrement = 0;
    try { decrement = std::stoll(parts[2]); }
    catch (...) {
        return resp_error("value is not an integer or out of range");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        db.kvstore[parts[1]] = ValueEntry{std::to_string(-decrement)};
        return resp_integer(-decrement);
    }

    if (auto* str_val = std::get_if<std::string>(&it->second.data)) {
        try {
            long long value = std::stoll(*str_val);
            value -= decrement;
            *str_val = std::to_string(value);
            return resp_integer(value);
        }
        catch (...) {
            return resp_error("value is not an integer or out of range");
        }
    }
    return wrong_type();
}

std::string db_exists(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() < 2) {
        return resp_error("wrong number of arguments");
    }

    long long count = 0;
    for (size_t i = 1; i < parts.size(); ++i) {
        if (db.kvstore.find(parts[i]) != db.kvstore.end()) {
            ++count;
        }
    }
    return resp_integer(count);
}

std::string db_del(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() < 2) {
        return resp_error("wrong number of arguments");
    }

    long long deleted = 0;
    for (size_t i = 1; i < parts.size(); ++i) {
        deleted += static_cast<long long>(db.kvstore.erase(parts[i]));
    }
    return resp_integer(deleted);
}

std::string db_type(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        return resp_simple_string("none");
    }

    const auto& data = it->second.data;
    if (std::holds_alternative<std::string>(data)) return resp_simple_string("string");
    if (std::holds_alternative<ListType>(data)) return resp_simple_string("list");
    if (std::holds_alternative<SetType>(data)) return resp_simple_string("set");
    if (std::holds_alternative<HashType>(data)) return resp_simple_string("hash");

    return resp_simple_string("none");
}

std::string db_rpush(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() < 3) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        auto result = db.kvstore.emplace(parts[1], ValueEntry{ListType{}});
        it = result.first;
    }

    if (auto* list_val = std::get_if<ListType>(&it->second.data)) {
        for (size_t i = 2; i < parts.size(); ++i) {
            list_val->push_back(parts[i]);
        }
        return resp_integer(static_cast<long long>(list_val->size()));
    }
    return wrong_type();
}

std::string db_lpush(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() < 3) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        auto result = db.kvstore.emplace(parts[1], ValueEntry{ListType{}});
        it = result.first;
    }

    if (auto* list_val = std::get_if<ListType>(&it->second.data)) {
        for (size_t i = 2; i < parts.size(); ++i) {
            list_val->push_front(parts[i]);
        }
        return resp_integer(static_cast<long long>(list_val->size()));
    }
    return wrong_type();
}

std::string db_lpop(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_null();

    if (auto* list_val = std::get_if<ListType>(&it->second.data)) {
        if (list_val->empty()) return resp_null();

        std::string value = std::move(list_val->front());
        list_val->pop_front();
        return resp_bulk_string(value);
    }
    return wrong_type();
}

std::string db_rpop(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_null();

    if (auto* list_val = std::get_if<ListType>(&it->second.data)) {
        if (list_val->empty()) return resp_null();

        std::string value = std::move(list_val->back());
        list_val->pop_back();
        return resp_bulk_string(value);
    }
    return wrong_type();
}

std::string db_llen(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_integer(0);

    if (auto* list_val = std::get_if<ListType>(&it->second.data)) {
        return resp_integer(static_cast<long long>(list_val->size()));
    }
    return wrong_type();
}

std::string db_lindex(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_null();

    if (auto* list_val = std::get_if<ListType>(&it->second.data)) {
        long long index = 0;
        try { index = std::stoll(parts[2]); }
        catch (...) {
            return resp_error("value is not an integer or out of range");
        }

        long long size = static_cast<long long>(list_val->size());
        if (index < 0) index = size + index;
        if (index < 0 || index >= size) return resp_null();
        return resp_bulk_string((*list_val)[index]);
    }
    return wrong_type();
}

std::string db_lrange(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 4) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_array({});

    if (auto* list_val = std::get_if<ListType>(&it->second.data)) {
        long long start, stop;
        try {
            start = std::stoll(parts[2]);
            stop = std::stoll(parts[3]);
        }
        catch (...) {
            return resp_error("value is not an integer or out of range");
        }
    
        long long list_size = static_cast<long long>(list_val->size());
        if (start < 0) start = list_size + start;
        if (stop < 0) stop = list_size + stop;
        if (start < 0) start = 0;
        if (stop >= list_size) stop = list_size - 1;
        if (start > stop || start >= list_size) {
            return resp_array({});
        }

        std::vector<std::string> values;
        for (long long i = start; i <= stop; ++i) {
            values.push_back((*list_val)[i]);
        }
        return resp_array(values);
    }
    return wrong_type();
}

std::string db_hset(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() < 4 || parts.size() % 2 != 0) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        auto result = db.kvstore.emplace(parts[1], ValueEntry{HashType{}});
        it = result.first;
    }

    if (auto* hash_val = std::get_if<HashType>(&it->second.data)) {
        long long added = 0;
        for (size_t i = 2; i < parts.size(); i += 2) {
            if (hash_val->insert_or_assign(parts[i], parts[i+1]).second) {
                added++;
            }
        }
        return resp_integer(added);
    }
    return wrong_type();
}

std::string db_hget(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_null();

    if (auto* hash_val = std::get_if<HashType>(&it->second.data)) {
        auto field_it = hash_val->find(parts[2]);
        if (field_it == hash_val->end()) return resp_null();
        return resp_bulk_string(field_it->second);
    }
    return wrong_type();
}

std::string db_hdel(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() < 3) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_integer(0);

    if (auto* hash_val = std::get_if<HashType>(&it->second.data)) {
        long long deleted = 0;
        for (size_t i = 2; i < parts.size(); ++i) {
            deleted += hash_val->erase(parts[i]);
        }
        return resp_integer(deleted);
    }
    return wrong_type();
}

std::string db_hexists(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_integer(0);

    if (auto* hash_val = std::get_if<HashType>(&it->second.data)) {
        return resp_integer(hash_val->count(parts[2]));
    }
    return wrong_type();
}

std::string db_hlen(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_integer(0);

    if (auto* hash_val = std::get_if<HashType>(&it->second.data)) {
        return resp_integer(static_cast<long long>(hash_val->size()));
    }
    return wrong_type();
}

std::string db_hkeys(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_array({});

    if (auto* hash_val = std::get_if<HashType>(&it->second.data)) {
        std::vector<std::string> keys;
        for (const auto& kv : *hash_val) {
            keys.push_back(kv.first);
        }
        return resp_array(keys);
    }
    return wrong_type();
}

std::string db_hvals(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_array({});

    if (auto* hash_val = std::get_if<HashType>(&it->second.data)) {
        std::vector<std::string> vals;
        for (const auto& kv : *hash_val) {
            vals.push_back(kv.second);
        }
        return resp_array(vals);
    }
    return wrong_type();
}

std::string db_hgetall(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_array({});

    if (auto* hash_val = std::get_if<HashType>(&it->second.data)) {
        std::vector<std::string> result;
        for (const auto& kv : *hash_val) {
            result.push_back(kv.first);
            result.push_back(kv.second);
        }
        return resp_array(result);
    }
    return wrong_type();
}

std::string db_sadd(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() < 3) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) {
        auto result = db.kvstore.emplace(parts[1], ValueEntry{SetType{}});
        it = result.first;
    }

    if (auto* set_val = std::get_if<SetType>(&it->second.data)) {
        long long added = 0;
        for (size_t i = 2; i < parts.size(); ++i) {
            if (set_val->insert(parts[i]).second) {
                added++;
            }
        }
        return resp_integer(added);
    }
    return wrong_type();
}

std::string db_srem(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() < 3) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_integer(0);

    if (auto* set_val = std::get_if<SetType>(&it->second.data)) {
        long long removed = 0;
        for (size_t i = 2; i < parts.size(); ++i) {
            removed += set_val->erase(parts[i]);
        }
        return resp_integer(removed);
    }
    return wrong_type();
}

std::string db_scard(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_integer(0);

    if (auto* set_val = std::get_if<SetType>(&it->second.data)) {
        return resp_integer(static_cast<long long>(set_val->size()));
    }
    return wrong_type();
}

std::string db_smembers(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 2) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_array({});

    if (auto* set_val = std::get_if<SetType>(&it->second.data)) {
        std::vector<std::string> members;
        for (const auto& member : *set_val) {
            members.push_back(member);
        }
        return resp_array(members);
    }
    return wrong_type();
}

std::string db_sismember(DB& db, const std::vector<std::string>& parts) {
    if (parts.size() != 3) {
        return resp_error("wrong number of arguments");
    }

    auto it = db.kvstore.find(parts[1]);
    if (it == db.kvstore.end()) return resp_integer(0);

    if (auto* set_val = std::get_if<SetType>(&it->second.data)) {
        return resp_integer(set_val->count(parts[2]));
    }
    return wrong_type();
}