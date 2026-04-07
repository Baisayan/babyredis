#include <fstream>
#include <iostream>
#include <vector>
#include <cstdint>
#include "common.h"

// helper to read size-encoded values
uint32_t read_size(std::ifstream& file, bool& is_special) {
    uint8_t first_byte;
    file.read(reinterpret_cast<char*>(&first_byte), 1);
    uint8_t type = (first_byte & 0xC0) >> 6;

    if (type == 0x00) { // 0b00: 6 bits
        is_special = false;
        return first_byte & 0x3F;
    } else if (type == 0x01) { // 0b01: 14 bits
        uint8_t second_byte;
        file.read(reinterpret_cast<char*>(&second_byte), 1);
        is_special = false;
        return ((first_byte & 0x3F) << 8) | second_byte;
    } else if (type == 0x02) { // 0b10: 4 bytes
        uint32_t size;
        file.read(reinterpret_cast<char*>(&size), 4);
        is_special = false;
        return __builtin_bswap32(size);
    } else { // 0b11: Special encoding
        is_special = true;
        return first_byte & 0x3F;
    }
}

// Helper to read string-encoded values
std::string read_string(std::ifstream& file) {
    bool is_special;
    uint32_t length = read_size(file, is_special);

    if (is_special) {
        if (length == 0) { // 0xC0: 8-bit integer
            int8_t val;
            file.read(reinterpret_cast<char*>(&val), 1);
            return std::to_string(val);
        } else if (length == 1) { // 0xC1: 16-bit integer
            int16_t val;
            file.read(reinterpret_cast<char*>(&val), 2);
            return std::to_string(val);
        } else if (length == 2) { // 0xC2: 32-bit integer
            int32_t val;
            file.read(reinterpret_cast<char*>(&val), 4);
            return std::to_string(val);
        }
    }

    std::vector<char> buffer(length);
    file.read(buffer.data(), length);
    return std::string(buffer.begin(), buffer.end());
}

void load_rdb() {
    std::string path = g_config.dir + "/" + g_config.dbfilename;
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) return;

    // Skip Header - 9 bytes
    file.seekg(9, std::ios::cur);

    uint8_t marker;
    while (file.read(reinterpret_cast<char*>(&marker), 1)) {
        if (marker == 0xFF) break; // End of file
        if (marker == 0xFA) { // Metadata
            read_string(file); // Name
            read_string(file); // Value
            continue;
        }
        if (marker == 0xFE) { // Database selector
            bool special;
            read_size(file, special); // Skip DB Index
            continue;
        }
        if (marker == 0xFB) { // Resize DB
            bool special;
            read_size(file, special); // Skip HT size
            read_size(file, special); // Skip Expire size
            continue;
        }

        // Handle Keys/Values
        uint64_t expiry = 0;
        bool has_expiry = false;

        if (marker == 0xFC) { // Milliseconds expiry
            file.read(reinterpret_cast<char*>(&expiry), 8);
            file.read(reinterpret_cast<char*>(&marker), 1); // Get value type
            has_expiry = true;
        } else if (marker == 0xFD) { // Seconds expiry
            uint32_t expiry_sec;
            file.read(reinterpret_cast<char*>(&expiry_sec), 4);
            expiry = (uint64_t)expiry_sec * 1000;
            file.read(reinterpret_cast<char*>(&marker), 1); // Get value type
            has_expiry = true;
        }

        if (marker == 0x00) { // String Value Type
            std::string key = read_string(file);
            std::string value = read_string(file);

            ValueEntry entry;
            entry.type = ValueType::STRING;
            entry.value = value;
            if (has_expiry) {
                entry.has_expiry = true;
                auto now_system = std::chrono::system_clock::now().time_since_epoch();
                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now_system).count();
                auto diff = (long long)expiry - now_ms;
                entry.expiry_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(diff);
            }
            g_kv_store[key] = entry;
        }
    }
}