#pragma once
#include <string>
#include <vector>
#include <chrono>
#include "db.h"
#include "config.h"

struct Aof {
    int fd = -1;
    AppendFsync fsync = AppendFsync::EVERYSEC;
    std::chrono::steady_clock::time_point last_fsync;
};

bool aof_replay(DB& db, const RedisConfig& config, std::string& error);
bool aof_open(Aof& aof, const RedisConfig& config, std::string& error);
bool aof_append(Aof& aof, std::vector<std::string> command, std::string& error);
bool aof_maybe_fsync(Aof& aof, std::string& error);
void aof_close(Aof& aof);