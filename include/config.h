#pragma once
#include <string>

enum class AppendFsync {ALWAYS, EVERYSEC, NO};

struct RedisConfig {
    int port = 6379;
    bool appendonly = true;
    std::string appendfilename = "appendonly.aof";
    AppendFsync appendfsync = AppendFsync::EVERYSEC;
};