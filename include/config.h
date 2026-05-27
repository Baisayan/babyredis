#pragma once

struct RedisConfig {
    int port = 6379;
};

extern RedisConfig g_config;