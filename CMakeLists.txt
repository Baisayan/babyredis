cmake_minimum_required(VERSION 3.15)
project(babyredis LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(SOURCES
    src/main.cpp
    src/db.cpp
    src/commands.cpp
    src/resp.cpp
    src/aof.cpp
)

add_executable(babyredis ${SOURCES})
target_include_directories(babyredis PRIVATE include)