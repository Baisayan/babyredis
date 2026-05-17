cmake_minimum_required(VERSION 3.13)
project(babyredis)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(SOURCE_FILES 
    src/main.cpp 
    src/commands.cpp 
    src/resp_parser.cpp
)
add_executable(babyredis ${SOURCE_FILES})
target_include_directories(babyredis PRIVATE src)