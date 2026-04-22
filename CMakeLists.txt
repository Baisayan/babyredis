cmake_minimum_required(VERSION 3.13)
project(babyredis)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(SOURCE_FILES 
    src/main.cpp 
    src/commands.cpp 
    src/resp_parser.cpp
)
add_executable(babyredis ${SOURCE_FILES})

find_package(Threads REQUIRED)
target_link_libraries(babyredis PRIVATE Threads::Threads)
target_include_directories(babyredis PRIVATE src)