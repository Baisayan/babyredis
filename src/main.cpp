#include <iostream>
#include <vector>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>
#include <algorithm>
#include <cerrno>
#include <fstream>
#include <filesystem>
#include "common.h"

void handle_client(int client_fd);
RedisConfig g_config;
namespace fs = std::filesystem;

void replay_aof(const std::string& aof_path) {
    std::ifstream aof_file(aof_path, std::ios::binary);
    if (!aof_file.is_open()) return;

    std::string content((std::istreambuf_iterator<char>(aof_file)),
                         std::istreambuf_iterator<char>());

    aof_file.close();
    if (content.empty()) return;
    std::vector<std::string> all_parts = split_resp(content);

    size_t i = 0;
    while (i < all_parts.size()) {
        if (all_parts[i].empty() || all_parts[i][0] != '*') { i++; continue; }

        try {
            int num_elements = std::stoi(all_parts[i].substr(1));
            std::vector<std::string> parts;
            
            size_t elements_to_capture = 1 + (num_elements * 2);
            if (i + elements_to_capture > all_parts.size()) break;

            for (size_t j = 0; j < elements_to_capture; ++j) {
                parts.push_back(all_parts[i++]);
            }

            if (parts.size() >= 3) { 
                dispatch_command(-1, parts);
            }
        } catch (...) { break; }
    }
}

int main(int argc, char** argv) {
    char cwd[4096];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        g_config.dir = std::string(cwd);
    }

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (i + 1 < argc) {
            if (arg == "--port") {
                g_config.port = std::stoi(argv[++i]);
            } else if (arg == "--dir") {
                g_config.dir = argv[++i];
            } else if (arg == "--appendonly") {
                g_config.appendonly = argv[++i];
            } else if (arg == "--appenddirname") {
                g_config.appenddirname = argv[++i];
            } else if (arg == "--appendfilename") {
                g_config.appendfilename = argv[++i];
            } else if (arg == "--appendfsync") {
                g_config.appendfsync = argv[++i];
            }
        }
    }
    
    if (g_config.appendonly == "yes") {
        try {
            fs::path base_path(g_config.dir);
            fs::path aof_dir = base_path / g_config.appenddirname;
            if (!fs::exists(aof_dir)) fs::create_directories(aof_dir);

            std::string manifest_filename = g_config.appendfilename + ".manifest";
            fs::path manifest_path = aof_dir / manifest_filename;

            if (fs::exists(manifest_path)) {
                std::ifstream manifest_file(manifest_path);
                std::string line;
                std::string replay_path = "";

                while (std::getline(manifest_file, line)) {
                    if (line.find("type i") != std::string::npos) {
                        std::stringstream ss(line);
                        std::string token, filename;
                        ss >> token; 
                        ss >> filename; 
                        replay_path = (aof_dir / filename).string();
                        g_config.active_aof_path = replay_path;
                        break;
                    }
                }
                manifest_file.close();
                if (!replay_path.empty() && fs::exists(replay_path)) {
                    std::cout << "Replaying AOF: " << replay_path << std::endl;
                    replay_aof(replay_path);
                }
            } else {
                std::string incr_name = g_config.appendfilename + ".1.incr.aof";
                std::ofstream mf(manifest_path);
                mf << "file " << incr_name << " seq 1 type i\n";
                mf.close();
                
                fs::path aof_path = aof_dir / incr_name;
                std::ofstream af(aof_path);
                af.close();
                g_config.active_aof_path = aof_path.string();
            }
        } catch (const std::exception& e) {
            std::cerr << "AOF init error: " << e.what() << std::endl;
        }
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr = {AF_INET, htons(g_config.port), INADDR_ANY};
    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Failed to bind to port " << g_config.port << std::endl;
        return 1;
    }
    listen(server_fd, 5);

    std::vector<pollfd> poll_fds;
    poll_fds.push_back({server_fd, POLLIN, 0});

    std::cout << "BabyRedis server listening on port " << g_config.port << "...\n";

    while (true) {
        if (poll(poll_fds.data(), poll_fds.size(), -1) < 0) {
            if (errno == EINTR) continue;
            break;
        }

        // Check if the server socket has a new connection
        if (poll_fds[0].revents & POLLIN) {
            int client_fd = accept(server_fd, nullptr, nullptr);
            if (client_fd >= 0) {
                poll_fds.push_back({client_fd, POLLIN, 0});
            }
        }

        // handle existing clients
        for (size_t i = 1; i < poll_fds.size(); ++i) {
            if (poll_fds[i].revents & POLLIN) {
                handle_client(poll_fds[i].fd);
            }

            // Check for disconnect or errors
            if (poll_fds[i].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                close(poll_fds[i].fd);
                poll_fds.erase(poll_fds.begin() + i);
                i--;
                continue;
            }

            // check if client disconnected
            char dummy;
            int res = recv(poll_fds[i].fd, &dummy, 1, MSG_PEEK | MSG_DONTWAIT);
            if (res == 0 || (res <= 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                close(poll_fds[i].fd);
                poll_fds.erase(poll_fds.begin() + i);
                i--;
            }
        }
    }
    close(server_fd);
    return 0;
}