#include <iostream>
#include <vector>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>
#include <algorithm>
#include <cerrno>
#include "common.h"

void handle_client(int client_fd);
RedisConfig g_config;

std::vector<WaitingClient> g_waiting_clients;
std::unordered_map<int, long long> g_replica_offsets;

// helper to check if fd is blocked
bool is_blocked_client(int fd) {
    bool blocked_blpop = std::any_of(
        g_blocked_clients_list.begin(),
        g_blocked_clients_list.end(),
        [&](const BlockedClient& bc) {return bc.fd == fd;}
    );

    bool blocked_wait = std::any_of(
        g_waiting_clients.begin(),
        g_waiting_clients.end(),
        [&](const WaitingClient& wc) { return wc.fd == fd; }
    );

    return blocked_blpop || blocked_wait;
}

int main(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--dir" && i + 1 < argc) {
            g_config.dir = argv[++i];
        } else if (arg == "--dbfilename" && i + 1 < argc) {
            g_config.dbfilename = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            g_config.port = std::stoi(argv[++i]);
        } else if (arg == "--replicaof" && i + 1 < argc) {
            g_config.is_replica = true;
            std::string master_info = argv[++i];
            size_t space = master_info.find(' ');
            if (space != std::string::npos) {
                g_config.master_host = master_info.substr(0, space);
                g_config.master_port = std::stoi(master_info.substr(space + 1));
            }
        }
    }

    load_rdb();

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

    if (g_config.is_replica) {
        int m_fd = initiate_replica_handshake();
        if (m_fd != -1) {
            poll_fds.push_back({m_fd, POLLIN, 0});
        }
    }

    std::cout << "BabyRedis server listening on port " << g_config.port << "...\n";

    while (true) {
        if (poll(poll_fds.data(), poll_fds.size(), 10) < 0) break;
        auto now = std::chrono::steady_clock::now();

        for (auto it = g_blocked_clients_list.begin(); it != g_blocked_clients_list.end(); ) {
            if (it->has_timeout && now >= it->deadline) {
                send(it->fd, "*-1\r\n", 5, 0); // Send null array
                it = g_blocked_clients_list.erase(it);
            } else {
                ++it;
            }
        }

        for (auto it = g_waiting_clients.begin(); it != g_waiting_clients.end(); ) {
            int current_sync_count = 0;
            for (int replica_fd : g_replicas) {
                if (g_replica_offsets[replica_fd] >= it->target_offset) {
                    current_sync_count++;
                }
            }
            if (current_sync_count >= it->target_count || now >= it->deadline) {
                std::string resp = ":" + std::to_string(current_sync_count) + "\r\n";
                send(it->fd, resp.c_str(), resp.length(), 0);
                it = g_waiting_clients.erase(it);
            } else { ++it; }
        }

        // Check if the server socket has a new connection
        if (poll_fds[0].revents & POLLIN) {
            int client_fd = accept(server_fd, nullptr, nullptr);
            if (client_fd >= 0) {
                poll_fds.push_back({client_fd, POLLIN, 0});
            }
        }

        // Check existing client sockets for data
        for (size_t i = 1; i < poll_fds.size(); ++i) {
            int fd = poll_fds[i].fd;
            bool blocked = is_blocked_client(fd);

            // only read if not blocked
            if (!blocked && (poll_fds[i].revents & POLLIN)) {
                handle_client(fd);
            }
            
            // check if client disconnected
            char dummy;
            int res = recv(fd, &dummy, 1, MSG_PEEK | MSG_DONTWAIT);
            if ((res <= 0 && errno != EAGAIN && errno != EWOULDBLOCK) && !blocked) {
                int fd_to_close = fd;

                if (fd_to_close == g_master_fd) {
                    g_master_fd = -1;
                }

                g_replicas.erase(
                    std::remove(g_replicas.begin(), g_replicas.end(), fd_to_close),
                    g_replicas.end()
                );
                g_replica_offsets.erase(fd_to_close);

                g_blocked_clients_list.erase(
                    std::remove_if(g_blocked_clients_list.begin(), g_blocked_clients_list.end(),
                        [&](const BlockedClient& bc) { return bc.fd == fd_to_close; }),
                    g_blocked_clients_list.end()
                );
                g_waiting_clients.erase(
                    std::remove_if(g_waiting_clients.begin(), g_waiting_clients.end(),
                        [&](const WaitingClient& wc) { return wc.fd == fd_to_close; }),
                    g_waiting_clients.end()
                );

                close(fd_to_close);
                poll_fds.erase(poll_fds.begin() + i);
                i--;
            }
        }
    }

    close(server_fd);
    return 0;
}