#include <iostream>
#include <vector>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>
#include <algorithm>
#include "common.h"

void handle_client(int client_fd);

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr = {AF_INET, htons(6379), INADDR_ANY};
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);

    std::vector<pollfd> poll_fds;
    poll_fds.push_back({server_fd, POLLIN, 0});

    std::cout << "BabyRedis server listening on 6379...\n";

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

        // Check if the server socket has a new connection
        if (poll_fds[0].revents & POLLIN) {
            int client_fd = accept(server_fd, nullptr, nullptr);
            if (client_fd >= 0) {
                poll_fds.push_back({client_fd, POLLIN, 0});
            }
        }

        // Check existing client sockets for data
        for (size_t i = 1; i < poll_fds.size(); ++i) {
            if (poll_fds[i].revents & POLLIN) {
                handle_client(poll_fds[i].fd);
            }
            
            char dummy;
            if (recv(poll_fds[i].fd, &dummy, 1, MSG_PEEK | MSG_DONTWAIT) == 0) {
                int fd_to_close = poll_fds[i].fd;

                g_blocked_clients_list.erase(
                    std::remove_if(g_blocked_clients_list.begin(), g_blocked_clients_list.end(), [fd_to_close](const BlockedClient& bc) {
                            return bc.fd == fd_to_close;
                        }),
                    g_blocked_clients_list.end()
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