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

int main(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            g_config.port = std::stoi(argv[++i]);
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