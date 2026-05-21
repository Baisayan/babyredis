#include <iostream>
#include <vector>

#include <unistd.h>
#include <fcntl.h>

#include <sys/socket.h>
#include <netinet/in.h>

#include <poll.h>
#include <cerrno>

#include "common.h"

RedisConfig g_config;

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);

    if (flags == -1) {
        return;
    }

    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void close_client(
    std::unordered_map<int, Client>& clients,
    std::vector<pollfd>& poll_fds,
    size_t index
) {
    int fd = poll_fds[index].fd;

    close(fd);

    clients.erase(fd);

    poll_fds.erase(poll_fds.begin() + index);
}

int main(int argc, char** argv) {   
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (i + 1 < argc && arg == "--port") {
                g_config.port = std::stoi(argv[++i]);
        }
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Socket creation failed\n";
        return 1;
    }

    set_nonblocking(server_fd);

    int reuse = 1;

    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(g_config.port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Failed to bind to port " << g_config.port << std::endl;
        return 1;
    }
    
    if (listen(server_fd, SOMAXCONN) < 0) {
        std::cerr << "Listen failed\n";
        return 1;
    }

    std::unordered_map<int, Client> clients;
    std::vector<pollfd> poll_fds;
    poll_fds.push_back({server_fd, POLLIN, 0});

    std::cout << "BabyRedis server listening on port " << g_config.port << "...\n";

    while (true) {
        if (poll(poll_fds.data(), poll_fds.size(), -1) < 0) {
            if (errno == EINTR) continue;
            break;
        }

        if (poll_fds[0].revents & POLLIN) {
            while (true) {
                int client_fd = accept(server_fd, nullptr, nullptr);

                if (client_fd < 0) {
                    if (errno == EWOULDBLOCK || errno == EAGAIN) {
                        break;
                    }
                    continue;
                }
                set_nonblocking(client_fd);

                Client client{};
                client.fd = client_fd;

                clients[client_fd] = std::move(client);

                poll_fds.push_back({client_fd, POLLIN, 0});
            }
        }

        for (size_t i = 1; i < poll_fds.size(); ++i) {
            pollfd& pfd = poll_fds[i];
            auto it = clients.find(pfd.fd);

            if (it == clients.end()) {
                continue;
            }

            Client& client = it->second;

            if (pfd.revents & POLLIN) {
                handle_read(client);
            }

            if (pfd.revents & POLLOUT) {
                handle_write(client);
            }

            if (
                pfd.revents &
                (POLLERR | POLLHUP | POLLNVAL)
            ) {
                client.closed = true;
            }

            if (client.closed) {
                close_client(clients, poll_fds, i);
                --i;
                continue;
            }

            if (client.output_buffer.empty()) {
                pfd.events = POLLIN;
            }
            else {
                pfd.events = POLLIN | POLLOUT;
            }
        }
    }
    close(server_fd);
    return 0;
}