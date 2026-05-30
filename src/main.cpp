#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>

#include "db.h"
#include "config.h"
#include "resp.h"

RedisConfig config;

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags != -1) fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void close_client(
    std::unordered_map<int, Client>& clients,
    std::vector<pollfd>& poll_fds,
    size_t index
) {
    int fd = poll_fds[index].fd;
    close(fd);
    clients.erase(fd);
    if (index < poll_fds.size() - 1) {
        poll_fds[index] = poll_fds.back();
    }
    poll_fds.pop_back();
}

int main(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (i + 1 < argc) {
            if (arg == "--port") {
                config.port = std::stoi(argv[++i]);
            } else if (arg == "--appendonly") {
                std::string val = argv[++i];
                config.appendonly = (val == "yes");
            } else if (arg == "--appendfilename") {
                config.appendfilename = argv[++i];
            } else if (arg == "--appendfsync") {
                std::string val = argv[++i];
                if (val == "always") config.appendfsync = AppendFsync::ALWAYS;
                else if (val == "no") config.appendfsync = AppendFsync::NO;
                else if (val == "everysec") config.appendfsync = AppendFsync::EVERYSEC;
            }
        }
    }

    DB db;
    Aof aof;
    std::string err;
    
    std::cout << "Loading AOF file...\n";
    if (!aof_replay(db, config, err)) {
        std::cerr << "Fatal AOF Replay Error: " << err << "\n";
        return 1;
    }

    if (!aof_open(aof, config, err)) {
        std::cerr << "Fatal AOF Open Error: " << err << "\n";
        return 1;
    }    

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) return 1;

    set_nonblocking(server_fd);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(config.port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) return 1;
    if (listen(server_fd, SOMAXCONN) < 0) return 1;

    std::unordered_map<int, Client> clients;
    std::vector<pollfd> poll_fds;
    poll_fds.push_back({server_fd, POLLIN, 0});
    std::cout << "BabyRedis server listening on port " << config.port << "...\n";

    while (true) {
        int timeout = -1; 
        if (config.appendonly && config.appendfsync == AppendFsync::EVERYSEC) {
            timeout = 1000; 
        }
                
        if (poll(poll_fds.data(), poll_fds.size(), timeout) < 0) {
            if (errno == EINTR) continue;
            break;
        }

        if (!aof_maybe_fsync(aof, err)) {
            std::cerr << "[FATAL] " << err << "\n";
            break;
        }

        // accept new clients
        if (poll_fds[0].revents & POLLIN) {
            while (true) {
                int client_fd = accept(server_fd, nullptr, nullptr);
                if (client_fd < 0) {
                    if (errno == EWOULDBLOCK || errno == EAGAIN) break;
                    continue;
                }
                set_nonblocking(client_fd);
                Client client{};
                client.fd = client_fd;
                clients[client_fd] = std::move(client);
                poll_fds.push_back({client_fd, POLLIN, 0});
            }
        }

        // handle existing clients
        for (size_t i = 1; i < poll_fds.size(); ++i) {
            pollfd& pfd = poll_fds[i];
            auto it = clients.find(pfd.fd);
            if (it == clients.end()) continue;
            Client& client = it->second;

            if (pfd.revents & POLLIN) handle_read(db, aof, client);
            if (pfd.revents & POLLOUT) handle_write(client);
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) client.closed = true;
            if (client.closed) {
                close_client(clients, poll_fds, i);
                --i;
                continue;
            }
            if (client.output_buffer.empty()) pfd.events = POLLIN;
            else pfd.events = POLLIN | POLLOUT;
        }
    }
    aof_close(aof);
    close(server_fd);
    return 0;
}
