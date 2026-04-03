#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>
#include <chrono>

struct ValueEntry {
    std::string value;
    std::chrono::time_point<std::chrono::steady_clock> expiry_time;
    bool has_expiry = false;
};

std::unordered_map<std::string, ValueEntry> g_kv_store;

std::vector<std::string> split_resp(const std::string& s) {
    std::vector<std::string> parts;
    size_t start = 0, end = 0;
    while ((end = s.find("\r\n", start)) != std::string::npos) {
        parts.push_back(s.substr(start, end - start));
        start = end + 2;
    }
    return parts;
}

void handle_client(int client_fd) {
    char buffer[1024];
    int bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

    if (bytes_read <= 0) {
        close(client_fd);
        return;
    }

    buffer[bytes_read] = '\0';
    std::vector<std::string> parts = split_resp(std::string(buffer));
    
    if (parts.size() < 3) return;

    std::string command = parts[2];
    for (auto &c : command) c = toupper(c);

    if (command == "PING") {
        send(client_fd, "+PONG\r\n", 7, 0);
    } 
    else if (command == "ECHO") {
        std::string msg = parts[4];
        std::string resp = "$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n";
        send(client_fd, resp.c_str(), resp.length(), 0);
    }
    else if (command == "SET") {
        std::string key = parts[4];
        std::string value = parts[6];
        ValueEntry entry;
        entry.value = value;

        if (parts.size() >= 10) {
            std::string option = parts[8];
            for (auto &c : option) c = toupper(c);
            
            if (option == "PX") {
                long long ms = std::stoll(parts[10]);
                entry.has_expiry = true;
                entry.expiry_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
            }
        }

        g_kv_store[key] = entry;
        send(client_fd, "+OK\r\n", 5, 0);
    }
    else if (command == "GET") {
        std::string key = parts[4];

        if (g_kv_store.count(key)) {
            ValueEntry &entry = g_kv_store[key];

            // expiry check
            if (entry.has_expiry && std::chrono::steady_clock::now() >= entry.expiry_time) {
                g_kv_store.erase(key);
                send(client_fd, "$-1\r\n", 5, 0);
            } else {
                std::string resp = "$" + std::to_string(entry.value.length()) + "\r\n" + entry.value + "\r\n";
                send(client_fd, resp.c_str(), resp.length(), 0);
            }
        } else {
            send(client_fd, "$-1\r\n", 5, 0); // Null Bulk String
        }
    }
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr = {AF_INET, htons(6379), INADDR_ANY};
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);

    std::vector<pollfd> poll_fds;
    poll_fds.push_back({server_fd, POLLIN, 0});

    std::cout << "Single-threaded server listening on 6379...\n";

    while (true) {
        if (poll(poll_fds.data(), poll_fds.size(), -1) < 0) break;

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
                close(poll_fds[i].fd);
                poll_fds.erase(poll_fds.begin() + i);
                i--;
            }
        }
    }

    close(server_fd);
    return 0;
}