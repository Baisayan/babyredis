#include <sys/socket.h>
#include <cerrno>

#include "common.h"

void handle_read(Client& client) {
    char buffer[4096];

    while (true) {
        ssize_t bytes_read = recv(client.fd, buffer, sizeof(buffer), 0);

        if (bytes_read > 0) {
            client.input_buffer.append(buffer, bytes_read);
        }

        else if (bytes_read == 0) {
            client.closed = true;
            return;
        }

        else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            client.closed = true;
            return;
        }
    }

    if (client.input_buffer.empty()) return;

    std::vector<std::string> parts = split_resp(client.input_buffer);
    client.input_buffer.clear();

    if (parts.empty()) return;

    std::string response = dispatch_command(parts);

    if (!response.empty()) {
        client.output_buffer += response;
    }
}

void handle_write(Client& client) {
    while (!client.output_buffer.empty()) {
        ssize_t bytes_sent = send(
            client.fd,
            client.output_buffer.data(),
            client.output_buffer.size(),
            0
        );

        if (bytes_sent > 0) {
            client.output_buffer.erase(0, bytes_sent);
        }

        else if (bytes_sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            client.closed = true;
            return;
        }
    }
}