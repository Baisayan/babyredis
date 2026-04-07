#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include "common.h"

bool send_and_wait(int fd, const std::string& cmd) {
    if (send(fd, cmd.c_str(), cmd.length(), 0) < 0) return false;
    
    char buffer[1024];
    int bytes = recv(fd, buffer, sizeof(buffer) - 1, 0);
    return (bytes > 0); 
}

int g_master_fd = -1;

int initiate_replica_handshake() {
    if (!g_config.is_replica) return -1;

    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct hostent* server = gethostbyname(g_config.master_host.c_str());
    if (server == nullptr || master_fd < 0) return -1;

    struct sockaddr_in master_addr;
    std::memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    std::memcpy(&master_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    master_addr.sin_port = htons(g_config.master_port);

    // connect to the Master
    if (connect(master_fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        close(master_fd); return -1;
    }

    // ping the Master to verify connection and complete handshake
    if (!send_and_wait(master_fd, "*1\r\n$4\r\nPING\r\n")) {
        close(master_fd); return -1;
    }

    // send REPLCONF listening-port
    std::string port_str = std::to_string(g_config.port);
    std::string replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + 
                            std::to_string(port_str.length()) + "\r\n" + port_str + "\r\n";
    
    if (!send_and_wait(master_fd, replconf1)) {
        close(master_fd); return -1;
    }

    // send REPLCONF capa psync2
    if (!send_and_wait(master_fd, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")) {
        close(master_fd); return -1;
    }

    std::string psync_cmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    send(master_fd, psync_cmd.c_str(), psync_cmd.length(), 0);

    char res_buf[1024];
    recv(master_fd, res_buf, sizeof(res_buf), 0);

    g_master_fd = master_fd;
    return master_fd; 
}