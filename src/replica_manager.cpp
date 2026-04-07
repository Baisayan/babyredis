#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include "common.h"

void initiate_replica_handshake() {
    if (!g_config.is_replica) return;

    // create a socket to connect to the master
    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_fd < 0) {
        std::cerr << "Failed to create replica-to-master socket" << std::endl;
        return;
    }

    // handling localhost or IP
    struct hostent* server = gethostbyname(g_config.master_host.c_str());
    if (server == nullptr) {
        std::cerr << "Error: Could not resolve master host " << g_config.master_host << std::endl;
        return;
    }

    struct sockaddr_in master_addr;
    std::memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    std::memcpy(&master_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    master_addr.sin_port = htons(g_config.master_port);

    // connect to the Master
    if (connect(master_fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        std::cerr << "Failed to connect to master at " << g_config.master_host << ":" << g_config.master_port << std::endl;
        close(master_fd);
        return;
    }

    // send the Handshake: PING
    std::string ping_cmd = "*1\r\n$4\r\nPING\r\n";
    if (send(master_fd, ping_cmd.c_str(), ping_cmd.length(), 0) < 0) {
        std::cerr << "Failed to send PING to master" << std::endl;
    } else {
        std::cout << "Handshake Step 1: Sent PING to Master." << std::endl;
    }

    close(master_fd); 
}