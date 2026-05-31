#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

struct Client;

struct PubSub {
    std::unordered_map<std::string, std::unordered_set<int>> channels;
    std::unordered_map<int, std::unordered_set<std::string>> client_channels;
};  

void handle_pubsub(Client& client, const std::vector<std::string>& command, PubSub& pubsub, std::unordered_map<int, Client>& clients);
void remove_client(PubSub& pubsub, int client_fd);