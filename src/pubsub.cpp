#include "pubsub.h"
#include "resp.h"

void remove_client(PubSub& pubsub, int client_fd) {
    auto it = pubsub.client_channels.find(client_fd);
    if (it != pubsub.client_channels.end()) {
        for (const auto& channel : it->second) {
            pubsub.channels[channel].erase(client_fd);
            if (pubsub.channels[channel].empty()) {
                pubsub.channels.erase(channel);
            }
        }
        pubsub.client_channels.erase(it);
    }
}

void handle_pubsub(Client& client, const std::vector<std::string>& command, PubSub& pubsub, std::unordered_map<int, Client>& clients) {
    std::string cmd = command[0];
    for (char& c : cmd) c = toupper(c);

    if (cmd == "SUBSCRIBE") {
        if (command.size() < 2) {
            client.output_buffer += resp_error("wrong number of arguments");
            return;
        }
        for (size_t i = 1; i < command.size(); ++i) {
            const std::string& channel = command[i];
            pubsub.channels[channel].insert(client.fd);
            pubsub.client_channels[client.fd].insert(channel);
            
            client.subscribed_count = pubsub.client_channels[client.fd].size();
            client.is_subscribed = (client.subscribed_count > 0);

            std::string resp = "*3\r\n" + 
                                   resp_bulk_string("subscribe") + 
                                   resp_bulk_string(channel) + 
                                   resp_integer(client.subscribed_count);
            client.output_buffer += resp;
        }
    } 
    else if (cmd == "UNSUBSCRIBE") {
        std::vector<std::string> channels_to_unsub;
        if (command.size() == 1) {
            auto it = pubsub.client_channels.find(client.fd);
            if (it != pubsub.client_channels.end()) {
                for (const auto& ch : it->second) channels_to_unsub.push_back(ch);
            }
        } else {
            for (size_t i = 1; i < command.size(); ++i) channels_to_unsub.push_back(command[i]);
        }

        for (const auto& channel : channels_to_unsub) {
            auto& client_subs = pubsub.client_channels[client.fd];
            auto it = client_subs.find(channel);
            if (it != client_subs.end()) {
                client_subs.erase(it);
                pubsub.channels[channel].erase(client.fd);
                if (pubsub.channels[channel].empty()) pubsub.channels.erase(channel);
            }
            
            client.subscribed_count = pubsub.client_channels[client.fd].size();
            client.is_subscribed = (client.subscribed_count > 0);

            std::string resp = "*3\r\n" + 
                                   resp_bulk_string("unsubscribe") + 
                                   resp_bulk_string(channel) + 
                                   resp_integer(client.subscribed_count);
            client.output_buffer += resp;
        }
    } 
    else if (cmd == "PUBLISH") {
        if (command.size() != 3) {
            client.output_buffer += resp_error("wrong number of arguments");
            return;
        }
        const std::string& channel = command[1];
        const std::string& message = command[2];
        
        int receivers = 0;
        auto it = pubsub.channels.find(channel);
        if (it != pubsub.channels.end()) {
            for (int fd : it->second) {
                auto client_it = clients.find(fd);
                if (client_it != clients.end()) {
                    std::vector<std::string> msg_array = {"message", channel, message};
                    client_it->second.output_buffer += resp_array(msg_array);
                    receivers++;
                }
            }
        }
        client.output_buffer += resp_integer(receivers);
    }
}