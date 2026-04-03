#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <pthread.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
  int receiver_fd;
} communication_thread_args;

typedef struct {
  char *command;
  char **data;
} message;

typedef struct {
  char *parsed_string;
  char *buffer;
} parsed_string_data;

char *RESP_encode(char *input_string) {
  int length = strlen(input_string);
  std::string encoded_string =
      "$" + std::to_string(length) + "\r\n" + input_string + "\r\n";
  char *encoded_c_string = new char[1024];
  std::strcpy(encoded_c_string, encoded_string.c_str());
  return encoded_c_string;
}

parsed_string_data parse_string(char *buffer) {
  parsed_string_data string_data;
  string_data.parsed_string = new char[1024];
  int iter{0};
  while (buffer[iter] != '\r' || buffer[iter + 1] != '\n')
    iter++;
  buffer += iter + 2;
  iter = 0;
  while (buffer[iter] != '\r' || buffer[iter + 1] != '\n') {
    string_data.parsed_string[iter] = buffer[iter];
    iter++;
  }
  string_data.parsed_string[iter] = 0;
  string_data.buffer = buffer + iter + 2;
  return string_data;
}

char **parse_array(char *buffer) {
  std::cout << "parse_array invoked!\n";
  char **array = new char *[512];
  int iter{0};
  char array_size_string[1024];
  while (buffer[iter] != '\r' || buffer[iter + 1] != '\n') {
    array_size_string[iter] = buffer[iter];
    iter++;
  }
  array_size_string[iter] = 0;
  int array_size = std::atoi(array_size_string);
  buffer += iter + 2;
  for (int i = 0; i < array_size; i++)
    if (*buffer == '$') {
      parsed_string_data parsed_data = parse_string(buffer);
      array[i] = parsed_data.parsed_string;
      std::cout << "Line 75: " << parsed_data.parsed_string << std::endl;
      buffer = parsed_data.buffer;
    }
  return array;
}

message *parse_message(char *buffer) {
  std::cout << "parse_message invoked!\n";
  int iter{1};
  message *msg = new message;
  msg->data = parse_array(buffer + 1);
  msg->command = (char *)(msg->data[0]);
  return msg;
}

const char *echo(char **data) {
  char *string_to_encode = (data[1]);
  char *buffer = RESP_encode(string_to_encode);
  return buffer;
}

void *communication_thread(void *arg) {
  communication_thread_args *args = (communication_thread_args *)arg;
  char buf[1024];
  std::cout << "Communication thread for fd: " << args->receiver_fd << "\n";
  while (int bytes_read = recv(args->receiver_fd, buf, 1024, 0)) {
    std::cout << bytes_read << " bytes received" << std::endl;
    message *parsed_message = parse_message(buf);
    std::cout << parsed_message->command << std::endl;
    const char *resp_buf;
    if (strcmp(parsed_message->command, "PING") == 0) {
      resp_buf = "+PONG\r\n";
    } else if (strcmp(parsed_message->command, "ECHO") == 0) {
      resp_buf = echo(parsed_message->data);
    }
    send(args->receiver_fd, resp_buf, strlen(resp_buf), 0);
    delete parsed_message->data;
    for (int i = 0; i < 1024; i++)
      buf[i] = 0;
  }
  delete args;
  return NULL;
}

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) !=
      0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";

  // You can use print statements as follows for debugging, they'll be visible
  // when running tests.
  std::cout << "Logs from your program will appear here!\n";

  // Uncomment the code below to pass the first stage
  //
  while (true) {
    int receiver_fd = accept(server_fd, (struct sockaddr *)&client_addr,
                             (socklen_t *)&client_addr_len);
    pthread_t thread;
    communication_thread_args *arg = new communication_thread_args;
    *arg = {receiver_fd};
    pthread_create(&thread, NULL, communication_thread, arg);
  }
  close(server_fd);
  //

  return 0;
}