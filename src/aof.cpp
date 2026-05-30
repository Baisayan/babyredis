#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <cctype>
#include "aof.h"
#include "resp.h"
#include "commands.h"

static std::string reconstruct_resp_array(const std::vector<std::string>& parts) {
    std::string result = "*" + std::to_string(parts.size()) + "\r\n";
    for (const auto& part : parts) {
        result += "$" + std::to_string(part.size()) + "\r\n" + part + "\r\n";
    }
    return result;
}

bool aof_replay(DB& db, const RedisConfig& config, std::string& error) {
    if (!config.appendonly) return true;

    int fd = open(config.appendfilename.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) return true;
        error = "Failed to open AOF file for reading: " + std::string(strerror(errno));
        return false;
    }

    Client dummy_client{};
    char buffer[4096];

    while (true) {
        ssize_t bytes_read = read(fd, buffer, sizeof(buffer));
        if (bytes_read < 0) {
            error = "Error reading AOF file";
            close(fd);
            return false;
        }
        if (bytes_read == 0) break; // EOF

        dummy_client.input_buffer.append(buffer, bytes_read);

        while (true) {
            ParseResult result = parse_resp(dummy_client);
            if (result.type == ParseResultType::INCOMPLETE) break;
            
            if (result.type == ParseResultType::ERROR) {
                error = "Malformed RESP in AOF: " + result.error;
                close(fd);
                return false;
            }

            if (result.command.empty() || !is_write_command(result.command[0])) {
                error = "Invalid or read-only command found in AOF";
                close(fd);
                return false;
            }

            std::string response = dispatch_command(db, result.command);
            if (!response.empty() && response[0] == '-') {
                error = "AOF command replayed with error: " + response;
                close(fd);
                return false;
            }
        }
    }

    close(fd);
    return true;
}

bool aof_open(Aof& aof, const RedisConfig& config, std::string& error) {
    if (!config.appendonly) return true;

    aof.fd = open(config.appendfilename.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (aof.fd < 0) {
        error = "Failed to open AOF file for appending: " + std::string(strerror(errno));
        return false;
    }

    aof.fsync = config.appendfsync;
    aof.last_fsync = std::chrono::steady_clock::now();
    return true;
}

bool aof_append(Aof& aof, std::vector<std::string> command, std::string& error) {
    if (aof.fd < 0 || command.empty()) return true;

    // Canonicalize only the command name
    for (char& c : command[0]) c = toupper(c);

    std::string raw_resp = reconstruct_resp_array(command);
    
    ssize_t written = write(aof.fd, raw_resp.data(), raw_resp.size());
    if (written < 0 || static_cast<size_t>(written) != raw_resp.size()) {
        error = "Fatal write error to AOF: " + std::string(strerror(errno));
        return false;
    }

    if (aof.fsync == AppendFsync::ALWAYS) {
        if (fsync(aof.fd) < 0) {
            error = "Fatal fsync error on AOF: " + std::string(strerror(errno));
            return false;
        }
    }

    return true;
}

bool aof_maybe_fsync(Aof& aof, std::string& error) {
    if (aof.fd < 0 || aof.fsync != AppendFsync::EVERYSEC) return true;

    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - aof.last_fsync).count();

    if (duration >= 1) {
        if (fsync(aof.fd) < 0) {
            error = "Fatal fsync error on AOF: " + std::string(strerror(errno));
            return false;
        }
        aof.last_fsync = now;
    }
    return true;
}

void aof_close(Aof& aof) {
    if (aof.fd >= 0) {
        if (aof.fsync != AppendFsync::NO) {
            fsync(aof.fd);
        }
        close(aof.fd);
        aof.fd = -1;
    }
}