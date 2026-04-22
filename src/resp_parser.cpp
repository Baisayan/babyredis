#include "common.h"

std::vector<std::string> split_resp(const std::string& s) {
    std::vector<std::string> parts;
    size_t start = 0, end = 0;
    while ((end = s.find("\r\n", start)) != std::string::npos) {
        parts.push_back(s.substr(start, end - start));
        start = end + 2;
    }
    return parts;
}