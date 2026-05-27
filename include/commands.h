#pragma once
#include <string>
#include <vector>
#include <functional>

using CommandHandler = std::function<std::string(const std::vector<std::string>&)>;

std::string dispatch_command(const std::vector<std::string>& parts);
