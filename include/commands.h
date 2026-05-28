#pragma once
#include <string>
#include <vector>
#include "db.h"

using CommandHandler = std::string (*)(DB&, const std::vector<std::string>&);

std::string dispatch_command(DB& db, const std::vector<std::string>& parts);
