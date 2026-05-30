#pragma once
#include "db.h"

using CommandHandler = std::string (*)(DB&, const std::vector<std::string>&);

bool is_write_command(const std::string& command);

std::string dispatch(DB& db, const std::vector<std::string>& parts);
