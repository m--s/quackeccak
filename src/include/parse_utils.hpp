#pragma once

#include "duckdb.hpp"
#include <vector>
#include <string>

namespace duckdb {

class ParseUtils {
public:
	// Parse unsigned integer from Value - handles full uint64_t range properly
	static uint64_t ParseUnsignedInteger(const Value &val, uint64_t default_val = 0);

	// Parse uint64 from Value with hex support
	static uint64_t ParseHex64(const Value &val, uint64_t default_val = 0);

	// Parse uint32 from Value with hex support
	static uint32_t ParseHex32(const Value &val, uint32_t default_val = 0);

	// Validate Ethereum address format
	static bool IsValidAddress(const std::string &addr);

	// Validate hash format (32 bytes)
	static bool IsValidHash(const std::string &hash);

	// Setup return types for create2_mine table function
	static void SetupReturnTypes(std::vector<LogicalType> &return_types, std::vector<std::string> &names);
};

} // namespace duckdb
