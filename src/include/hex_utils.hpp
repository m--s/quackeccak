#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {

class HexUtils {
public:
	// Convert 20-byte address array to hex string
	static std::string FormatAddress(const uint8_t *address_bytes);

	// Convert 160-bit address representation to hex string
	static std::string AddressToHex(uint64_t hi8, uint64_t mid8, uint32_t lo4);

	// Convert to Solidity uint256 format: (uint256(hi) << 128) | uint256(lo)
	static std::string SaltToHex(uint64_t hi, uint64_t lo);

	// Convert hex string to bytes with validation
	static void HexStringToBytes(const std::string &hex_input, uint8_t *output, size_t expected_bytes,
	                             const char *param_name);

	// Check if string is a hex string (starts with 0x)
	static bool IsHexString(const std::string &s);

	// Parse hex string to uint64_t
	template <typename T>
	static T ParseHex(const std::string &s, T default_val = 0);
};

} // namespace duckdb
