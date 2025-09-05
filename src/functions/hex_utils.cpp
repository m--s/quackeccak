#include "hex_utils.hpp"
#include "duckdb/common/exception.hpp"
#include <string>
#include <cctype>
#include <cstdio>

namespace duckdb {

// Convert 20-byte address array to hex string
std::string HexUtils::FormatAddress(const uint8_t *address_bytes) {
	char hex_output[43]; // "0x" + 40 hex chars + null terminator
	hex_output[0] = '0';
	hex_output[1] = 'x';
	for (int i = 0; i < 20; i++) {
		std::snprintf(hex_output + 2 + i * 2, 3, "%02x", address_bytes[i]);
	}
	hex_output[42] = '\0';
	return std::string(hex_output);
}

// Convert 160-bit address representation to hex string
std::string HexUtils::AddressToHex(uint64_t hi8, uint64_t mid8, uint32_t lo4) {
	uint8_t address_bytes[20];
	// Convert from uint64_t representation to bytes (big-endian)
	for (int i = 0; i < 8; i++) {
		address_bytes[i] = (hi8 >> (56 - i * 8)) & 0xFF;
		address_bytes[i + 8] = (mid8 >> (56 - i * 8)) & 0xFF;
	}
	for (int i = 0; i < 4; i++) {
		address_bytes[i + 16] = (lo4 >> (24 - i * 8)) & 0xFF;
	}
	return FormatAddress(address_bytes);
}

// Convert to Solidity uint256 format: (uint256(hi) << 128) | uint256(lo)
std::string HexUtils::SaltToHex(uint64_t hi, uint64_t lo) {
	char hex_output[67]; // "0x" + 64 hex chars + null terminator
	hex_output[0] = '0';
	hex_output[1] = 'x';

	// Zero all 64 hex positions first
	for (int i = 2; i < 66; i++) {
		hex_output[i] = '0';
	}

	// Upper 128 bits (positions 2-33): hi value right-aligned
	// Since hi is 64 bits, it occupies the rightmost 64 bits of the upper 128
	// Positions 2-17: zeros (left padding)
	// Positions 18-33: hi value (16 hex chars for 64 bits)
	for (int i = 0; i < 8; i++) {
		uint8_t byte = (hi >> (56 - i * 8)) & 0xFF;
		hex_output[18 + i * 2] = "0123456789abcdef"[byte >> 4];
		hex_output[18 + i * 2 + 1] = "0123456789abcdef"[byte & 0xF];
	}

	// Lower 128 bits (positions 34-65): lo value right-aligned
	// Since lo is 64 bits, it occupies the rightmost 64 bits of the lower 128
	// Positions 34-49: zeros (left padding)
	// Positions 50-65: lo value (16 hex chars for 64 bits)
	for (int i = 0; i < 8; i++) {
		uint8_t byte = (lo >> (56 - i * 8)) & 0xFF;
		hex_output[50 + i * 2] = "0123456789abcdef"[byte >> 4];
		hex_output[50 + i * 2 + 1] = "0123456789abcdef"[byte & 0xF];
	}

	hex_output[66] = '\0';
	return std::string(hex_output);
}

// Convert hex string to bytes with validation
void HexUtils::HexStringToBytes(const std::string &hex_input, uint8_t *output, size_t expected_bytes,
                                const char *param_name) {
	std::string hex = hex_input;

	// Remove 0x prefix if present
	if (hex.size() >= 2 && (hex.substr(0, 2) == "0x" || hex.substr(0, 2) == "0X")) {
		hex = hex.substr(2);
	}

	// Validate hex string length
	if (hex.length() != expected_bytes * 2) {
		throw InvalidInputException("%s must be %zu bytes (%zu hex characters), got %zu characters", param_name,
		                            expected_bytes, expected_bytes * 2, hex.length());
	}

	// Validate all characters are hex
	for (char c : hex) {
		if (!std::isxdigit(c)) {
			throw InvalidInputException("%s contains non-hex characters", param_name);
		}
	}

	// Convert hex to bytes
	for (size_t i = 0; i < expected_bytes; i++) {
		std::string byte_str = hex.substr(i * 2, 2);
		output[i] = static_cast<uint8_t>(std::strtoul(byte_str.c_str(), nullptr, 16));
	}
}

// Check if string is a hex string (starts with 0x)
bool HexUtils::IsHexString(const std::string &s) {
	return s.size() > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X');
}

// Template specializations
template <>
uint64_t HexUtils::ParseHex<uint64_t>(const std::string &s, uint64_t default_val) {
	if (s.empty())
		return default_val;

	if (s == "0")
		return 0;

	if (IsHexString(s)) {
		return std::stoull(s.substr(2), nullptr, 16);
	}

	return std::stoull(s);
}

template <>
uint32_t HexUtils::ParseHex<uint32_t>(const std::string &s, uint32_t default_val) {
	if (s.empty())
		return default_val;

	if (s == "0")
		return 0;

	if (IsHexString(s)) {
		return std::stoul(s.substr(2), nullptr, 16);
	}

	return std::stoul(s);
}

} // namespace duckdb
