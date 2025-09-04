#include "parse_utils.hpp"
#include "hex_utils.hpp"
#include "duckdb/common/exception.hpp"
#include <string>

namespace duckdb {

// Parse unsigned integer from Value - handles full uint64_t range properly
uint64_t ParseUtils::ParseUnsignedInteger(const Value &val, uint64_t default_val) {
	if (val.IsNull()) {
		return default_val;
	}

	// If it's already a numeric type, return it directly
	switch (val.type().id()) {
	case LogicalTypeId::UBIGINT:
		return val.GetValue<uint64_t>();
	case LogicalTypeId::BIGINT:
		return static_cast<uint64_t>(val.GetValue<int64_t>());
	case LogicalTypeId::UINTEGER:
		return static_cast<uint64_t>(val.GetValue<uint32_t>());
	case LogicalTypeId::INTEGER:
		return static_cast<uint64_t>(val.GetValue<int32_t>());
	case LogicalTypeId::USMALLINT:
		return static_cast<uint64_t>(val.GetValue<uint16_t>());
	case LogicalTypeId::SMALLINT:
		return static_cast<uint64_t>(val.GetValue<int16_t>());
	case LogicalTypeId::UTINYINT:
		return static_cast<uint64_t>(val.GetValue<uint8_t>());
	case LogicalTypeId::TINYINT:
		return static_cast<uint64_t>(val.GetValue<int8_t>());
	case LogicalTypeId::VARCHAR: {
		std::string str = val.GetValue<std::string>();
		if (str.empty())
			return default_val;

		// Special case for "0"
		if (str == "0")
			return 0;

		// Handle hex strings
		if (HexUtils::IsHexString(str)) {
			return std::stoull(str.substr(2), nullptr, 16);
		}

		// Handle decimal strings - use stoull for full uint64_t range
		return std::stoull(str);
	}
	default:
		return default_val;
	}
}

// Parse uint64 from Value
uint64_t ParseUtils::ParseHex64(const Value &val, uint64_t default_val) {
	if (val.IsNull()) {
		return default_val;
	}

	// If it's already a numeric type, return it directly
	switch (val.type().id()) {
	case LogicalTypeId::UBIGINT:
		return val.GetValue<uint64_t>();
	case LogicalTypeId::BIGINT:
		return static_cast<uint64_t>(val.GetValue<int64_t>());
	case LogicalTypeId::VARCHAR: {
		std::string str = val.GetValue<std::string>();
		if (str.empty())
			return default_val;

		// Special case for "0"
		if (str == "0")
			return 0;

		// Handle hex strings
		if (HexUtils::IsHexString(str)) {
			// Validate even number of hex digits
			std::string hex = str.substr(2);
			if (hex.length() % 2 != 0) {
				throw BinderException("Hex string must have even number of digits: " + str);
			}
			return std::stoull(hex, nullptr, 16);
		}
		// Handle decimal strings
		return std::stoull(str);
	}
	default:
		return default_val;
	}
}

// Parse uint32 from Value
uint32_t ParseUtils::ParseHex32(const Value &val, uint32_t default_val) {
	if (val.IsNull()) {
		return default_val;
	}

	// If it's already a numeric type, return it directly
	switch (val.type().id()) {
	case LogicalTypeId::UINTEGER:
		return val.GetValue<uint32_t>();
	case LogicalTypeId::INTEGER:
		return static_cast<uint32_t>(val.GetValue<int32_t>());
	case LogicalTypeId::VARCHAR: {
		std::string str = val.GetValue<std::string>();
		if (str.empty())
			return default_val;

		// Special case for "0"
		if (str == "0")
			return 0;

		// Handle hex strings
		if (HexUtils::IsHexString(str)) {
			// Validate even number of hex digits
			std::string hex = str.substr(2);
			if (hex.length() % 2 != 0) {
				throw BinderException("Hex string must have even number of digits: " + str);
			}
			return std::stoul(hex, nullptr, 16);
		}
		// Handle decimal strings
		return std::stoul(str);
	}
	default:
		return default_val;
	}
}

// Validate Ethereum address format
bool ParseUtils::IsValidAddress(const std::string &addr) {
	std::string cleaned = addr;
	if (HexUtils::IsHexString(addr)) {
		cleaned = addr.substr(2);
	}
	return cleaned.length() == 40;
}

// Validate hash format (32 bytes)
bool ParseUtils::IsValidHash(const std::string &hash) {
	std::string cleaned = hash;
	if (HexUtils::IsHexString(hash)) {
		cleaned = hash.substr(2);
	}
	return cleaned.length() == 64;
}

void ParseUtils::SetupReturnTypes(std::vector<LogicalType> &return_types, std::vector<std::string> &names) {
	return_types = {
	    LogicalType::UBIGINT,  // salt_hi
	    LogicalType::UBIGINT,  // salt_lo
	    LogicalType::VARCHAR,  // salt (human-readable hex)
	    LogicalType::UBIGINT,  // addr_hi8
	    LogicalType::UBIGINT,  // addr_mid8
	    LogicalType::UINTEGER, // addr_lo4
	    LogicalType::VARCHAR,  // address (human-readable hex)
	    LogicalType::UTINYINT, // lz_bits (leading zeros)
	    LogicalType::UTINYINT  // tz_bits (trailing zeros)
	};

	names = {"salt_hi", "salt_lo", "salt", "addr_hi8", "addr_mid8", "addr_lo4", "address", "lz_bits", "tz_bits"};
}

} // namespace duckdb