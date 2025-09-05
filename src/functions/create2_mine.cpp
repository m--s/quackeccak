#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "create2_mine_data.hpp"
#include "parse_utils.hpp"
#include "hex_utils.hpp"
#include "keccak_wrapper.hpp"
#include <cstring>

namespace duckdb {

// Count leading zeros in address
static uint8_t CountLeadingZeros(uint64_t hi8, uint64_t mid8, uint32_t lo4) {
	uint8_t count = 0;

	// Check hi8 bytes (most significant 8 bytes)
	for (int i = 7; i >= 0; i--) {
		uint8_t byte = (hi8 >> (i * 8)) & 0xFF;
		if (byte == 0) {
			count += 8;
		} else {
			// Count leading zero bits in this byte
			if (byte >= 0x80)
				return count;
			if (byte >= 0x40)
				return count + 1;
			if (byte >= 0x20)
				return count + 2;
			if (byte >= 0x10)
				return count + 3;
			if (byte >= 0x08)
				return count + 4;
			if (byte >= 0x04)
				return count + 5;
			if (byte >= 0x02)
				return count + 6;
			return count + 7;
		}
	}

	// Check mid8 bytes (middle 8 bytes)
	for (int i = 7; i >= 0; i--) {
		uint8_t byte = (mid8 >> (i * 8)) & 0xFF;
		if (byte == 0) {
			count += 8;
		} else {
			if (byte >= 0x80)
				return count;
			if (byte >= 0x40)
				return count + 1;
			if (byte >= 0x20)
				return count + 2;
			if (byte >= 0x10)
				return count + 3;
			if (byte >= 0x08)
				return count + 4;
			if (byte >= 0x04)
				return count + 5;
			if (byte >= 0x02)
				return count + 6;
			return count + 7;
		}
	}

	// Check lo4 bytes (least significant 4 bytes)
	for (int i = 3; i >= 0; i--) {
		uint8_t byte = (lo4 >> (i * 8)) & 0xFF;
		if (byte == 0) {
			count += 8;
		} else {
			if (byte >= 0x80)
				return count;
			if (byte >= 0x40)
				return count + 1;
			if (byte >= 0x20)
				return count + 2;
			if (byte >= 0x10)
				return count + 3;
			if (byte >= 0x08)
				return count + 4;
			if (byte >= 0x04)
				return count + 5;
			if (byte >= 0x02)
				return count + 6;
			return count + 7;
		}
	}

	return count;
}

// Count trailing zeros in address
static uint8_t CountTrailingZeros(uint64_t hi8, uint64_t mid8, uint32_t lo4) {
	uint8_t count = 0;

	// Check lo4 bytes first (least significant)
	for (int i = 0; i < 4; i++) {
		uint8_t byte = (lo4 >> (i * 8)) & 0xFF;
		if (byte == 0) {
			count += 8;
		} else {
			if (byte & 0x01)
				return count;
			if (byte & 0x02)
				return count + 1;
			if (byte & 0x04)
				return count + 2;
			if (byte & 0x08)
				return count + 3;
			if (byte & 0x10)
				return count + 4;
			if (byte & 0x20)
				return count + 5;
			if (byte & 0x40)
				return count + 6;
			return count + 7;
		}
	}

	// Check mid8 bytes
	for (int i = 0; i < 8; i++) {
		uint8_t byte = (mid8 >> (i * 8)) & 0xFF;
		if (byte == 0) {
			count += 8;
		} else {
			if (byte & 0x01)
				return count;
			if (byte & 0x02)
				return count + 1;
			if (byte & 0x04)
				return count + 2;
			if (byte & 0x08)
				return count + 3;
			if (byte & 0x10)
				return count + 4;
			if (byte & 0x20)
				return count + 5;
			if (byte & 0x40)
				return count + 6;
			return count + 7;
		}
	}

	// Check hi8 bytes (most significant)
	for (int i = 0; i < 8; i++) {
		uint8_t byte = (hi8 >> (i * 8)) & 0xFF;
		if (byte == 0) {
			count += 8;
		} else {
			if (byte & 0x01)
				return count;
			if (byte & 0x02)
				return count + 1;
			if (byte & 0x04)
				return count + 2;
			if (byte & 0x08)
				return count + 3;
			if (byte & 0x10)
				return count + 4;
			if (byte & 0x20)
				return count + 5;
			if (byte & 0x40)
				return count + 6;
			return count + 7;
		}
	}

	return count;
}

// Check if address matches the mask pattern
static bool AddressMatchesMask(uint64_t addr_hi8, uint64_t addr_mid8, uint32_t addr_lo4, uint64_t mask_hi8,
                               uint64_t value_hi8, uint64_t mask_mid8, uint64_t value_mid8, uint32_t mask_lo4,
                               uint32_t value_lo4) {
	if ((addr_hi8 & mask_hi8) != value_hi8)
		return false;
	if ((addr_mid8 & mask_mid8) != value_mid8)
		return false;
	if ((addr_lo4 & mask_lo4) != value_lo4)
		return false;
	return true;
}

// Helper function to convert bind data to execution data
static unique_ptr<Create2MineData> ConvertBindToExecutionData(const Create2MineBindData &bind_data) {
	auto result = make_uniq<Create2MineData>();

	// Convert deployer and init_hash
	HexUtils::HexStringToBytes(bind_data.deployer, result->deployer, 20, "deployer");
	HexUtils::HexStringToBytes(bind_data.init_hash, result->init_hash, 32, "init_hash");

	// Copy parameters
	result->salt_start = bind_data.salt_start;
	result->salt_count = bind_data.salt_count;
	result->mask_hi8 = bind_data.mask_hi8;
	result->value_hi8 = bind_data.value_hi8;
	result->mask_mid8 = bind_data.mask_mid8;
	result->value_mid8 = bind_data.value_mid8;
	result->mask_lo4 = bind_data.mask_lo4;
	result->value_lo4 = bind_data.value_lo4;
	result->max_results = bind_data.max_results;
	result->has_pattern = bind_data.has_pattern;

	// Initialize state
	result->current_salt = result->salt_start;
	result->results_found = 0;
	result->finished = false;

	return result;
}

// MAIN BIND: Numeric types
unique_ptr<FunctionData> Create2MineBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<Create2MineBindData>();

	// Required strings
	if (input.inputs[0].IsNull()) {
		throw BinderException("deployer parameter cannot be NULL");
	}
	bind_data->deployer = input.inputs[0].GetValue<string>();

	if (input.inputs[1].IsNull()) {
		throw BinderException("init_hash parameter cannot be NULL");
	}
	bind_data->init_hash = input.inputs[1].GetValue<string>();

	// Validate addresses
	if (!ParseUtils::IsValidAddress(bind_data->deployer)) {
		throw BinderException("Invalid deployer address: expected 40 hex characters (with or without 0x prefix)");
	}
	if (!ParseUtils::IsValidHash(bind_data->init_hash)) {
		throw BinderException("Invalid init hash: expected 64 hex characters (with or without 0x prefix)");
	}

	// Numeric params - handle NULL as default
	bind_data->salt_start = input.inputs[2].IsNull() ? 0 : input.inputs[2].GetValue<uint64_t>();
	bind_data->salt_count = input.inputs[3].IsNull() ? 100 : input.inputs[3].GetValue<uint64_t>();

	// Masks - NULL means 0 (no pattern)
	bind_data->mask_hi8 = input.inputs[4].IsNull() ? 0 : input.inputs[4].GetValue<uint64_t>();
	bind_data->value_hi8 = input.inputs[5].IsNull() ? 0 : input.inputs[5].GetValue<uint64_t>();
	bind_data->mask_mid8 = input.inputs[6].IsNull() ? 0 : input.inputs[6].GetValue<uint64_t>();
	bind_data->value_mid8 = input.inputs[7].IsNull() ? 0 : input.inputs[7].GetValue<uint64_t>();
	bind_data->mask_lo4 = input.inputs[8].IsNull() ? 0 : input.inputs[8].GetValue<uint32_t>();
	bind_data->value_lo4 = input.inputs[9].IsNull() ? 0 : input.inputs[9].GetValue<uint32_t>();
	bind_data->max_results = input.inputs[10].IsNull() ? 100 : input.inputs[10].GetValue<uint64_t>();

	// Set optimization flag
	bind_data->has_pattern = (bind_data->mask_hi8 || bind_data->mask_mid8 || bind_data->mask_lo4);

	ParseUtils::SetupReturnTypes(return_types, names);
	return ConvertBindToExecutionData(*bind_data);
}

// VARCHAR BIND: All VARCHAR for hex strings (11 params)
static unique_ptr<FunctionData> Create2MineBindVarchar(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<Create2MineBindData>();

	// Required strings
	if (input.inputs[0].IsNull()) {
		throw BinderException("deployer parameter cannot be NULL");
	}
	bind_data->deployer = input.inputs[0].GetValue<string>();

	if (input.inputs[1].IsNull()) {
		throw BinderException("init_hash parameter cannot be NULL");
	}
	bind_data->init_hash = input.inputs[1].GetValue<string>();

	// Validate addresses
	if (!ParseUtils::IsValidAddress(bind_data->deployer)) {
		throw BinderException("Invalid deployer address: expected 40 hex characters (with or without 0x prefix)");
	}
	if (!ParseUtils::IsValidHash(bind_data->init_hash)) {
		throw BinderException("Invalid init hash: expected 64 hex characters (with or without 0x prefix)");
	}

	// Parse VARCHAR inputs - handle NULL, empty, hex
	bind_data->salt_start = ParseUtils::ParseUnsignedInteger(input.inputs[2], 0);
	bind_data->salt_count = ParseUtils::ParseUnsignedInteger(input.inputs[3], 100);

	// Parse hex masks
	bind_data->mask_hi8 = ParseUtils::ParseHex64(input.inputs[4], 0);
	bind_data->value_hi8 = ParseUtils::ParseHex64(input.inputs[5], 0);
	bind_data->mask_mid8 = ParseUtils::ParseHex64(input.inputs[6], 0);
	bind_data->value_mid8 = ParseUtils::ParseHex64(input.inputs[7], 0);
	bind_data->mask_lo4 = ParseUtils::ParseHex32(input.inputs[8], 0);
	bind_data->value_lo4 = ParseUtils::ParseHex32(input.inputs[9], 0);
	bind_data->max_results = ParseUtils::ParseUnsignedInteger(input.inputs[10], 100);

	bind_data->has_pattern = (bind_data->mask_hi8 || bind_data->mask_mid8 || bind_data->mask_lo4);

	ParseUtils::SetupReturnTypes(return_types, names);
	return ConvertBindToExecutionData(*bind_data);
}

static void Create2MineFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<Create2MineData>();

	if (data.finished) {
		output.SetCardinality(0);
		return;
	}

	auto salt_hi_data = FlatVector::GetData<uint64_t>(output.data[0]);
	auto salt_lo_data = FlatVector::GetData<uint64_t>(output.data[1]);
	auto salt_formatted_data = FlatVector::GetData<string_t>(output.data[2]);
	auto addr_hi8_data = FlatVector::GetData<uint64_t>(output.data[3]);
	auto addr_mid8_data = FlatVector::GetData<uint64_t>(output.data[4]);
	auto addr_lo4_data = FlatVector::GetData<uint32_t>(output.data[5]);
	auto address_formatted_data = FlatVector::GetData<string_t>(output.data[6]);
	auto lz_bits_data = FlatVector::GetData<uint8_t>(output.data[7]);
	auto tz_bits_data = FlatVector::GetData<uint8_t>(output.data[8]);

	idx_t result_idx = 0;
	const idx_t max_chunk_size = STANDARD_VECTOR_SIZE;

	while (result_idx < max_chunk_size && (data.current_salt - data.salt_start) < data.salt_count &&
	       data.results_found < data.max_results) {

		// Build CREATE2 preimage: 0xff || deployer || salt || init_code_hash
		uint8_t buffer[85];
		buffer[0] = 0xff;
		memcpy(buffer + 1, data.deployer, 20);

		// Salt as 32-byte big-endian (matching Solidity uint256)
		uint8_t salt_bytes[32];
		memset(salt_bytes, 0, 32);

		// current_salt goes in last 8 bytes (EVM uint256 format)
		for (int j = 0; j < 8; j++) {
			salt_bytes[24 + j] = (data.current_salt >> (56 - j * 8)) & 0xFF;
		}
		memcpy(buffer + 21, salt_bytes, 32);
		memcpy(buffer + 53, data.init_hash, 32);

		// Compute Keccak256 hash
		uint8_t hash[32];
		KeccakWrapper::Hash256(buffer, 85, hash);

		// Extract address (last 20 bytes of hash)
		uint8_t address_bytes[20];
		memcpy(address_bytes, hash + 12, 20);

		// Convert to integer representation for filtering
		uint64_t mock_addr_hi8 = 0, mock_addr_mid8 = 0;
		uint32_t mock_addr_lo4 = 0;

		for (int j = 0; j < 8; j++) {
			mock_addr_hi8 = (mock_addr_hi8 << 8) | address_bytes[j];
			mock_addr_mid8 = (mock_addr_mid8 << 8) | address_bytes[j + 8];
		}
		for (int j = 0; j < 4; j++) {
			mock_addr_lo4 = (mock_addr_lo4 << 8) | address_bytes[j + 16];
		}

		// Check if address matches pattern (skip if no pattern set for optimization)
		bool matches = !data.has_pattern ||
		               AddressMatchesMask(mock_addr_hi8, mock_addr_mid8, mock_addr_lo4, data.mask_hi8, data.value_hi8,
		                                  data.mask_mid8, data.value_mid8, data.mask_lo4, data.value_lo4);

		if (matches) {
			// Properly split 128-bit salt: detect if we wrapped around UINT64_MAX
			salt_hi_data[result_idx] = (data.current_salt < data.salt_start) ? 1 : 0; // Wrapped around
			salt_lo_data[result_idx] = data.current_salt;                             // Lower 64 bits of salt

			// Add formatted salt (human-readable)
			std::string salt_hex = HexUtils::SaltToHex(salt_hi_data[result_idx], salt_lo_data[result_idx]);
			salt_formatted_data[result_idx] = StringVector::AddString(output.data[2], salt_hex);

			addr_hi8_data[result_idx] = mock_addr_hi8;
			addr_mid8_data[result_idx] = mock_addr_mid8;
			addr_lo4_data[result_idx] = mock_addr_lo4;

			// Add formatted address (human-readable)
			std::string addr_hex = HexUtils::AddressToHex(mock_addr_hi8, mock_addr_mid8, mock_addr_lo4);
			address_formatted_data[result_idx] = StringVector::AddString(output.data[6], addr_hex);

			lz_bits_data[result_idx] = CountLeadingZeros(mock_addr_hi8, mock_addr_mid8, mock_addr_lo4);
			tz_bits_data[result_idx] = CountTrailingZeros(mock_addr_hi8, mock_addr_mid8, mock_addr_lo4);

			result_idx++;
			data.results_found++;
		}

		data.current_salt++;
	}

	if ((data.current_salt - data.salt_start) >= data.salt_count || data.results_found >= data.max_results) {
		data.finished = true;
	}

	output.SetCardinality(result_idx);
}

void RegisterCreate2Mine(DatabaseInstance &instance) {
	TableFunctionSet create2_set("create2_mine");

	TableFunction create2_main(
	    {
	        LogicalType::VARCHAR,  // deployer
	        LogicalType::VARCHAR,  // init_hash
	        LogicalType::UBIGINT,  // salt_start
	        LogicalType::UBIGINT,  // salt_count
	        LogicalType::UBIGINT,  // mask_hi8
	        LogicalType::UBIGINT,  // value_hi8
	        LogicalType::UBIGINT,  // mask_mid8
	        LogicalType::UBIGINT,  // value_mid8
	        LogicalType::UINTEGER, // mask_lo4
	        LogicalType::UINTEGER, // value_lo4
	        LogicalType::UBIGINT   // max_results
	    },
	    Create2MineFunction, Create2MineBind);

	TableFunction create2_varchar(
	    {
	        LogicalType::VARCHAR, // deployer
	        LogicalType::VARCHAR, // init_hash
	        LogicalType::VARCHAR, // salt_start (decimal/hex)
	        LogicalType::VARCHAR, // salt_count
	        LogicalType::VARCHAR, // mask_hi8 (hex string)
	        LogicalType::VARCHAR, // value_hi8
	        LogicalType::VARCHAR, // mask_mid8
	        LogicalType::VARCHAR, // value_mid8
	        LogicalType::VARCHAR, // mask_lo4
	        LogicalType::VARCHAR, // value_lo4
	        LogicalType::VARCHAR  // max_results
	    },
	    Create2MineFunction, Create2MineBindVarchar);

	create2_set.AddFunction(create2_varchar);
	create2_set.AddFunction(create2_main);

	ExtensionUtil::RegisterFunction(instance, create2_set);
}

} // namespace duckdb
