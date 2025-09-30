#include "keccak.hpp"
#include "keccak_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "../types/bytes32.hpp"
#include "../types/address.hpp"
#include <vector>
#include <cstring>

namespace duckdb {

static LogicalType Bytes32Type() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("BYTES32");
	return t;
}

// Fast hex conversion
static inline int HexVal(unsigned char c) {
	if (c >= '0' && c <= '9') {
		return c - '0';
	}
	c |= 0x20;
	if (c >= 'a' && c <= 'f') {
		return 10 + (c - 'a');
	}
	return -1;
}

// Unified function for BLOB types (handles ADDRESS, BYTES32, BLOB)
static void Keccak256UnifiedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<string_t>(result);

	// Stack buffer for typical concatenations
	uint8_t stack_buffer[1024];
	std::vector<uint8_t> heap_buffer;

	for (idx_t row = 0; row < args.size(); row++) {
		size_t total_size = 0;
		uint8_t *buffer_ptr = stack_buffer;
		bool has_null = false;

		// Calculate total size first
		for (idx_t col = 0; col < args.ColumnCount(); col++) {
			UnifiedVectorFormat fmt;
			args.data[col].ToUnifiedFormat(args.size(), fmt);
			auto idx = fmt.sel->get_index(row);

			if (!fmt.validity.RowIsValid(idx)) {
				has_null = true;
				break;
			}

			auto data = UnifiedVectorFormat::GetData<string_t>(fmt);
			total_size += data[idx].GetSize();
		}

		if (has_null) {
			FlatVector::SetNull(result, row, true);
			continue;
		}

		// Use heap buffer if needed
		if (total_size > sizeof(stack_buffer)) {
			heap_buffer.resize(total_size);
			buffer_ptr = heap_buffer.data();
		}

		// Copy all data
		size_t offset = 0;
		for (idx_t col = 0; col < args.ColumnCount(); col++) {
			UnifiedVectorFormat fmt;
			args.data[col].ToUnifiedFormat(args.size(), fmt);
			auto idx = fmt.sel->get_index(row);
			auto data = UnifiedVectorFormat::GetData<string_t>(fmt);
			string_t input = data[idx];

			memcpy(buffer_ptr + offset, input.GetData(), input.GetSize());
			offset += input.GetSize();
		}

		// Hash and store result
		uint8_t hash[32];
		Keccak::Hash256(buffer_ptr, total_size, hash);
		result_data[row] = StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(hash), 32);
	}
}

// VARCHAR wrapper with hex detection
static void Keccak256VarcharFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat fmt;
	args.data[0].ToUnifiedFormat(args.size(), fmt);

	auto input_data = UnifiedVectorFormat::GetData<string_t>(fmt);
	auto result_data = FlatVector::GetData<string_t>(result);

	uint8_t hex_buffer[512]; // Stack buffer for hex parsing

	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = fmt.sel->get_index(i);

		if (!fmt.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		string_t input = input_data[idx];
		const char *data = input.GetData();
		size_t len = input.GetSize();

		uint8_t hash[32];

		// Check for hex prefix
		if (len >= 2 && data[0] == '0' && data[1] == 'x') {
			data += 2;
			len -= 2;

			if (len % 2 != 0) {
				throw InvalidInputException("Invalid hex string: odd length");
			}

			size_t byte_count = len / 2;
			if (byte_count > sizeof(hex_buffer)) {
				throw InvalidInputException("Hex string too long");
			}

			// Parse hex
			for (size_t j = 0; j < byte_count; j++) {
				int hi = HexVal(static_cast<unsigned char>(data[j * 2]));
				int lo = HexVal(static_cast<unsigned char>(data[j * 2 + 1]));
				if (hi < 0 || lo < 0) {
					throw InvalidInputException("Invalid hex character");
				}
				hex_buffer[j] = static_cast<uint8_t>((hi << 4) | lo);
			}

			Keccak::Hash256(hex_buffer, byte_count, hash);
		} else {
			// Hash raw string bytes
			Keccak::Hash256(reinterpret_cast<const uint8_t *>(data), len, hash);
		}

		result_data[i] = StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(hash), 32);
	}
}

void RegisterKeccakFunctions(DatabaseInstance &instance) {
	ScalarFunctionSet keccak_set("keccak256");

	// VARCHAR with hex detection
	keccak_set.AddFunction(
	    ScalarFunction("keccak256", {LogicalType::VARCHAR}, Bytes32Type(), Keccak256VarcharFunction));

	// Single BLOB (handles ADDRESS, BYTES32, and raw BLOB via implicit casting)
	keccak_set.AddFunction(ScalarFunction("keccak256", {LogicalType::BLOB}, Bytes32Type(), Keccak256UnifiedFunction));

	// Two BLOBs
	keccak_set.AddFunction(
	    ScalarFunction("keccak256", {LogicalType::BLOB, LogicalType::BLOB}, Bytes32Type(), Keccak256UnifiedFunction));

	// Three BLOBs (common for Merkle trees)
	keccak_set.AddFunction(ScalarFunction("keccak256", {LogicalType::BLOB, LogicalType::BLOB, LogicalType::BLOB},
	                                      Bytes32Type(), Keccak256UnifiedFunction));

	ExtensionUtil::RegisterFunction(instance, keccak_set);
}

} // namespace duckdb
