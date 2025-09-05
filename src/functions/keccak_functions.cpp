#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "keccak_wrapper.hpp"
#include <sstream>
#include <iomanip>
#include <cstring>
#include <vector>

namespace duckdb {

static void Keccak256Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];

	UnifiedVectorFormat input_format;
	input_vector.ToUnifiedFormat(args.size(), input_format);

	auto input_data = UnifiedVectorFormat::GetData<string_t>(input_format);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = input_format.sel->get_index(i);

		if (!input_format.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		string_t input_str = input_data[idx];
		const uint8_t *input_bytes;
		size_t input_len;

		// Check if input has 0x prefix (hex string)
		if (input_str.GetSize() >= 2 && input_str.GetData()[0] == '0' && input_str.GetData()[1] == 'x') {
			// Convert hex string to bytes
			std::string hex(input_str.GetData() + 2, input_str.GetSize() - 2);

			if (hex.length() % 2 != 0) {
				throw InvalidInputException("Invalid hex string: odd number of characters after '0x'. "
				                            "EVM requires even-length hex strings (e.g., '0x0123' not '0x123')");
			}

			// Validate all characters are hex digits
			for (char c : hex) {
				if (!std::isxdigit(c)) {
					throw InvalidInputException("Invalid hex string: contains non-hex characters");
				}
			}

			size_t byte_len = hex.length() / 2;
			std::vector<uint8_t> bytes(byte_len);

			for (size_t j = 0; j < byte_len; j++) {
				std::string byte_str = hex.substr(j * 2, 2);
				bytes[j] = static_cast<uint8_t>(std::strtoul(byte_str.c_str(), nullptr, 16));
			}

			std::string hash_hex = KeccakWrapper::HashToHex(bytes.data(), byte_len);
			result_data[i] = StringVector::AddString(result, hash_hex);
		} else {
			// Hash raw string bytes
			input_bytes = reinterpret_cast<const uint8_t *>(input_str.GetData());
			input_len = input_str.GetSize();
			std::string hash_hex = KeccakWrapper::HashToHex(input_bytes, input_len);
			result_data[i] = StringVector::AddString(result, hash_hex);
		}
	}
}

// Overload for BLOB input
static void Keccak256BlobFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];

	UnifiedVectorFormat input_format;
	input_vector.ToUnifiedFormat(args.size(), input_format);

	auto input_data = UnifiedVectorFormat::GetData<string_t>(input_format);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = input_format.sel->get_index(i);

		if (!input_format.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		string_t blob = input_data[idx];
		const uint8_t *bytes = reinterpret_cast<const uint8_t *>(blob.GetData());
		size_t len = blob.GetSize();

		std::string hash_hex = KeccakWrapper::HashToHex(bytes, len);
		result_data[i] = StringVector::AddString(result, hash_hex);
	}
}

void RegisterKeccakFunctions(DatabaseInstance &instance) {
	ScalarFunctionSet keccak_set("keccak256");

	keccak_set.AddFunction(
	    ScalarFunction("keccak256", {LogicalType::VARCHAR}, LogicalType::VARCHAR, Keccak256Function));

	keccak_set.AddFunction(
	    ScalarFunction("keccak256", {LogicalType::BLOB}, LogicalType::VARCHAR, Keccak256BlobFunction));

	ExtensionUtil::RegisterFunction(instance, keccak_set);
}

} // namespace duckdb
