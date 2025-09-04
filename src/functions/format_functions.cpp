#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "hex_utils.hpp"

namespace duckdb {

static void FormatAddressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &hi8_vector = args.data[0];
	auto &mid8_vector = args.data[1];
	auto &lo4_vector = args.data[2];

	auto hi8_data = FlatVector::GetData<uint64_t>(hi8_vector);
	auto mid8_data = FlatVector::GetData<uint64_t>(mid8_vector);
	auto lo4_data = FlatVector::GetData<uint32_t>(lo4_vector);

	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		std::string formatted = HexUtils::AddressToHex(hi8_data[i], mid8_data[i], lo4_data[i]);
		result_data[i] = StringVector::AddString(result, formatted);
	}
}

static void FormatSaltFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &salt_hi_vector = args.data[0];
	auto &salt_lo_vector = args.data[1];

	auto salt_hi_data = FlatVector::GetData<uint64_t>(salt_hi_vector);
	auto salt_lo_data = FlatVector::GetData<uint64_t>(salt_lo_vector);

	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		std::string formatted = HexUtils::SaltToHex(salt_hi_data[i], salt_lo_data[i]);
		result_data[i] = StringVector::AddString(result, formatted);
	}
}

void RegisterFormatFunctions(DatabaseInstance &instance) {
	// Register format_address scalar function
	auto format_address_func =
	    ScalarFunction("format_address", {LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UINTEGER},
	                   LogicalType::VARCHAR, FormatAddressFunction);
	ExtensionUtil::RegisterFunction(instance, format_address_func);

	// Register format_salt scalar function
	auto format_salt_func = ScalarFunction("format_salt", {LogicalType::UBIGINT, LogicalType::UBIGINT},
	                                       LogicalType::VARCHAR, FormatSaltFunction);
	ExtensionUtil::RegisterFunction(instance, format_salt_func);
}

} // namespace duckdb