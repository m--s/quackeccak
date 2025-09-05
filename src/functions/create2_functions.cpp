#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "keccak_wrapper.hpp"
#include "hex_utils.hpp"
#include <cstring>

namespace duckdb {

static void Create2PredictFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &deployer_vector = args.data[0];
	auto &salt_vector = args.data[1];
	auto &init_hash_vector = args.data[2];

	auto deployer_data = FlatVector::GetData<string_t>(deployer_vector);
	auto salt_data = FlatVector::GetData<string_t>(salt_vector);
	auto init_hash_data = FlatVector::GetData<string_t>(init_hash_vector);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		// Parse deployer address (20 bytes)
		uint8_t deployer[20];
		HexUtils::HexStringToBytes(deployer_data[i].GetString(), deployer, 20, "deployer");

		// Parse salt (32 bytes)
		uint8_t salt[32];
		std::string salt_str = salt_data[i].GetString();

		// Handle both hex string and numeric salt
		if (salt_str.find("0x") == 0 || salt_str.find("0X") == 0) {
			HexUtils::HexStringToBytes(salt_str, salt, 32, "salt");
		} else {
			// Treat as numeric value, convert to big-endian bytes
			uint64_t salt_num = std::stoull(salt_str);
			memset(salt, 0, 32);
			for (int j = 0; j < 8; j++) {
				salt[24 + j] = (salt_num >> (56 - j * 8)) & 0xFF;
			}
		}

		// Parse init code hash (32 bytes)
		uint8_t init_hash[32];
		HexUtils::HexStringToBytes(init_hash_data[i].GetString(), init_hash, 32, "init_hash");

		// Build CREATE2 input: 0xff ++ deployer ++ salt ++ init_hash
		uint8_t buffer[85];
		buffer[0] = 0xff;
		memcpy(buffer + 1, deployer, 20);
		memcpy(buffer + 21, salt, 32);
		memcpy(buffer + 53, init_hash, 32);

		// Hash and extract address (last 20 bytes)
		uint8_t hash[32];
		KeccakWrapper::Hash256(buffer, 85, hash);

		// Format address as hex string
		std::string address_hex = KeccakWrapper::BytesToHex(hash + 12, 20);
		result_data[i] = StringVector::AddString(result, address_hex);
	}
}

void RegisterCreate2Functions(DatabaseInstance &instance) {
	// Register create2_predict function
	auto create2_predict_func =
	    ScalarFunction("create2_predict", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, Create2PredictFunction);
	ExtensionUtil::RegisterFunction(instance, create2_predict_func);
}

} // namespace duckdb
