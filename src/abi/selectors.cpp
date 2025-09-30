#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "keccak.hpp"
#include "selectors.hpp"
#include "yyjson.hpp"

namespace duckdb {

static LogicalType Bytes4Type() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("BYTES4");
	return t;
}

static LogicalType Bytes32Type() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("BYTES32");
	return t;
}

static size_t BuildSignatureFromJson(const char *json_str, size_t json_len, char *buffer) {
	using duckdb_yyjson::yyjson_doc;
	using duckdb_yyjson::yyjson_doc_free;
	using duckdb_yyjson::yyjson_doc_get_root;
	using duckdb_yyjson::yyjson_get_str;
	using duckdb_yyjson::yyjson_is_arr;
	using duckdb_yyjson::yyjson_is_str;
	using duckdb_yyjson::yyjson_obj_get;
	using duckdb_yyjson::yyjson_read;
	using duckdb_yyjson::yyjson_val;
	size_t offset = 0;

	yyjson_doc *doc = yyjson_read(json_str, json_len, 0);
	if (!doc) {
		throw InvalidInputException("Invalid ABI JSON");
	}

	yyjson_val *root = yyjson_doc_get_root(doc);

	yyjson_val *name = yyjson_obj_get(root, "name");
	if (!name || !yyjson_is_str(name)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid ABI JSON: missing 'name' field");
	}

	const char *name_str = yyjson_get_str(name);
	size_t name_len = strlen(name_str);
	memcpy(buffer + offset, name_str, name_len);
	offset += name_len;
	buffer[offset++] = '(';

	yyjson_val *inputs = yyjson_obj_get(root, "inputs");
	if (inputs && yyjson_is_arr(inputs)) {
		size_t idx, max;
		yyjson_val *input;
		bool first = true;

		yyjson_arr_foreach(inputs, idx, max, input) {
			yyjson_val *type = yyjson_obj_get(input, "type");
			if (type && yyjson_is_str(type)) {
				if (!first) {
					buffer[offset++] = ',';
				}
				first = false;

				const char *type_str = yyjson_get_str(type);
				size_t type_len = strlen(type_str);
				memcpy(buffer + offset, type_str, type_len);
				offset += type_len;
			}
		}
	}

	buffer[offset++] = ')';
	yyjson_doc_free(doc);
	return offset;
}

template <size_t RESULT_SIZE>
static void ProcessAbiJson(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](const string_t &abi_json) {
		char signature_buffer[1024] = {0};
		size_t sig_len = BuildSignatureFromJson(abi_json.GetData(), abi_json.GetSize(), signature_buffer);

		alignas(64) uint8_t hash[32];
		Keccak::Hash256(reinterpret_cast<const uint8_t *>(signature_buffer), sig_len, hash);

		return StringVector::AddStringOrBlob(result, const_char_ptr_cast(hash), RESULT_SIZE);
	});
}

template <size_t RESULT_SIZE>
static void ProcessSignatureString(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](const string_t &signature) {
		alignas(64) uint8_t hash[32];
		Keccak::Hash256(reinterpret_cast<const uint8_t *>(signature.GetData()), signature.GetSize(), hash);
		return StringVector::AddStringOrBlob(result, const_char_ptr_cast(hash), RESULT_SIZE);
	});
}

static void EventSignatureFromJson(DataChunk &args, ExpressionState &state, Vector &result) {
	ProcessAbiJson<32>(args, state, result);
}

static void EventSignatureFromString(DataChunk &args, ExpressionState &state, Vector &result) {
	ProcessSignatureString<32>(args, state, result);
}

static void FunctionSelectorFromJson(DataChunk &args, ExpressionState &state, Vector &result) {
	ProcessAbiJson<4>(args, state, result);
}

static void FunctionSelectorFromString(DataChunk &args, ExpressionState &state, Vector &result) {
	ProcessSignatureString<4>(args, state, result);
}

static void ErrorSelectorFromJson(DataChunk &args, ExpressionState &state, Vector &result) {
	ProcessAbiJson<4>(args, state, result);
}

static void ErrorSelectorFromString(DataChunk &args, ExpressionState &state, Vector &result) {
	ProcessSignatureString<4>(args, state, result);
}

void RegisterABISelectorFunctions(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db,
	                                ScalarFunction("event_signature_json", vector<LogicalType> {LogicalType::JSON()},
	                                               Bytes32Type(), EventSignatureFromJson));

	ExtensionUtil::RegisterFunction(db,
	                                ScalarFunction("function_selector_json", vector<LogicalType> {LogicalType::JSON()},
	                                               Bytes4Type(), FunctionSelectorFromJson));

	ExtensionUtil::RegisterFunction(db, ScalarFunction("error_selector_json", vector<LogicalType> {LogicalType::JSON()},
	                                                   Bytes4Type(), ErrorSelectorFromJson));

	ExtensionUtil::RegisterFunction(db, ScalarFunction("event_signature", vector<LogicalType> {LogicalType::VARCHAR},
	                                                   Bytes32Type(), EventSignatureFromString));

	ExtensionUtil::RegisterFunction(db, ScalarFunction("function_selector", vector<LogicalType> {LogicalType::VARCHAR},
	                                                   Bytes4Type(), FunctionSelectorFromString));

	ExtensionUtil::RegisterFunction(db, ScalarFunction("error_selector", vector<LogicalType> {LogicalType::VARCHAR},
	                                                   Bytes4Type(), ErrorSelectorFromString));
}

} // namespace duckdb