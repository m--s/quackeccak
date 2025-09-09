#include "bytes32.hpp"
#include "fixed_bytes_utils.hpp"

namespace duckdb {

static constexpr idx_t BYTES32_SIZE = 32;

static void ToBytes32Function(DataChunk &args, ExpressionState &state, Vector &result) {
	CastParameters params;
	CastVarcharToFixedBytes<BYTES32_SIZE>(args.data[0], result, args.size(), params);
}

static LogicalType Bytes32Type() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("BYTES32");
	return t;
}

void RegisterBytes32Type(DatabaseInstance &db) {
	ExtensionUtil::RegisterType(db, "BYTES32", Bytes32Type());

	// VARCHAR <-> BYTES32
	ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, Bytes32Type(),
	                                    BoundCastInfo(CastVarcharToFixedBytes<BYTES32_SIZE>), 1);
	ExtensionUtil::RegisterCastFunction(db, Bytes32Type(), LogicalType::VARCHAR,
	                                    BoundCastInfo(CastFixedBytesToVarchar<BYTES32_SIZE>), 0);

	// Explicit conversion function
	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction("to_bytes32", {LogicalType::VARCHAR}, Bytes32Type(), ToBytes32Function));

	// BYTES32 <-> BLOB
	ExtensionUtil::RegisterCastFunction(
	    db, Bytes32Type(), LogicalType::BLOB,
	    BoundCastInfo([](Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		    result.Reference(source);
		    return true;
	    }),
	    10);

	ExtensionUtil::RegisterCastFunction(
	    db, LogicalType::BLOB, Bytes32Type(),
	    BoundCastInfo([](Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		    UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
		        source, result, count, [&](const string_t &input, ValidityMask &mask, idx_t idx) {
			        if (input.GetSize() != BYTES32_SIZE) {
				        mask.SetInvalid(idx);
				        return string_t();
			        }
			        return input;
		        });
		    return true;
	    }),
	    10);
}

} // namespace duckdb
