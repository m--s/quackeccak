#include "bytes4.hpp"
#include "fixed_bytes_utils.hpp"

namespace duckdb {

static constexpr idx_t BYTE4_SIZE = 4;

static void ToBytes4Function(DataChunk &args, ExpressionState &state, Vector &result) {
	CastParameters params;
	CastVarcharToFixedBytes<BYTE4_SIZE>(args.data[0], result, args.size(), params);
}

static LogicalType Bytes4Type() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("BYTES4");
	return t;
}

void RegisterBytes4Type(DatabaseInstance &db) {
	ExtensionUtil::RegisterType(db, "BYTES4", Bytes4Type());

	// VARCHAR <-> BYTES32
	ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, Bytes4Type(),
	                                    BoundCastInfo(CastVarcharToFixedBytes<BYTE4_SIZE>), 1);
	ExtensionUtil::RegisterCastFunction(db, Bytes4Type(), LogicalType::VARCHAR,
	                                    BoundCastInfo(CastFixedBytesToVarchar<BYTE4_SIZE>), 0);

	// Explicit conversion function
	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction("to_bytes4", {LogicalType::VARCHAR}, Bytes4Type(), ToBytes4Function));

	// BYTES4 <-> BLOB
	ExtensionUtil::RegisterCastFunction(
	    db, Bytes4Type(), LogicalType::BLOB,
	    BoundCastInfo([](Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		    result.Reference(source);
		    return true;
	    }),
	    10);

	ExtensionUtil::RegisterCastFunction(
	    db, LogicalType::BLOB, Bytes4Type(),
	    BoundCastInfo([](Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		    UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
		        source, result, count, [&](const string_t &input, ValidityMask &mask, idx_t idx) {
			        if (input.GetSize() != BYTE4_SIZE) {
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
