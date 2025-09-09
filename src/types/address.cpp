#include "address.hpp"
#include "fixed_bytes_utils.hpp"

namespace duckdb {

static constexpr idx_t ADDRESS_SIZE = 20;

static void ToAddressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	CastParameters params;
	CastVarcharToFixedBytes<ADDRESS_SIZE>(args.data[0], result, args.size(), params);
}

static LogicalType AddressType() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("ADDRESS");
	return t;
}

void RegisterAddressType(DatabaseInstance &db) {
	ExtensionUtil::RegisterType(db, "ADDRESS", AddressType());

	// VARCHAR <-> ADDRESS
	ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, AddressType(),
	                                    BoundCastInfo(CastVarcharToFixedBytes<ADDRESS_SIZE>), 1);
	ExtensionUtil::RegisterCastFunction(db, AddressType(), LogicalType::VARCHAR,
	                                    BoundCastInfo(CastFixedBytesToVarchar<ADDRESS_SIZE>), 0);

	// Explicit conversion function
	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction("to_address", {LogicalType::VARCHAR}, AddressType(), ToAddressFunction));

	// ADDRESS <-> BLOB
	ExtensionUtil::RegisterCastFunction(
	    db, AddressType(), LogicalType::BLOB,
	    BoundCastInfo([](Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		    // ADDRESS -> BLOB is just a copy (they're both blobs internally)
		    result.Reference(source);
		    return true;
	    }),
	    10);

	ExtensionUtil::RegisterCastFunction(
	    db, LogicalType::BLOB, AddressType(),
	    BoundCastInfo([](Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		    // BLOB -> ADDRESS requires size validation
		    UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
		        source, result, count, [&](const string_t &input, ValidityMask &mask, idx_t idx) {
			        if (input.GetSize() != ADDRESS_SIZE) {
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