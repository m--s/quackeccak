#include "cross_casts.hpp"
#include "fixed_bytes_utils.hpp"

namespace duckdb {

// Constants for type sizes
static constexpr idx_t ADDRESS_SIZE = 20;
static constexpr idx_t BYTES32_SIZE = 32;

// Helper to get type aliases
static LogicalType AddressType() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("ADDRESS");
	return t;
}

static LogicalType Bytes32Type() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("BYTES32");
	return t;
}

void RegisterCrossTypeCasts(DatabaseInstance &db) {
	// ADDRESS -> BYTES32 (pad left with zeros)
	ExtensionUtil::RegisterCastFunction(db, AddressType(), Bytes32Type(),
	                                    BoundCastInfo(CastBetweenFixedBytes<ADDRESS_SIZE, BYTES32_SIZE>), 50);

	// BYTES32 -> ADDRESS (truncate or fail if non-zero bytes would be lost)
	ExtensionUtil::RegisterCastFunction(db, Bytes32Type(), AddressType(),
	                                    BoundCastInfo(CastBetweenFixedBytes<BYTES32_SIZE, ADDRESS_SIZE>), 50);
}

} // namespace duckdb
