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

static LogicalType Uint256Type() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("UINT256");
	return t;
}

void RegisterCrossTypeCasts(DatabaseInstance &db) {
	// ADDRESS -> BYTES32: Pads the 20-byte address with 12 zero bytes on the left
	ExtensionUtil::RegisterCastFunction(db, AddressType(), Bytes32Type(),
	                                    BoundCastInfo(CastBetweenFixedBytes<ADDRESS_SIZE, BYTES32_SIZE>), 50);

	// BYTES32 -> ADDRESS: Takes the rightmost 20 bytes, fails if leftmost 12 bytes are non-zero
	ExtensionUtil::RegisterCastFunction(db, Bytes32Type(), AddressType(),
	                                    BoundCastInfo(CastBetweenFixedBytes<BYTES32_SIZE, ADDRESS_SIZE>), 50);

	// ADDRESS -> UINT256: Zero-pads on the left to create a 256-bit integer
	ExtensionUtil::RegisterCastFunction(db, AddressType(), Uint256Type(), BoundCastInfo(CastBetweenFixedBytes<20, 32>),
	                                    1);

	// UINT256 -> ADDRESS: Truncates to rightmost 160 bits (20 bytes)
	ExtensionUtil::RegisterCastFunction(db, Uint256Type(), AddressType(), BoundCastInfo(CastBetweenFixedBytes<32, 20>),
	                                    1);
}

} // namespace duckdb
