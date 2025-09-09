#include "evm_types.hpp"
#include "address.hpp"
#include "bytes32.hpp"
#include "cross_casts.hpp"

namespace duckdb {

void RegisterEvmTypes(DatabaseInstance &db) {
	RegisterAddressType(db);
	RegisterBytes32Type(db);
	RegisterCrossTypeCasts(db);
}

} // namespace duckdb
