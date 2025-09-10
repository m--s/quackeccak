#include "evm_types.hpp"
#include "address.hpp"
#include "bytes32.hpp"
#include "cross_casts.hpp"
#include "uint265.hpp"

namespace duckdb {

void RegisterEvmTypes(DatabaseInstance &db) {
	RegisterAddressType(db);
	RegisterBytes32Type(db);
	RegisterUint256Type(db);
	RegisterCrossTypeCasts(db);
}

} // namespace duckdb
