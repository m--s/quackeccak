#define DUCKDB_EXTENSION_MAIN

#include "quackeccak_extension.hpp"
#include "types/evm_types.hpp"
#include "keccak/keccak.hpp"
#include "create2.hpp"
#include "duckdb.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	RegisterEvmTypes(instance);
	RegisterKeccakFunctions(instance);
	RegisterCreate2Functions(instance);
}

void QuackeccakExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string QuackeccakExtension::Name() {
	return "quackeccak";
}

std::string QuackeccakExtension::Version() const {
#ifdef EXT_VERSION_QUACKECCAK
	return EXT_VERSION_QUACKECCAK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void quackeccak_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::QuackeccakExtension>();
}

DUCKDB_EXTENSION_API const char *quackeccak_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
