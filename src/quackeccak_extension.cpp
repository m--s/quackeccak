#define DUCKDB_EXTENSION_MAIN

#include "quackeccak_extension.hpp"
#include "keccak_functions.hpp"
#include "create2_functions.hpp"
#include "create2_mine.hpp"
#include "format_functions.hpp"
#include "duckdb.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	RegisterKeccakFunctions(instance);
	RegisterCreate2Functions(instance);
	RegisterCreate2Mine(instance);
	RegisterFormatFunctions(instance);
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