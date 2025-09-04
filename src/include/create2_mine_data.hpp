#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct Create2MineBindData : public TableFunctionData {
	std::string deployer;
	std::string init_hash;

	uint64_t salt_start = 0;
	uint64_t salt_count = 100;
	uint64_t mask_hi8 = 0;
	uint64_t value_hi8 = 0;
	uint64_t mask_mid8 = 0;
	uint64_t value_mid8 = 0;
	uint32_t mask_lo4 = 0;
	uint32_t value_lo4 = 0;
	uint64_t max_results = 100;

	bool has_pattern = false;
};

struct Create2MineData : public TableFunctionData {
	uint8_t deployer[20];
	uint8_t init_hash[32];
	uint64_t salt_start;
	uint64_t salt_count;
	uint64_t mask_hi8;
	uint64_t value_hi8;
	uint64_t mask_mid8;
	uint64_t value_mid8;
	uint32_t mask_lo4;
	uint32_t value_lo4;
	uint64_t max_results;
	bool has_pattern;

	uint64_t current_salt;
	uint64_t results_found;
	bool finished;
};

} // namespace duckdb