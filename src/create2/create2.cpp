#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "keccak_wrapper.hpp"
#include <cstring>
#include <thread>
#include <atomic>
#include <algorithm>

namespace duckdb {

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

static inline void ComputeCreate2Address(const uint8_t deployer[20], const uint8_t salt[32],
                                         const uint8_t init_hash[32], uint8_t output[20]) {
	alignas(64) uint8_t buffer[85];
	buffer[0] = 0xff;
	memcpy(buffer + 1, deployer, 20);
	memcpy(buffer + 21, salt, 32);
	memcpy(buffer + 53, init_hash, 32);

	alignas(64) uint8_t hash[32];
	KeccakWrapper::Hash256(buffer, 85, hash);
	memcpy(output, hash + 12, 20);
}

static inline void SaltToBytes32(uint64_t salt, uint8_t output[32]) {
	memset(output, 0, 32);
	for (int i = 0; i < 8; i++) {
		output[24 + i] = (salt >> (56 - i * 8)) & 0xFF;
	}
}

// create2_predict

static void Create2PredictFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat deployer_fmt, salt_fmt, init_hash_fmt;
	args.data[0].ToUnifiedFormat(args.size(), deployer_fmt);
	args.data[1].ToUnifiedFormat(args.size(), salt_fmt);
	args.data[2].ToUnifiedFormat(args.size(), init_hash_fmt);

	auto deployer_data = UnifiedVectorFormat::GetData<string_t>(deployer_fmt);
	auto salt_data = UnifiedVectorFormat::GetData<string_t>(salt_fmt);
	auto init_hash_data = UnifiedVectorFormat::GetData<string_t>(init_hash_fmt);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto deployer_idx = deployer_fmt.sel->get_index(i);
		auto salt_idx = salt_fmt.sel->get_index(i);
		auto init_hash_idx = init_hash_fmt.sel->get_index(i);

		if (!deployer_fmt.validity.RowIsValid(deployer_idx) || !salt_fmt.validity.RowIsValid(salt_idx) ||
		    !init_hash_fmt.validity.RowIsValid(init_hash_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		uint8_t address[20];
		ComputeCreate2Address(reinterpret_cast<const uint8_t *>(deployer_data[deployer_idx].GetData()),
		                      reinterpret_cast<const uint8_t *>(salt_data[salt_idx].GetData()),
		                      reinterpret_cast<const uint8_t *>(init_hash_data[init_hash_idx].GetData()), address);

		result_data[i] = StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(address), 20);
	}
}

static void Create2PredictWithNumericSalt(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat deployer_fmt, salt_fmt, init_hash_fmt;
	args.data[0].ToUnifiedFormat(args.size(), deployer_fmt);
	args.data[1].ToUnifiedFormat(args.size(), salt_fmt);
	args.data[2].ToUnifiedFormat(args.size(), init_hash_fmt);

	auto deployer_data = UnifiedVectorFormat::GetData<string_t>(deployer_fmt);
	auto salt_data = UnifiedVectorFormat::GetData<int64_t>(salt_fmt);
	auto init_hash_data = UnifiedVectorFormat::GetData<string_t>(init_hash_fmt);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto deployer_idx = deployer_fmt.sel->get_index(i);
		auto salt_idx = salt_fmt.sel->get_index(i);
		auto init_hash_idx = init_hash_fmt.sel->get_index(i);

		if (!deployer_fmt.validity.RowIsValid(deployer_idx) || !salt_fmt.validity.RowIsValid(salt_idx) ||
		    !init_hash_fmt.validity.RowIsValid(init_hash_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		uint8_t salt_bytes[32];
		SaltToBytes32(salt_data[salt_idx], salt_bytes);

		uint8_t address[20];
		ComputeCreate2Address(reinterpret_cast<const uint8_t *>(deployer_data[deployer_idx].GetData()), salt_bytes,
		                      reinterpret_cast<const uint8_t *>(init_hash_data[init_hash_idx].GetData()), address);

		result_data[i] = StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(address), 20);
	}
}

// create2_mine

struct Create2MineData : public TableFunctionData {
	uint8_t deployer[20];
	uint8_t init_hash[32];
	uint64_t salt_start;
	uint64_t salt_count;
	uint64_t current_salt = 0;

	uint8_t mask[20] = {0};
	uint8_t value[20] = {0};
	int priority_bytes[20];

	uint64_t max_results = 100;
	bool has_pattern = false;
	bool finished = false;

	std::atomic<uint64_t> global_salt_counter {0};
	std::atomic<uint64_t> global_results_found {0};
	std::vector<std::pair<uint64_t, std::array<uint8_t, 20>>> result_buffer;
	std::vector<std::vector<std::pair<uint64_t, std::array<uint8_t, 20>>>> thread_results;
	bool workers_launched = false;
};

static void CalculatePriorityBytes(const uint8_t *mask, int priority[20]) {
	struct BytePriority {
		int index;
		int bits;
	} priorities[20];

	for (int i = 0; i < 20; i++) {
		priorities[i].index = i;
		priorities[i].bits = __builtin_popcount(mask[i]);
	}

	std::sort(priorities, priorities + 20,
	          [](const BytePriority &a, const BytePriority &b) { return a.bits > b.bits; });

	for (int i = 0; i < 20; i++) {
		priority[i] = priorities[i].index;
	}
}

static inline bool AddressMatchesPattern(const uint8_t *addr, const uint8_t *mask, const uint8_t *value,
                                         const int *priority) {
	for (int i = 0; i < 20; i++) {
		int idx = priority[i];
		if (mask[idx] && ((addr[idx] & mask[idx]) != value[idx])) {
			return false;
		}
	}
	return true;
}

static void ProcessBatch(const uint8_t deployer[20], const uint8_t init_hash[32], uint64_t salt_start,
                         uint64_t salt_end, Create2MineData *data,
                         std::vector<std::pair<uint64_t, std::array<uint8_t, 20>>> &results) {
	constexpr int BATCH_SIZE = 32;
	alignas(64) uint8_t salt_bytes[BATCH_SIZE][32];
	alignas(64) uint8_t addresses[BATCH_SIZE][20];

	for (uint64_t batch_start = salt_start; batch_start < salt_end; batch_start += BATCH_SIZE) {
		int count = std::min(BATCH_SIZE, (int)(salt_end - batch_start));

		for (int i = 0; i < count; i++) {
			SaltToBytes32(batch_start + i, salt_bytes[i]);
			ComputeCreate2Address(deployer, salt_bytes[i], init_hash, addresses[i]);
		}

		for (int i = 0; i < count; i++) {
			if (!data->has_pattern ||
			    AddressMatchesPattern(addresses[i], data->mask, data->value, data->priority_bytes)) {

				if (data->global_results_found.fetch_add(1) < data->max_results) {
					std::array<uint8_t, 20> addr_array;
					memcpy(addr_array.data(), addresses[i], 20);
					results.emplace_back(batch_start + i, addr_array);
				} else {
					return;
				}
			}
		}

		if (data->global_results_found >= data->max_results) {
			return;
		}
	}
}

static void Worker(Create2MineData *data, int thread_id) {
	constexpr uint64_t CHUNK_SIZE = 4096;
	auto &results = data->thread_results[thread_id];

	while (true) {
		uint64_t start = data->global_salt_counter.fetch_add(CHUNK_SIZE);
		if (start >= data->salt_start + data->salt_count || data->global_results_found >= data->max_results) {
			break;
		}

		uint64_t end = std::min(start + CHUNK_SIZE, data->salt_start + data->salt_count);
		ProcessBatch(data->deployer, data->init_hash, start, end, data, results);
	}
}

static unique_ptr<FunctionData> Create2MineBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<Create2MineData>();

	auto deployer_blob = StringValue::Get(input.inputs[0]);
	memcpy(data->deployer, deployer_blob.data(), 20);

	auto init_hash_blob = StringValue::Get(input.inputs[1]);
	memcpy(data->init_hash, init_hash_blob.data(), 32);

	data->salt_start = input.inputs[2].IsNull() ? 0 : input.inputs[2].GetValue<uint64_t>();
	data->salt_count = input.inputs[3].IsNull() ? 100 : input.inputs[3].GetValue<uint64_t>();

	if (input.inputs.size() == 7) {
		if (!input.inputs[4].IsNull()) {
			auto mask_blob = StringValue::Get(input.inputs[4]);
			memcpy(data->mask, mask_blob.data(), 20);
			data->has_pattern = true;
		}

		if (!input.inputs[5].IsNull()) {
			auto value_blob = StringValue::Get(input.inputs[5]);
			memcpy(data->value, value_blob.data(), 20);
			data->has_pattern = true;
		}

		if (!input.inputs[6].IsNull()) {
			data->max_results = input.inputs[6].GetValue<uint64_t>();
		}
	}

	if (data->has_pattern) {
		CalculatePriorityBytes(data->mask, data->priority_bytes);
	} else {
		for (int i = 0; i < 20; i++) {
			data->priority_bytes[i] = i;
		}
	}

	data->global_salt_counter = data->salt_start;

	return_types = {LogicalType::UBIGINT, AddressType()};
	names = {"salt", "address"};

	return std::move(data);
}

static void Create2MineFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<Create2MineData>();

	if (data.finished) {
		output.SetCardinality(0);
		return;
	}

	if (!data.workers_launched) {
		data.workers_launched = true;

		int num_threads =
		    std::min((int)std::thread::hardware_concurrency(), std::max(1, (int)(data.salt_count / 10000)));

		data.thread_results.resize(num_threads);

		if (num_threads == 1) {
			Worker(&data, 0);
			data.result_buffer = std::move(data.thread_results[0]);
		} else {
			std::vector<std::thread> workers;
			workers.reserve(num_threads);
			for (int i = 0; i < num_threads; i++) {
				workers.emplace_back(Worker, &data, i);
			}
			for (auto &w : workers) {
				w.join();
			}

			for (auto &tr : data.thread_results) {
				data.result_buffer.insert(data.result_buffer.end(), std::make_move_iterator(tr.begin()),
				                          std::make_move_iterator(tr.end()));
			}
		}

		std::sort(data.result_buffer.begin(), data.result_buffer.end());

		if (data.result_buffer.size() > data.max_results) {
			data.result_buffer.resize(data.max_results);
		}
	}

	auto salt_data = FlatVector::GetData<uint64_t>(output.data[0]);
	auto address_data = FlatVector::GetData<string_t>(output.data[1]);

	idx_t result_idx = 0;
	while (result_idx < STANDARD_VECTOR_SIZE && data.current_salt < data.result_buffer.size()) {
		auto &[salt, addr] = data.result_buffer[data.current_salt];

		salt_data[result_idx] = salt;
		address_data[result_idx] =
		    StringVector::AddStringOrBlob(output.data[1], reinterpret_cast<const char *>(addr.data()), 20);

		result_idx++;
		data.current_salt++;
	}

	if (data.current_salt >= data.result_buffer.size()) {
		data.finished = true;
	}

	output.SetCardinality(result_idx);
}

void RegisterCreate2Functions(DatabaseInstance &instance) {
	ExtensionUtil::RegisterFunction(instance,
	                                ScalarFunction("create2_predict", {AddressType(), Bytes32Type(), Bytes32Type()},
	                                               AddressType(), Create2PredictFunction));

	ExtensionUtil::RegisterFunction(instance, ScalarFunction("create2_predict",
	                                                         {AddressType(), LogicalType::BIGINT, Bytes32Type()},
	                                                         AddressType(), Create2PredictWithNumericSalt));

	TableFunctionSet create2_set("create2_mine");

	TableFunction create2_mine_basic({AddressType(), Bytes32Type(), LogicalType::BIGINT, LogicalType::BIGINT},
	                                 Create2MineFunction, Create2MineBind);

	TableFunction create2_mine_extended({AddressType(), Bytes32Type(), LogicalType::BIGINT, LogicalType::BIGINT,
	                                     AddressType(), AddressType(), LogicalType::BIGINT},
	                                    Create2MineFunction, Create2MineBind);

	create2_set.AddFunction(create2_mine_basic);
	create2_set.AddFunction(create2_mine_extended);

	ExtensionUtil::RegisterFunction(instance, create2_set);
}

} // namespace duckdb
