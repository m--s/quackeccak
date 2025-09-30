#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "keccak.hpp"
#include <cstring>
#include <thread>
#include <atomic>
#include <algorithm>

#ifdef _MSC_VER
#include <intrin.h>
#define __builtin_popcount __popcnt
#endif

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

static inline void SaltToBytes32(uint64_t salt, uint8_t output[32]) {
	memset(output, 0, 24);
	for (int i = 0; i < 8; i++) {
		output[24 + i] = (salt >> (56 - i * 8)) & 0xFF;
	}
}

static void ValidateAndCopyBlob(const string_t &blob, uint8_t *output, idx_t expected_size, const char *name) {
	if (blob.GetSize() != expected_size) {
		throw InvalidInputException("Invalid %s: expected %lld bytes, got %lld", name, expected_size, blob.GetSize());
	}
	memcpy(output, blob.GetData(), expected_size);
}

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

		if (deployer_data[deployer_idx].GetSize() != 20 || salt_data[salt_idx].GetSize() != 32 ||
		    init_hash_data[init_hash_idx].GetSize() != 32) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		uint8_t address[20];
		Keccak::Create2(reinterpret_cast<const uint8_t *>(deployer_data[deployer_idx].GetData()),
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

		if (deployer_data[deployer_idx].GetSize() != 20 || init_hash_data[init_hash_idx].GetSize() != 32) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		uint8_t salt_bytes[32];
		SaltToBytes32(salt_data[salt_idx], salt_bytes);

		uint8_t address[20];
		Keccak::Create2(reinterpret_cast<const uint8_t *>(deployer_data[deployer_idx].GetData()), salt_bytes,
		                reinterpret_cast<const uint8_t *>(init_hash_data[init_hash_idx].GetData()), address);

		result_data[i] = StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(address), 20);
	}
}

struct Create2MineData : public TableFunctionData {
	uint8_t deployer[20];
	uint8_t init_hash[32];
	uint64_t salt_start;
	uint64_t salt_count;
	uint8_t mask[20] = {0};
	uint8_t target[20] = {0};
	uint64_t max_results = 100;
	bool has_pattern = false;
};

struct Create2MineGlobalState : public GlobalTableFunctionState {
	std::atomic<uint64_t> global_salt_counter {0};
	std::atomic<uint64_t> global_results_found {0};
	std::vector<std::pair<uint64_t, std::array<uint8_t, 20>>> result_buffer;
	std::vector<std::vector<std::pair<uint64_t, std::array<uint8_t, 20>>>> thread_results;
	bool workers_finished = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct Create2MineLocalState : public LocalTableFunctionState {
	uint64_t current_salt = 0;
	bool finished = false;
};

static inline bool AddressMatchesPattern(const uint8_t *addr, const uint8_t *mask, const uint8_t *target) {
	for (int i = 0; i < 20; i++) {
		if ((addr[i] & mask[i]) != target[i]) {
			return false;
		}
	}
	return true;
}

static void ProcessBatch(const Create2MineData *data, Create2MineGlobalState *gstate, uint64_t salt_start,
                         uint64_t salt_end, std::vector<std::pair<uint64_t, std::array<uint8_t, 20>>> &results) {
	uint8_t salt_bytes[32];
	uint8_t address[20];

	Keccak::Create2MiningContext ctx;
	ctx.init(data->deployer, data->init_hash);

	for (uint64_t salt = salt_start; salt < salt_end; salt++) {
		SaltToBytes32(salt, salt_bytes);
		ctx.compute(salt_bytes, address);

		bool should_include = !data->has_pattern || AddressMatchesPattern(address, data->mask, data->target);

		if (should_include) {
			if (gstate->global_results_found.fetch_add(1) < data->max_results) {
				std::array<uint8_t, 20> addr_array;
				memcpy(addr_array.data(), address, 20);
				results.emplace_back(salt, addr_array);
			} else {
				return;
			}
		}
	}
}

static void Worker(const Create2MineData *data, Create2MineGlobalState *gstate, int thread_id) {
	constexpr uint64_t CHUNK_SIZE = 16384;
	auto &results = gstate->thread_results[thread_id];

	while (true) {
		uint64_t start = gstate->global_salt_counter.fetch_add(CHUNK_SIZE);
		if (start >= data->salt_start + data->salt_count || gstate->global_results_found >= data->max_results) {
			break;
		}

		uint64_t end = std::min(start + CHUNK_SIZE, data->salt_start + data->salt_count);
		ProcessBatch(data, gstate, start, end, results);
	}
}

static unique_ptr<FunctionData> Create2MineBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<Create2MineData>();

	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw InvalidInputException("Deployer and init_hash cannot be NULL");
	}

	auto deployer_blob = StringValue::Get(input.inputs[0]);
	ValidateAndCopyBlob(deployer_blob, data->deployer, 20, "deployer address");

	auto init_hash_blob = StringValue::Get(input.inputs[1]);
	ValidateAndCopyBlob(init_hash_blob, data->init_hash, 32, "init_hash");

	data->salt_start = input.inputs[2].IsNull() ? 0 : input.inputs[2].GetValue<uint64_t>();
	data->salt_count = input.inputs[3].IsNull() ? 100 : input.inputs[3].GetValue<uint64_t>();

	if (input.inputs.size() == 7 && !input.inputs[4].IsNull() && !input.inputs[5].IsNull()) {
		auto mask_blob = StringValue::Get(input.inputs[4]);
		auto value_blob = StringValue::Get(input.inputs[5]);

		ValidateAndCopyBlob(mask_blob, data->mask, 20, "mask");
		ValidateAndCopyBlob(value_blob, data->target, 20, "value");

		for (int i = 0; i < 20; i++) {
			data->target[i] &= data->mask[i];
			if (data->mask[i] != 0) {
				data->has_pattern = true;
			}
		}

		if (!input.inputs[6].IsNull()) {
			data->max_results = input.inputs[6].GetValue<uint64_t>();
			if (data->max_results == 0) {
				throw InvalidInputException("max_results must be greater than 0");
			}
		}
	}

	return_types = {AddressType(), LogicalType::UBIGINT, AddressType()};
	names = {"deployer", "salt", "address"};

	return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> Create2MineInit(ClientContext &, TableFunctionInitInput &input) {
	auto gstate = make_uniq<Create2MineGlobalState>();
	auto &data = input.bind_data->Cast<Create2MineData>();

	gstate->global_salt_counter = data.salt_start;

	int num_threads = std::min((int)std::thread::hardware_concurrency(), std::max(1, (int)(data.salt_count / 10000)));
	gstate->thread_results.resize(num_threads);

	if (num_threads == 1) {
		Worker(&data, gstate.get(), 0);
		gstate->result_buffer = std::move(gstate->thread_results[0]);
	} else {
		std::vector<std::thread> workers;
		workers.reserve(num_threads);

		for (int i = 0; i < num_threads; i++) {
			workers.emplace_back(Worker, &data, gstate.get(), i);
		}

		for (auto &w : workers) {
			w.join();
		}

		for (auto &tr : gstate->thread_results) {
			gstate->result_buffer.insert(gstate->result_buffer.end(), std::make_move_iterator(tr.begin()),
			                             std::make_move_iterator(tr.end()));
		}
	}

	std::sort(gstate->result_buffer.begin(), gstate->result_buffer.end());

	if (gstate->result_buffer.size() > data.max_results) {
		gstate->result_buffer.resize(data.max_results);
	}

	gstate->workers_finished = true;

	return std::move(gstate);
}

static unique_ptr<LocalTableFunctionState>
Create2MineLocalInit(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
	return make_uniq<Create2MineLocalState>();
}

static double Create2MineProgress(ClientContext &context, const FunctionData *bind_data_p,
                                  const GlobalTableFunctionState *global_state) {
	auto &data = bind_data_p->Cast<Create2MineData>();
	auto &gstate = global_state->Cast<Create2MineGlobalState>();

	if (data.salt_count == 0) {
		return 100.0;
	}

	if (gstate.workers_finished) {
		return 100.0;
	}

	uint64_t processed = gstate.global_salt_counter.load() - data.salt_start;
	return std::min(100.0, (processed * 100.0) / data.salt_count);
}

static void Create2MineFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<Create2MineData>();
	auto &gstate = data_p.global_state->Cast<Create2MineGlobalState>();
	auto &lstate = data_p.local_state->Cast<Create2MineLocalState>();

	if (lstate.finished) {
		output.SetCardinality(0);
		return;
	}

	auto deployer_data = FlatVector::GetData<string_t>(output.data[0]);
	auto salt_data = FlatVector::GetData<uint64_t>(output.data[1]);
	auto address_data = FlatVector::GetData<string_t>(output.data[2]);

	idx_t result_idx = 0;
	while (result_idx < STANDARD_VECTOR_SIZE && lstate.current_salt < gstate.result_buffer.size()) {
		auto &[salt, addr] = gstate.result_buffer[lstate.current_salt];

		deployer_data[result_idx] =
		    StringVector::AddStringOrBlob(output.data[0], reinterpret_cast<const char *>(data.deployer), 20);
		salt_data[result_idx] = salt;
		address_data[result_idx] =
		    StringVector::AddStringOrBlob(output.data[2], reinterpret_cast<const char *>(addr.data()), 20);
		result_idx++;
		lstate.current_salt++;
	}

	if (lstate.current_salt >= gstate.result_buffer.size()) {
		lstate.finished = true;
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
	                                 Create2MineFunction, Create2MineBind, Create2MineInit);
	create2_mine_basic.init_local = Create2MineLocalInit;
	create2_mine_basic.table_scan_progress = Create2MineProgress;

	TableFunction create2_mine_extended({AddressType(), Bytes32Type(), LogicalType::BIGINT, LogicalType::BIGINT,
	                                     AddressType(), AddressType(), LogicalType::BIGINT},
	                                    Create2MineFunction, Create2MineBind, Create2MineInit);
	create2_mine_extended.init_local = Create2MineLocalInit;
	create2_mine_extended.table_scan_progress = Create2MineProgress;

	create2_set.AddFunction(create2_mine_basic);
	create2_set.AddFunction(create2_mine_extended);

	ExtensionUtil::RegisterFunction(instance, create2_set);
}

} // namespace duckdb