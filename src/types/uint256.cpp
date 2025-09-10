#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "fixed_bytes_utils.hpp"
#include <intx.hpp>
#include <cstring>

namespace duckdb {

static constexpr idx_t UINT256_SIZE = 32;

static LogicalType Uint256Type() {
	LogicalType t(LogicalTypeId::BLOB);
	t.SetAlias("UINT256");
	return t;
}

static bool CastNumberToUint256(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnaryExecutor::Execute<int64_t, string_t>(source, result, count, [&](int64_t input) {
		if (input < 0) {
			throw InvalidInputException("Cannot cast negative number to uint256");
		}
		intx::uint256 val = input;
		uint8_t bytes[UINT256_SIZE];
		intx::be::store(bytes, val);
		return StringVector::AddStringOrBlob(result, const_char_ptr_cast(bytes), UINT256_SIZE);
	});
	return true;
}

static void AddOp(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](const string_t &left, const string_t &right) {
		    intx::uint256 a = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(left.GetData()));
		    intx::uint256 b = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(right.GetData()));
		    intx::uint256 sum = a + b;
		    uint8_t out[UINT256_SIZE];
		    intx::be::store(out, sum);
		    return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), UINT256_SIZE);
	    });
}

static void SubOp(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](const string_t &left, const string_t &right) {
		    intx::uint256 a = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(left.GetData()));
		    intx::uint256 b = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(right.GetData()));
		    intx::uint256 diff = a - b;
		    uint8_t out[UINT256_SIZE];
		    intx::be::store(out, diff);
		    return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), UINT256_SIZE);
	    });
}

static void MulOp(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](const string_t &left, const string_t &right) {
		    intx::uint256 a = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(left.GetData()));
		    intx::uint256 b = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(right.GetData()));
		    intx::uint256 prod = a * b;
		    uint8_t out[UINT256_SIZE];
		    intx::be::store(out, prod);
		    return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), UINT256_SIZE);
	    });
}

static void DivOp(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](const string_t &left, const string_t &right) {
		    intx::uint256 a = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(left.GetData()));
		    intx::uint256 b = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(right.GetData()));
		    if (b == 0) {
			    throw InvalidInputException("Division by zero");
		    }
		    intx::uint256 quot = a / b;
		    uint8_t out[UINT256_SIZE];
		    intx::be::store(out, quot);
		    return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), UINT256_SIZE);
	    });
}

void RegisterUint256Type(DatabaseInstance &db) {
	auto type = Uint256Type();
	ExtensionUtil::RegisterType(db, "UINT256", type);

	ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, type,
	                                    BoundCastInfo(CastVarcharToFixedBytes<UINT256_SIZE>), 1);
	ExtensionUtil::RegisterCastFunction(db, type, LogicalType::VARCHAR,
	                                    BoundCastInfo(CastFixedBytesToVarchar<UINT256_SIZE>), 0);

	ExtensionUtil::RegisterCastFunction(db, LogicalType::BIGINT, type, BoundCastInfo(CastNumberToUint256), 1);
	ExtensionUtil::RegisterCastFunction(db, LogicalType::INTEGER, type, BoundCastInfo(CastNumberToUint256), 1);
	ExtensionUtil::RegisterCastFunction(
	    db, LogicalType::UBIGINT, type,
	    BoundCastInfo([](Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		    UnaryExecutor::Execute<uint64_t, string_t>(source, result, count, [&](uint64_t input) {
			    intx::uint256 val = input;
			    uint8_t bytes[UINT256_SIZE];
			    intx::be::store(bytes, val);
			    return StringVector::AddStringOrBlob(result, const_char_ptr_cast(bytes), UINT256_SIZE);
		    });
		    return true;
	    }),
	    1);

	ExtensionUtil::RegisterCastFunction(
	    db, type, LogicalType::BLOB,
	    BoundCastInfo([](Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		    result.Reference(source);
		    return true;
	    }),
	    10);

	ExtensionUtil::RegisterCastFunction(
	    db, LogicalType::BLOB, type,
	    BoundCastInfo([](Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		    UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
		        source, result, count, [&](const string_t &input, ValidityMask &mask, idx_t idx) {
			        if (input.GetSize() != UINT256_SIZE) {
				        mask.SetInvalid(idx);
				        return string_t();
			        }
			        return input;
		        });
		    return true;
	    }),
	    10);

	ExtensionUtil::RegisterFunction(db, ScalarFunction("+", {type, type}, type, AddOp));
	ExtensionUtil::RegisterFunction(db, ScalarFunction("-", {type, type}, type, SubOp));
	ExtensionUtil::RegisterFunction(db, ScalarFunction("*", {type, type}, type, MulOp));
	ExtensionUtil::RegisterFunction(db, ScalarFunction("/", {type, type}, type, DivOp));

	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction(
	            "<", {type, type}, LogicalType::BOOLEAN, [](DataChunk &args, ExpressionState &state, Vector &result) {
		            BinaryExecutor::Execute<string_t, string_t, bool>(
		                args.data[0], args.data[1], result, args.size(), [](const string_t &l, const string_t &r) {
			                intx::uint256 a =
			                    intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(l.GetData()));
			                intx::uint256 b =
			                    intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(r.GetData()));
			                return a < b;
		                });
	            }));

	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction(
	            "=", {type, type}, LogicalType::BOOLEAN, [](DataChunk &args, ExpressionState &state, Vector &result) {
		            BinaryExecutor::Execute<string_t, string_t, bool>(
		                args.data[0], args.data[1], result, args.size(), [](const string_t &l, const string_t &r) {
			                intx::uint256 a =
			                    intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(l.GetData()));
			                intx::uint256 b =
			                    intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(r.GetData()));
			                return a == b;
		                });
	            }));

	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction("&", {type, type}, type, [](DataChunk &args, ExpressionState &state, Vector &result) {
		    BinaryExecutor::Execute<string_t, string_t, string_t>(
		        args.data[0], args.data[1], result, args.size(), [&](const string_t &left, const string_t &right) {
			        auto a = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(left.GetData()));
			        auto b = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(right.GetData()));
			        auto res = a & b;
			        uint8_t out[32];
			        intx::be::store(out, res);
			        return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), 32);
		        });
	    }));

	// Bitwise OR
	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction("|", {type, type}, type, [](DataChunk &args, ExpressionState &state, Vector &result) {
		    BinaryExecutor::Execute<string_t, string_t, string_t>(
		        args.data[0], args.data[1], result, args.size(), [&](const string_t &left, const string_t &right) {
			        auto a = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(left.GetData()));
			        auto b = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(right.GetData()));
			        auto res = a | b;
			        uint8_t out[32];
			        intx::be::store(out, res);
			        return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), 32);
		        });
	    }));

	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction("xor", {type, type}, type, [](DataChunk &args, ExpressionState &state, Vector &result) {
		    BinaryExecutor::Execute<string_t, string_t, string_t>(
		        args.data[0], args.data[1], result, args.size(), [&](const string_t &left, const string_t &right) {
			        auto a = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(left.GetData()));
			        auto b = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(right.GetData()));
			        auto res = a ^ b; // intx supports ^
			        uint8_t out[32];
			        intx::be::store(out, res);
			        return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), 32);
		        });
	    }));

	// Left shift
	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction(
	            "<<", {type, LogicalType::INTEGER}, type, [](DataChunk &args, ExpressionState &state, Vector &result) {
		            BinaryExecutor::Execute<string_t, int32_t, string_t>(
		                args.data[0], args.data[1], result, args.size(), [&](const string_t &left, int32_t shift) {
			                auto a = intx::be::unsafe::load<intx::uint256>(
			                    reinterpret_cast<const uint8_t *>(left.GetData()));
			                auto res = a << shift;
			                uint8_t out[32];
			                intx::be::store(out, res);
			                return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), 32);
		                });
	            }));

	// Right shift
	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction(
	            ">>", {type, LogicalType::INTEGER}, type, [](DataChunk &args, ExpressionState &state, Vector &result) {
		            BinaryExecutor::Execute<string_t, int32_t, string_t>(
		                args.data[0], args.data[1], result, args.size(), [&](const string_t &left, int32_t shift) {
			                auto a = intx::be::unsafe::load<intx::uint256>(
			                    reinterpret_cast<const uint8_t *>(left.GetData()));
			                auto res = a >> shift;
			                uint8_t out[32];
			                intx::be::store(out, res);
			                return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), 32);
		                });
	            }));

	// Bitwise NOT
	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction("~", {type}, type, [](DataChunk &args, ExpressionState &state, Vector &result) {
		    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](const string_t &input) {
			    auto a = intx::be::unsafe::load<intx::uint256>(reinterpret_cast<const uint8_t *>(input.GetData()));
			    auto res = ~a;
			    uint8_t out[32];
			    intx::be::store(out, res);
			    return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), 32);
		    });
	    }));
}

} // namespace duckdb
