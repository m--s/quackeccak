#pragma once

#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

static inline int HexVal(unsigned char c) {
	if (c >= '0' && c <= '9') {
		return c - '0';
	}
	c |= 0x20; // to lower
	if (c >= 'a' && c <= 'f') {
		return 10 + (c - 'a');
	}
	return -1;
}

static constexpr char HEX_LOWER[] = "0123456789abcdef";

template <idx_t SIZE>
static bool CastVarcharToFixedBytes(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    source, result, count, [&](const string_t &input, ValidityMask &mask, idx_t idx) {
		    const char *p = input.GetData();
		    idx_t len = input.GetSize();

		    // Skip 0x prefix if present
		    if (len >= 2 && p[0] == '0' && (p[1] == 'x' || p[1] == 'X')) {
			    p += 2;
			    len -= 2;
		    }

		    // Can't be more than SIZE*2 hex chars
		    if (len > SIZE * 2) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }

		    uint8_t out[SIZE] {}; // Zero-initialized for left-padding

		    // Parse from right to left to handle left-padding correctly
		    idx_t w = SIZE;
		    idx_t i = len;

		    while (i > 0) {
			    int lo = HexVal(static_cast<unsigned char>(p[--i]));
			    if (lo < 0) {
				    mask.SetInvalid(idx);
				    return string_t();
			    }

			    int hi = 0;
			    if (i > 0) {
				    int v = HexVal(static_cast<unsigned char>(p[--i]));
				    if (v < 0) {
					    mask.SetInvalid(idx);
					    return string_t();
				    }
				    hi = v;
			    }

			    if (w == 0) {
				    mask.SetInvalid(idx);
				    return string_t();
			    }

			    out[--w] = static_cast<uint8_t>((hi << 4) | lo);
		    }

		    return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), SIZE);
	    });
	return true;
}

template <idx_t SIZE>
static bool CastFixedBytesToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](const string_t &blob) {
		if (blob.GetSize() != SIZE) {
			throw InvalidInputException("Invalid bytes size");
		}

		const auto *data = const_data_ptr_cast(blob.GetData());
		char out[2 + SIZE * 2]; // "0x" + hex chars
		out[0] = '0';
		out[1] = 'x';

		for (idx_t j = 0; j < SIZE; j++) {
			uint8_t b = data[j];
			out[2 + j * 2] = HEX_LOWER[b >> 4];
			out[2 + j * 2 + 1] = HEX_LOWER[b & 0x0F];
		}

		return StringVector::AddString(result, out, 2 + SIZE * 2);
	});
	return true;
}

template <idx_t FROM_SIZE, idx_t TO_SIZE>
static bool CastBetweenFixedBytes(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    source, result, count, [&](const string_t &input, ValidityMask &mask, idx_t idx) {
		    if (input.GetSize() != FROM_SIZE) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }

		    const auto *data = const_data_ptr_cast(input.GetData());
		    uint8_t out[TO_SIZE] {};

		    if (FROM_SIZE < TO_SIZE) {
			    // Pad left with zeros
			    idx_t offset = TO_SIZE - FROM_SIZE;
			    memcpy(out + offset, data, FROM_SIZE);
		    } else {
			    // Truncate from left (keep rightmost bytes)
			    // Check if we're losing non-zero bytes
			    for (idx_t i = 0; i < FROM_SIZE - TO_SIZE; i++) {
				    if (data[i] != 0) {
					    mask.SetInvalid(idx);
					    return string_t();
				    }
			    }
			    memcpy(out, data + (FROM_SIZE - TO_SIZE), TO_SIZE);
		    }

		    return StringVector::AddStringOrBlob(result, const_char_ptr_cast(out), TO_SIZE);
	    });
	return true;
}

} // namespace duckdb
