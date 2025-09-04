PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=quackeccak
EXT_CONFIG=${PROJ_DIR}/extension_config.cmake
GEN=ninja

include extension-ci-tools/makefiles/duckdb_extension.Makefile

BUILD_DIR ?= build
DUCKDB_EXEC := $(BUILD_DIR)/release/duckdb
EXTENSION_PATH := $(BUILD_DIR)/release/extension/$(EXT_NAME)/$(EXT_NAME).duckdb_extension
CMAKE_GENERATOR ?= $(shell which ninja >/dev/null 2>&1 && echo "Ninja" || echo "Unix Makefiles")

.PHONY: wasm-extension wasm-clean

# Build just the extension as WASM (not all of DuckDB) - fast standalone build
wasm-extension:
	@if [ -z "$$(which emcc)" ]; then \
		echo "Error: Emscripten not found. Please source emsdk_env.sh"; \
		exit 1; \
	fi
	@echo "Building standalone WASM extension..."
	@mkdir -p $(BUILD_DIR)/wasm_ext
	@echo "Compiling source files..."
	@for file in src/quackeccak_extension.cpp \
		src/functions/keccak_functions.cpp \
		src/functions/create2_functions.cpp \
		src/functions/create2_mine.cpp \
		src/functions/format_functions.cpp \
		src/functions/hex_utils.cpp \
		src/functions/parse_utils.cpp; do \
		emcc -O3 -fPIC \
			-I./src/include \
			-I./duckdb/src/include \
			-c -o $(BUILD_DIR)/wasm_ext/$$(basename $$file .cpp).o $$file; \
	done
	@emcc -O3 -fPIC -c -o $(BUILD_DIR)/wasm_ext/keccak.o src/vendor/keccak.c
	@echo "Linking WASM module..."
	@emcc -O3 -fPIC \
		-s SIDE_MODULE=2 \
		-s EXPORT_ALL=1 \
		-o $(BUILD_DIR)/wasm_ext/$(EXT_NAME).duckdb_extension.wasm \
		$(BUILD_DIR)/wasm_ext/*.o
	@echo "WASM extension: $(BUILD_DIR)/wasm_ext/$(EXT_NAME).duckdb_extension.wasm"
	@ls -lh $(BUILD_DIR)/wasm_ext/$(EXT_NAME).duckdb_extension.wasm

wasm-clean:
	rm -rf $(BUILD_DIR)/wasm_ext