PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=quackeccak
EXT_CONFIG=${PROJ_DIR}/extension_config.cmake

ifneq ($(filter wasm_mvp wasm_eh wasm_threads,$(MAKECMDGOALS)),)
    GEN=make
    CMAKE_GENERATOR=Unix Makefiles
else
    GEN=ninja
    CMAKE_GENERATOR=Ninja
endif

include extension-ci-tools/makefiles/duckdb_extension.Makefile

BUILD_DIR ?= build
DUCKDB_EXEC := $(BUILD_DIR)/release/duckdb
EXTENSION_PATH := $(BUILD_DIR)/release/extension/$(EXT_NAME)/$(EXT_NAME).duckdb_extension
CMAKE_GENERATOR ?= $(shell which ninja >/dev/null 2>&1 && echo "Ninja" || echo "Unix Makefiles")

.PHONY: fix
fix:
	@echo "Fixing formatting..."
	@find src -type f \( -name "*.cpp" -o -name "*.hpp" -o -name "*.h" -o -name "*.c" \) | xargs clang-format -i -style=file
	@echo "Fixing linting issues..."
	@if [ ! -d "build/release" ]; then \
		echo "Building first to generate compile_commands.json..."; \
		$(MAKE) release; \
	fi
	@find src -type f -name "*.cpp" | xargs clang-tidy \
		-p build/release \
		--fix \
		--fix-errors \
		--quiet 2>/dev/null || true
	@echo "All fixes applied!"