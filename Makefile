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
