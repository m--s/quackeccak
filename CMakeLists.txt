cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME quackeccak)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
# OpenSSL was needed for WASM builds - keeping commented for now
# if(NOT EMSCRIPTEN)
#     find_package(OpenSSL REQUIRED)
# endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_INCLUDE_CURRENT_DIR ON)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)

set(EXTENSION_SOURCES 
    src/quackeccak_extension.cpp
    src/functions/keccak_functions.cpp
    src/functions/create2_functions.cpp
    src/functions/create2_mine.cpp
    src/functions/format_functions.cpp
    src/functions/hex_utils.cpp
    src/functions/parse_utils.cpp
    src/vendor/keccak.c
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Safe performance optimizations for CREATE2 mining
if(CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    # Core optimization flags (proven safe with DuckDB)
    set(PERF_FLAGS "-O3")                              # Maximum optimization level
    
    include(CheckCXXCompilerFlag)
    
    # Test and add safe flags individually
    check_cxx_compiler_flag("-march=native" COMPILER_SUPPORTS_NATIVE)
    if(COMPILER_SUPPORTS_NATIVE)
        set(PERF_FLAGS "${PERF_FLAGS} -march=native")  # Use CPU-specific instructions
    endif()
    
    check_cxx_compiler_flag("-funroll-loops" COMPILER_SUPPORTS_UNROLL)
    if(COMPILER_SUPPORTS_UNROLL)
        set(PERF_FLAGS "${PERF_FLAGS} -funroll-loops") # Unroll small loops
    endif()
    
    check_cxx_compiler_flag("-finline-functions" COMPILER_SUPPORTS_INLINE)
    if(COMPILER_SUPPORTS_INLINE)
        set(PERF_FLAGS "${PERF_FLAGS} -finline-functions") # Inline functions
    endif()
    
    # Apply flags to extension targets only (not all DuckDB)
    set_target_properties(${EXTENSION_NAME} PROPERTIES COMPILE_FLAGS "${PERF_FLAGS}")
    set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES COMPILE_FLAGS "${PERF_FLAGS}")
else()
    message(STATUS "QuackECCAK: Debug build - no optimizations")
endif()

if(EMSCRIPTEN)
    include_directories(${CMAKE_CURRENT_LIST_DIR}/src/include)
    get_filename_component(REAL_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}" REALPATH)
    include_directories(${REAL_SOURCE_DIR}/src/include)
endif()

# OpenSSL linking
# if(NOT EMSCRIPTEN)
#   target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
#   target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
# endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
