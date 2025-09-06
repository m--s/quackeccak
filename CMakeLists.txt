cmake_minimum_required(VERSION 3.10)
set(TARGET_NAME quackeccak)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_INCLUDE_CURRENT_DIR ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)

if(EMSCRIPTEN)
    get_filename_component(REAL_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}" REALPATH)
    include_directories(${REAL_SOURCE_DIR}/src/include)
    message(STATUS "QuackECCAK: WASM build detected, using portable optimizations")
endif()

# Collect source files
file(GLOB_RECURSE EXTENSION_SOURCES
    src/*.cpp
    src/*.c
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(WIN32)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE
        _SILENCE_STDEXT_ARR_ITERS_DEPRECATION_WARNING
        _SILENCE_ALL_MS_EXT_DEPRECATION_WARNINGS
    )
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE
        _SILENCE_STDEXT_ARR_ITERS_DEPRECATION_WARNING
        _SILENCE_ALL_MS_EXT_DEPRECATION_WARNINGS
    )
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(PERF_FLAGS "-O3")
    
    if(NOT EMSCRIPTEN AND NOT CMAKE_CROSSCOMPILING)
        include(CheckCXXCompilerFlag)
        
        check_cxx_compiler_flag("-funroll-loops" COMPILER_SUPPORTS_UNROLL)
        if(COMPILER_SUPPORTS_UNROLL)
            set(PERF_FLAGS "${PERF_FLAGS} -funroll-loops")
        endif()
        
        check_cxx_compiler_flag("-finline-functions" COMPILER_SUPPORTS_INLINE)
        if(COMPILER_SUPPORTS_INLINE)
            set(PERF_FLAGS "${PERF_FLAGS} -finline-functions")
        endif()
    endif()
    
    set_target_properties(${EXTENSION_NAME} PROPERTIES
        COMPILE_FLAGS "${PERF_FLAGS}"
    )
    set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES
        COMPILE_FLAGS "${PERF_FLAGS}"
    )
    
    message(STATUS "QuackECCAK: Release build with optimizations: ${PERF_FLAGS}")
else()
    message(STATUS "QuackECCAK: Debug build - no optimizations")
endif()

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)