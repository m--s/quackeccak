cmake_minimum_required(VERSION 3.5)

# Set extension name here
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
endif()

# Extension sources
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

# Build the extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Platform-specific fixes
if(WIN32)
    # Silence Windows warnings about deprecated array iterators
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
        
        if(NOT DEFINED ENV{CI} AND NOT DEFINED ENV{GITHUB_ACTIONS})
            check_cxx_compiler_flag("-march=native" COMPILER_SUPPORTS_NATIVE)
            if(COMPILER_SUPPORTS_NATIVE)
                set(PERF_FLAGS "${PERF_FLAGS} -march=native")
            endif()
        endif()
        
        check_cxx_compiler_flag("-funroll-loops" COMPILER_SUPPORTS_UNROLL)
        if(COMPILER_SUPPORTS_UNROLL)
            set(PERF_FLAGS "${PERF_FLAGS} -funroll-loops")
        endif()
        
        check_cxx_compiler_flag("-finline-functions" COMPILER_SUPPORTS_INLINE)
        if(COMPILER_SUPPORTS_INLINE)
            set(PERF_FLAGS "${PERF_FLAGS} -finline-functions")
        endif()
    endif()
    
    if(PERF_FLAGS)
        set_target_properties(${EXTENSION_NAME} PROPERTIES 
            COMPILE_FLAGS "${PERF_FLAGS}"
        )
        set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES 
            COMPILE_FLAGS "${PERF_FLAGS}"
        )
    endif()
    
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