cmake_minimum_required(VERSION 3.16)
project(sensor_express VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Compile options ────────────────────────────────────────────────────────
add_compile_options(
    -Wall -Wextra -Wpedantic
    -O2
    -fno-omit-frame-pointer   # helpful for profiling RT threads
)

# ── Find dependencies ──────────────────────────────────────────────────────

# Mosquitto
find_library(MOSQUITTO_LIB mosquitto REQUIRED)
find_path(MOSQUITTO_INCLUDE_DIR mosquitto.h REQUIRED)

# yaml-cpp
find_package(yaml-cpp REQUIRED)

# spdlog
find_package(spdlog REQUIRED)

# libcurl
find_package(CURL REQUIRED)

# nlohmann/json
find_package(nlohmann_json REQUIRED)

# Threads (pthreads)
find_package(Threads REQUIRED)

# httplib (single-header — expected at /usr/local/include/httplib.h)
find_path(HTTPLIB_INCLUDE_DIR httplib.h
    PATHS /usr/local/include /usr/include
    REQUIRED)

# ── Library: core pipeline components ─────────────────────────────────────
add_library(sensor_express_core STATIC
    src/mqtt_subscriber.cpp
    src/rt_processor.cpp
    src/alert_dispatcher.cpp
    src/metrics_server.cpp
)

target_include_directories(sensor_express_core PUBLIC
    include
    ${MOSQUITTO_INCLUDE_DIR}
    ${HTTPLIB_INCLUDE_DIR}
)

target_link_libraries(sensor_express_core PUBLIC
    ${MOSQUITTO_LIB}
    yaml-cpp
    spdlog::spdlog
    CURL::libcurl
    nlohmann_json::nlohmann_json
    Threads::Threads
)

# ── Executable ─────────────────────────────────────────────────────────────
add_executable(sensor_express src/main.cpp)
target_link_libraries(sensor_express PRIVATE sensor_express_core)

# ── Tests (Google Test) ────────────────────────────────────────────────────
find_package(GTest QUIET)

if(GTest_FOUND OR TARGET GTest::gtest)
    enable_testing()

    add_executable(test_pipeline tests/test_pipeline.cpp)
    target_include_directories(test_pipeline PRIVATE include)
    target_link_libraries(test_pipeline PRIVATE
        sensor_express_core
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
    )

    include(GoogleTest)
    gtest_discover_tests(test_pipeline)

    message(STATUS "Google Test found — test target enabled")
else()
    # Fallback: link against system libgtest directly
    find_library(GTEST_LIB     gtest      HINTS /usr/local/lib /usr/lib)
    find_library(GTEST_MAIN_LIB gtest_main HINTS /usr/local/lib /usr/lib)
    find_path(GTEST_INCLUDE_DIR gtest/gtest.h
        HINTS /usr/local/include /usr/include
    )

    if(GTEST_LIB AND GTEST_INCLUDE_DIR)
        enable_testing()

        add_executable(test_pipeline tests/test_pipeline.cpp)
        target_include_directories(test_pipeline PRIVATE include ${GTEST_INCLUDE_DIR})
        target_link_libraries(test_pipeline PRIVATE
            sensor_express_core
            ${GTEST_MAIN_LIB}
            ${GTEST_LIB}
            Threads::Threads
        )
        add_test(NAME PipelineTests COMMAND test_pipeline)
        message(STATUS "GTest found via find_library — test target enabled")
    else()
        message(WARNING "Google Test not found — skipping test target")
    endif()
endif()

# ── Install ────────────────────────────────────────────────────────────────
install(TARGETS sensor_express DESTINATION bin)
install(DIRECTORY config/ DESTINATION etc/sensor_express)
