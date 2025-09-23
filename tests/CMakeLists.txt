cmake_minimum_required(VERSION 3.16)

include(FetchContent)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/release-1.13.0.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

add_executable(WeightProcessorTests
    WeightProcessor.test.cpp
)

target_link_libraries(WeightProcessorTests
    PRIVATE
        GTest::gtest_main
        rarexsec::processing
)

include(GoogleTest)
gtest_discover_tests(WeightProcessorTests)
