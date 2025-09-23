add_executable(WeightProcessorTests
    WeightProcessor.test.cpp
)

set_target_properties(WeightProcessorTests PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

target_link_libraries(WeightProcessorTests
    PRIVATE
        rarexsec::processing
)

add_test(NAME WeightProcessorTests COMMAND WeightProcessorTests)
