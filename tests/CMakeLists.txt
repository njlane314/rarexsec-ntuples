add_executable(snapshot-pipeline-builder-tests SnapshotPipelineBuilderTests.cpp)

target_link_libraries(snapshot-pipeline-builder-tests PRIVATE rarexsec::processing)

target_compile_features(snapshot-pipeline-builder-tests PRIVATE cxx_std_17)

add_test(NAME SnapshotPipelineBuilderTests COMMAND snapshot-pipeline-builder-tests)
