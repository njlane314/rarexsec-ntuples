add_executable(snapshot_analysis_regression_test SnapshotAnalysisRegressionTest.cpp)

add_dependencies(snapshot_analysis_regression_test snapshot-analysis)

target_link_libraries(snapshot_analysis_regression_test
    PRIVATE
        ROOT::RIO
        ROOT::Tree
        nlohmann_json::nlohmann_json)

target_compile_features(snapshot_analysis_regression_test PRIVATE cxx_std_17)

add_test(
    NAME snapshot-analysis-regression
    COMMAND snapshot_analysis_regression_test $<TARGET_FILE:snapshot-analysis>
)
