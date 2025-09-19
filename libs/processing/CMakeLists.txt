set(target rarexsec_processing)

add_library(${target})
add_library(rarexsec::processing ALIAS ${target})
set_target_properties(${target} PROPERTIES EXPORT_NAME processing)

target_sources(${target}
    PRIVATE
        src/AnalysisDataLoader.cpp
        src/BlipProcessor.cpp
        src/MuonSelectionProcessor.cpp
        src/NuMuCCSelectionProcessor.cpp
        src/PreselectionProcessor.cpp
        src/ReconstructionProcessor.cpp
        src/RunConfig.cpp
        src/RunConfigLoader.cpp
        src/RunConfigRegistry.cpp
        src/SampleDefinition.cpp
        src/TruthChannelProcessor.cpp
        src/WeightProcessor.cpp
)

target_include_directories(${target}
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(${target}
    PUBLIC
        ROOT::RIO
        ROOT::Tree
        ROOT::Hist
        ROOT::ROOTDataFrame
        nlohmann_json::nlohmann_json
)

target_compile_features(${target} PUBLIC cxx_std_17)

install(TARGETS ${target}
    EXPORT rarexsecTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
