set(rarexsec_processing_sources
    processing/AnalysisDataLoader.cpp
    processing/BeamPeriodConfiguration.cpp
    processing/BeamPeriodConfigurationLoader.cpp
    processing/BeamPeriodConfigurationRegistry.cpp
    processing/BlipProcessor.cpp
    processing/ConfiguredSample.cpp
    processing/MuonSelectionProcessor.cpp
    processing/NuMuCCSelectionProcessor.cpp
    processing/PreselectionProcessor.cpp
    processing/ReconstructionProcessor.cpp
    processing/TruthChannelProcessor.cpp
    processing/WeightProcessor.cpp
)

add_library(rarexsec_processing)
add_library(rarexsec::processing ALIAS rarexsec_processing)

target_sources(rarexsec_processing
    PRIVATE
        ${rarexsec_processing_sources}
)

target_include_directories(rarexsec_processing
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(rarexsec_processing
    PUBLIC
        ROOT::RIO
        ROOT::Tree
        ROOT::Hist
        ROOT::ROOTDataFrame
        nlohmann_json::nlohmann_json
)

target_compile_features(rarexsec_processing PUBLIC cxx_std_17)

set_target_properties(rarexsec_processing PROPERTIES EXPORT_NAME processing)

install(
    TARGETS rarexsec_processing
    EXPORT ${RAREXSEC_EXPORT_SET}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(
    DIRECTORY ${PROJECT_SOURCE_DIR}/include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
