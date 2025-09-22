set(rarexsec_processing_sources
    SnapshotPipelineBuilder.cpp
    RunConfig.cpp
    RunConfigLoader.cpp
    RunConfigRegistry.cpp
    BlipProcessor.cpp
    SamplePipeline.cpp
    MuonSelectionProcessor.cpp
    Selections.cpp
    PreselectionProcessor.cpp
    ReconstructionProcessor.cpp
    TruthChannelProcessor.cpp
    WeightProcessor.cpp
)

set(rarexsec_processing_dictionary_headers
    ${PROJECT_SOURCE_DIR}/include/rarexsec/TruthDerived.h
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
        $<INSTALL_INTERFACE:include>
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

ROOT_GENERATE_DICTIONARY(G__rarexsec
    ${rarexsec_processing_dictionary_headers}
    MODULE rarexsec_processing
    LINKDEF RarexsecDictLinkDef.h
)

ROOT_GET_LIBRARY_OUTPUT_DIR(rarexsec_processing_library_output_dir)

set(rarexsec_processing_dictionary_rootmap
    ${rarexsec_processing_library_output_dir}/${CMAKE_SHARED_LIBRARY_PREFIX}rarexsec_processing.rootmap)
set(rarexsec_processing_dictionary_pcm
    ${rarexsec_processing_library_output_dir}/${CMAKE_SHARED_LIBRARY_PREFIX}rarexsec_processing_rdict.pcm)

set(rarexsec_processing_dictionary_files)
if(rarexsec_processing_dictionary_pcm)
    list(APPEND rarexsec_processing_dictionary_files ${rarexsec_processing_dictionary_pcm})
endif()
if(rarexsec_processing_dictionary_rootmap)
    list(APPEND rarexsec_processing_dictionary_files ${rarexsec_processing_dictionary_rootmap})
endif()

install(
    TARGETS rarexsec_processing
    EXPORT ${RAREXSEC_EXPORT_SET}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

if(rarexsec_processing_dictionary_files)
    install(
        FILES ${rarexsec_processing_dictionary_files}
        DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()

install(
    DIRECTORY ${PROJECT_SOURCE_DIR}/include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
