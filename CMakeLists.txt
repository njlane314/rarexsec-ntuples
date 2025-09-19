cmake_minimum_required(VERSION 3.16)
project(rarexsec_processing VERSION 0.1.0 LANGUAGES CXX)

include(CTest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(GNUInstallDirs)
include(FetchContent)

FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.2
)

FetchContent_MakeAvailable(nlohmann_json)

find_package(ROOT REQUIRED COMPONENTS RIO Tree Hist ROOTDataFrame)

set(INSTALL_CONFIGDIR ${CMAKE_INSTALL_LIBDIR}/cmake/rarexsec)

add_library(rarexsec_processing
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

add_library(rarexsec::processing ALIAS rarexsec_processing)

target_include_directories(rarexsec_processing
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
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

add_executable(rarexsec-runner apps/rarexsec-runner/main.cpp)

target_link_libraries(rarexsec-runner PRIVATE rarexsec_processing)

target_compile_features(rarexsec-runner PRIVATE cxx_std_17)

install(TARGETS rarexsec_processing rarexsec-runner
    EXPORT rarexsecTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT rarexsecTargets
    FILE rarexsecTargets.cmake
    NAMESPACE rarexsec::
    DESTINATION ${INSTALL_CONFIGDIR}
)

include(CMakePackageConfigHelpers)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/rarexsecConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/rarexsecConfig.cmake
    INSTALL_DESTINATION ${INSTALL_CONFIGDIR}
    PATH_VARS CMAKE_INSTALL_INCLUDEDIR
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/rarexsecConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/rarexsecConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/rarexsecConfigVersion.cmake
    DESTINATION ${INSTALL_CONFIGDIR}
)

