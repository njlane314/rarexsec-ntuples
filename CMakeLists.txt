cmake_minimum_required(VERSION 3.16)

project(rarexsec VERSION 0.1.0 LANGUAGES CXX)

set(RAREXSEC_DEFAULT_INSTALL_PREFIX "${CMAKE_SOURCE_DIR}/install")
option(RAREXSEC_ALLOW_SYSTEM_INSTALL "Allow installing into system prefixes such as /usr/local" OFF)

set(_rarexsec_use_local_prefix OFF)
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(_rarexsec_use_local_prefix ON)
elseif(NOT RAREXSEC_ALLOW_SYSTEM_INSTALL)
    get_filename_component(_rarexsec_prefix_abs "${CMAKE_INSTALL_PREFIX}" ABSOLUTE)
    if(_rarexsec_prefix_abs STREQUAL "/usr/local")
        set(_rarexsec_use_local_prefix ON)
        message(WARNING "CMAKE_INSTALL_PREFIX was /usr/local; defaulting to ${RAREXSEC_DEFAULT_INSTALL_PREFIX}. "
                        "Set RAREXSEC_ALLOW_SYSTEM_INSTALL=ON to keep the system prefix.")
    endif()
endif()

if(_rarexsec_use_local_prefix)
    set(CMAKE_INSTALL_PREFIX "${RAREXSEC_DEFAULT_INSTALL_PREFIX}" CACHE PATH "Installation prefix" FORCE)
endif()

unset(_rarexsec_use_local_prefix)
unset(_rarexsec_prefix_abs)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(RAREXSEC_BUILD_LIB "Build the rarexsec processing library" ON)
option(RAREXSEC_BUILD_APPS "Build the rarexsec executable tools" ON)

include(CTest)

if(RAREXSEC_BUILD_APPS AND NOT RAREXSEC_BUILD_LIB)
    message(FATAL_ERROR "RAREXSEC_BUILD_APPS=ON requires RAREXSEC_BUILD_LIB=ON because the executables depend on the processing library.")
endif()

set(RAREXSEC_ROOT_COMPONENTS RIO Tree Hist ROOTDataFrame)
set(RAREXSEC_JSON_VERSION 3.11.2)

if(RAREXSEC_BUILD_LIB)
    find_package(ROOT REQUIRED COMPONENTS ${RAREXSEC_ROOT_COMPONENTS})
    find_package(nlohmann_json ${RAREXSEC_JSON_VERSION} CONFIG REQUIRED)
endif()

set(RAREXSEC_EXPORT_SET rarexsecTargets)

if(RAREXSEC_BUILD_LIB)
    add_subdirectory(src)
endif()

if(RAREXSEC_BUILD_APPS)
    add_subdirectory(app)
endif()

if(BUILD_TESTING)
    if(NOT RAREXSEC_BUILD_LIB)
        message(FATAL_ERROR "BUILD_TESTING=ON requires RAREXSEC_BUILD_LIB=ON to access processing components.")
    endif()
    add_subdirectory(tests)
endif()

if(RAREXSEC_BUILD_LIB OR RAREXSEC_BUILD_APPS)
    set(INSTALL_CONFIGDIR ${CMAKE_INSTALL_LIBDIR}/cmake/rarexsec)

    install(
        EXPORT ${RAREXSEC_EXPORT_SET}
        FILE rarexsecTargets.cmake
        NAMESPACE rarexsec::
        DESTINATION ${INSTALL_CONFIGDIR}
    )

    configure_package_config_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/rarexsec-config.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/rarexsec-config.cmake
        INSTALL_DESTINATION ${INSTALL_CONFIGDIR}
        PATH_VARS CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_BINDIR
    )

    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/rarexsec-config-version.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )

    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/rarexsec-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/rarexsec-config-version.cmake
        DESTINATION ${INSTALL_CONFIGDIR}
    )
endif()
