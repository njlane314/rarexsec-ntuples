add_executable(rarexsec-runner)
add_executable(rarexsec::runner ALIAS rarexsec-runner)

target_sources(rarexsec-runner PRIVATE main.cpp)

target_link_libraries(rarexsec-runner PRIVATE rarexsec::processing)

target_compile_features(rarexsec-runner PRIVATE cxx_std_17)

set_target_properties(rarexsec-runner PROPERTIES EXPORT_NAME runner)

install(
    TARGETS rarexsec-runner
    EXPORT ${RAREXSEC_EXPORT_SET}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
