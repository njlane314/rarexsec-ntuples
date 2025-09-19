add_executable(rarexsec-runner)
add_executable(rarexsec::runner ALIAS rarexsec-runner)

target_sources(rarexsec-runner PRIVATE main.cpp)

target_link_libraries(rarexsec-runner PRIVATE rarexsec::processing)

target_compile_features(rarexsec-runner PRIVATE cxx_std_17)

set_target_properties(rarexsec-runner PROPERTIES EXPORT_NAME runner)

add_executable(rarexsec-cnn-snapshot)
add_executable(rarexsec::cnn_snapshot ALIAS rarexsec-cnn-snapshot)

target_sources(rarexsec-cnn-snapshot PRIVATE cnn_snapshot.cpp)

target_link_libraries(rarexsec-cnn-snapshot PRIVATE rarexsec::processing)

target_compile_features(rarexsec-cnn-snapshot PRIVATE cxx_std_17)

set_target_properties(rarexsec-cnn-snapshot PROPERTIES EXPORT_NAME cnn_snapshot)

install(
    TARGETS rarexsec-runner rarexsec-cnn-snapshot
    EXPORT ${RAREXSEC_EXPORT_SET}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
