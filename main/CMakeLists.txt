add_executable(rarexsec-runner)
add_executable(rarexsec::runner ALIAS rarexsec-runner)

target_sources(rarexsec-runner PRIVATE main.cpp)

target_link_libraries(rarexsec-runner PRIVATE rarexsec::processing)

target_compile_features(rarexsec-runner PRIVATE cxx_std_17)

set_target_properties(rarexsec-runner PROPERTIES EXPORT_NAME runner)

add_executable(rarexsec-training-pool)
add_executable(rarexsec::training_pool ALIAS rarexsec-training-pool)

target_sources(rarexsec-training-pool PRIVATE training_pool.cpp)

target_link_libraries(rarexsec-training-pool PRIVATE rarexsec::processing)

target_compile_features(rarexsec-training-pool PRIVATE cxx_std_17)

set_target_properties(rarexsec-training-pool PROPERTIES EXPORT_NAME training_pool)

install(
    TARGETS rarexsec-runner rarexsec-training-pool
    EXPORT ${RAREXSEC_EXPORT_SET}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
