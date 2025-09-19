add_executable(rarexsec-runner)
add_executable(rarexsec::runner ALIAS rarexsec-runner)

target_sources(rarexsec-runner PRIVATE main.cpp)

target_link_libraries(rarexsec-runner PRIVATE rarexsec::processing)

target_compile_features(rarexsec-runner PRIVATE cxx_std_17)

set_target_properties(rarexsec-runner PROPERTIES EXPORT_NAME runner)

add_executable(rarexsec-training-pool-generator)
add_executable(rarexsec::training_pool_generator ALIAS rarexsec-training-pool-generator)

target_sources(rarexsec-training-pool-generator PRIVATE training_pool_generator.cpp)

target_link_libraries(rarexsec-training-pool-generator PRIVATE rarexsec::processing)

target_compile_features(rarexsec-training-pool-generator PRIVATE cxx_std_17)

set_target_properties(rarexsec-training-pool-generator PROPERTIES EXPORT_NAME training_pool_generator)

install(
    TARGETS rarexsec-runner rarexsec-training-pool-generator
    EXPORT ${RAREXSEC_EXPORT_SET}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
