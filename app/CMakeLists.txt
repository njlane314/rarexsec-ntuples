add_executable(snapshot)
add_executable(rarexsec::runner ALIAS snapshot)

target_sources(snapshot PRIVATE snapshot.cpp)

target_link_libraries(snapshot PRIVATE rarexsec::processing)

target_compile_features(snapshot PRIVATE cxx_std_17)

set_target_properties(snapshot PROPERTIES EXPORT_NAME runner)

add_executable(training-pool)
add_executable(rarexsec::training_pool ALIAS training-pool)

target_sources(training-pool PRIVATE training_pool.cpp)

target_link_libraries(training-pool PRIVATE rarexsec::processing)

target_compile_features(training-pool PRIVATE cxx_std_17)

set_target_properties(training-pool PROPERTIES EXPORT_NAME training_pool)

install(
    TARGETS snapshot training-pool
    EXPORT ${RAREXSEC_EXPORT_SET}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
