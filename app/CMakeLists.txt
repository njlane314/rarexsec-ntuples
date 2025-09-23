add_executable(snapshot-analysis)
add_executable(rarexsec::runner ALIAS snapshot-analysis)

target_sources(snapshot-analysis PRIVATE snapshot_analysis.cpp)

target_link_libraries(snapshot-analysis PRIVATE rarexsec::processing)

target_compile_features(snapshot-analysis PRIVATE cxx_std_17)

set_target_properties(snapshot-analysis PROPERTIES EXPORT_NAME runner)

add_executable(snapshot-training)
add_executable(rarexsec::training_pool ALIAS snapshot-training)

target_sources(snapshot-training PRIVATE snapshot_training.cpp)

target_link_libraries(snapshot-training PRIVATE rarexsec::processing)

target_compile_features(snapshot-training PRIVATE cxx_std_17)

set_target_properties(snapshot-training PROPERTIES EXPORT_NAME training_pool)

add_executable(hub-attach-friends)

target_sources(hub-attach-friends PRIVATE hub_attach_friends.cpp)

target_link_libraries(hub-attach-friends PRIVATE rarexsec::processing)

target_compile_features(hub-attach-friends PRIVATE cxx_std_17)

install(
    TARGETS snapshot-analysis snapshot-training hub-attach-friends
    EXPORT ${RAREXSEC_EXPORT_SET}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
