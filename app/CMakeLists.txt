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

add_executable(cnn-friend-register)

target_sources(cnn-friend-register PRIVATE cnn_friend_register.cpp)

target_link_libraries(cnn-friend-register PRIVATE rarexsec::processing)

target_compile_features(cnn-friend-register PRIVATE cxx_std_17)

set_target_properties(cnn-friend-register PROPERTIES EXPORT_NAME cnn_friend_register)

install(
    TARGETS snapshot-analysis snapshot-training cnn-friend-register
    EXPORT ${RAREXSEC_EXPORT_SET}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
