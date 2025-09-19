add_executable(rarexsec-runner main.cpp)

target_link_libraries(rarexsec-runner
    PRIVATE
        rarexsec::processing
)

target_compile_features(rarexsec-runner PRIVATE cxx_std_17)

install(TARGETS rarexsec-runner
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
