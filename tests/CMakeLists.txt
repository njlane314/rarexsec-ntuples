add_executable(selection_catalogue_tests
    selection_catalogue_tests.cpp
)

# The tests validate helper predicates that live in headers, so linking the
# processing library is sufficient to provide include directories and
# dependency visibility.
target_link_libraries(selection_catalogue_tests
    PRIVATE
        rarexsec::processing
)

add_test(
    NAME selection_catalogue_tests
    COMMAND selection_catalogue_tests
)
