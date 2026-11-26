add_executable(bigquery_filter_tests EXCLUDE_FROM_ALL
               cpp/test_filter_translation.cpp)
target_include_directories(bigquery_filter_tests
                           PRIVATE ${DUCKDB_MODULE_BASE_DIR}/third_party/catch)
target_link_libraries(bigquery_filter_tests ${EXTENSION_NAME})
link_libraries_to_extension(bigquery_filter_tests)

add_executable(bigquery_rest_tests EXCLUDE_FROM_ALL
               cpp/test_rest_conversion.cpp)
target_include_directories(bigquery_rest_tests
                           PRIVATE ${DUCKDB_MODULE_BASE_DIR}/third_party/catch)
target_link_libraries(bigquery_rest_tests ${EXTENSION_NAME})
link_libraries_to_extension(bigquery_rest_tests)

add_executable(bigquery_config_tests EXCLUDE_FROM_ALL
               cpp/test_bigquery_config.cpp)
target_include_directories(bigquery_config_tests
                           PRIVATE ${DUCKDB_MODULE_BASE_DIR}/third_party/catch)
target_link_libraries(bigquery_config_tests ${EXTENSION_NAME})
link_libraries_to_extension(bigquery_config_tests)
