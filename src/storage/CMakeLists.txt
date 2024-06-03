
set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_catalog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_catalog_set.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_schema_entry.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_schema_set.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_table_entry.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_table_set.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_transaction.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_insert.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_delete.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bigquery_update.cpp
    PARENT_SCOPE)
