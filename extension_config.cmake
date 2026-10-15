# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(bigquery
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

duckdb_extension_statically_link(bigquery)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
