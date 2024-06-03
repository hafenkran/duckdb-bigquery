#pragma once

#include "duckdb.hpp"

namespace duckdb {

class BigqueryExtension : public Extension {
public:
    std::string Name() override {
        return "bigquery";
    }
    void Load(DuckDB &db) override;
};

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void bigquery_init(duckdb::DatabaseInstance &db);
DUCKDB_EXTENSION_API const char *bigquery_version();
DUCKDB_EXTENSION_API void bigquery_storage_init(duckdb::DBConfig &config);
}
