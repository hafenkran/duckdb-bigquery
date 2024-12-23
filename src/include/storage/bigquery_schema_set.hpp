#pragma once

#include "storage/bigquery_catalog_set.hpp"
#include "storage/bigquery_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

namespace bigquery {

class BigquerySchemaSet : public BigqueryCatalogSet {
public:
    explicit BigquerySchemaSet(Catalog &catalog);

    optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);

protected:
    void LoadEntries(ClientContext &context);
};


} // namespace bigquery
} // namespace duckdb
