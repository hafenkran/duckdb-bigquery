#pragma once

#include "storage/bigquery_catalog_set.hpp"
#include "storage/bigquery_table_entry.hpp"

namespace duckdb {
namespace bigquery {

class BigquerySchemaEntry;


class BigqueryTableSet : public BigqueryInSchemaSet {
public:
    explicit BigqueryTableSet(BigquerySchemaEntry &schema);

public:
    optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);
    optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

    static unique_ptr<BigqueryTableInfo> GetTableInfo(ClientContext &context,
                                                      BigquerySchemaEntry &schema,
                                                      const string &table_name);

protected:
    void LoadEntries(ClientContext &context) override;
};

} // namespace bigquery
} // namespace duckdb
