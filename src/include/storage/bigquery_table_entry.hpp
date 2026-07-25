#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "bigquery_table_info.hpp"
#include "bigquery_utils.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryTableEntry : public TableCatalogEntry {
public:
    BigqueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
    BigqueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, BigqueryTableInfo &info);

public:
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    void BindUpdateConstraints(Binder &binder,
                               LogicalGet &get,
                               LogicalProjection &proj,
                               LogicalUpdate &update,
                               ClientContext &context) override;

    BigqueryReadMode ReadMode() const {
        return relation.ReadMode();
    }

    bool SupportsInsert() const {
        return relation.SupportsInsert();
    }

    bool SupportsUpdateDelete() const {
        return relation.SupportsUpdateDelete();
    }

    string RelationTypeName() const {
        return relation.TypeName();
    }

    const char *ReadModeName() const {
        return relation.ReadModeName();
    }

private:
    BigqueryRelationMetadata relation;
};

} // namespace bigquery
} // namespace duckdb
