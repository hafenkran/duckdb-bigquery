#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "bigquery_utils.hpp"

namespace duckdb {
namespace bigquery {

struct BigqueryTableInfo {
    BigqueryTableInfo() {
        create_info = make_uniq<CreateTableInfo>();
        create_info->columns.SetAllowDuplicates(true);
    }
    BigqueryTableInfo(const string &project_id, const string &dataset_id, const string &table_id) {
        create_info = make_uniq<CreateTableInfo>(project_id, dataset_id, table_id);
        create_info->columns.SetAllowDuplicates(true);
    }
    BigqueryTableInfo(const BigqueryTableRef &table_ref)
        : BigqueryTableInfo(table_ref.project_id, table_ref.dataset_id, table_ref.table_id) {
    }
    BigqueryTableInfo(unique_ptr<CreateTableInfo> info) : create_info(std::move(info)) {
        create_info->columns.SetAllowDuplicates(true);
    }

    const string &GetTableName() const {
        return create_info->table;
    }

    unique_ptr<CreateTableInfo> create_info;
    vector<BigqueryType> bigquery_types;
    vector<string> bigquery_names;
};


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
};

} // namespace bigquery
} // namespace duckdb
