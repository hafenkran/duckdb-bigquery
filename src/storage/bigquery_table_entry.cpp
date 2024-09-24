#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "bigquery_scan.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_table_entry.hpp"
#include "storage/bigquery_transaction.hpp"

#include <iostream>

namespace duckdb {
namespace bigquery {

BigqueryTableEntry::BigqueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
}

BigqueryTableEntry::BigqueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, BigqueryTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
}

unique_ptr<BaseStatistics> BigqueryTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
    return nullptr;
}

TableFunction BigqueryTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
    auto catalog_transaction = bigquery_catalog.GetCatalogTransaction(context);
    auto bigquery_transaction = dynamic_cast<BigqueryTransaction *>(catalog_transaction.transaction.get());

    auto result = make_uniq<BigqueryBindData>();
	result->table_ref = BigqueryTableRef(bigquery_catalog.GetProjectID(), schema.name, name);
    result->bq_client = bigquery_transaction->GetBigqueryClient();

    for (auto &column : columns.Logical()) {
        result->names.push_back(column.GetName());
        result->types.push_back(column.GetType());
    }
    bind_data = std::move(result);
    // result->bq_client->CreateArrowReader(result->dataset_id, result->table_id, 1);

    auto function = BigqueryScanFunction();
    Value filter_pushdown;
    if (context.TryGetCurrentSetting("bq_experimental_filter_pushdown", filter_pushdown)) {
        function.filter_pushdown = BooleanValue::Get(filter_pushdown);
    }
    return function;
}

TableStorageInfo BigqueryTableEntry::GetStorageInfo(ClientContext &context) {
    TableStorageInfo result;
    result.cardinality = 100000; // TODO
    result.index_info = vector<IndexInfo>();
    return result;
}

void BigqueryTableEntry::BindUpdateConstraints(Binder &binder,
                                               LogicalGet &get,
                                               LogicalProjection &proj,
                                               LogicalUpdate &update,
                                               ClientContext &context) {
    // nothing todo
}

} // namespace bigquery
} // namespace duckdb
