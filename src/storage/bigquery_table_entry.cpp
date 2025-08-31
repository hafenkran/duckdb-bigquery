#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "bigquery_arrow_scan.hpp"
#include "bigquery_scan.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_table_entry.hpp"
#include "storage/bigquery_transaction.hpp"

#include <arrow/c/bridge.h>
#include <arrow/util/iterator.h>

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

    auto use_legacy_scan = BigquerySettings::UseLegacyScan();
    if (!use_legacy_scan) {
        // Use the new Arrow scan function (bigquery_arrow_scan)
        auto result = make_uniq<BigqueryArrowScanBindData>();
        result->table_ref = BigqueryTableRef(bigquery_catalog.GetProjectID(), schema.name, name);
        result->bq_client = bigquery_transaction->GetBigqueryClient();
        result->bq_catalog = &bigquery_catalog;
        result->bq_table_entry = *this;

        auto arrow_schema_ptr = BigqueryUtils::BuildArrowSchema(columns);
        auto status = arrow::ExportSchema(*arrow_schema_ptr, &result->schema_root.arrow_schema);
        if (!status.ok()) {
            throw BinderException("Arrow schema export failed: " + status.ToString());
        }

        vector<LogicalType> mapped_bq_types;
        BigqueryUtils::PopulateAndMapArrowTableTypes(context,
                                                     result->arrow_table,
                                                     result->schema_root,
                                                     result->names,
                                                     result->all_types,
                                                     mapped_bq_types,
                                                     &columns);
        if (!mapped_bq_types.empty()) {
            result->requires_cast = true;
            result->mapped_bq_types = std::move(mapped_bq_types);
        } else {
            result->requires_cast = false;
        }

        bind_data = std::move(result);

        auto function = BigqueryArrowScanFunction();
        Value filter_pushdown;
        if (context.TryGetCurrentSetting("bq_experimental_filter_pushdown", filter_pushdown)) {
            function.filter_pushdown = BooleanValue::Get(filter_pushdown);
        }
        return function;
    } else {
        // Use the old Bigquery scan function (bigquery_scan)

        // Check if geography_as_geometry is enabled with legacy scan and GEOGRAPHY columns present
        if (BigquerySettings::GeographyAsGeometry()) {
            for (const auto &column : columns.Logical()) {
                const auto &type = column.GetType();
                if (BigqueryUtils::IsGeographyType(type)) {
                    throw BinderException(
                        "BigQuery GEOGRAPHY columns with geography_as_geometry=true are not supported in legacy scan. "
                        "Please either set bq_use_legacy_scan=false (recommended) or set "
                        "bq_geography_as_geometry=false.");
                }
            }
        }

        auto result = make_uniq<BigqueryLegacyScanBindData>();
        result->table_ref = BigqueryTableRef(bigquery_catalog.GetProjectID(), schema.name, name);
        result->bq_client = bigquery_transaction->GetBigqueryClient();
        result->bq_catalog = &bigquery_catalog;
        result->bq_table_entry = *this;

        for (auto &column : columns.Logical()) {
            result->names.push_back(column.GetName());
            result->types.push_back(column.GetType());
        }
        bind_data = std::move(result);

        auto function = BigqueryScanFunction();
        Value filter_pushdown;
        if (context.TryGetCurrentSetting("bq_experimental_filter_pushdown", filter_pushdown)) {
            function.filter_pushdown = BooleanValue::Get(filter_pushdown);
        }
        return function;
    }
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
