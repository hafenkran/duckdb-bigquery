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
        auto result = make_uniq<BigqueryArrowScanBindData>();
        result->table_ref = BigqueryTableRef(bigquery_catalog.GetProjectID(), schema.name, name);
        result->bq_client = bigquery_transaction->GetBigqueryClient();
        result->bq_catalog = &bigquery_catalog;
        result->bq_table_entry = *this;
        result->bq_config = bigquery_catalog.config;

        auto arrow_schema_ptr = BigqueryUtils::BuildArrowSchema(columns);
        auto status = arrow::ExportSchema(*arrow_schema_ptr, &result->schema_root.arrow_schema);
        if (!status.ok()) {
            throw BinderException("Arrow schema export failed: " + status.ToString());
        }

        vector<string> physical_names;             // names extracted from arrow schema
        vector<LogicalType> physical_return_types; // physical DuckDB logical types derived from Arrow
        vector<LogicalType> util_mapped_bq_types;  // (unused for table entry logical exposure)
        BigqueryUtils::PopulateAndMapArrowTableTypes(context,
                                                     result->arrow_table,
                                                     result->schema_root,
                                                     physical_names,
                                                     physical_return_types,
                                                     util_mapped_bq_types,
                                                     &columns);
        if (physical_return_types.empty()) {
            throw BinderException("BigQuery table has no columns");
        }

        vector<LogicalType> user_return_types;
        user_return_types.reserve(columns.LogicalColumnCount());
        vector<string> user_names;
        user_names.reserve(columns.LogicalColumnCount());
        for (auto &col : columns.Logical()) {
            user_return_types.push_back(col.GetType());
            user_names.push_back(col.GetName());
        }

        if (user_return_types.size() != physical_return_types.size()) {
            throw InternalException("Arrow schema column count (%llu) != catalog column count (%llu) for table %s",
                                    (unsigned long long)physical_return_types.size(),
                                    (unsigned long long)user_return_types.size(),
                                    name);
        }

        // Build mapping: if physical type differs from user-visible, we will cast (incl. spatial handling)
        bool requires_cast = false;
        vector<LogicalType> mapped_bq_types; // physical source types aligned with user types
        mapped_bq_types.reserve(user_return_types.size());
        for (idx_t i = 0; i < user_return_types.size(); i++) {
            const auto &user_type = user_return_types[i];
            const auto &arrow_phys = physical_return_types[i];

            auto spatial_source = BigqueryUtils::CastToBigqueryTypeWithSpatialConversion(user_type, &context);

            LogicalType chosen_physical;
            // SPECIAL CASE (GEOGRAPHY -> GEOMETRY):
            // Catalog/user type: BLOB alias GEOMETRY (target user-facing GEOMETRY)
            // Actual BigQuery Arrow stream delivers WKT as VARCHAR alias GEOGRAPHY.
            // CastToBigqueryTypeWithSpatialConversion returns VARCHAR GEOGRAPHY. We must keep that as the mapped
            // physical source so that cast logic triggers and converts WKT->GEOMETRY blob.
            bool is_geometry_target = BigqueryUtils::IsGeometryType(user_type);
            bool spatial_inversion = BigqueryUtils::IsGeographyType(spatial_source);
            if (is_geometry_target && spatial_inversion) {
                // Keep spatial_source (VARCHAR GEOGRAPHY) as mapped physical
                chosen_physical = spatial_source;
            } else {
                // Otherwise trust Arrow's physical interpretation (incl. decimals/structs) – aliases already copied
                chosen_physical = arrow_phys;
            }

            if (chosen_physical != user_type) {
                requires_cast = true;
            }
            mapped_bq_types.push_back(chosen_physical);
        }
        if (!requires_cast) {
            mapped_bq_types.clear(); // signal no cast path needed
        }

        result->names = std::move(user_names);
        result->all_types = std::move(user_return_types);
        if (requires_cast) {
            result->mapped_bq_types = std::move(mapped_bq_types);
        }
        result->requires_cast = requires_cast;

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
