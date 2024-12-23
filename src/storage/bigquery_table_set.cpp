#include <iostream>

#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "bigquery_client.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_table_set.hpp"
#include "storage/bigquery_transaction.hpp"


namespace duckdb {
namespace bigquery {

BigqueryTableSet::BigqueryTableSet(BigquerySchemaEntry &schema) : BigqueryInSchemaSet(schema) {
}

optional_ptr<CatalogEntry> BigqueryTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    auto &bq_catalog = dynamic_cast<BigqueryCatalog &>(catalog);
    auto bqclient = transaction.GetBigqueryClient();
    auto &create_table_info = info.Base();

    BigqueryTableRef table_ref;
    table_ref.project_id = bq_catalog.GetProjectID();
    table_ref.dataset_id = schema.name;
    table_ref.table_id = create_table_info.table;

    auto query = BigquerySQL::CreateTableInfoToSQL(bq_catalog.GetProjectID(), create_table_info);
    bqclient->ExecuteQuery(query);

    auto table_entry = make_uniq<BigqueryTableEntry>(catalog, schema, info.Base());
    return CreateEntry(std::move(table_entry));
}

optional_ptr<CatalogEntry> BigqueryTableSet::RefreshTable(ClientContext &context, const string &table_name) {
    auto table_info = GetTableInfo(context, schema, table_name);
    auto table_entry = make_uniq<BigqueryTableEntry>(catalog, schema, *table_info);
    auto table_ptr = table_entry.get();
    CreateEntry(std::move(table_entry));
    return table_ptr;
}

unique_ptr<BigqueryTableInfo> BigqueryTableSet::GetTableInfo(ClientContext &context,
                                                             BigquerySchemaEntry &schema,
                                                             const string &table_name) {
    auto &catalog = schema.ParentCatalog();
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    auto bqclient = transaction.GetBigqueryClient();

    auto project_id = dynamic_cast<BigqueryCatalog &>(catalog).GetProjectID();
    auto dataset_id = schema.name;

    auto info = make_uniq<BigqueryTableInfo>(project_id, schema.name, table_name);
    bqclient->GetTableInfo(dataset_id, table_name, info->create_info->columns, info->create_info->constraints);
    return info;
}

void BigqueryTableSet::AlterTable(ClientContext &context, AlterTableInfo &info) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    auto &bq_catalog = dynamic_cast<BigqueryCatalog &>(catalog);
    auto bqclient = transaction.GetBigqueryClient();

    auto query = BigquerySQL::AlterTableInfoToSQL(bq_catalog.GetProjectID(), info);
    bqclient->ExecuteQuery(query);
    ClearEntries();
}

void BigqueryTableSet::LoadEntries(ClientContext &context) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    auto bqclient = transaction.GetBigqueryClient();

    if (BigquerySettings::ExperimentalFetchCatalogFromInformationSchema()) {
        for (auto &table_info : schema.GetTableInfos()) {
            auto table_entry = make_uniq<BigqueryTableEntry>(catalog, schema, table_info);
            CreateEntry(std::move(table_entry));
        }
    } else {
        unique_ptr<BigqueryTableInfo> info;
        vector<unique_ptr<BigqueryTableInfo>> table_infos;

        vector<BigqueryTableRef> table_refs = bqclient->GetTables(schema.name);
        for (auto &table_ref : table_refs) {
            info = make_uniq<BigqueryTableInfo>(table_ref);
            bqclient->GetTableInfo(table_ref.dataset_id,
                                   table_ref.table_id,
                                   info->create_info->columns,
                                   info->create_info->constraints);
            auto table_entry = make_uniq<BigqueryTableEntry>(catalog, schema, *info);
            CreateEntry(std::move(table_entry));
        }
    }
}

} // namespace bigquery
} // namespace duckdb
