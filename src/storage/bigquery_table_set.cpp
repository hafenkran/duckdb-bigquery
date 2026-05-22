#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/common/string_util.hpp"

#include "bigquery_client.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_table_set.hpp"
#include "storage/bigquery_transaction.hpp"


namespace duckdb {
namespace bigquery {
namespace {

static bool HasUnsupportedColumns(const BigqueryTableInfo &table_info) {
    return table_info.has_unsupported_columns;
}

static string UnsupportedColumnsMessage(const BigqueryTableInfo &table_info) {
    if (table_info.unsupported_columns.empty()) {
        return "unknown unsupported column";
    }
    return StringUtil::Join(table_info.unsupported_columns, ", ");
}

} // namespace

BigqueryTableSet::BigqueryTableSet(BigquerySchemaEntry &schema) : BigqueryInSchemaSet(schema) {
}

optional_ptr<CatalogEntry> BigqueryTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
    BigqueryTransaction::CheckReadWrite(context, catalog, "create tables");
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    auto &bq_catalog = dynamic_cast<BigqueryCatalog &>(catalog);
    auto bqclient = transaction.GetBigqueryClient();
    auto &create_table_info = info.Base();

    BigqueryTableRef table_ref;
    table_ref.project_id = bq_catalog.GetProjectID();
    table_ref.dataset_id = schema.name;
    table_ref.table_id = create_table_info.table;

    bqclient->CreateTable(create_table_info, table_ref);
    auto table_entry = make_shared_ptr<BigqueryTableEntry>(catalog, schema, info.Base());
    return CreateEntry(transaction, std::move(table_entry));
}

optional_ptr<CatalogEntry> BigqueryTableSet::RefreshTable(ClientContext &context, const string &table_name) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    auto table_info = GetTableInfo(context, schema, table_name);
    auto table_entry = make_shared_ptr<BigqueryTableEntry>(catalog, schema, *table_info);
    return CreateEntry(transaction, std::move(table_entry));
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
    bqclient->GetTableInfo(dataset_id, table_name, *info);
    return info;
}

void BigqueryTableSet::AlterTable(ClientContext &context, AlterTableInfo &info) {
    BigqueryTransaction::CheckReadWrite(context, catalog, "alter tables");
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    auto &bq_catalog = dynamic_cast<BigqueryCatalog &>(catalog);
    auto bqclient = transaction.GetBigqueryClient();

    auto query = BigquerySQL::AlterTableInfoToSQL(bq_catalog.GetProjectID(), info);
    bqclient->ExecuteQuery(query);
    ClearEntries();
}

void BigqueryTableSet::LoadEntries(ClientContext &, BigqueryTransaction &transaction) {
    auto bqclient = transaction.GetBigqueryClient();

    if (BigquerySettings::ExperimentalFetchCatalogFromInformationSchema()) {
        auto &prefetched_table_infos = schema.GetTableInfos();
        if (prefetched_table_infos.has_value()) {
            for (auto &table_info : prefetched_table_infos.value()) {
                if (HasUnsupportedColumns(table_info)) {
                    continue;
                }
                auto table_entry = make_shared_ptr<BigqueryTableEntry>(catalog, schema, table_info);
                CreateEntry(transaction, std::move(table_entry));
            }
        } else {
            std::map<string, BigqueryTableInfo> table_infos;
            bqclient->GetTableInfosFromDataset(schema.GetBigqueryDatasetRef(), table_infos);

            for (auto &table_info : table_infos) {
                if (HasUnsupportedColumns(table_info.second)) {
                    continue;
                }
                auto table_entry = make_shared_ptr<BigqueryTableEntry>(catalog, schema, table_info.second);
                CreateEntry(transaction, std::move(table_entry));
            }
        }
    } else {
        unique_ptr<BigqueryTableInfo> info;

        vector<BigqueryTableRef> table_refs = bqclient->GetTables(schema.name);
        for (auto &table_ref : table_refs) {
            info = make_uniq<BigqueryTableInfo>(table_ref);
            if (!bqclient->TryGetTableInfo(table_ref.dataset_id, table_ref.table_id, *info)) {
                continue;
            }
            if (HasUnsupportedColumns(*info)) {
                continue;
            }
            auto table_entry = make_shared_ptr<BigqueryTableEntry>(catalog, schema, *info);
            CreateEntry(transaction, std::move(table_entry));
        }
    }
}

optional_ptr<CatalogEntry> BigqueryTableSet::ReloadEntry(ClientContext &,
                                                         BigqueryTransaction &transaction,
                                                         const string &table_name) {
    auto bqclient = transaction.GetBigqueryClient();
    auto project_id = dynamic_cast<BigqueryCatalog &>(catalog).GetProjectID();
    auto table_info = make_uniq<BigqueryTableInfo>(project_id, schema.name, table_name);
    if (!bqclient->TryGetTableInfo(schema.name, table_name, *table_info)) {
        return nullptr;
    }
    if (HasUnsupportedColumns(*table_info)) {
        auto table_ref = BigqueryUtils::FormatTableString(project_id, schema.name, table_name);
        throw BinderException("BigQuery table \"%s\" contains unsupported columns: %s",
                              table_ref,
                              UnsupportedColumnsMessage(*table_info));
    }
    auto table_entry = make_shared_ptr<BigqueryTableEntry>(catalog, schema, *table_info);
    return CreateEntry(transaction, std::move(table_entry));
}

void BigqueryTableSet::ClearEntries() {
    schema.GetTableInfos().reset();
    BigqueryCatalogSet::ClearEntries();
}

} // namespace bigquery
} // namespace duckdb
