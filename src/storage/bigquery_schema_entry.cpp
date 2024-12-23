
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "storage/bigquery_catalog_set.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {
namespace bigquery {

BigquerySchemaEntry::BigquerySchemaEntry(Catalog &catalog, CreateSchemaInfo &info, BigqueryDatasetRef &bq_dataset_ref)
    : SchemaCatalogEntry(catalog, info), tables(*this), bq_dataset_ref(std::move(bq_dataset_ref)) {
}

BigquerySchemaEntry::BigquerySchemaEntry(Catalog &catalog,
                                         CreateSchemaInfo &info,
                                         BigqueryDatasetRef &bq_dataset_ref,
                                         vector<CreateTableInfo> &table_infos)
    : BigquerySchemaEntry(catalog, info, bq_dataset_ref) {
    this->prefetched_table_infos = std::make_optional(std::move(table_infos));
}

void BigquerySchemaEntry::TryDropEntry(ClientContext &context, CatalogType type, const string &name) {
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreateTable(CatalogTransaction transaction,
                                                            BoundCreateTableInfo &info) {
    auto &create_table_info = info.Base();
    auto table_name = create_table_info.table;
    if (create_table_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
        DropInfo drop_info;
        drop_info.catalog = create_table_info.catalog;
        drop_info.name = table_name;
        drop_info.type = CatalogType::TABLE_ENTRY;
        drop_info.cascade = false;
        drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
        DropEntry(transaction.GetContext(), drop_info);
    }
    return tables.CreateTable(transaction.GetContext(), info);
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                               CreateFunctionInfo &info) {
    throw BinderException("BigQuery extension does not support creating functions.");
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreateIndex(CatalogTransaction transaction,
                                                            CreateIndexInfo &info,
                                                            TableCatalogEntry &table) {
    throw BinderException("BigQuery extension does not support creating indexes.");
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
    if (info.sql.empty()) {
        throw BinderException("Cannot create view in BigQuery from an empty SQL statement.");
    }
    if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
        info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
        auto current_entry = GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
        if (current_entry) {
            if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
                return current_entry;
            }
            // CREATE OR REPLACE
            DropInfo drop_info;
            drop_info.catalog = info.schema;
            drop_info.name = info.view_name;
            drop_info.type = CatalogType::VIEW_ENTRY;
            drop_info.cascade = false;
            drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
            DropEntry(transaction.GetContext(), drop_info);
        }
    }
    auto &bq_transaction = BigqueryTransaction::Get(transaction.GetContext(), catalog);
    auto bqclient = bq_transaction.GetBigqueryClient();
    bqclient->ExecuteQuery(info.sql);
    return tables.RefreshTable(transaction.GetContext(), info.view_name);
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                               CreateSequenceInfo &info) {
    throw BinderException("BigQuery extension does not support creating sequences.");
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                    CreateTableFunctionInfo &info) {
    throw BinderException("BigQuery extension does not support creating table functions.");
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                   CreateCopyFunctionInfo &info) {
    throw BinderException("BigQuery extension does not support creating copy functions.");
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                     CreatePragmaFunctionInfo &info) {
    throw BinderException("BigQuery extension does not support creating pragma functions.");
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                CreateCollationInfo &info) {
    throw BinderException("BigQuery extension does not support creating collations.");
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
    throw BinderException("BigQuery extension does not support creating types.");
}

void BigquerySchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
    if (info.type != AlterType::ALTER_TABLE) {
        throw BinderException("Only altering tables is supported.");
    }
    auto &alter = info.Cast<AlterTableInfo>();
    tables.AlterTable(transaction.GetContext(), alter);
}

void BigquerySchemaEntry::Scan(ClientContext &context,
                               CatalogType type,
                               const std::function<void(CatalogEntry &)> &callback) {
    switch (type) {
    case CatalogType::TABLE_ENTRY:
    case CatalogType::VIEW_ENTRY: {
        tables.Scan(context, callback);
        return;
    }
    default:
        return;
    }
}

void BigquerySchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    throw NotImplementedException("Scan without context not supported");
}

void BigquerySchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
    info.schema = name;
    GetCatalogSet(info.type).DropEntry(context, info);
}

optional_ptr<CatalogEntry> BigquerySchemaEntry::GetEntry(CatalogTransaction transaction,
                                                         CatalogType type,
                                                         const string &name) {
    switch (type) {
    case CatalogType::TABLE_ENTRY:
    case CatalogType::VIEW_ENTRY:
        return tables.GetEntry(transaction.GetContext(), name);
    default:
        // other types do not exist/are not supported.
        return nullptr;
    }
}

BigqueryCatalogSet &BigquerySchemaEntry::GetCatalogSet(CatalogType type) {
    switch (type) {
    case CatalogType::TABLE_ENTRY:
    case CatalogType::VIEW_ENTRY:
        return tables;
    default:
        throw InternalException("Type not supported for GetCatalogSet: %d", (int)type);
    }
}


} // namespace bigquery
} // namespace duckdb
