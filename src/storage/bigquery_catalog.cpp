#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/storage/database_size.hpp"

#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_options.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_schema_set.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {
namespace bigquery {

BigqueryCatalog::BigqueryCatalog(AttachedDatabase &db_p, const string &connection_str, BigqueryOptions options_p)
    : Catalog(db_p), options(options_p), schemas(*this) {
    con_details = BigqueryUtils::ParseConnectionString(connection_str);
    if (!con_details.is_valid()) {
        throw BinderException("Invalid connection string: %s", connection_str);
    }
}

BigqueryCatalog::~BigqueryCatalog() {
}

void BigqueryCatalog::Initialize(bool load_builtin) {
    // // Set the default dataset/schema
    // if (!con_details.dataset_id.empty()) {
    //     CreateSchemaInfo info;
    //     info.catalog = con_details.project_id;
    //     info.schema = con_details.dataset_id;
    //     BigqueryDatasetRef dataset_ref;
    //     dataset_ref.project_id = con_details.project_id;
    //     dataset_ref.dataset_id = con_details.dataset_id;
    //     // dataset_ref.location = BigqueryClient::DefaultBigqueryLocation();

    //     default_dataset = make_uniq<BigquerySchemaEntry>(*this, info, dataset_ref);
    // }
}

void BigqueryCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    if (!con_details.dataset_id.empty()) {
        if (!default_dataset) {
            auto bq_transaction = dynamic_cast<BigqueryTransaction *>(GetCatalogTransaction(context).transaction.get());
            auto bq_client = bq_transaction->GetBigqueryClient();
            auto dataset_ref = bq_client->GetDataset(con_details.dataset_id);
            CreateSchemaInfo info;
            info.catalog = con_details.project_id;
            info.schema = con_details.dataset_id;
            default_dataset = make_uniq<BigquerySchemaEntry>(*this, info, dataset_ref);
        }
        callback(*default_dataset);
        return;
    }

    schemas.Scan(context, [&](CatalogEntry &schema) { //
        callback(schema.Cast<BigquerySchemaEntry>());
    });
}

optional_ptr<CatalogEntry> BigqueryCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
    if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
        throw BinderException("BigQuery does not support REPLACE ON CONFLICT");
    }
    return schemas.CreateSchema(transaction.GetContext(), info);
}

optional_ptr<SchemaCatalogEntry> BigqueryCatalog::GetSchema(CatalogTransaction transaction,
                                                            const string &schema_name,
                                                            OnEntryNotFound if_not_found,
                                                            QueryErrorContext error_context) {
    auto schema_entry = schemas.GetEntry(transaction.GetContext(), schema_name);
    if (!schema_entry && if_not_found == OnEntryNotFound::RETURN_NULL) {
        throw BinderException("Schema with name \"%s\" not found", schema_name);
    }
    return reinterpret_cast<SchemaCatalogEntry *>(schema_entry.get());
}

void BigqueryCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    return schemas.DropEntry(context, info);
}


unique_ptr<LogicalOperator> BigqueryCatalog::BindCreateIndex(Binder &binder,
                                                             CreateStatement &stmt,
                                                             TableCatalogEntry &table,
                                                             unique_ptr<LogicalOperator> plan) {
    throw BinderException("BigQuery does not support creating indexes");
}

DatabaseSize BigqueryCatalog::GetDatabaseSize(ClientContext &context) {
    // Use the tables.list API method to list all tables in the dataset
    // for each table, get the "numBytes" property
    // Sum these up to get the total size of the dataset
    DatabaseSize size;
    size.free_blocks = 0;
    size.total_blocks = 0;
    size.used_blocks = 0;
    size.wal_size = 0;
    size.block_size = 0;
    size.bytes = 0;
    // return size;
    throw BinderException("BigQuery does not support getting database size");
}

vector<MetadataBlockInfo> BigqueryCatalog::GetMetadataInfo(ClientContext &context) {
    throw BinderException("BigQuery does not support getting metadata info");
};


bool BigqueryCatalog::InMemory() {
    return false;
};

string BigqueryCatalog::GetDBPath() {
    return con_details.project_id;
};

void BigqueryCatalog::ClearCache() {
    schemas.ClearEntries();
}

} // namespace bigquery
} // namespace duckdb
