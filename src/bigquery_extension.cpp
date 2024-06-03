#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "bigquery_attach.hpp"
#include "bigquery_clear_cache.hpp"
#include "bigquery_client.hpp"
#include "bigquery_extension.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_storage.hpp"
#include "bigquery_execute.hpp"

namespace duckdb {

static void SetBigqueryDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
    bigquery::BigqueryClient::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

static void LoadInternal(DatabaseInstance &instance) {

    bigquery::BigqueryAttachFunction bigquery_attach_function;
    ExtensionUtil::RegisterFunction(instance, bigquery_attach_function);

    bigquery::BigqueryScanFunction bigquery_scan_function;
    ExtensionUtil::RegisterFunction(instance, bigquery_scan_function);

    bigquery::BigqueryClearCacheFunction clear_cache_function;
    ExtensionUtil::RegisterFunction(instance, clear_cache_function);

    bigquery::BigQueryExecuteFunction bigquery_execute_function;
    ExtensionUtil::RegisterFunction(instance, bigquery_execute_function);

    auto &config = DBConfig::GetConfig(instance);
    config.storage_extensions["bigquery"] = make_uniq<bigquery::BigqueryStorageExtension>();

    config.AddExtensionOption("bq_experimental_filter_pushdown",
                              "Whether to use filter pushdown (currently experimental)",
                              LogicalType::BOOLEAN,
                              Value(true));
    config.AddExtensionOption("bq_debug_show_queries",
                              "DEBUG SETTING: print all queries sent to BigQuery to stdout",
                              LogicalType::BOOLEAN,
                              Value(true),
                              SetBigqueryDebugQueryPrint);
}

void BigqueryExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

} // namespace duckdb

extern "C" {
using namespace duckdb;

DUCKDB_EXTENSION_API void bigquery_init(DatabaseInstance &db) {
    DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<BigqueryExtension>();
}

DUCKDB_EXTENSION_API const char *bigquery_version() {
    return DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API void bigquery_storage_init(DBConfig &config) {
    config.storage_extensions["bigquery"] = make_uniq<duckdb::bigquery::BigqueryStorageExtension>();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
