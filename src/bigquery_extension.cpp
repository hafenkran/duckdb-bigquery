#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "bigquery_attach.hpp"
#include "bigquery_clear_cache.hpp"
#include "bigquery_client.hpp"
#include "bigquery_execute.hpp"
#include "bigquery_extension.hpp"
#include "bigquery_jobs.hpp"
#include "bigquery_parser.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_arrow_scan.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_storage.hpp"

#include <iostream>

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {

    bigquery::BigqueryAttachFunction bigquery_attach_function;
    ExtensionUtil::RegisterFunction(instance, bigquery_attach_function);

    bigquery::BigqueryScanFunction bigquery_scan_function;
    ExtensionUtil::RegisterFunction(instance, bigquery_scan_function);

	bigquery::BigQueryArrowScanFunction bigquery_arrow_scan_function;
	ExtensionUtil::RegisterFunction(instance, bigquery_arrow_scan_function);

    bigquery::BigqueryQueryFunction bigquery_query_function;
    ExtensionUtil::RegisterFunction(instance, bigquery_query_function);

    bigquery::BigqueryClearCacheFunction clear_cache_function;
    ExtensionUtil::RegisterFunction(instance, clear_cache_function);

    bigquery::BigQueryExecuteFunction bigquery_execute_function;
    ExtensionUtil::RegisterFunction(instance, bigquery_execute_function);

    bigquery::BigQueryListJobsFunction bigquery_list_jobs_function;
    ExtensionUtil::RegisterFunction(instance, bigquery_list_jobs_function);

    auto &config = DBConfig::GetConfig(instance);
    config.storage_extensions["bigquery"] = make_uniq<bigquery::BigqueryStorageExtension>();


    bigquery::BigqueryParserExtension bigquery_parser_extension;
    config.parser_extensions.push_back(bigquery_parser_extension);

    auto operator_extension = make_uniq<bigquery::BigqueryOperatorExtension>();
    config.operator_extensions.push_back(move(operator_extension));

    config.AddExtensionOption("bq_bignumeric_as_varchar",
                              "Read BigQuery BIGNUMERIC data type as VARCHAR",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::BignumericAsVarchar()),
                              bigquery::BigquerySettings::SetBignumericAsVarchar);
    config.AddExtensionOption("bq_default_location",
                              "Default location for BigQuery queries",
                              LogicalType::VARCHAR,
                              Value(bigquery::BigquerySettings::DefaultLocation()),
                              bigquery::BigquerySettings::SetDefaultLocation);
    config.AddExtensionOption("bq_query_timeout_ms",
                              "Timeout for BigQuery queries in milliseconds",
                              LogicalType::BIGINT,
                              Value(bigquery::BigquerySettings::QueryTimeoutMs()),
                              bigquery::BigquerySettings::SetQueryTimeoutMs);
    config.AddExtensionOption("bq_experimental_filter_pushdown",
                              "Whether to use filter pushdown (currently experimental)",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::ExperimentalFilterPushdown()),
                              bigquery::BigquerySettings::SetExperimentalFilterPushdown);
    config.AddExtensionOption("bq_experimental_use_info_schema",
                              "Whether to fetch table infos from BQ information schema (currently experimental). Can "
                              "be significantly faster than fetching from REST API.",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::ExperimentalFetchCatalogFromInformationSchema()),
                              bigquery::BigquerySettings::SetExperimentalFetchCatalogFromInformationSchema);
    config.AddExtensionOption("bq_experimental_enable_bigquery_options",
                              "Whether to enable BigQuery OPTIONS in CREATE statements",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::ExperimentalEnableBigqueryOptions()),
                              bigquery::BigquerySettings::SetExperimentalEnableBigqueryOptions);
    config.AddExtensionOption("bq_debug_show_queries",
                              "DEBUG SETTING: print all queries sent to BigQuery to stdout",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::DebugQueryPrint()),
                              bigquery::BigquerySettings::SetDebugQueryPrint);
    config.AddExtensionOption("bq_curl_ca_bundle_path",
                              "Path to the CA bundle for curl",
                              LogicalType::VARCHAR,
                              Value(bigquery::BigquerySettings::CurlCaBundlePath()),
                              bigquery::BigquerySettings::SetCurlCaBundlePath);
    config.AddExtensionOption("bq_max_read_streams",
                              "Maximum number of read streams for BigQuery Storage Read. Set to 0 to automatically "
                              "match the number of DuckDB threads. `preserve_insertion_order` must be false for "
                              "parallelization to work.",
                              LogicalType::BIGINT,
                              Value(bigquery::BigquerySettings::MaxReadStreams()),
                              bigquery::BigquerySettings::SetMaxReadStreams);
    config.AddExtensionOption("bq_arrow_compression",
                              "Compression codec for BigQuery Storage Read API. Options: UNSPECIFIED, LZ4_FRAME, ZSTD."
                              "Default is LZ4_FRAME.",
                              LogicalType::VARCHAR,
                              Value(bigquery::BigquerySettings::ArrowCompression()),
                              bigquery::BigquerySettings::SetArrowCompression);
    config.AddExtensionOption("bq_experimental_use_incubating_scan",
                              "Whether to use the incubating BigQuery scan implementation. This is currently "
                              "experimental and is targeted to become the default in the future. ",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::ExperimentalIncubatingScan()),
                              bigquery::BigquerySettings::SetExperimentalIncubatingScan);
}

void BigqueryExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void bigquery_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::BigqueryExtension>();
}

DUCKDB_EXTENSION_API const char *bigquery_version() {
    return duckdb::DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API void bigquery_storage_init(duckdb::DBConfig &config) {
    config.storage_extensions["bigquery"] = duckdb::make_uniq<duckdb::bigquery::BigqueryStorageExtension>();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
