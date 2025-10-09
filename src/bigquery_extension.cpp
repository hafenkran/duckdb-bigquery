#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "bigquery_arrow_scan.hpp"
#include "bigquery_attach.hpp"
#include "bigquery_clear_cache.hpp"
#include "bigquery_client.hpp"
#include "bigquery_execute.hpp"
#include "bigquery_extension.hpp"
#include "bigquery_geometry_cast.hpp"
#include "bigquery_jobs.hpp"
#include "bigquery_parser.hpp"
#include "bigquery_query.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_secrets.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_storage.hpp"

#include <iostream>

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {

    bigquery::BigqueryAttachFunction bigquery_attach_function;
    loader.RegisterFunction(bigquery_attach_function);

    bigquery::BigqueryScanFunction bigquery_scan_function;
    loader.RegisterFunction(bigquery_scan_function);

    bigquery::BigqueryArrowScanFunction bigquery_arrow_scan_function;
    loader.RegisterFunction(bigquery_arrow_scan_function);

    bigquery::BigqueryQueryFunction bigquery_query_function;
    loader.RegisterFunction(bigquery_query_function);

    bigquery::BigqueryClearCacheFunction clear_cache_function;
    loader.RegisterFunction(clear_cache_function);

    bigquery::BigQueryExecuteFunction bigquery_execute_function;
    loader.RegisterFunction(bigquery_execute_function);

    bigquery::BigQueryListJobsFunction bigquery_list_jobs_function;
    loader.RegisterFunction(bigquery_list_jobs_function);

    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    config.storage_extensions["bigquery"] = make_uniq<bigquery::BigqueryStorageExtension>();

	bigquery::RegisterBigquerySecretType(loader.GetDatabaseInstance());

    // Register WKT->GEOMETRY cast (runtime lookup of spatial extension's ST_GeomFromText)
    bigquery::RegisterWKTGeometryCast(loader.GetDatabaseInstance());

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
    config.AddExtensionOption("bq_use_legacy_scan",
                              "Whether to use legacy scan implementation for BigQuery tables. "
                              "Default is false (uses optimized Arrow-based implementation).",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::UseLegacyScan()),
                              bigquery::BigquerySettings::SetUseLegacyScan);
    config.AddExtensionOption("bq_geography_as_geometry",
                              "Whether to return BigQuery GEOGRAPHY columns as DuckDB GEOMETRY types "
                              "(requires spatial extension). Default is false (returns WKT strings).",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::GeographyAsGeometry()),
                              bigquery::BigquerySettings::SetGeographyAsGeometry);

    // Deprecated setting
    config.AddExtensionOption(
        "bq_experimental_use_incubating_scan",
        "Whether to use the incubating BigQuery scan implementation. This is currently "
        "experimental and is targeted to become the default in the future. "
        "DEPRECATED: Use bq_use_legacy_scan instead. This setting will be removed in a future version.",
        LogicalType::BOOLEAN,
        Value(true),
        bigquery::BigquerySettings::SetExperimentalIncubatingScan);
}

void BigqueryExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string BigqueryExtension::Name() {
    return "bigquery";
}

std::string BigqueryExtension::Version() const {
#ifdef EXT_VERSION_BIGQUERY
    return EXT_VERSION_BIGQUERY;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(bigquery, loader) {
    duckdb::LoadInternal(loader);
}
}
