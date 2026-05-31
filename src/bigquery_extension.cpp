#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/planner/extension_callback.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "bigquery_attach.hpp"
#include "bigquery_clear_cache.hpp"
#include "bigquery_client.hpp"
#include "bigquery_execute.hpp"
#include "bigquery_export.hpp"
#include "bigquery_extension.hpp"
#include "bigquery_geography.hpp"
#include "bigquery_jobs.hpp"
#include "bigquery_load.hpp"
#include "bigquery_parser.hpp"
#include "bigquery_query.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_secrets.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_storage.hpp"

namespace duckdb {

static constexpr const char *BIGQUERY_EXTENSION_STATE = "bigquery_extension";

class BigqueryExtensionState : public ClientContextState {
public:
    bool CanRequestRebind() override {
        return true;
    }

    RebindQueryInfo OnPlanningError(ClientContext &context, SQLStatement &statement, ErrorData &error) override {
        (void)statement;
        if (error.Type() != ExceptionType::BINDER) {
            return RebindQueryInfo::DO_NOT_REBIND;
        }

        auto &extra_info = error.ExtraInfo();
        auto entry = extra_info.find("error_subtype");
        if (entry == extra_info.end() || entry->second != "COLUMN_NOT_FOUND") {
            return RebindQueryInfo::DO_NOT_REBIND;
        }

        bigquery::BigqueryClearCacheFunction::ClearBigqueryCaches(context);
        return RebindQueryInfo::ATTEMPT_TO_REBIND;
    }
};

class BigqueryExtensionCallback : public ExtensionCallback {
public:
    void OnConnectionOpened(ClientContext &context) override {
        context.registered_state->Insert(BIGQUERY_EXTENSION_STATE, make_shared_ptr<BigqueryExtensionState>());
    }
};

static void LoadInternal(ExtensionLoader &loader) {

    bigquery::BigqueryAttachFunction bigquery_attach_function;
    loader.RegisterFunction(bigquery_attach_function);

    bigquery::BigqueryScanFunction bigquery_scan_function;
    loader.RegisterFunction(bigquery_scan_function);

    bigquery::BigqueryQueryFunction bigquery_query_function;
    loader.RegisterFunction(bigquery_query_function);

    bigquery::BigqueryClearCacheFunction clear_cache_function;
    loader.RegisterFunction(clear_cache_function);

    bigquery::BigQueryExecuteFunction bigquery_execute_function;
    loader.RegisterFunction(bigquery_execute_function);

    bigquery::BigQueryExportFunction bigquery_export_function;
    loader.RegisterFunction(bigquery_export_function);

    bigquery::BigQueryListJobsFunction bigquery_list_jobs_function;
    loader.RegisterFunction(bigquery_list_jobs_function);

    bigquery::BigQueryLoadFunction bigquery_load_function;
    loader.RegisterFunction(bigquery_load_function);

    ScalarFunction normalize_geography("bigquery_normalize_geography",
                                       {LogicalType::GEOMETRY()},
                                       LogicalType::GEOMETRY(),
                                       bigquery::BqNormalizeGeographyFunction);
    loader.RegisterFunction(normalize_geography);

    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    StorageExtension::Register(config, "bigquery", make_shared_ptr<bigquery::BigqueryStorageExtension>());
    ExtensionCallback::Register(config, make_shared_ptr<BigqueryExtensionCallback>());
    for (auto &connection : ConnectionManager::Get(loader.GetDatabaseInstance()).GetConnectionList()) {
        connection->registered_state->Insert(BIGQUERY_EXTENSION_STATE, make_shared_ptr<BigqueryExtensionState>());
    }

    bigquery::RegisterBigquerySecretType(loader.GetDatabaseInstance());

    // Register BigQuery GEOGRAPHY -> DuckDB GEOMETRY cast using core geometry parsing.
    bigquery::RegisterGeographyCast(loader.GetDatabaseInstance());

    bigquery::BigqueryParserExtension bigquery_parser_extension;
    ParserExtension::Register(config, bigquery_parser_extension);

    auto operator_extension = make_shared_ptr<bigquery::BigqueryOperatorExtension>();
    OperatorExtension::Register(config, std::move(operator_extension));

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
    config.AddExtensionOption(
        "bq_query_timeout_ms",
        "Maximum time to wait for BigQuery query completion in milliseconds; 0 waits until completion",
        LogicalType::BIGINT,
        Value(bigquery::BigquerySettings::QueryTimeoutMs()),
        bigquery::BigquerySettings::SetQueryTimeoutMs);
    config.AddExtensionOption("bq_auth_timeout_s",
                              "Timeout for BigQuery authentication token fetches in seconds",
                              LogicalType::BIGINT,
                              Value(bigquery::BigquerySettings::AuthTimeoutSeconds()),
                              bigquery::BigquerySettings::SetAuthTimeoutSeconds);
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
    config.AddExtensionOption("bq_experimental_enable_sql_parser",
                              "Whether to enable BigQuery CREATE TABLE clause parsing extensions",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::ExperimentalEnableSqlParser()),
                              bigquery::BigquerySettings::SetExperimentalEnableSqlParser);
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
                              "Maximum number of read streams requested for BigQuery Storage Read. Set to 0 to match "
                              "the number of DuckDB threads. `preserve_insertion_order` must be false for "
                              "parallelization to work, and BigQuery may return fewer streams than requested.",
                              LogicalType::BIGINT,
                              Value(bigquery::BigquerySettings::MaxReadStreams()),
                              bigquery::BigquerySettings::SetMaxReadStreams);
    config.AddExtensionOption("bq_enable_inflight_request_windowing",
                              "Whether to allow multiple BigQuery Storage Write AppendRows requests to remain in "
                              "flight before waiting for acknowledgements. Usually faster, but slightly less memory "
                              "efficient because more unacknowledged requests can be buffered at once.",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::EnableInflightRequestWindowing()),
                              bigquery::BigquerySettings::SetEnableInflightRequestWindowing);
    config.AddExtensionOption("bq_arrow_compression",
                              "Compression codec for BigQuery Storage Read API. Options: UNSPECIFIED, LZ4_FRAME, ZSTD."
                              "Default is LZ4_FRAME.",
                              LogicalType::VARCHAR,
                              Value(bigquery::BigquerySettings::ArrowCompression()),
                              bigquery::BigquerySettings::SetArrowCompression);
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
