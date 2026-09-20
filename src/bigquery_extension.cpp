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
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/extension_callback.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "bigquery_attach.hpp"
#include "bigquery_clear_cache.hpp"
#include "bigquery_client.hpp"
#include "bigquery_execute.hpp"
#include "bigquery_extension.hpp"
#include "bigquery_extract.hpp"
#include "bigquery_geography.hpp"
#include "bigquery_jobs.hpp"
#include "bigquery_load.hpp"
#include "bigquery_parser.hpp"
#include "bigquery_query.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_secrets.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_storage.hpp"
#include "storage/bigquery_optimizer.hpp"

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

namespace bigquery {

static void RegisterDocumentedTableFunction(ExtensionLoader &loader,
                                            TableFunction function,
                                            vector<string> parameter_names,
                                            string description,
                                            string example,
                                            string category) {
    CreateTableFunctionInfo info(std::move(function));
    auto &registered_function = info.functions.GetFunctionReferenceByOffset(0);
    D_ASSERT(parameter_names.size() == registered_function.arguments.size());
    FunctionDescription documentation;
    documentation.parameter_types = registered_function.arguments;
    documentation.parameter_names = std::move(parameter_names);
    // duckdb_functions() replaces all parameter names when a description supplies any names.
    // Append named options in the same order used by DuckDB to expose their types.
    for (const auto &parameter : registered_function.named_parameters) {
        documentation.parameter_names.push_back(parameter.first);
    }
    documentation.description = std::move(description);
    documentation.examples = {std::move(example)};
    documentation.categories = {"bigquery", std::move(category)};
    info.descriptions.push_back(std::move(documentation));
    info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
}

} // namespace bigquery

static void LoadInternal(ExtensionLoader &loader) {

    bigquery::BigqueryAttachFunction bigquery_attach_function;
    bigquery::RegisterDocumentedTableFunction(
        loader,
        std::move(bigquery_attach_function),
        {"dataset"},
        "Create local DuckDB views backed by bigquery_scan for tables in a BigQuery dataset as a compatibility helper; "
        "prefer ATTACH ... (TYPE bigquery) for catalog integration.",
        "SELECT * FROM bigquery_attach('my-gcp-project.my_dataset');",
        "utility");

    bigquery::BigqueryScanFunction bigquery_scan_function;
    bigquery::RegisterDocumentedTableFunction(
        loader,
        std::move(bigquery_scan_function),
        {"table"},
        "Read a fully qualified native BigQuery table through the Storage Read API without creating a DuckDB catalog.",
        "SELECT * FROM bigquery_scan('my-gcp-project.my_dataset.my_table');",
        "read");

    bigquery::BigqueryQueryFunction bigquery_query_function;
    bigquery::RegisterDocumentedTableFunction(
        loader,
        std::move(bigquery_query_function),
        {"project_or_catalog", "sql"},
        "Run GoogleSQL in a BigQuery project or attached catalog and return result rows, optionally binding additional "
        "positional values to ? query parameters.",
        "SELECT * FROM bigquery_query('my-gcp-project', 'SELECT 42 AS answer');",
        "read");

    bigquery::BigqueryClearCacheFunction clear_cache_function;
    bigquery::RegisterDocumentedTableFunction(loader,
                                              std::move(clear_cache_function),
                                              {},
                                              "Clear the local metadata caches of all attached BigQuery catalogs.",
                                              "SELECT * FROM bigquery_clear_cache();",
                                              "utility");

    bigquery::BigQueryExecuteFunction bigquery_execute_function;
    bigquery::RegisterDocumentedTableFunction(
        loader,
        std::move(bigquery_execute_function),
        {"project_or_catalog", "sql"},
        "Run a GoogleSQL statement or script in a BigQuery project or attached catalog and return execution metadata.",
        "SELECT * FROM bigquery_execute('my-gcp-project', 'SELECT 1 AS result');",
        "jobs");

    bigquery::BigQueryExtractFunction bigquery_extract_function;
    bigquery::RegisterDocumentedTableFunction(
        loader,
        std::move(bigquery_extract_function),
        {"project_or_catalog"},
        "Export a BigQuery table to Cloud Storage objects using an extract job and return job metadata.",
        "SELECT * FROM bigquery_extract('my-gcp-project', source_table := 'my_dataset.my_table', "
        "destination_uris := ['gs://my-bucket/export-*.parquet'], format := 'PARQUET');",
        "jobs");

    bigquery::BigQueryListJobsFunction bigquery_list_jobs_function;
    bigquery::RegisterDocumentedTableFunction(loader,
                                              std::move(bigquery_list_jobs_function),
                                              {"project_or_catalog"},
                                              "List BigQuery jobs or retrieve one job by jobId in a project or "
                                              "attached catalog without creating a query job.",
                                              "SELECT * FROM bigquery_jobs('my-gcp-project', maxResults := 10);",
                                              "jobs");

    bigquery::BigQueryLoadFunction bigquery_load_function;
    bigquery::RegisterDocumentedTableFunction(
        loader,
        std::move(bigquery_load_function),
        {"project_or_catalog", "destination_table"},
        "Load a local file, Cloud Storage objects, or a DuckDB table or view into a BigQuery table using a load job "
        "and return job metadata.",
        "SELECT * FROM bigquery_load('my-gcp-project', 'my_dataset.my_table', "
        "source_uris := ['gs://my-bucket/input.parquet'], write_disposition := 'WRITE_EMPTY');",
        "jobs");

    ScalarFunction normalize_geography("bigquery_normalize_geography",
                                       {LogicalType::GEOMETRY()},
                                       LogicalType::GEOMETRY(),
                                       bigquery::BqNormalizeGeographyFunction);
    FunctionDescription normalize_geography_description;
    normalize_geography_description.parameter_types = normalize_geography.arguments;
    normalize_geography_description.parameter_names = {"geometry"};
    normalize_geography_description.description =
        "Normalize a DuckDB geometry locally for BigQuery geography writes, including polygon winding and "
        "touching-hole topology.";
    normalize_geography_description.examples = {
        "bigquery_normalize_geography('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::GEOMETRY('OGC:CRS84'))"};
    normalize_geography_description.categories = {"bigquery", "geometry"};
    CreateScalarFunctionInfo normalize_geography_info(std::move(normalize_geography));
    normalize_geography_info.descriptions.push_back(std::move(normalize_geography_description));
    normalize_geography_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    loader.RegisterFunction(std::move(normalize_geography_info));

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

    // Register BigQuery optimizer extension, which rewrites supported aggregates to bigquery_query queries
    OptimizerExtension bigquery_optimizer;
    bigquery_optimizer.optimize_function = bigquery::BigqueryOptimizer::Optimize;
    OptimizerExtension::Register(config, std::move(bigquery_optimizer));

    // Register configuration options
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
                              "Maximum time to wait for BigQuery query completion in milliseconds; "
                              "0 waits until completion",
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
    config.AddExtensionOption("bq_enable_aggregate_pushdown",
                              "EXPERIMENTAL: rewrite supported BigQuery aggregate queries to query jobs. Unsupported "
                              "shapes fall back before a remote query is started. Runtime errors from started BigQuery "
                              "jobs are not retried locally, and GoogleSQL cast/string/float semantics may differ from "
                              "DuckDB.",
                              LogicalType::BOOLEAN,
                              Value(bigquery::BigquerySettings::EnableAggregatePushdown()),
                              bigquery::BigquerySettings::SetEnableAggregatePushdown);
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
