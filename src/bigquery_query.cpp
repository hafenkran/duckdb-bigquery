#include "bigquery_query.hpp"
#include "bigquery_arrow_scan.hpp"
#include "bigquery_client.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include <arrow/c/bridge.h>

namespace duckdb {
namespace bigquery {

static unique_ptr<FunctionData> BigqueryQueryBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto query_string = input.inputs[1].GetValue<string>();

    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, dbname_or_project_id);

    auto params = BigQueryCommonParameters::ParseFromNamedParameters(input.named_parameters);

    // Handle dry_run case separately
    if (params.dry_run) {
        return_types.emplace_back(LogicalTypeId::BIGINT);
        names.emplace_back("total_bytes_processed");
        return_types.emplace_back(LogicalTypeId::BOOLEAN);
        names.emplace_back("cache_hit");
        return_types.emplace_back(LogicalTypeId::VARCHAR);
        names.emplace_back("location");

        auto result = make_uniq<BigqueryQueryDryRunBindData>();
        result->query = query_string;

        if (database) {
            auto &catalog = database->GetCatalog();
            if (catalog.GetCatalogType() != "bigquery") {
                throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
            }
            if (!params.api_endpoint.empty() || !params.grpc_endpoint.empty()) {
                throw BinderException("Named parameters are not supported for attached databases");
            }

            auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
            auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);

            result->config = bigquery_catalog.config;
            result->bq_client = transaction.GetBigqueryClient();
        } else {
            auto bq_config = BigqueryConfig(dbname_or_project_id)
                                 .SetApiEndpoint(params.api_endpoint)
                                 .SetGrpcEndpoint(params.grpc_endpoint);
            auto bq_client = make_shared_ptr<BigqueryClient>(bq_config);

            result->config = bq_config;
            result->bq_client = bq_client;
        }
        return result;
    }

    if (!params.use_legacy_scan) {
        auto bind_data = make_uniq<BigqueryArrowScanBindData>();
        bind_data->query = query_string;
        bind_data->estimated_row_count = 1;

        if (database) {
            auto &catalog = database->GetCatalog();
            if (catalog.GetCatalogType() != "bigquery") {
                throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
            }
            if (!params.billing_project.empty() || !params.api_endpoint.empty() || !params.grpc_endpoint.empty()) {
                throw BinderException("Named parameters are not supported for attached databases");
            }

            auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
            auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);

            bind_data->bq_config = bigquery_catalog.config;
            bind_data->bq_client = transaction.GetBigqueryClient();
        } else {
            auto bq_config = BigqueryConfig(dbname_or_project_id)
                                 .SetBillingProjectId(params.billing_project)
                                 .SetApiEndpoint(params.api_endpoint)
                                 .SetGrpcEndpoint(params.grpc_endpoint);
            auto bq_client = make_shared_ptr<BigqueryClient>(bq_config);

            bind_data->bq_config = bq_config;
            bind_data->bq_client = bq_client;
        }

        ColumnList columns;
        vector<unique_ptr<Constraint>> constraints;
        bind_data->bq_client->GetTableInfoForQuery(query_string, columns, constraints);

        auto arrow_schema_ptr = BigqueryUtils::BuildArrowSchema(columns);
        auto status = arrow::ExportSchema(*std::move(arrow_schema_ptr), &bind_data->schema_root.arrow_schema);
        if (!status.ok()) {
            throw BinderException("Arrow schema export failed: " + status.ToString());
        }

        // Populate names/types using updated DuckDB Arrow API
        ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context),
                                                     bind_data->arrow_table,
                                                     bind_data->schema_root.arrow_schema);
        names = bind_data->arrow_table.GetNames();
        return_types = bind_data->arrow_table.GetTypes();

        if (return_types.empty()) {
            throw BinderException("BigQuery query has no columns: " + query_string);
        }

        bind_data->names = names;
        bind_data->all_types = return_types;

        // Check if we need type mapping for enhanced BigQuery types (including WKT to GEOMETRY conversion)
        bool requires_cast = false;
        vector<LogicalType> mapped_bq_types;
        for (idx_t i = 0; i < return_types.size(); i++) {
            auto bq_type = BigqueryUtils::CastToBigqueryTypeWithSpatialConversion(return_types[i], &context);
            if (bq_type != return_types[i]) {
                requires_cast = true;
            }
            mapped_bq_types.push_back(bq_type);
        }

        if (requires_cast) {
            bind_data->mapped_bq_types = std::move(mapped_bq_types);
        }
        bind_data->requires_cast = requires_cast;

        return std::move(bind_data);
    } else {
        // Legacy implementation (V1)
        auto bind_data = make_uniq<BigqueryLegacyScanBindData>();
        bind_data->query = query_string;
        bind_data->estimated_row_count = 1;

        if (database) {
            auto &catalog = database->GetCatalog();
            if (catalog.GetCatalogType() != "bigquery") {
                throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
            }
            if (!params.billing_project.empty() || !params.api_endpoint.empty() || !params.grpc_endpoint.empty()) {
                throw BinderException("Named parameters are not supported for attached databases");
            }

            auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
            auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);

            bind_data->config = bigquery_catalog.config;
            bind_data->bq_client = transaction.GetBigqueryClient();
        } else {
            // Use the provided project_id of the gcp project
            auto bq_config = BigqueryConfig(dbname_or_project_id)
                                 .SetBillingProjectId(params.billing_project)
                                 .SetApiEndpoint(params.api_endpoint)
                                 .SetGrpcEndpoint(params.grpc_endpoint);
            auto bq_client = make_shared_ptr<BigqueryClient>(bq_config);

            bind_data->config = bq_config;
            bind_data->bq_client = bq_client;
        }

        ColumnList columns;
        vector<unique_ptr<Constraint>> constraints;
        bind_data->bq_client->GetTableInfoForQuery(query_string, columns, constraints);

        for (auto &column : columns.Logical()) {
            names.push_back(column.GetName());
            return_types.push_back(column.GetType());
        }
        if (names.empty()) {
            throw std::runtime_error("no columns for query: " + query_string);
        }

        if (BigquerySettings::GeographyAsGeometry()) {
            for (const auto &column : columns.Logical()) {
                if (BigqueryUtils::IsGeographyType(column.GetType())) {
                    throw BinderException(
                        "BigQuery GEOGRAPHY columns with geography_as_geometry=true are not supported in legacy scan. "
                        "Please either set use_legacy_scan=false (recommended) or set bq_geography_as_geometry=false.");
                }
            }
        }

        bind_data->names = names;
        bind_data->types = return_types;
        return std::move(bind_data);
    }
}

static unique_ptr<GlobalTableFunctionState> BigqueryQueryInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    // Dry run
    if (dynamic_cast<const BigqueryQueryDryRunBindData *>(input.bind_data.get())) {
        return make_uniq<GlobalTableFunctionState>();
    }
	// New Scan
    if (dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        // Arrow scan implementation
        auto &mutable_bind_data = input.bind_data->CastNoConst<BigqueryArrowScanBindData>();

        // Execute the query and get destination table
        auto query_response = mutable_bind_data.bq_client->ExecuteQuery(mutable_bind_data.query);
        auto job = mutable_bind_data.bq_client->GetJobByReference(query_response.job_reference());

        if (job.status().has_error_result()) {
            throw BinderException(job.status().error_result().message());
        }

        auto destination_table = job.configuration().query().destination_table();
        auto table_ref = BigqueryTableRef(destination_table.project_id(),
                                          destination_table.dataset_id(),
                                          destination_table.table_id());
        mutable_bind_data.table_ref = table_ref;
        return BigqueryArrowScanFunction::BigqueryArrowScanInitGlobal(context, input);
    } else {
        // Legacy scan implementation
        auto &bind_data = input.bind_data->CastNoConst<BigqueryLegacyScanBindData>();

        // Execute the query and get destination table
        auto query_response = bind_data.bq_client->ExecuteQuery(bind_data.query);
        auto job = bind_data.bq_client->GetJobByReference(query_response.job_reference());

        if (job.status().has_error_result()) {
            throw BinderException(job.status().error_result().message());
        }

        auto destination_table = job.configuration().query().destination_table();
        auto table_ref = BigqueryTableRef(destination_table.project_id(),
                                          destination_table.dataset_id(),
                                          destination_table.table_id());
        bind_data.table_ref = table_ref;
        return BigqueryLegacyScanFunction::BigqueryLegacyScanInitGlobalState(context, input);
    }
}

static unique_ptr<LocalTableFunctionState> BigqueryQueryInitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
    // Dry run
    if (dynamic_cast<const BigqueryQueryDryRunBindData *>(input.bind_data.get())) {
        return make_uniq<LocalTableFunctionState>();
    }
    // New Scan
    if (dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        return BigqueryArrowScanFunction::BigqueryArrowScanInitLocal(context, input, global_state);
    }
    // Legacy scan
    return BigqueryLegacyScanFunction::BigqueryLegacyScanInitLocalState(context, input, global_state);
}

static void BigqueryQueryExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    D_ASSERT(data.bind_data);
    const auto *base = data.bind_data.get();

	// Dry run
    if (dynamic_cast<const BigqueryQueryDryRunBindData *>(base)) {
        auto &bind_data = base->CastNoConst<BigqueryQueryDryRunBindData>();
        if (bind_data.finished) {
            return;
        }

        auto response = bind_data.bq_client->ExecuteQuery(bind_data.query, "", true);
        bind_data.finished = true;

        output.SetValue(0, 0, Value::BIGINT(response.total_bytes_processed().value()));
        output.SetValue(1, 0, Value::BOOLEAN(response.cache_hit().value()));
        output.SetValue(2, 0, response.job_reference().location().value());
        output.SetCardinality(1);
        return;
    }

    // New Scan
    if (dynamic_cast<const BigqueryArrowScanBindData *>(data.bind_data.get())) {
        BigqueryArrowScanFunction::BigqueryArrowScanExecute(context, data, output);
        return;
    }

    // Legacy scan
    BigqueryLegacyScanFunction::BigqueryLegacyScanExecute(context, data, output);
}

static BindInfo BigqueryQueryGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    D_ASSERT(bind_data_p);
    const auto *base = bind_data_p.get();
    BindInfo info(ScanType::EXTERNAL);

    // Dry Run
    if (dynamic_cast<const BigqueryQueryDryRunBindData *>(base)) {
        return info;
    }

    // New Scan
    if (const auto *arrow = dynamic_cast<const BigqueryArrowScanBindData *>(base)) {
        if (arrow->bq_table_entry) {
            info.table = arrow->bq_table_entry.get_mutable();
        }
        return info;
    }

    // Legacy scan
    const auto &legacy = base->Cast<BigqueryLegacyScanBindData>();
    if (legacy.bq_table_entry) {
        info.table = legacy.bq_table_entry.get_mutable();
    }
    return info;
}

static InsertionOrderPreservingMap<string> BigqueryQueryToString(TableFunctionToStringInput &input) {
    D_ASSERT(input.bind_data);

    InsertionOrderPreservingMap<string> result;
    const auto *base = input.bind_data.get();

    // Dry Run
    if (const auto *dry_run_bind_data = dynamic_cast<const BigqueryQueryDryRunBindData *>(base)) {
        result["Query"] = dry_run_bind_data->query;
        result["Type"] = "Dry Run";
        return result;
    }

    // New Scan
    if (const auto *arrow_bind_data = dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        result["Query"] = arrow_bind_data->query;
        result["Table"] = arrow_bind_data->TableString();
        return result;
    }

    // Legacy scan
    const auto &bind_data = input.bind_data->Cast<BigqueryLegacyScanBindData>();
    result["Query"] = bind_data.query;
    result["Table"] = bind_data.TableString();
    return result;
}

static unique_ptr<NodeStatistics> BigqueryQueryCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    D_ASSERT(bind_data_p);
    const auto *base = bind_data_p;

    // Dry run
    if (dynamic_cast<const BigqueryQueryDryRunBindData *>(base)) {
        return make_uniq<NodeStatistics>(1, 1);
    }

    // New Scan
    if (const auto *arrow = dynamic_cast<const BigqueryArrowScanBindData *>(base)) {
        const idx_t n = arrow->estimated_row_count;
        return make_uniq<NodeStatistics>(n, n);
    }

    // Legacy scan
    const auto &legacy = base->Cast<BigqueryLegacyScanBindData>();
    const idx_t n = legacy.estimated_row_count;
    return make_uniq<NodeStatistics>(n, n);
}

static double BigqueryQueryProgress(ClientContext &context,
                                    const FunctionData *bind_data_p,
                                    const GlobalTableFunctionState *global_state) {
    D_ASSERT(bind_data_p);
    D_ASSERT(global_state);

    // Dry run
    if (dynamic_cast<const BigqueryQueryDryRunBindData *>(bind_data_p)) {
        return 100.0;
    }

    const auto *b = bind_data_p;
    const auto *gs = global_state;

    idx_t estimated = 0;
    idx_t position = 0;

    // New Scan
    if (const auto *arrow = dynamic_cast<const BigqueryArrowScanBindData *>(b)) {
        auto &gstate = gs->Cast<BigqueryArrowScanGlobalState>();
        estimated = arrow->estimated_row_count;
        if (estimated > 0) {
            lock_guard<mutex> glock(gstate.lock);
            position = gstate.position;
        }
    } else {
        // Legacy scan
        const auto &bind = b->Cast<BigqueryLegacyScanBindData>();
        auto &gstate = gs->Cast<BigqueryGlobalFunctionState>();
        estimated = bind.estimated_row_count;
        if (estimated > 0) {
            lock_guard<mutex> glock(gstate.lock);
            position = gstate.position;
        }
    }

    double progress = 0.0;
    if (estimated > 0) {
        progress = 100.0 * static_cast<double>(position) / static_cast<double>(estimated);
    }
    return MinValue<double>(100.0, progress);
}

BigqueryQueryFunction::BigqueryQueryFunction()
    : TableFunction("bigquery_query",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    BigqueryQueryExecute,
                    BigqueryQueryBind,
                    BigqueryQueryInitGlobal,
                    BigqueryQueryInitLocal) {
    to_string = BigqueryQueryToString;
    cardinality = BigqueryQueryCardinality;
    table_scan_progress = BigqueryQueryProgress;
    get_bind_info = BigqueryQueryGetBindInfo;

    projection_pushdown = true;
    filter_pushdown = true;
    filter_prune = true;

    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["use_legacy_scan"] = LogicalType::BOOLEAN;
    named_parameters["dry_run"] = LogicalType::BOOLEAN;
}

} // namespace bigquery
} // namespace duckdb
