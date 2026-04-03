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

//! Parse BigQuery's canonical interval format: "Y-M D H:M:S[.F]"
//! e.g. "1-2 3 4:5:6" = 1 year, 2 months, 3 days, 4 hours, 5 minutes, 6 seconds
static Value ParseBigQueryInterval(const string &str) {
    int32_t years = 0, months_part = 0, days = 0, hours = 0, minutes = 0, seconds = 0, micros = 0;
    // Format: "Y-M D H:Min:S" or "Y-M D H:Min:S.F"
    // Find the dash separating years and months
    auto dash_pos = str.find('-');
    if (dash_pos == string::npos) {
        throw InvalidInputException("Cannot parse BigQuery INTERVAL: %s", str);
    }
    years = std::stoi(str.substr(0, dash_pos));

    auto space1 = str.find(' ', dash_pos);
    if (space1 == string::npos) {
        throw InvalidInputException("Cannot parse BigQuery INTERVAL: %s", str);
    }
    months_part = std::stoi(str.substr(dash_pos + 1, space1 - dash_pos - 1));

    auto space2 = str.find(' ', space1 + 1);
    if (space2 == string::npos) {
        throw InvalidInputException("Cannot parse BigQuery INTERVAL: %s", str);
    }
    days = std::stoi(str.substr(space1 + 1, space2 - space1 - 1));

    // Parse H:M:S[.F]
    auto colon1 = str.find(':', space2);
    auto colon2 = str.find(':', colon1 + 1);
    if (colon1 == string::npos || colon2 == string::npos) {
        throw InvalidInputException("Cannot parse BigQuery INTERVAL: %s", str);
    }
    hours = std::stoi(str.substr(space2 + 1, colon1 - space2 - 1));
    minutes = std::stoi(str.substr(colon1 + 1, colon2 - colon1 - 1));

    auto dot_pos = str.find('.', colon2);
    if (dot_pos != string::npos) {
        seconds = std::stoi(str.substr(colon2 + 1, dot_pos - colon2 - 1));
        auto frac_str = str.substr(dot_pos + 1);
        // Pad or truncate to 6 digits for microseconds
        while (frac_str.size() < 6) frac_str += '0';
        micros = std::stoi(frac_str.substr(0, 6));
    } else {
        seconds = std::stoi(str.substr(colon2 + 1));
    }

    int32_t total_months = years * 12 + months_part;
    int64_t total_micros = (static_cast<int64_t>(hours) * 3600 + static_cast<int64_t>(minutes) * 60 +
                            static_cast<int64_t>(seconds)) * 1000000 + micros;
    return Value::INTERVAL(interval_t{total_months, days, total_micros});
}

//! Decode a base64-encoded string to raw bytes (for BYTES type)
static string Base64Decode(const string &encoded) {
    static const string base64_chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    string decoded;
    int val = 0, valb = -8;
    for (char c : encoded) {
        if (c == '=' || c == '\n' || c == '\r') break;
        auto pos = base64_chars.find(c);
        if (pos == string::npos) continue;
        val = (val << 6) + static_cast<int>(pos);
        valb += 6;
        if (valb >= 0) {
            decoded.push_back(static_cast<char>((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    return decoded;
}

//! Convert a single REST API value (from the "v" field of a row struct) to a DuckDB Value.
//! BigQuery REST API returns all values as strings in the JSON response.
static Value RestValueToValue(const google::protobuf::Value &val, const LogicalType &type) {
    if (val.kind_case() == google::protobuf::Value::kNullValue) {
        return Value(type);
    }

    // STRUCT and LIST types arrive as nested protobuf structures, not strings.
    // The REST path does not support these — use the Storage API path instead.
    if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::LIST) {
        throw NotImplementedException("REST API path (use_rest_api=true) does not support %s columns. "
                                      "Remove use_rest_api to use the Storage API path instead.",
                                      LogicalTypeIdToString(type.id()));
    }

    auto str = val.string_value();

    switch (type.id()) {
    case LogicalTypeId::VARCHAR:
        return Value(str);
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_TZ: {
        // BigQuery REST API returns TIMESTAMP as epoch seconds (fractional)
        // but DATETIME as ISO string "YYYY-MM-DDTHH:MM:SS[.SSSSSS]".
        // Detect which format we have.
        if (!str.empty() && (str.find('T') != string::npos || str.find('-') != string::npos)) {
            // ISO format (DATETIME) — let DuckDB parse it
            return Value(str).DefaultCastAs(type);
        }
        // Epoch seconds (TIMESTAMP)
        double epoch_seconds = std::stod(str);
        int64_t micros = static_cast<int64_t>(epoch_seconds * 1000000.0);
        return Value::TIMESTAMP(timestamp_t(micros));
    }
    case LogicalTypeId::BLOB: {
        // BigQuery REST API returns BYTES as base64-encoded strings
        auto decoded = Base64Decode(str);
        return Value::BLOB(decoded);
    }
    case LogicalTypeId::INTERVAL:
        // BigQuery REST API returns INTERVAL as "Y-M D H:M:S[.F]"
        return ParseBigQueryInterval(str);
    default:
        // BigQuery REST API returns all scalar values as strings.
        // This handles BOOLEAN, INTEGER, FLOAT, DATE, TIME, NUMERIC,
        // GEOGRAPHY (WKT string → GEOMETRY), and other scalar types.
        return Value(str).DefaultCastAs(type);
    }
}

//! Fill a DataChunk from REST API inline rows
static void FillChunkFromRestRows(
    const google::protobuf::RepeatedPtrField<::google::protobuf::Struct> &rows,
    idx_t start_row,
    idx_t count,
    const vector<LogicalType> &types,
    DataChunk &output) {

    idx_t actual_count = MinValue<idx_t>(count, static_cast<idx_t>(rows.size()) - start_row);
    for (idx_t row_idx = 0; row_idx < actual_count; row_idx++) {
        auto &row = rows[static_cast<int>(start_row + row_idx)];
        const auto &fields = row.fields();
        const auto &field_list = fields.at("f").list_value().values();

        for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
            const auto &field_val = field_list[static_cast<int>(col_idx)].struct_value().fields().at("v");
            auto value = RestValueToValue(field_val, types[col_idx]);
            output.SetValue(col_idx, row_idx, value);
        }
    }
    output.SetCardinality(actual_count);
}

static unique_ptr<FunctionData> BigqueryQueryBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto query_string = input.inputs[1].GetValue<string>();
    vector<Value> query_parameters;
    for (idx_t i = 2; i < input.inputs.size(); i++) {
        query_parameters.emplace_back(input.inputs[i]);
    }

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
        result->query_parameters = query_parameters;

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
            auto bq_client = make_shared_ptr<BigqueryClient>(context, bq_config);

            result->config = bq_config;
            result->bq_client = bq_client;
        }
        return result;
    }

    // REST-only path (opt-in, fast, no Storage API overhead)
    if (params.use_rest_api) {
        auto bind_data = make_uniq<BigqueryQueryRestBindData>();
        bind_data->query = query_string;
        bind_data->query_parameters = query_parameters;

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
            auto bq_config = BigqueryConfig(dbname_or_project_id)
                                 .SetBillingProjectId(params.billing_project)
                                 .SetApiEndpoint(params.api_endpoint)
                                 .SetGrpcEndpoint(params.grpc_endpoint);
            auto bq_client = make_shared_ptr<BigqueryClient>(context, bq_config);

            bind_data->config = bq_config;
            bind_data->bq_client = bq_client;
        }

        ColumnList columns;
        vector<unique_ptr<Constraint>> constraints;
        bind_data->bq_client->GetTableInfoForQuery(query_string, bind_data->query_parameters, columns, constraints);

        for (auto &column : columns.Logical()) {
            names.push_back(column.GetName());
            return_types.push_back(column.GetType());
        }
        if (names.empty()) {
            throw BinderException("BigQuery query has no columns: " + query_string);
        }

        bind_data->names = names;
        bind_data->types = return_types;
        return std::move(bind_data);
    }

    // Default path: Storage API
    if (!params.use_legacy_scan) {
        auto bind_data = make_uniq<BigqueryArrowScanBindData>();
        bind_data->query = query_string;
        bind_data->query_parameters = query_parameters;
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
            auto bq_client = make_shared_ptr<BigqueryClient>(context, bq_config);

            bind_data->bq_config = bq_config;
            bind_data->bq_client = bq_client;
        }

        ColumnList columns;
        vector<unique_ptr<Constraint>> constraints;
        bind_data->bq_client->GetTableInfoForQuery(query_string, bind_data->query_parameters, columns, constraints);

        auto arrow_schema_ptr = BigqueryUtils::BuildArrowSchema(columns);
        auto status = arrow::ExportSchema(*std::move(arrow_schema_ptr), &bind_data->schema_root.arrow_schema);
        if (!status.ok()) {
            throw BinderException("Arrow schema export failed: " + status.ToString());
        }

        vector<LogicalType> mapped_bq_types;
        BigqueryUtils::PopulateAndMapArrowTableTypes(context,
                                                     bind_data->arrow_table,
                                                     bind_data->schema_root,
                                                     names,
                                                     return_types,
                                                     mapped_bq_types,
                                                     &columns);

        if (return_types.empty()) {
            throw BinderException("BigQuery query has no columns: " + query_string);
        }

        bind_data->names = names;
        bind_data->all_types = return_types;

        if (!mapped_bq_types.empty()) {
            bind_data->mapped_bq_types = std::move(mapped_bq_types);
            bind_data->requires_cast = true;
        } else {
            bind_data->requires_cast = false;
        }

        return std::move(bind_data);
    } else {
        // Legacy implementation (V1) with Storage API
        auto bind_data = make_uniq<BigqueryLegacyScanBindData>();
        bind_data->query = query_string;
        bind_data->query_parameters = query_parameters;
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
            auto bq_config = BigqueryConfig(dbname_or_project_id)
                                 .SetBillingProjectId(params.billing_project)
                                 .SetApiEndpoint(params.api_endpoint)
                                 .SetGrpcEndpoint(params.grpc_endpoint);
            auto bq_client = make_shared_ptr<BigqueryClient>(context, bq_config);

            bind_data->config = bq_config;
            bind_data->bq_client = bq_client;
        }

        ColumnList columns;
        vector<unique_ptr<Constraint>> constraints;
        bind_data->bq_client->GetTableInfoForQuery(query_string, bind_data->query_parameters, columns, constraints);

        for (auto &column : columns.Logical()) {
            names.push_back(column.GetName());
            return_types.push_back(column.GetType());
        }
        if (names.empty()) {
            throw std::runtime_error("no columns for query: " + query_string);
        }

        for (const auto &column : columns.Logical()) {
            if (BigqueryUtils::IsGeometryType(column.GetType())) {
                throw BinderException("BigQuery GEOGRAPHY columns are not supported in legacy scan. "
                                      "Please set use_legacy_scan=false (recommended).");
            }
        }

        bind_data->names = names;
        bind_data->types = return_types;
        return std::move(bind_data);
    }
}

//! Global state for REST inline results (optional job creation fast path)
struct BigqueryQueryInlineGlobalState : public GlobalTableFunctionState {
    google::protobuf::RepeatedPtrField<::google::protobuf::Struct> rows;
    vector<LogicalType> types;

    mutable mutex lock;
    idx_t current_row = 0;
    bool done = false;
};

static unique_ptr<GlobalTableFunctionState> BigqueryQueryInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    // Dry run
    if (dynamic_cast<const BigqueryQueryDryRunBindData *>(input.bind_data.get())) {
        return make_uniq<GlobalTableFunctionState>();
    }
    // REST-only path (opt-in via use_rest_api=true)
    if (dynamic_cast<const BigqueryQueryRestBindData *>(input.bind_data.get())) {
        auto &bind_data = input.bind_data->CastNoConst<BigqueryQueryRestBindData>();

        // Execute the query (with JOB_CREATION_OPTIONAL)
        auto query_response = bind_data.bq_client->ExecuteQuery(bind_data.query, "", false, bind_data.query_parameters,
                                                                 /*optional_job_creation=*/true);

        if (!query_response.has_job_complete() || !query_response.job_complete().value()) {
            throw BinderException("Query did not complete within the timeout.");
        }

        auto gstate = make_uniq<BigqueryQueryInlineGlobalState>();
        gstate->types = bind_data.types;
        gstate->rows = query_response.rows();

        // Paginate if there are more results
        string page_token = query_response.page_token();
        if (!page_token.empty() && query_response.has_job_reference()) {
            auto job_ref = query_response.job_reference();
            while (!page_token.empty()) {
                auto next_page = bind_data.bq_client->GetQueryResults(job_ref, page_token);
                gstate->rows.MergeFrom(next_page.rows());
                page_token = next_page.page_token();
            }
        }

        return std::move(gstate);
    }
    // Default path: Storage API (Arrow scan)
    if (dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        auto &mutable_bind_data = input.bind_data->CastNoConst<BigqueryArrowScanBindData>();

        // Force job creation (not optional) since Storage API needs a destination table
        auto query_response = mutable_bind_data.bq_client->ExecuteQuery(mutable_bind_data.query,
                                                                        "",
                                                                        false,
                                                                        mutable_bind_data.query_parameters,
                                                                        /*optional_job_creation=*/false);
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
        // Storage API: Legacy scan path (use_legacy_scan=true)
        auto &bind_data = input.bind_data->CastNoConst<BigqueryLegacyScanBindData>();

        // Force job creation (not optional) since Storage API needs a destination table
        auto query_response = bind_data.bq_client->ExecuteQuery(bind_data.query, "", false, bind_data.query_parameters,
                                                                 /*optional_job_creation=*/false);
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
    // Inline results (fast path)
    if (dynamic_cast<BigqueryQueryInlineGlobalState *>(global_state)) {
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

        auto response = bind_data.bq_client->ExecuteQuery(bind_data.query, "", true, bind_data.query_parameters);
        bind_data.finished = true;

        output.SetValue(0, 0, Value::BIGINT(response.total_bytes_processed().value()));
        output.SetValue(1, 0, Value::BOOLEAN(response.cache_hit().value()));
        output.SetValue(2, 0, response.job_reference().location().value());
        output.SetCardinality(1);
        return;
    }

    // Inline results (fast path - optional job creation)
    if (auto *gstate = dynamic_cast<BigqueryQueryInlineGlobalState *>(data.global_state.get())) {
        lock_guard<mutex> glock(gstate->lock);
        if (gstate->done) {
            return;
        }

        idx_t rows_to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE,
                                              static_cast<idx_t>(gstate->rows.size()) - gstate->current_row);
        if (rows_to_read == 0) {
            gstate->done = true;
            return;
        }

        output.Reset();
        FillChunkFromRestRows(gstate->rows, gstate->current_row, rows_to_read,
                              gstate->types, output);
        gstate->current_row += output.size();

        if (gstate->current_row >= static_cast<idx_t>(gstate->rows.size())) {
            gstate->done = true;
        }
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

    // REST path
    if (dynamic_cast<const BigqueryQueryRestBindData *>(base)) {
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

    // REST path
    if (const auto *rest_bind_data = dynamic_cast<const BigqueryQueryRestBindData *>(base)) {
        result["Query"] = rest_bind_data->query;
        result["Type"] = "REST";
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

    // REST path
    if (const auto *rest = dynamic_cast<const BigqueryQueryRestBindData *>(base)) {
        const idx_t n = rest->estimated_row_count;
        return make_uniq<NodeStatistics>(n, n);
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

    // Inline results
    if (const auto *inline_gs = dynamic_cast<const BigqueryQueryInlineGlobalState *>(global_state)) {
        lock_guard<mutex> glock(inline_gs->lock);
        if (inline_gs->done || inline_gs->rows.empty()) {
            return 100.0;
        }
        return 100.0 * static_cast<double>(inline_gs->current_row) / static_cast<double>(inline_gs->rows.size());
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
    named_parameters["use_rest_api"] = LogicalType::BOOLEAN;
    named_parameters["dry_run"] = LogicalType::BOOLEAN;
    varargs = LogicalType::ANY;
}

} // namespace bigquery
} // namespace duckdb
