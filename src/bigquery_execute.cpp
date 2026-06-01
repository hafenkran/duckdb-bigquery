#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "bigquery_execute.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

#include <optional>

namespace duckdb {
namespace bigquery {

struct BigQueryExecuteBindData : public TableFunctionData {
    BigqueryConfig config;
    shared_ptr<BigqueryClient> bq_client;
    string query;
    bool dry_run = false;
    std::optional<int> timeout_ms;
    bool finished = false;
    //! When set, the query result is materialised into `destination_table`
    //! (jobs.insert + JobConfigurationQuery) instead of being run via jobs.query.
    bool has_destination = false;
    BigqueryTableRef destination_table;
    string write_disposition = "WRITE_TRUNCATE";
    string create_disposition = "CREATE_IF_NEEDED";
};

//! Parse a VARCHAR named parameter, returning the default when absent or NULL.
static string GetStringParam(const named_parameter_map_t &named_parameters,
                             const string &name,
                             const string &default_value) {
    auto entry = named_parameters.find(name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return default_value;
    }
    return entry->second.GetValue<string>();
}

static string ParseDispositionParam(const named_parameter_map_t &named_parameters,
                                    const string &name,
                                    const string &default_value,
                                    const vector<string> &allowed_values) {
    auto value = GetStringParam(named_parameters, name, default_value);
    auto normalized = StringUtil::Upper(value);
    for (const auto &allowed : allowed_values) {
        if (normalized == allowed) {
            return normalized;
        }
    }
    throw BinderException("Invalid value for parameter '%s': %s", name, value);
}

//! Parse `project.dataset.table` or `dataset.table` (backticks tolerated) into a
//! BigqueryTableRef. The project falls back to `default_project` when omitted.
static BigqueryTableRef ParseDestinationTable(const string &table_string, const string &default_project) {
    auto cleaned = StringUtil::Replace(table_string, "`", "");
    auto parts = StringUtil::Split(cleaned, '.');
    BigqueryTableRef ref;
    if (parts.size() == 3) {
        ref.project_id = parts[0];
        ref.dataset_id = parts[1];
        ref.table_id = parts[2];
    } else if (parts.size() == 2) {
        ref.project_id = default_project;
        ref.dataset_id = parts[0];
        ref.table_id = parts[1];
    } else {
        throw BinderException("Invalid destination_table '%s' — expected 'dataset.table' or "
                              "'project.dataset.table'",
                              table_string);
    }
    if (ref.dataset_id.empty() || ref.table_id.empty()) {
        throw BinderException("Invalid destination_table '%s' — dataset and table must be non-empty", table_string);
    }
    return ref;
}

static duckdb::unique_ptr<FunctionData> BigQueryExecuteBind(ClientContext &context,
                                                            TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto query_string = input.inputs[1].GetValue<string>();

    // Parse named parameters using centralized function
    auto params = BigQueryCommonParameters::ParseFromNamedParameters(input.named_parameters);

    auto result = make_uniq<BigQueryExecuteBindData>();
    result->query = query_string;
    result->dry_run = params.dry_run;
    result->timeout_ms = params.timeout_ms;

    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, dbname_or_project_id);
    if (database) {
        // Use attached database for this operation
        auto &catalog = database->GetCatalog();
        if (catalog.GetCatalogType() != "bigquery") {
            throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
        }
        if (!params.api_endpoint.empty() || !params.grpc_endpoint.empty()) {
            throw BinderException("Named parameters are not supported for attached databases");
        }

        auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
        auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);
        if (transaction.GetAccessMode() == AccessMode::READ_ONLY) {
            throw BinderException("Cannot execute BigQuery query in read-only transaction");
        }

        result->config = bigquery_catalog.config;
        result->bq_client = transaction.GetBigqueryClient();
    } else {
        // Use the provided project_id of the gcp project
        result->config = BigqueryConfig(dbname_or_project_id) //
                             .SetApiEndpoint(params.api_endpoint)
                             .SetGrpcEndpoint(params.grpc_endpoint);
        result->bq_client = make_shared_ptr<BigqueryClient>(context, result->config);
    }

    auto destination_entry = input.named_parameters.find("destination_table");
    if (destination_entry != input.named_parameters.end() && !destination_entry->second.IsNull()) {
        if (params.dry_run) {
            throw BinderException("'destination_table' cannot be combined with 'dry_run'");
        }
        result->has_destination = true;
        result->destination_table =
            ParseDestinationTable(destination_entry->second.GetValue<string>(), result->config.project_id);
        result->write_disposition = ParseDispositionParam(input.named_parameters,
                                                          "write_disposition",
                                                          "WRITE_TRUNCATE",
                                                          {"WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"});
        result->create_disposition = ParseDispositionParam(input.named_parameters,
                                                           "create_disposition",
                                                           "CREATE_IF_NEEDED",
                                                           {"CREATE_IF_NEEDED", "CREATE_NEVER"});
    }

    if (!params.dry_run) {
        return_types.emplace_back(LogicalTypeId::BOOLEAN);
        names.emplace_back("success");
        return_types.emplace_back(LogicalTypeId::VARCHAR);
        names.emplace_back("job_id");
        return_types.emplace_back(LogicalTypeId::VARCHAR);
        names.emplace_back("project_id");
        return_types.emplace_back(LogicalTypeId::VARCHAR);
        names.emplace_back("location");
        return_types.emplace_back(LogicalTypeId::UBIGINT);
        names.emplace_back("total_rows");
        return_types.emplace_back(LogicalTypeId::BIGINT);
        names.emplace_back("total_bytes_processed");
        return_types.emplace_back(LogicalTypeId::BIGINT);
        names.emplace_back("num_dml_affected_rows");
    } else {
        return_types.emplace_back(LogicalTypeId::BIGINT);
        names.emplace_back("total_bytes_processed");
        return_types.emplace_back(LogicalTypeId::BOOLEAN);
        names.emplace_back("cache_hit");
        return_types.emplace_back(LogicalTypeId::VARCHAR);
        names.emplace_back("location");
    }

    return result;
}

static void BigQueryExecuteFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<BigQueryExecuteBindData>();
    if (data.finished) {
        return;
    }

    if (data.has_destination) {
        // jobs.insert + JobConfigurationQuery: materialise the query result into the
        // destination table (jobs.query, used below, cannot write to a table).
        auto job = data.bq_client->ExecuteQueryToTable(data.query,
                                                       data.destination_table,
                                                       data.write_disposition,
                                                       data.create_disposition,
                                                       "",
                                                       {},
                                                       data.timeout_ms);
        data.finished = true;

        const auto &job_ref = job.job_reference();
        const auto &query_stats = job.statistics().query();
        output.SetValue(0, 0, true);
        output.SetValue(1, 0, Value(job_ref.job_id()));
        output.SetValue(2, 0, Value(job_ref.project_id()));
        output.SetValue(3, 0, job_ref.has_location() ? Value(job_ref.location().value()) : Value(LogicalType::VARCHAR));
        // The query job statistics carry bytes processed and DML-affected rows, but not the
        // result row count. Fetch total_rows via the job's query results, matching the source
        // used by the inline jobs.query path. Anything BigQuery doesn't report stays NULL.
        auto results = data.bq_client->GetQueryResults(job_ref);
        output.SetValue(4,
                        0,
                        results.has_total_rows() ? Value::UBIGINT(results.total_rows().value())
                                                 : Value(LogicalType::UBIGINT));
        output.SetValue(5,
                        0,
                        query_stats.has_total_bytes_processed()
                            ? Value::BIGINT(query_stats.total_bytes_processed().value())
                            : Value(LogicalType::BIGINT));
        output.SetValue(6,
                        0,
                        query_stats.has_num_dml_affected_rows()
                            ? Value::BIGINT(query_stats.num_dml_affected_rows().value())
                            : Value(LogicalType::BIGINT));
        output.SetCardinality(1);
        return;
    }

    auto response = data.bq_client->ExecuteQuery(data.query, "", data.dry_run, {}, false, data.timeout_ms);
    data.finished = true;

    if (!data.dry_run) {
        output.SetValue(0, 0, true);
        output.SetValue(1, 0, response.job_reference().job_id());
        output.SetValue(2, 0, response.job_reference().project_id());
        output.SetValue(3, 0, response.job_reference().location().value());
        output.SetValue(4, 0, Value::UBIGINT(response.total_rows().value()));
        output.SetValue(5, 0, Value::BIGINT(response.total_bytes_processed().value()));
        output.SetValue(6, 0, Value::BIGINT(response.num_dml_affected_rows().value()));
    } else {
        output.SetValue(0, 0, Value::BIGINT(response.total_bytes_processed().value()));
        output.SetValue(1, 0, Value::BOOLEAN(response.cache_hit().value()));
        output.SetValue(2, 0, response.job_reference().location().value());
    }
    output.SetCardinality(1);
}

BigQueryExecuteFunction::BigQueryExecuteFunction()
    : TableFunction("bigquery_execute",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    BigQueryExecuteFunc,
                    BigQueryExecuteBind) {

    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["dry_run"] = LogicalType::BOOLEAN;
    named_parameters["timeout_ms"] = LogicalType::BIGINT;
    named_parameters["destination_table"] = LogicalType::VARCHAR;
    named_parameters["write_disposition"] = LogicalType::VARCHAR;
    named_parameters["create_disposition"] = LogicalType::VARCHAR;
}

} // namespace bigquery
} // namespace duckdb
