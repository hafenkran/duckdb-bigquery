#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/planner.hpp"

#include <google/protobuf/util/json_util.h>

#include "absl/status/status.h"
#include "bigquery_client.hpp"
#include "bigquery_load.hpp"
#include "bigquery_settings.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

#include <cstdio>
#include <optional>

namespace duckdb {
namespace bigquery {

struct BigQueryLoadBindData : public TableFunctionData {
    BigqueryConfig config;
    shared_ptr<BigqueryClient> bq_client;
    BigqueryTableRef destination_table;
    std::optional<string> source_file;
    std::optional<string> source_table;
    string write_disposition;
    string create_disposition;
    string location;
    bool remove_staged_source_file = false;
    bool finished = false;

    ~BigQueryLoadBindData() override {
        if (remove_staged_source_file && source_file.has_value()) {
            std::remove(source_file->c_str());
        }
    }
};

struct BigQueryLoadParameters {
    std::optional<string> source_file;
    std::optional<string> source_table;
    string write_disposition = "WRITE_TRUNCATE";
    string create_disposition = "CREATE_IF_NEEDED";
    std::optional<string> location;
    string api_endpoint;
};

static string NormalizeEnumValue(const string &value) {
    return StringUtil::Upper(value);
}

static string ParseEnumParameter(const named_parameter_map_t &named_parameters,
                                 const string &parameter_name,
                                 const string &default_value,
                                 const vector<string> &allowed_values) {
    auto entry = named_parameters.find(parameter_name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return default_value;
    }

    auto normalized = NormalizeEnumValue(entry->second.GetValue<string>());
    for (const auto &allowed_value : allowed_values) {
        if (normalized == allowed_value) {
            return normalized;
        }
    }

    throw BinderException("Invalid value for parameter '%s': %s", parameter_name, normalized);
}

static std::optional<string> ParseOptionalStringParameter(const named_parameter_map_t &named_parameters,
                                                          const string &parameter_name) {
    auto entry = named_parameters.find(parameter_name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return std::nullopt;
    }
    return entry->second.GetValue<string>();
}

static std::optional<string> ParseOptionalAliasedStringParameter(const named_parameter_map_t &named_parameters,
                                                                 const string &parameter_name,
                                                                 const string &legacy_parameter_name) {
    auto value = ParseOptionalStringParameter(named_parameters, parameter_name);
    auto legacy_value = ParseOptionalStringParameter(named_parameters, legacy_parameter_name);
    if (value && legacy_value) {
        throw BinderException("Cannot specify both named parameters '%s' and '%s'", parameter_name,
                              legacy_parameter_name);
    }
    return value ? value : legacy_value;
}

static BigQueryLoadParameters ParseLoadParameters(const named_parameter_map_t &named_parameters) {
    BigQueryLoadParameters params;
    params.source_file = ParseOptionalAliasedStringParameter(named_parameters, "source_file", "file");
    params.source_table = ParseOptionalAliasedStringParameter(named_parameters, "source_table", "table");
    params.location = ParseOptionalStringParameter(named_parameters, "location");
    auto api_endpoint = ParseOptionalStringParameter(named_parameters, "api_endpoint");
    params.api_endpoint = api_endpoint ? *api_endpoint : string();
    params.write_disposition = ParseEnumParameter(named_parameters,
                                                  "write_disposition",
                                                  "WRITE_TRUNCATE",
                                                  {"WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"});
    params.create_disposition = ParseEnumParameter(named_parameters,
                                                   "create_disposition",
                                                   "CREATE_IF_NEEDED",
                                                   {"CREATE_IF_NEEDED", "CREATE_NEVER"});

    const auto source_count =
        static_cast<int>(params.source_file.has_value()) + static_cast<int>(params.source_table.has_value());
    if (source_count != 1) {
        throw BinderException(
            "Exactly one of the named parameters 'source_file'/'file' or 'source_table'/'table' must be provided");
    }

    return params;
}

static string GetLoadStagingDirectory(FileSystem &fs) {
    auto temp_dir = FileSystem::GetEnvVariable("TMPDIR");
    if (temp_dir.empty()) {
        temp_dir = FileSystem::GetEnvVariable("TEMP");
    }
    if (temp_dir.empty()) {
        temp_dir = FileSystem::GetEnvVariable("TMP");
    }
    if (temp_dir.empty()) {
        temp_dir = FileSystem::GetWorkingDirectory();
    }
    return fs.ExpandPath(temp_dir);
}

static string MaterializeSourceTableToParquet(ClientContext &context, const string &table_name) {
    auto &fs = FileSystem::GetFileSystem(context);
    auto temp_dir = GetLoadStagingDirectory(fs);
    if (!fs.DirectoryExists(temp_dir)) {
        fs.CreateDirectoriesRecursive(temp_dir);
    }

    auto temp_file_name = "duckdb-bigquery-load-" + StringUtil::GenerateRandomName() + ".parquet";
    auto temp_file_path = fs.JoinPath(temp_dir, temp_file_name);
    auto escaped_temp_file_path = StringUtil::Replace(temp_file_path, "'", "''");
    auto relation_name = QualifiedName::Parse(table_name).ToString();

    ExtensionHelper::AutoLoadExtension(context, "parquet");

    auto copy_query =
        "COPY (SELECT * FROM " + relation_name + ") TO '" + escaped_temp_file_path + "' (FORMAT PARQUET)";

    try {
        Parser parser(context.GetParserOptions());
        parser.ParseQuery(copy_query);
        if (parser.statements.size() != 1) {
            throw InternalException("Expected a single COPY statement for source table materialization");
        }

        Planner logical_planner(context);
        logical_planner.CreatePlan(std::move(parser.statements[0]));
        auto logical_plan = std::move(logical_planner.plan);
        if (!logical_plan) {
            throw InternalException("Failed to plan source table materialization");
        }

#ifdef DEBUG
        logical_plan->Verify(context);
#endif
        if (ClientConfig::GetConfig(context).enable_optimizer && logical_plan->RequireOptimizer()) {
            Optimizer optimizer(*logical_planner.binder, context);
            logical_plan = optimizer.Optimize(std::move(logical_plan));
#ifdef DEBUG
            logical_plan->Verify(context);
#endif
        }

        PhysicalPlanGenerator physical_planner(context);
        auto physical_plan = physical_planner.Plan(std::move(logical_plan));

        Executor executor(context);
        executor.Initialize(physical_plan->Root());

        while (true) {
            auto execution_result = executor.ExecuteTask(false);
            if (execution_result == PendingExecutionResult::BLOCKED) {
                executor.WaitForTask();
                continue;
            }
            if (execution_result == PendingExecutionResult::RESULT_READY ||
                execution_result == PendingExecutionResult::EXECUTION_FINISHED) {
                break;
            }
        }

        if (executor.HasResultCollector()) {
            auto result = executor.GetResult();
            if (result->HasError()) {
                result->ThrowError();
            }
        }

        return temp_file_path;
    } catch (std::exception &ex) {
        fs.TryRemoveFile(temp_file_path);
        throw BinderException("Failed to materialize DuckDB source table for bigquery_load: %s", ex.what());
    } catch (...) {
        fs.TryRemoveFile(temp_file_path);
        throw;
    }
}

static void InitializeNamesAndReturnTypes(vector<LogicalType> &return_types, vector<string> &names) {
    names.emplace_back("success");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));

    names.emplace_back("job_id");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));

    names.emplace_back("project_id");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));

    names.emplace_back("location");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));

    names.emplace_back("destination_table");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));

    names.emplace_back("output_rows");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));

    names.emplace_back("status");
    return_types.emplace_back(LogicalType::JSON());
}

static unique_ptr<FunctionData> BigQueryLoadBind(ClientContext &context,
                                                 TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types,
                                                 vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto destination_table_string = input.inputs[1].GetValue<string>();
    auto params = ParseLoadParameters(input.named_parameters);

    auto bind_data = make_uniq<BigQueryLoadBindData>();
    bind_data->write_disposition = params.write_disposition;
    bind_data->create_disposition = params.create_disposition;
    bind_data->source_file = params.source_file;
    bind_data->source_table = params.source_table;
    bind_data->location = params.location ? *params.location : BigquerySettings::DefaultLocation();

    auto destination_table = BigqueryUtils::ParseDatasetTableString(destination_table_string);

    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, dbname_or_project_id);
    if (database) {
        auto &catalog = database->GetCatalog();
        if (catalog.GetCatalogType() != "bigquery") {
            throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
        }
        if (!params.api_endpoint.empty()) {
            throw BinderException("'api_endpoint' named parameter is not supported for attached databases");
        }

        auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
        auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);
        if (transaction.GetAccessMode() == AccessMode::READ_ONLY) {
            throw BinderException("Cannot run BigQuery load jobs in read-only transaction");
        }

        bind_data->config = bigquery_catalog.config;
        bind_data->bq_client = transaction.GetBigqueryClient();
    } else {
        bind_data->config = BigqueryConfig(dbname_or_project_id).SetApiEndpoint(params.api_endpoint);
        bind_data->bq_client = make_shared_ptr<BigqueryClient>(context, bind_data->config);
    }

    destination_table.project_id = bind_data->config.project_id;
    bind_data->destination_table = std::move(destination_table);

    if (bind_data->source_table.has_value()) {
        bind_data->source_file = MaterializeSourceTableToParquet(context, *bind_data->source_table);
        bind_data->source_table.reset();
        bind_data->remove_staged_source_file = true;
    }

    InitializeNamesAndReturnTypes(return_types, names);
    return bind_data;
}

static void BigQueryLoadFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<BigQueryLoadBindData>();
    if (data.finished) {
        return;
    }

    google::cloud::bigquery::v2::Job job;
    if (data.source_file.has_value()) {
        job = data.bq_client->LoadParquetFile(data.destination_table,
                                              *data.source_file,
                                              data.write_disposition,
                                              data.create_disposition,
                                              data.location);
    } else {
        job = data.bq_client->LoadDuckDBTable(*data.source_table,
                                              data.destination_table,
                                              data.write_disposition,
                                              data.create_disposition,
                                              data.location);
    }

    std::string status_json;
    absl::Status status = google::protobuf::util::MessageToJsonString(job.status(), &status_json);
    if (!status.ok()) {
        throw BinderException("Failed to convert load job status to JSON");
    }

    output.SetValue(0, 0, Value::BOOLEAN(true));
    output.SetValue(1, 0, Value(job.job_reference().job_id()));
    output.SetValue(2, 0, Value(job.job_reference().project_id()));
    output.SetValue(3, 0, Value(job.job_reference().location().value()));
    output.SetValue(4, 0, Value(BigqueryUtils::FormatTableStringSimple(data.destination_table)));

    if (job.statistics().has_load() && job.statistics().load().has_output_rows()) {
        output.SetValue(5, 0, Value::UBIGINT(job.statistics().load().output_rows().value()));
    } else {
        output.SetValue(5, 0, Value());
    }

    output.SetValue(6, 0, Value(status_json));
    output.SetCardinality(1);
    data.finished = true;
}

BigQueryLoadFunction::BigQueryLoadFunction()
    : TableFunction("bigquery_load",
                    {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
                    BigQueryLoadFunc,
                    BigQueryLoadBind) {
    named_parameters["source_file"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["file"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["source_table"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["table"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["write_disposition"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["create_disposition"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["location"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["api_endpoint"] = LogicalType(LogicalTypeId::VARCHAR);
}

} // namespace bigquery
} // namespace duckdb
