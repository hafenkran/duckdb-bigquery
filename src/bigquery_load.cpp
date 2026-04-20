#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <google/protobuf/util/json_util.h>

#include "absl/status/status.h"
#include "bigquery_client.hpp"
#include "bigquery_load.hpp"
#include "bigquery_settings.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

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
    bool finished = false;
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

static BigQueryLoadParameters ParseLoadParameters(const named_parameter_map_t &named_parameters) {
    BigQueryLoadParameters params;
    params.source_file = ParseOptionalStringParameter(named_parameters, "file");
    params.source_table = ParseOptionalStringParameter(named_parameters, "table");
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
        throw BinderException("Exactly one of the named parameters 'file' or 'table' must be provided");
    }

    return params;
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
    named_parameters["file"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["table"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["write_disposition"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["create_disposition"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["location"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["api_endpoint"] = LogicalType(LogicalTypeId::VARCHAR);
}

} // namespace bigquery
} // namespace duckdb
