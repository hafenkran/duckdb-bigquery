#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <google/protobuf/util/json_util.h>

#include "absl/status/status.h"
#include "bigquery_client.hpp"
#include "bigquery_export.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

#include <algorithm>
#include <google/protobuf/repeated_field.h>
#include <map>
#include <optional>

namespace duckdb {
namespace bigquery {

struct BigQueryExportBindData : public TableFunctionData {
    BigqueryConfig config;
    shared_ptr<BigqueryClient> bq_client;
    BigqueryTableRef source_table;
    vector<string> destination_uris;
    string destination_format;
    string location;
    string compression;
    std::optional<bool> print_header;
    string field_delimiter;
    std::optional<bool> use_avro_logical_types;
    std::map<string, string> labels;
    std::optional<int> timeout_ms;
    bool finished = false;
};

struct BigQueryExportParameters {
    string source_table;
    vector<string> destination_uris;
    string destination_format;
    std::optional<string> location;
    string billing_project;
    string api_endpoint;
    string compression;
    std::optional<bool> print_header;
    string field_delimiter;
    std::optional<bool> use_avro_logical_types;
    std::map<string, string> labels;
    std::optional<int> timeout_ms;
};

static string NormalizeEnumValue(const string &value) {
    return StringUtil::Upper(value);
}

static std::optional<string> ParseOptionalStringParameter(const named_parameter_map_t &named_parameters,
                                                          const string &parameter_name) {
    auto entry = named_parameters.find(parameter_name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return std::nullopt;
    }
    return entry->second.GetValue<string>();
}

static string ParseRequiredStringParameter(const named_parameter_map_t &named_parameters,
                                           const string &parameter_name) {
    auto value = ParseOptionalStringParameter(named_parameters, parameter_name);
    if (!value || value->empty()) {
        throw BinderException("Parameter '%s' is required", parameter_name);
    }
    return *value;
}

static string ParseRequiredEnumParameter(const named_parameter_map_t &named_parameters,
                                         const string &parameter_name,
                                         const vector<string> &allowed_values) {
    auto normalized = NormalizeEnumValue(ParseRequiredStringParameter(named_parameters, parameter_name));
    for (const auto &allowed_value : allowed_values) {
        if (normalized == allowed_value) {
            return normalized;
        }
    }

    throw BinderException("Invalid value for parameter '%s': %s", parameter_name, normalized);
}

static string ParseOptionalEnumParameter(const named_parameter_map_t &named_parameters,
                                         const string &parameter_name,
                                         const vector<string> &allowed_values) {
    auto value = ParseOptionalStringParameter(named_parameters, parameter_name);
    if (!value) {
        return string();
    }

    auto normalized = NormalizeEnumValue(*value);
    for (const auto &allowed_value : allowed_values) {
        if (normalized == allowed_value) {
            return normalized;
        }
    }

    throw BinderException("Invalid value for parameter '%s': %s", parameter_name, normalized);
}

static std::optional<bool> ParseOptionalBoolParameter(const named_parameter_map_t &named_parameters,
                                                      const string &parameter_name) {
    auto entry = named_parameters.find(parameter_name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return std::nullopt;
    }
    return BooleanValue::Get(entry->second);
}

static std::optional<bool> ParseOptionalAliasedBoolParameter(const named_parameter_map_t &named_parameters,
                                                             const string &parameter_name,
                                                             const string &alias_parameter_name) {
    auto value = ParseOptionalBoolParameter(named_parameters, parameter_name);
    auto alias_value = ParseOptionalBoolParameter(named_parameters, alias_parameter_name);
    if (value && alias_value) {
        throw BinderException("Cannot specify both named parameters '%s' and '%s'",
                              parameter_name,
                              alias_parameter_name);
    }
    return value ? value : alias_value;
}

static std::optional<vector<string>> ParseDestinationUrisParameter(const named_parameter_map_t &named_parameters) {
    auto entry = named_parameters.find("destination_uris");
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return std::nullopt;
    }
    if (entry->second.type().id() != LogicalTypeId::LIST) {
        throw BinderException("Parameter 'destination_uris' must be LIST<VARCHAR>");
    }

    vector<string> destination_uris;
    for (const auto &child : ListValue::GetChildren(entry->second)) {
        if (child.IsNull()) {
            throw BinderException("Null entries are not allowed in parameter 'destination_uris'");
        }
        if (child.type().id() != LogicalTypeId::VARCHAR) {
            throw BinderException("Parameter 'destination_uris' must contain only VARCHAR entries");
        }
        destination_uris.push_back(child.GetValue<string>());
    }
    return destination_uris;
}

static void ValidateDestinationUris(const vector<string> &destination_uris) {
    if (destination_uris.empty()) {
        throw BinderException("Parameter 'destination_uris' must contain at least one URI");
    }
    for (const auto &destination_uri : destination_uris) {
        if (destination_uri.empty()) {
            throw BinderException("BigQuery export destination URIs cannot be empty");
        }
        if (!StringUtil::CIStartsWith(destination_uri, "gs://")) {
            throw BinderException("BigQuery extract destination URI must use the gs:// scheme: %s", destination_uri);
        }
    }
}

static BigQueryExportParameters ParseExportParameters(const named_parameter_map_t &named_parameters) {
    if (named_parameters.find("overwrite") != named_parameters.end()) {
        throw BinderException(
            "Parameter 'overwrite' is not supported by JobConfigurationExtract-based bigquery_export");
    }

    BigQueryExportParameters params;
    params.source_table = ParseRequiredStringParameter(named_parameters, "source_table");
    params.destination_format =
        ParseRequiredEnumParameter(named_parameters, "format", {"CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "PARQUET"});
    params.compression =
        ParseOptionalEnumParameter(named_parameters, "compression", {"DEFLATE", "GZIP", "NONE", "SNAPPY", "ZSTD"});
    params.print_header = ParseOptionalAliasedBoolParameter(named_parameters, "print_header", "header");
    auto field_delimiter = ParseOptionalStringParameter(named_parameters, "field_delimiter");
    params.field_delimiter = field_delimiter ? *field_delimiter : string();
    params.use_avro_logical_types = ParseOptionalBoolParameter(named_parameters, "use_avro_logical_types");
    params.location = ParseOptionalStringParameter(named_parameters, "location");
    auto billing_project = ParseOptionalStringParameter(named_parameters, "billing_project");
    params.billing_project = billing_project ? *billing_project : string();
    auto api_endpoint = ParseOptionalStringParameter(named_parameters, "api_endpoint");
    params.api_endpoint = api_endpoint ? *api_endpoint : string();
    params.labels = BigqueryUtils::ParseOptionalLabelsParameter(named_parameters);
    params.timeout_ms = BigqueryUtils::ParseTimeoutMsParameter(named_parameters);

    auto uri = ParseOptionalStringParameter(named_parameters, "uri");
    auto destination_uris = ParseDestinationUrisParameter(named_parameters);
    if (uri && destination_uris) {
        throw BinderException("Cannot specify both named parameters 'uri' and 'destination_uris'");
    }
    if (!uri && !destination_uris) {
        throw BinderException("Exactly one of the named parameters 'uri' or 'destination_uris' must be provided");
    }
    params.destination_uris = uri ? vector<string>{*uri} : *destination_uris;
    ValidateDestinationUris(params.destination_uris);

    return params;
}

static BigqueryTableRef ParseSourceTable(const string &source_table_string, const string &default_project_id) {
    try {
        const auto simple_dataset_table = source_table_string.find(':') == string::npos &&
                                          !StringUtil::CIStartsWith(source_table_string, "projects/") &&
                                          std::count(source_table_string.begin(), source_table_string.end(), '.') == 1;
        if (simple_dataset_table) {
            auto source_table = BigqueryUtils::ParseDatasetTableString(source_table_string);
            source_table.project_id = default_project_id;
            return source_table;
        }

        auto source_table = BigqueryUtils::ParseTableString(source_table_string);
        if (source_table.project_id.empty()) {
            source_table.project_id = default_project_id;
        }
        if (source_table.dataset_id.empty() || source_table.table_id.empty()) {
            throw std::invalid_argument("Expected table reference with dataset and table IDs");
        }
        return source_table;
    } catch (std::exception &ex) {
        throw BinderException("Invalid source_table value '%s': %s", source_table_string, ex.what());
    }
}

static Value StringListValue(const vector<string> &values) {
    vector<Value> children;
    children.reserve(values.size());
    for (const auto &value : values) {
        children.emplace_back(value);
    }
    return Value::LIST(LogicalType::VARCHAR, std::move(children));
}

static Value Int64ListValue(const google::protobuf::RepeatedField<int64_t> &values) {
    vector<Value> children;
    children.reserve(values.size());
    for (const auto &value : values) {
        children.emplace_back(Value::BIGINT(value));
    }
    return Value::LIST(LogicalType::BIGINT, std::move(children));
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

    names.emplace_back("source_table");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));

    names.emplace_back("destination_uris");
    return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

    names.emplace_back("format");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));

    names.emplace_back("destination_uri_file_counts");
    return_types.emplace_back(LogicalType::LIST(LogicalType::BIGINT));

    names.emplace_back("input_bytes");
    return_types.emplace_back(LogicalType(LogicalTypeId::BIGINT));

    names.emplace_back("status");
    return_types.emplace_back(LogicalType::JSON());
}

static unique_ptr<FunctionData> BigQueryExportBind(ClientContext &context,
                                                   TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types,
                                                   vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto params = ParseExportParameters(input.named_parameters);

    auto bind_data = make_uniq<BigQueryExportBindData>();
    bind_data->destination_uris = params.destination_uris;
    bind_data->destination_format = params.destination_format;
    bind_data->location = params.location ? *params.location : BigquerySettings::DefaultLocation();
    bind_data->compression = params.compression;
    bind_data->print_header = params.print_header;
    bind_data->field_delimiter = params.field_delimiter;
    bind_data->use_avro_logical_types = params.use_avro_logical_types;
    bind_data->labels = params.labels;
    bind_data->timeout_ms = params.timeout_ms;

    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, dbname_or_project_id);
    if (database) {
        auto &catalog = database->GetCatalog();
        if (catalog.GetCatalogType() != "bigquery") {
            throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
        }
        if (!params.billing_project.empty()) {
            throw BinderException("'billing_project' named parameter is not supported for attached databases; "
                                  "set billing_project in ATTACH instead");
        }
        if (!params.api_endpoint.empty()) {
            throw BinderException("'api_endpoint' named parameter is not supported for attached databases");
        }

        auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
        auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);
        if (transaction.GetAccessMode() == AccessMode::READ_ONLY) {
            throw BinderException("Cannot run BigQuery extract jobs in read-only transaction");
        }

        bind_data->config = bigquery_catalog.config;
        bind_data->bq_client = transaction.GetBigqueryClient();
    } else {
        bind_data->config = BigqueryConfig(dbname_or_project_id)
                                .SetBillingProjectId(params.billing_project)
                                .SetApiEndpoint(params.api_endpoint);
        bind_data->bq_client = make_shared_ptr<BigqueryClient>(context, bind_data->config);
    }

    bind_data->source_table = ParseSourceTable(params.source_table, bind_data->config.project_id);

    InitializeNamesAndReturnTypes(return_types, names);
    return bind_data;
}

static void BigQueryExportFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<BigQueryExportBindData>();
    if (data.finished) {
        return;
    }

    auto job = data.bq_client->ExtractTableToGcs(data.source_table,
                                                 data.destination_uris,
                                                 data.destination_format,
                                                 data.location,
                                                 data.compression,
                                                 data.print_header,
                                                 data.field_delimiter,
                                                 data.use_avro_logical_types,
                                                 data.labels,
                                                 data.timeout_ms);

    std::string status_json;
    absl::Status status = google::protobuf::util::MessageToJsonString(job.status(), &status_json);
    if (!status.ok()) {
        throw BinderException("Failed to convert extract job status to JSON");
    }

    output.SetValue(0, 0, Value::BOOLEAN(true));
    output.SetValue(1, 0, Value(job.job_reference().job_id()));
    output.SetValue(2, 0, Value(job.job_reference().project_id()));
    output.SetValue(3, 0, Value(job.job_reference().location().value()));
    output.SetValue(4, 0, Value(BigqueryUtils::FormatTableStringSimple(data.source_table)));
    output.SetValue(5, 0, StringListValue(data.destination_uris));
    output.SetValue(6, 0, Value(data.destination_format));

    if (job.statistics().has_extract()) {
        const auto &extract_stats = job.statistics().extract();
        output.SetValue(7, 0, Int64ListValue(extract_stats.destination_uri_file_counts()));
        if (extract_stats.has_input_bytes()) {
            output.SetValue(8, 0, Value::BIGINT(extract_stats.input_bytes().value()));
        } else {
            output.SetValue(8, 0, Value());
        }
    } else {
        output.SetValue(7, 0, Value());
        output.SetValue(8, 0, Value());
    }

    output.SetValue(9, 0, Value(status_json));
    output.SetCardinality(1);
    data.finished = true;
}

BigQueryExportFunction::BigQueryExportFunction()
    : TableFunction("bigquery_export", {LogicalType(LogicalTypeId::VARCHAR)}, BigQueryExportFunc, BigQueryExportBind) {
    named_parameters["source_table"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["uri"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["destination_uris"] = LogicalType::ANY;
    named_parameters["format"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["compression"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["print_header"] = LogicalType(LogicalTypeId::BOOLEAN);
    named_parameters["header"] = LogicalType(LogicalTypeId::BOOLEAN);
    named_parameters["field_delimiter"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["use_avro_logical_types"] = LogicalType(LogicalTypeId::BOOLEAN);
    named_parameters["location"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["labels"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
    named_parameters["billing_project"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["api_endpoint"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["timeout_ms"] = LogicalType::BIGINT;
    named_parameters["overwrite"] = LogicalType(LogicalTypeId::BOOLEAN);
}

} // namespace bigquery
} // namespace duckdb
