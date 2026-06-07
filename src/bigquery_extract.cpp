#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <google/protobuf/util/json_util.h>

#include "absl/status/status.h"
#include "bigquery_client.hpp"
#include "bigquery_extract.hpp"
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

struct BigQueryExtractBindData : public TableFunctionData {
    BigqueryConfig config;
    shared_ptr<BigqueryClient> bq_client;
    BigqueryTableRef source_table;
    vector<string> destination_uris;
    string destination_format;
    string location;
    string compression;
    std::optional<bool> csv_print_header;
    string csv_field_delimiter;
    std::optional<bool> avro_use_logical_types;
    std::map<string, string> labels;
    std::optional<int> timeout_ms;
    bool finished = false;
};

struct BigQueryExtractParameters {
    string source_table;
    vector<string> destination_uris;
    string destination_format;
    std::optional<string> location;
    string billing_project;
    string api_endpoint;
    string compression;
    std::optional<bool> csv_print_header;
    string csv_field_delimiter;
    std::optional<bool> avro_use_logical_types;
    std::map<string, string> labels;
    std::optional<int> timeout_ms;
};

static bool IsAllowedValue(const string &value, const vector<string> &allowed_values) {
    for (const auto &allowed_value : allowed_values) {
        if (value == allowed_value) {
            return true;
        }
    }
    return false;
}

static bool IsCompressionSupported(const string &format, const string &compression) {
    if (compression.empty()) {
        return true;
    }
    if (format == "CSV" || format == "NEWLINE_DELIMITED_JSON") {
        return IsAllowedValue(compression, {"NONE", "GZIP"});
    }
    if (format == "AVRO") {
        return IsAllowedValue(compression, {"NONE", "DEFLATE", "SNAPPY"});
    }
    if (format == "PARQUET") {
        return IsAllowedValue(compression, {"NONE", "GZIP", "SNAPPY", "ZSTD"});
    }
    return false;
}

static std::optional<string> ExtractFormatFromUri(const string &destination_uri) {
    auto lower_uri = StringUtil::Lower(destination_uri);
    if (StringUtil::EndsWith(lower_uri, ".csv") || StringUtil::EndsWith(lower_uri, ".csv.gz")) {
        return "CSV";
    }
    if (StringUtil::EndsWith(lower_uri, ".json") || StringUtil::EndsWith(lower_uri, ".json.gz")) {
        return "NEWLINE_DELIMITED_JSON";
    }
    if (StringUtil::EndsWith(lower_uri, ".avro")) {
        return "AVRO";
    }
    if (StringUtil::EndsWith(lower_uri, ".parquet")) {
        return "PARQUET";
    }
    return std::nullopt;
}

static string NormalizeExtractFormat(const string &format) {
    auto normalized = StringUtil::Upper(format);
    if (normalized == "JSON") {
        return "NEWLINE_DELIMITED_JSON";
    }
    return normalized;
}

static BigQueryExtractParameters ParseExtractParameters(const named_parameter_map_t &named_parameters) {
    BigQueryExtractParameters params;

    auto source_table = named_parameters.find("source_table");
    if (source_table == named_parameters.end() || source_table->second.IsNull()) {
        throw BinderException("Parameter 'source_table' is required");
    }
    params.source_table = source_table->second.GetValue<string>();
    if (params.source_table.empty()) {
        throw BinderException("Parameter 'source_table' is required");
    }

    auto destination_uris = named_parameters.find("destination_uris");
    if (destination_uris == named_parameters.end() || destination_uris->second.IsNull()) {
        throw BinderException("Parameter 'destination_uris' is required");
    }
    if (destination_uris->second.type().id() == LogicalTypeId::VARCHAR) {
        params.destination_uris.push_back(destination_uris->second.GetValue<string>());
    } else if (destination_uris->second.type().id() == LogicalTypeId::LIST) {
        for (const auto &child : ListValue::GetChildren(destination_uris->second)) {
            if (child.IsNull()) {
                throw BinderException("Null entries are not allowed in parameter 'destination_uris'");
            }
            if (child.type().id() != LogicalTypeId::VARCHAR) {
                throw BinderException("Parameter 'destination_uris' must contain only VARCHAR entries");
            }
            params.destination_uris.push_back(child.GetValue<string>());
        }
    } else {
        throw BinderException("Parameter 'destination_uris' must be VARCHAR or LIST<VARCHAR>");
    }
    if (params.destination_uris.empty()) {
        throw BinderException("Parameter 'destination_uris' must contain at least one URI");
    }
    for (const auto &destination_uri : params.destination_uris) {
        if (destination_uri.empty()) {
            throw BinderException("BigQuery extract destination URIs cannot be empty");
        }
        if (!StringUtil::CIStartsWith(destination_uri, "gs://")) {
            throw BinderException("BigQuery extract destination URI must use the gs:// scheme: %s", destination_uri);
        }
    }

    auto format = named_parameters.find("format");
    if (format != named_parameters.end() && !format->second.IsNull()) {
        params.destination_format = NormalizeExtractFormat(format->second.GetValue<string>());
        if (!IsAllowedValue(params.destination_format, {"CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "PARQUET"})) {
            throw BinderException("Invalid value for parameter 'format': %s", params.destination_format);
        }
    } else {
        for (const auto &destination_uri : params.destination_uris) {
            auto inferred_format = ExtractFormatFromUri(destination_uri);
            if (!inferred_format) {
                throw BinderException("Parameter 'format' is required when destination URI file extension is not one "
                                      "of: .csv, .csv.gz, .json, .json.gz, .avro, .parquet");
            }
            if (params.destination_format.empty()) {
                params.destination_format = *inferred_format;
            } else if (params.destination_format != *inferred_format) {
                throw BinderException(
                    "Parameter 'format' is required when destination URI file extensions imply different formats");
            }
        }
    }

    auto compression = named_parameters.find("compression");
    if (compression != named_parameters.end() && !compression->second.IsNull()) {
        params.compression = StringUtil::Upper(compression->second.GetValue<string>());
        if (!IsAllowedValue(params.compression, {"DEFLATE", "GZIP", "NONE", "SNAPPY", "ZSTD"})) {
            throw BinderException("Invalid value for parameter 'compression': %s", params.compression);
        }
        if (!IsCompressionSupported(params.destination_format, params.compression)) {
            throw BinderException("Compression '%s' is not supported for format '%s'",
                                  params.compression,
                                  params.destination_format);
        }
    }

    auto csv_print_header = named_parameters.find("csv_print_header");
    if (csv_print_header != named_parameters.end() && !csv_print_header->second.IsNull()) {
        if (params.destination_format != "CSV") {
            throw BinderException("Parameter 'csv_print_header' is only supported for CSV extracts");
        }
        params.csv_print_header = BooleanValue::Get(csv_print_header->second);
    }

    auto csv_field_delimiter = named_parameters.find("csv_field_delimiter");
    if (csv_field_delimiter != named_parameters.end() && !csv_field_delimiter->second.IsNull()) {
        if (params.destination_format != "CSV") {
            throw BinderException("Parameter 'csv_field_delimiter' is only supported for CSV extracts");
        }
        params.csv_field_delimiter = csv_field_delimiter->second.GetValue<string>();
    }

    auto avro_use_logical_types = named_parameters.find("avro_use_logical_types");
    if (avro_use_logical_types != named_parameters.end() && !avro_use_logical_types->second.IsNull()) {
        if (params.destination_format != "AVRO") {
            throw BinderException("Parameter 'avro_use_logical_types' is only supported for AVRO extracts");
        }
        params.avro_use_logical_types = BooleanValue::Get(avro_use_logical_types->second);
    }

    auto location = named_parameters.find("location");
    if (location != named_parameters.end() && !location->second.IsNull()) {
        params.location = location->second.GetValue<string>();
    }

    auto billing_project = named_parameters.find("billing_project");
    if (billing_project != named_parameters.end() && !billing_project->second.IsNull()) {
        params.billing_project = billing_project->second.GetValue<string>();
    }

    auto api_endpoint = named_parameters.find("api_endpoint");
    if (api_endpoint != named_parameters.end() && !api_endpoint->second.IsNull()) {
        params.api_endpoint = api_endpoint->second.GetValue<string>();
    }

    params.labels = BigqueryUtils::ParseOptionalLabelsParameter(named_parameters);
    params.timeout_ms = BigqueryUtils::ParseTimeoutMsParameter(named_parameters);

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

static unique_ptr<FunctionData> BigQueryExtractBind(ClientContext &context,
                                                    TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types,
                                                    vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto params = ParseExtractParameters(input.named_parameters);

    auto bind_data = make_uniq<BigQueryExtractBindData>();
    bind_data->destination_uris = params.destination_uris;
    bind_data->destination_format = params.destination_format;
    bind_data->location = params.location ? *params.location : BigquerySettings::DefaultLocation();
    bind_data->compression = params.compression;
    bind_data->csv_print_header = params.csv_print_header;
    bind_data->csv_field_delimiter = params.csv_field_delimiter;
    bind_data->avro_use_logical_types = params.avro_use_logical_types;
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

static void BigQueryExtractFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<BigQueryExtractBindData>();
    if (data.finished) {
        return;
    }

    auto job = data.bq_client->ExtractTableToGcs(data.source_table,
                                                 data.destination_uris,
                                                 data.destination_format,
                                                 data.location,
                                                 data.compression,
                                                 data.csv_print_header,
                                                 data.csv_field_delimiter,
                                                 data.avro_use_logical_types,
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

BigQueryExtractFunction::BigQueryExtractFunction()
    : TableFunction("bigquery_extract",
                    {LogicalType(LogicalTypeId::VARCHAR)},
                    BigQueryExtractFunc,
                    BigQueryExtractBind) {
    named_parameters["source_table"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["destination_uris"] = LogicalType::ANY;
    named_parameters["format"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["compression"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["csv_print_header"] = LogicalType(LogicalTypeId::BOOLEAN);
    named_parameters["csv_field_delimiter"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["avro_use_logical_types"] = LogicalType(LogicalTypeId::BOOLEAN);
    named_parameters["location"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["labels"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
    named_parameters["billing_project"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["api_endpoint"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["timeout_ms"] = LogicalType::BIGINT;
}

} // namespace bigquery
} // namespace duckdb
