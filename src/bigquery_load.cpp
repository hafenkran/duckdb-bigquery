#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
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
#include <initializer_list>
#include <limits>
#include <map>
#include <optional>

namespace duckdb {
namespace bigquery {

struct BigQueryLoadBindData : public TableFunctionData {
    BigqueryConfig config;
    shared_ptr<BigqueryClient> bq_client;
    BigqueryTableRef destination_table;
    std::optional<string> source_file;
    vector<string> source_uris;
    std::optional<string> source_table;
    BigQueryLoadOptions options;
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
    vector<string> source_uris;
    std::optional<string> source_table;
    string billing_project;
    bool source_format_explicit = false;
    BigQueryLoadOptions options;
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

static std::optional<vector<string>> ParseOptionalSourceUrisParameter(const named_parameter_map_t &named_parameters,
                                                                      const string &parameter_name) {
    auto entry = named_parameters.find(parameter_name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return std::nullopt;
    }

    if (entry->second.type().id() == LogicalTypeId::VARCHAR) {
        return vector<string>{entry->second.GetValue<string>()};
    }
    if (entry->second.type().id() != LogicalTypeId::LIST) {
        throw BinderException("Parameter '%s' must be a VARCHAR or LIST<VARCHAR>", parameter_name);
    }

    vector<string> result;
    for (const auto &child : ListValue::GetChildren(entry->second)) {
        if (child.IsNull()) {
            throw BinderException("Null entries are not allowed in parameter '%s'", parameter_name);
        }
        if (child.type().id() != LogicalTypeId::VARCHAR) {
            throw BinderException("Parameter '%s' must contain only VARCHAR entries", parameter_name);
        }
        result.push_back(child.GetValue<string>());
    }
    return result;
}

static std::optional<string> ParseOptionalAliasedStringParameter(const named_parameter_map_t &named_parameters,
                                                                 const string &parameter_name,
                                                                 const string &legacy_parameter_name) {
    auto value = ParseOptionalStringParameter(named_parameters, parameter_name);
    auto legacy_value = ParseOptionalStringParameter(named_parameters, legacy_parameter_name);
    if (value && legacy_value) {
        throw BinderException("Cannot specify both named parameters '%s' and '%s'",
                              parameter_name,
                              legacy_parameter_name);
    }
    return value ? value : legacy_value;
}

static std::optional<bool> ParseOptionalBoolParameter(const named_parameter_map_t &named_parameters,
                                                      const string &parameter_name) {
    auto entry = named_parameters.find(parameter_name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return std::nullopt;
    }
    return entry->second.GetValue<bool>();
}

static std::optional<int> ParseOptionalNonNegativeInt32Parameter(const named_parameter_map_t &named_parameters,
                                                                 const string &parameter_name) {
    auto entry = named_parameters.find(parameter_name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return std::nullopt;
    }

    auto value = entry->second.GetValue<int64_t>();
    if (value < 0) {
        throw InvalidInputException("Parameter '%s' must be non-negative", parameter_name);
    }
    if (value > std::numeric_limits<int>::max()) {
        throw InvalidInputException("Parameter '%s' must fit in a 32-bit integer", parameter_name);
    }
    return static_cast<int>(value);
}

static vector<string> ParseOptionalStringListParameter(const named_parameter_map_t &named_parameters,
                                                       const string &parameter_name,
                                                       const bool normalize_upper = false) {
    auto entry = named_parameters.find(parameter_name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return {};
    }

    vector<string> result;
    if (entry->second.type().id() == LogicalTypeId::VARCHAR) {
        auto value = entry->second.GetValue<string>();
        result.push_back(normalize_upper ? NormalizeEnumValue(value) : value);
        return result;
    }
    if (entry->second.type().id() != LogicalTypeId::LIST) {
        throw BinderException("Parameter '%s' must be a VARCHAR or LIST<VARCHAR>", parameter_name);
    }

    for (const auto &child : ListValue::GetChildren(entry->second)) {
        if (child.IsNull()) {
            throw BinderException("Null entries are not allowed in parameter '%s'", parameter_name);
        }
        if (child.type().id() != LogicalTypeId::VARCHAR) {
            throw BinderException("Parameter '%s' must contain only VARCHAR entries", parameter_name);
        }
        auto value = child.GetValue<string>();
        result.push_back(normalize_upper ? NormalizeEnumValue(value) : value);
    }
    return result;
}

static bool HasAnyOption(const BigQueryLoadOptions &options, const std::initializer_list<bool> option_presence) {
    for (auto present : option_presence) {
        if (present) {
            return true;
        }
    }
    return false;
}

static bool HasCsvOptions(const BigQueryLoadOptions &options) {
    return HasAnyOption(options,
                        {!options.csv_field_delimiter.empty(),
                         options.csv_skip_leading_rows.has_value(),
                         options.csv_quote.has_value(),
                         options.csv_allow_quoted_newlines.has_value(),
                         options.csv_allow_jagged_rows.has_value(),
                         !options.csv_encoding.empty(),
                         options.csv_null_marker.has_value(),
                         !options.csv_null_markers.empty(),
                         options.csv_preserve_ascii_control_characters.has_value()});
}

static bool HasTemporalOptions(const BigQueryLoadOptions &options) {
    return HasAnyOption(options,
                        {!options.date_format.empty(),
                         !options.datetime_format.empty(),
                         !options.time_format.empty(),
                         !options.timestamp_format.empty(),
                         !options.time_zone.empty()});
}

static string InferSourceFormatFromPath(const string &source) {
    auto lower = StringUtil::Lower(source);
    auto query_pos = lower.find('?');
    if (query_pos != string::npos) {
        lower = lower.substr(0, query_pos);
    }

    const vector<string> compression_suffixes = {".gz", ".gzip", ".bz2", ".zst", ".zstd", ".snappy", ".deflate"};
    for (const auto &suffix : compression_suffixes) {
        if (lower.size() > suffix.size() && lower.rfind(suffix) == lower.size() - suffix.size()) {
            lower = lower.substr(0, lower.size() - suffix.size());
            break;
        }
    }

    if (lower.size() >= 4 && lower.rfind(".csv") == lower.size() - 4) {
        return "CSV";
    }
    if (lower.size() >= 5 && lower.rfind(".json") == lower.size() - 5) {
        return "NEWLINE_DELIMITED_JSON";
    }
    if (lower.size() >= 7 && lower.rfind(".ndjson") == lower.size() - 7) {
        return "NEWLINE_DELIMITED_JSON";
    }
    if (lower.size() >= 5 && lower.rfind(".avro") == lower.size() - 5) {
        return "AVRO";
    }
    if (lower.size() >= 4 && lower.rfind(".orc") == lower.size() - 4) {
        return "ORC";
    }
    if (lower.size() >= 8 && lower.rfind(".parquet") == lower.size() - 8) {
        return "PARQUET";
    }
    if (lower.size() >= 5 && lower.rfind(".parq") == lower.size() - 5) {
        return "PARQUET";
    }
    return "";
}

static string InferSourceFormatFromSources(const vector<string> &sources) {
    string inferred_format;
    for (const auto &source : sources) {
        auto source_format = InferSourceFormatFromPath(source);
        if (source_format.empty()) {
            return "";
        }
        if (inferred_format.empty()) {
            inferred_format = source_format;
        } else if (inferred_format != source_format) {
            return "";
        }
    }
    return inferred_format;
}

static void ValidateEnumList(const vector<string> &values,
                             const string &parameter_name,
                             const vector<string> &allowed_values) {
    for (const auto &value : values) {
        bool found = false;
        for (const auto &allowed_value : allowed_values) {
            if (value == allowed_value) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw BinderException("Invalid value for parameter '%s': %s", parameter_name, value);
        }
    }
}

static void ValidateLoadOptionCombinations(const BigQueryLoadOptions &options, const bool source_is_uris) {
    const auto &format = options.source_format;
    if (HasCsvOptions(options) && format != "CSV") {
        throw BinderException("CSV-specific bigquery_load options require source_format='CSV'");
    }
    if (!options.json_extension.empty() && format != "NEWLINE_DELIMITED_JSON") {
        throw BinderException("json_extension requires source_format='NEWLINE_DELIMITED_JSON'");
    }
    if (options.avro_use_logical_types.has_value() && format != "AVRO") {
        throw BinderException("avro_use_logical_types requires source_format='AVRO'");
    }
    if ((options.parquet_enable_list_inference.has_value() || options.parquet_enum_as_string.has_value()) &&
        format != "PARQUET") {
        throw BinderException("Parquet-specific bigquery_load options require source_format='PARQUET'");
    }
    if (HasTemporalOptions(options) && format != "CSV" && format != "NEWLINE_DELIMITED_JSON") {
        throw BinderException("Temporal parsing options require source_format='CSV' or "
                              "source_format='NEWLINE_DELIMITED_JSON'");
    }
    if (!options.reference_file_schema_uri.empty() && format != "AVRO" && format != "PARQUET" && format != "ORC") {
        throw BinderException("reference_file_schema_uri requires source_format='AVRO', 'PARQUET', or 'ORC'");
    }
    if (!options.decimal_target_types.empty() && format != "AVRO" && format != "PARQUET" && format != "ORC") {
        throw BinderException("decimal_target_types requires source_format='AVRO', 'PARQUET', or 'ORC'");
    }
    if ((!options.hive_partitioning_mode.empty() || !options.hive_partitioning_source_uri_prefix.empty()) &&
        !source_is_uris) {
        throw BinderException("Hive partitioning options require source_uris");
    }
    if (options.csv_null_marker.has_value() && !options.csv_null_markers.empty()) {
        throw BinderException("Cannot specify both csv_null_marker and csv_null_markers");
    }
}

static BigQueryLoadParameters ParseLoadParameters(const named_parameter_map_t &named_parameters) {
    BigQueryLoadParameters params;
    params.source_file = ParseOptionalAliasedStringParameter(named_parameters, "source_file", "file");
    params.source_table = ParseOptionalAliasedStringParameter(named_parameters, "source_table", "table");
    auto source_uris = ParseOptionalSourceUrisParameter(named_parameters, "source_uris");
    if (source_uris) {
        params.source_uris = *source_uris;
    }
    if (source_uris && source_uris->empty()) {
        throw BinderException("Parameter 'source_uris' must contain at least one URI");
    }
    auto source_format = ParseOptionalStringParameter(named_parameters, "source_format");
    if (source_format.has_value()) {
        params.source_format_explicit = true;
        params.options.source_format = NormalizeEnumValue(*source_format);
        ValidateEnumList({params.options.source_format},
                         "source_format",
                         {"PARQUET", "CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "ORC"});
    }

    auto location = ParseOptionalStringParameter(named_parameters, "location");
    params.options.location = location ? *location : string();
    auto billing_project = ParseOptionalStringParameter(named_parameters, "billing_project");
    params.billing_project = billing_project ? *billing_project : string();
    params.options.labels = BigqueryUtils::ParseOptionalLabelsParameter(named_parameters);
    params.options.timeout_ms = BigqueryUtils::ParseTimeoutMsParameter(named_parameters);
    params.options.write_disposition = ParseEnumParameter(named_parameters,
                                                          "write_disposition",
                                                          "WRITE_TRUNCATE",
                                                          {"WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"});
    params.options.create_disposition = ParseEnumParameter(named_parameters,
                                                           "create_disposition",
                                                           "CREATE_IF_NEEDED",
                                                           {"CREATE_IF_NEEDED", "CREATE_NEVER"});
    params.options.autodetect = ParseOptionalBoolParameter(named_parameters, "autodetect");
    params.options.schema_update_options =
        ParseOptionalStringListParameter(named_parameters, "schema_update_options", true);
    ValidateEnumList(params.options.schema_update_options,
                     "schema_update_options",
                     {"ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"});
    params.options.max_bad_records = ParseOptionalNonNegativeInt32Parameter(named_parameters, "max_bad_records");
    params.options.ignore_unknown_values = ParseOptionalBoolParameter(named_parameters, "ignore_unknown_values");

    auto csv_field_delimiter = ParseOptionalStringParameter(named_parameters, "csv_field_delimiter");
    params.options.csv_field_delimiter = csv_field_delimiter ? *csv_field_delimiter : string();
    params.options.csv_skip_leading_rows =
        ParseOptionalNonNegativeInt32Parameter(named_parameters, "csv_skip_leading_rows");
    params.options.csv_quote = ParseOptionalStringParameter(named_parameters, "csv_quote");
    params.options.csv_allow_quoted_newlines =
        ParseOptionalBoolParameter(named_parameters, "csv_allow_quoted_newlines");
    params.options.csv_allow_jagged_rows = ParseOptionalBoolParameter(named_parameters, "csv_allow_jagged_rows");
    auto csv_encoding = ParseOptionalStringParameter(named_parameters, "csv_encoding");
    params.options.csv_encoding = csv_encoding ? NormalizeEnumValue(*csv_encoding) : string();
    params.options.csv_null_marker = ParseOptionalStringParameter(named_parameters, "csv_null_marker");
    params.options.csv_null_markers = ParseOptionalStringListParameter(named_parameters, "csv_null_markers");
    params.options.csv_preserve_ascii_control_characters =
        ParseOptionalBoolParameter(named_parameters, "csv_preserve_ascii_control_characters");

    auto date_format = ParseOptionalStringParameter(named_parameters, "date_format");
    params.options.date_format = date_format ? *date_format : string();
    auto datetime_format = ParseOptionalStringParameter(named_parameters, "datetime_format");
    params.options.datetime_format = datetime_format ? *datetime_format : string();
    auto time_format = ParseOptionalStringParameter(named_parameters, "time_format");
    params.options.time_format = time_format ? *time_format : string();
    auto timestamp_format = ParseOptionalStringParameter(named_parameters, "timestamp_format");
    params.options.timestamp_format = timestamp_format ? *timestamp_format : string();
    auto time_zone = ParseOptionalStringParameter(named_parameters, "time_zone");
    params.options.time_zone = time_zone ? *time_zone : string();

    auto json_extension = ParseOptionalStringParameter(named_parameters, "json_extension");
    params.options.json_extension = json_extension ? NormalizeEnumValue(*json_extension) : string();
    if (!params.options.json_extension.empty()) {
        ValidateEnumList({params.options.json_extension}, "json_extension", {"GEOJSON"});
    }
    params.options.avro_use_logical_types = ParseOptionalBoolParameter(named_parameters, "avro_use_logical_types");
    params.options.parquet_enable_list_inference =
        ParseOptionalBoolParameter(named_parameters, "parquet_enable_list_inference");
    params.options.parquet_enum_as_string = ParseOptionalBoolParameter(named_parameters, "parquet_enum_as_string");
    auto reference_file_schema_uri = ParseOptionalStringParameter(named_parameters, "reference_file_schema_uri");
    params.options.reference_file_schema_uri = reference_file_schema_uri ? *reference_file_schema_uri : string();
    params.options.decimal_target_types =
        ParseOptionalStringListParameter(named_parameters, "decimal_target_types", true);
    ValidateEnumList(params.options.decimal_target_types, "decimal_target_types", {"NUMERIC", "BIGNUMERIC", "STRING"});

    auto hive_partitioning_mode = ParseOptionalStringParameter(named_parameters, "hive_partitioning_mode");
    params.options.hive_partitioning_mode =
        hive_partitioning_mode ? NormalizeEnumValue(*hive_partitioning_mode) : string();
    if (!params.options.hive_partitioning_mode.empty()) {
        ValidateEnumList({params.options.hive_partitioning_mode},
                         "hive_partitioning_mode",
                         {"AUTO", "STRINGS", "CUSTOM"});
    }
    auto hive_partitioning_source_uri_prefix =
        ParseOptionalStringParameter(named_parameters, "hive_partitioning_source_uri_prefix");
    params.options.hive_partitioning_source_uri_prefix =
        hive_partitioning_source_uri_prefix ? *hive_partitioning_source_uri_prefix : string();

    const auto source_count = static_cast<int>(params.source_file.has_value()) +
                              static_cast<int>(params.source_table.has_value()) +
                              static_cast<int>(source_uris.has_value());
    if (source_count != 1) {
        throw BinderException("Exactly one of the named parameters 'source_file'/'file', 'source_uris', or "
                              "'source_table'/'table' must be provided");
    }

    return params;
}

static bool IsGcsUri(const string &source) {
    return StringUtil::CIStartsWith(source, "gs://");
}

static void ValidateGcsSourceUris(const vector<string> &source_uris) {
    for (const auto &source_uri : source_uris) {
        if (source_uri.empty()) {
            throw BinderException("BigQuery load source URIs cannot be empty");
        }
        if (!IsGcsUri(source_uri)) {
            throw BinderException("BigQuery load source URI must use the gs:// scheme: %s", source_uri);
        }
    }
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

    auto copy_query = "COPY (SELECT * FROM " + relation_name + ") TO '" + escaped_temp_file_path + "' (FORMAT PARQUET)";

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
    bind_data->source_file = params.source_file;
    bind_data->source_uris = params.source_uris;
    bind_data->source_table = params.source_table;
    bind_data->options = params.options;
    if (bind_data->options.location.empty()) {
        bind_data->options.location = BigquerySettings::DefaultLocation();
    }

    auto destination_table = BigqueryUtils::ParseDatasetTableString(destination_table_string);

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

        auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
        auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);
        if (transaction.GetAccessMode() == AccessMode::READ_ONLY) {
            throw BinderException("Cannot run BigQuery load jobs in read-only transaction");
        }

        bind_data->config = bigquery_catalog.config;
        bind_data->bq_client = transaction.GetBigqueryClient();
    } else {
        bind_data->config = BigqueryConfig(dbname_or_project_id).SetBillingProjectId(params.billing_project);
        bind_data->bq_client = make_shared_ptr<BigqueryClient>(context, bind_data->config);
    }

    destination_table.project_id = bind_data->config.project_id;
    bind_data->destination_table = std::move(destination_table);

    if (bind_data->source_file.has_value() && IsGcsUri(*bind_data->source_file)) {
        bind_data->source_uris.push_back(*bind_data->source_file);
        bind_data->source_file.reset();
    }
    if (!bind_data->source_uris.empty()) {
        ValidateGcsSourceUris(bind_data->source_uris);
    }

    if (!params.source_format_explicit) {
        string inferred_format;
        if (bind_data->source_file.has_value()) {
            inferred_format = InferSourceFormatFromPath(*bind_data->source_file);
        } else if (!bind_data->source_uris.empty()) {
            inferred_format = InferSourceFormatFromSources(bind_data->source_uris);
        }
        if (!inferred_format.empty()) {
            bind_data->options.source_format = inferred_format;
        }
    }

    if (bind_data->source_table.has_value() && bind_data->options.source_format != "PARQUET") {
        throw BinderException("source_table loads are staged as Parquet and require source_format='PARQUET'");
    }
    ValidateLoadOptionCombinations(bind_data->options, !bind_data->source_uris.empty());

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
        job = data.bq_client->LoadFile(data.destination_table, *data.source_file, data.options);
    } else if (!data.source_uris.empty()) {
        job = data.bq_client->LoadUris(data.destination_table, data.source_uris, data.options);
    } else {
        throw InternalException("bigquery_load has no source to load");
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
    named_parameters["source_uris"] = LogicalType::ANY;
    named_parameters["source_table"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["table"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["source_format"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["write_disposition"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["create_disposition"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["location"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["billing_project"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["labels"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
    named_parameters["timeout_ms"] = LogicalType::BIGINT;
    named_parameters["autodetect"] = LogicalType::BOOLEAN;
    named_parameters["schema_update_options"] = LogicalType::ANY;
    named_parameters["max_bad_records"] = LogicalType::BIGINT;
    named_parameters["ignore_unknown_values"] = LogicalType::BOOLEAN;

    named_parameters["csv_field_delimiter"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["csv_skip_leading_rows"] = LogicalType::BIGINT;
    named_parameters["csv_quote"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["csv_allow_quoted_newlines"] = LogicalType::BOOLEAN;
    named_parameters["csv_allow_jagged_rows"] = LogicalType::BOOLEAN;
    named_parameters["csv_encoding"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["csv_null_marker"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["csv_null_markers"] = LogicalType::ANY;
    named_parameters["csv_preserve_ascii_control_characters"] = LogicalType::BOOLEAN;

    named_parameters["date_format"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["datetime_format"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["time_format"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["timestamp_format"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["time_zone"] = LogicalType(LogicalTypeId::VARCHAR);

    named_parameters["json_extension"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["avro_use_logical_types"] = LogicalType::BOOLEAN;
    named_parameters["parquet_enable_list_inference"] = LogicalType::BOOLEAN;
    named_parameters["parquet_enum_as_string"] = LogicalType::BOOLEAN;
    named_parameters["reference_file_schema_uri"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["decimal_target_types"] = LogicalType::ANY;

    named_parameters["hive_partitioning_mode"] = LogicalType(LogicalTypeId::VARCHAR);
    named_parameters["hive_partitioning_source_uri_prefix"] = LogicalType(LogicalTypeId::VARCHAR);
}

} // namespace bigquery
} // namespace duckdb
