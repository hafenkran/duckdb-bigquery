#include "bigquery_table_storage.hpp"
#include "bigquery_arrow_scan.hpp"
#include "bigquery_query.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include <cctype>
#include <cmath>
#include <cstdint>
#include <optional>

namespace duckdb {
namespace bigquery {

struct BigqueryTableStorageBindData : public TableFunctionData {
    string query;
    shared_ptr<BigqueryClient> bq_client;
    BigqueryConfig bq_config;
    bool use_legacy_scan = false;

    vector<string> names;
    vector<LogicalType> types;

    bool direct_results = false;
    bool finished = false;
    idx_t position = 0;
    google::cloud::bigquery::v2::QueryResponse query_response;
    mutable mutex lock;

    unique_ptr<BigqueryArrowScanBindData> arrow_bind;
    unique_ptr<BigqueryLegacyScanBindData> legacy_bind;
};

struct BigqueryTableStorageGlobalState : public GlobalTableFunctionState {
    unique_ptr<GlobalTableFunctionState> scan_global;
};

struct BigqueryTableStorageLocalState : public LocalTableFunctionState {
    unique_ptr<LocalTableFunctionState> scan_local;
};

static string ResolveProjectId(ClientContext &context, const string &dbname_or_project_id) {
    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, dbname_or_project_id);
    if (database) {
        auto &catalog = database->GetCatalog();
        if (catalog.GetCatalogType() != "bigquery") {
            throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
        }
        auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
        return bigquery_catalog.config.project_id;
    }
    return dbname_or_project_id;
}

static bool IsNumericString(const string &value) {
    if (value.empty()) {
        return false;
    }
    bool seen_digit = false;
    bool seen_dot = false;
    for (idx_t i = 0; i < value.size(); i++) {
        auto c = value[i];
        if (std::isdigit(c)) {
            seen_digit = true;
            continue;
        }
        if ((c == '-' || c == '+') && i == 0) {
            continue;
        }
        if (c == '.' && !seen_dot) {
            seen_dot = true;
            continue;
        }
        return false;
    }
    return seen_digit;
}

static bool ExtractValueString(const google::protobuf::Value &value, string &out) {
    switch (value.kind_case()) {
    case google::protobuf::Value::kStringValue:
        out = value.string_value();
        return true;
    case google::protobuf::Value::kNumberValue:
        out = StringUtil::ToString(value.number_value());
        return true;
    case google::protobuf::Value::kBoolValue:
        out = value.bool_value() ? "true" : "false";
        return true;
    case google::protobuf::Value::kNullValue:
    case google::protobuf::Value::KIND_NOT_SET:
        return false;
    default:
        return false;
    }
}

static Value ConvertScalarValue(const google::protobuf::Value &value, const LogicalType &type) {
    string value_str;
    if (!ExtractValueString(value, value_str)) {
        return Value();
    }

    switch (type.id()) {
    case LogicalTypeId::VARCHAR:
        return Value(value_str);
    case LogicalTypeId::BLOB:
        return Value::BLOB(value_str);
    case LogicalTypeId::BIGINT:
        return Value::BIGINT(std::stoll(value_str));
    case LogicalTypeId::UBIGINT:
        return Value::UBIGINT(std::stoull(value_str));
    case LogicalTypeId::DOUBLE:
        return Value::DOUBLE(std::stod(value_str));
    case LogicalTypeId::BOOLEAN:
        return Value::BOOLEAN(StringUtil::Lower(value_str) == "true" || value_str == "1");
    case LogicalTypeId::DATE:
        return Value::DATE(Date::FromString(value_str, false));
    case LogicalTypeId::TIME:
        return Value::TIME(Time::FromString(value_str, false, nullptr));
    case LogicalTypeId::TIMESTAMP:
        if (IsNumericString(value_str)) {
            auto seconds = std::stod(value_str);
            auto micros = static_cast<int64_t>(std::llround(seconds * 1000000.0));
            return Value::TIMESTAMP(Timestamp::FromEpochMicroSeconds(micros));
        }
        return Value::TIMESTAMP(Timestamp::FromString(value_str, false));
    default:
        throw BinderException("Unsupported type for direct results: " + type.ToString());
    }
}

static unique_ptr<FunctionData> BigqueryTableStorageBind(ClientContext &context,
                                                         TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types,
                                                         vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto params = BigQueryCommonParameters::ParseFromNamedParameters(input.named_parameters);

    std::optional<string> region;
    std::optional<string> dataset;
    for (auto &param : input.named_parameters) {
        auto key = StringUtil::Lower(param.first);
        if (key == "region") {
            region = param.second.GetValue<string>();
        } else if (key == "dataset") {
            dataset = param.second.GetValue<string>();
        }
    }

    if (region.has_value() == dataset.has_value()) {
        throw BinderException("Provide exactly one of 'region' or 'dataset'");
    }

    string dataset_id;
    if (region.has_value()) {
        if (region->empty()) {
            throw BinderException("'region' cannot be empty");
        }
        if (StringUtil::StartsWith(StringUtil::Lower(*region), "region-")) {
            dataset_id = *region;
        } else {
            dataset_id = "region-" + *region;
        }
    } else {
        if (dataset->empty()) {
            throw BinderException("'dataset' cannot be empty");
        }
        dataset_id = *dataset;
    }

    auto project_id = ResolveProjectId(context, dbname_or_project_id);
    auto table_string =
        BigqueryUtils::FormatTableStringSimple(project_id, dataset_id, "INFORMATION_SCHEMA.TABLE_STORAGE");
    auto query_string = "SELECT * FROM `" + table_string + "`";

    if (params.dry_run) {
        throw BinderException("'dry_run' is not supported for bigquery_table_storage");
    }

    auto scan_bind_base =
        BigqueryQueryBindInternal(context, dbname_or_project_id, query_string, params, return_types, names, false);
    auto bind_data = make_uniq<BigqueryTableStorageBindData>();
    bind_data->query = query_string;
    bind_data->names = names;
    bind_data->types = return_types;

    if (auto *arrow = dynamic_cast<BigqueryArrowScanBindData *>(scan_bind_base.get())) {
        bind_data->arrow_bind = unique_ptr<BigqueryArrowScanBindData>(
            static_cast<BigqueryArrowScanBindData *>(scan_bind_base.release()));
        bind_data->bq_client = bind_data->arrow_bind->bq_client;
        bind_data->bq_config = bind_data->arrow_bind->bq_config;
        bind_data->use_legacy_scan = false;
    } else {
        bind_data->legacy_bind = unique_ptr<BigqueryLegacyScanBindData>(
            static_cast<BigqueryLegacyScanBindData *>(scan_bind_base.release()));
        bind_data->bq_client = bind_data->legacy_bind->bq_client;
        bind_data->bq_config = bind_data->legacy_bind->config;
        bind_data->use_legacy_scan = true;
    }

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> BigqueryTableStorageInitGlobal(ClientContext &context,
                                                                           TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->CastNoConst<BigqueryTableStorageBindData>();
    auto response = bind_data.bq_client->ExecuteQuery(bind_data.query);
    auto job = bind_data.bq_client->GetJobByReference(response.job_reference());
    if (job.status().has_error_result()) {
        throw BinderException(job.status().error_result().message());
    }

    auto result = make_uniq<BigqueryTableStorageGlobalState>();
    if (response.page_token().empty()) {
        bind_data.direct_results = true;
        bind_data.query_response = std::move(response);
        return std::move(result);
    }

    bind_data.direct_results = false;
    auto destination_table = job.configuration().query().destination_table();
    auto table_ref = BigqueryTableRef(destination_table.project_id(),
                                      destination_table.dataset_id(),
                                      destination_table.table_id());

    if (bind_data.use_legacy_scan) {
        bind_data.legacy_bind->table_ref = table_ref;
        TableFunctionInitInput scan_input(optional_ptr<const FunctionData>(bind_data.legacy_bind.get()),
                                          input.column_ids,
                                          input.projection_ids,
                                          input.filters,
                                          input.sample_options,
                                          input.op);
        result->scan_global = BigqueryLegacyScanFunction::BigqueryLegacyScanInitGlobalState(context, scan_input);
    } else {
        bind_data.arrow_bind->table_ref = table_ref;
        TableFunctionInitInput scan_input(optional_ptr<const FunctionData>(bind_data.arrow_bind.get()),
                                          input.column_ids,
                                          input.projection_ids,
                                          input.filters,
                                          input.sample_options,
                                          input.op);
        result->scan_global = BigqueryArrowScanFunction::BigqueryArrowScanInitGlobal(context, scan_input);
    }

    return std::move(result);
}

static unique_ptr<LocalTableFunctionState> BigqueryTableStorageInitLocal(ExecutionContext &context,
                                                                         TableFunctionInitInput &input,
                                                                         GlobalTableFunctionState *global_state) {
    auto &bind_data = input.bind_data->CastNoConst<BigqueryTableStorageBindData>();
    auto result = make_uniq<BigqueryTableStorageLocalState>();
    if (bind_data.direct_results) {
        return std::move(result);
    }

    auto &gstate = global_state->Cast<BigqueryTableStorageGlobalState>();
    if (bind_data.use_legacy_scan) {
        TableFunctionInitInput scan_input(optional_ptr<const FunctionData>(bind_data.legacy_bind.get()),
                                          input.column_ids,
                                          input.projection_ids,
                                          input.filters,
                                          input.sample_options,
                                          input.op);
        result->scan_local = BigqueryLegacyScanFunction::BigqueryLegacyScanInitLocalState(context, scan_input,
                                                                                          gstate.scan_global.get());
    } else {
        TableFunctionInitInput scan_input(optional_ptr<const FunctionData>(bind_data.arrow_bind.get()),
                                          input.column_ids,
                                          input.projection_ids,
                                          input.filters,
                                          input.sample_options,
                                          input.op);
        result->scan_local =
            BigqueryArrowScanFunction::BigqueryArrowScanInitLocal(context, scan_input, gstate.scan_global.get());
    }
    return std::move(result);
}

static void BigqueryTableStorageExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<BigqueryTableStorageBindData>();
    auto &gstate = data_p.global_state->Cast<BigqueryTableStorageGlobalState>();
    auto &lstate = data_p.local_state->Cast<BigqueryTableStorageLocalState>();

    if (!bind_data.direct_results) {
        if (bind_data.use_legacy_scan) {
            TableFunctionInput scan_input(optional_ptr<const FunctionData>(bind_data.legacy_bind.get()),
                                          lstate.scan_local.get(),
                                          gstate.scan_global.get());
            BigqueryLegacyScanFunction::BigqueryLegacyScanExecute(context, scan_input, output);
        } else {
            TableFunctionInput scan_input(optional_ptr<const FunctionData>(bind_data.arrow_bind.get()),
                                          lstate.scan_local.get(),
                                          gstate.scan_global.get());
            BigqueryArrowScanFunction::BigqueryArrowScanExecute(context, scan_input, output);
        }
        return;
    }

    lock_guard<mutex> glock(bind_data.lock);
    if (bind_data.finished) {
        return;
    }

    const auto &rows = bind_data.query_response.rows();
    idx_t out_idx = 0;
    while (out_idx < STANDARD_VECTOR_SIZE && bind_data.position < static_cast<idx_t>(rows.size())) {
        const auto &row = rows.Get(static_cast<int>(bind_data.position));
        const auto &fields = row.fields();
        auto it = fields.find("f");
        if (it == fields.end()) {
            throw BinderException("Unexpected row format for bigquery_table_storage");
        }
        const auto &field_list = it->second.list_value().values();
        for (idx_t col_idx = 0; col_idx < bind_data.types.size(); col_idx++) {
            if (col_idx >= static_cast<idx_t>(field_list.size())) {
                output.SetValue(col_idx, out_idx, Value());
                continue;
            }
            const auto &cell_struct = field_list[static_cast<int>(col_idx)].struct_value();
            auto vit = cell_struct.fields().find("v");
            if (vit == cell_struct.fields().end()) {
                output.SetValue(col_idx, out_idx, Value());
                continue;
            }
            output.SetValue(col_idx, out_idx, ConvertScalarValue(vit->second, bind_data.types[col_idx]));
        }
        bind_data.position++;
        out_idx++;
    }

    output.SetCardinality(out_idx);
    if (bind_data.position >= static_cast<idx_t>(rows.size())) {
        bind_data.finished = true;
    }
}

static InsertionOrderPreservingMap<string> BigqueryTableStorageToString(TableFunctionToStringInput &input) {
    auto &bind_data = input.bind_data->Cast<BigqueryTableStorageBindData>();
    InsertionOrderPreservingMap<string> result;
    if (bind_data.direct_results) {
        result["Query"] = bind_data.query;
        result["Type"] = "Direct Results";
        return result;
    }
    optional_ptr<const FunctionData> scan_bind = bind_data.use_legacy_scan
                                                     ? optional_ptr<const FunctionData>(bind_data.legacy_bind.get())
                                                     : optional_ptr<const FunctionData>(bind_data.arrow_bind.get());
    TableFunctionToStringInput scan_input(input.table_function, scan_bind);
    return BigqueryQueryToString(scan_input);
}

static unique_ptr<NodeStatistics> BigqueryTableStorageCardinality(ClientContext &context,
                                                                  const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BigqueryTableStorageBindData>();
    if (bind_data.direct_results) {
        idx_t n = 0;
        if (bind_data.query_response.has_total_rows()) {
            n = static_cast<idx_t>(bind_data.query_response.total_rows().value());
        }
        return make_uniq<NodeStatistics>(n, n);
    }
    optional_ptr<const FunctionData> scan_bind = bind_data.use_legacy_scan
                                                     ? optional_ptr<const FunctionData>(bind_data.legacy_bind.get())
                                                     : optional_ptr<const FunctionData>(bind_data.arrow_bind.get());
    return BigqueryQueryCardinality(context, scan_bind.get());
}

static double BigqueryTableStorageProgress(ClientContext &context,
                                           const FunctionData *bind_data_p,
                                           const GlobalTableFunctionState *global_state) {
    auto &bind_data = bind_data_p->Cast<BigqueryTableStorageBindData>();
    if (bind_data.direct_results) {
        if (bind_data.query_response.has_total_rows() && bind_data.query_response.total_rows().value() > 0) {
            double pos = static_cast<double>(bind_data.position);
            double total = static_cast<double>(bind_data.query_response.total_rows().value());
            return MinValue<double>(100.0, 100.0 * pos / total);
        }
        return bind_data.finished ? 100.0 : 0.0;
    }
    auto &gstate = global_state->Cast<BigqueryTableStorageGlobalState>();
    optional_ptr<const FunctionData> scan_bind = bind_data.use_legacy_scan
                                                     ? optional_ptr<const FunctionData>(bind_data.legacy_bind.get())
                                                     : optional_ptr<const FunctionData>(bind_data.arrow_bind.get());
    return BigqueryQueryProgress(context, scan_bind.get(), gstate.scan_global.get());
}

static BindInfo BigqueryTableStorageGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BigqueryTableStorageBindData>();
    if (bind_data.direct_results) {
        return BindInfo(ScanType::EXTERNAL);
    }
    optional_ptr<const FunctionData> scan_bind = bind_data.use_legacy_scan
                                                     ? optional_ptr<const FunctionData>(bind_data.legacy_bind.get())
                                                     : optional_ptr<const FunctionData>(bind_data.arrow_bind.get());
    return BigqueryQueryGetBindInfo(scan_bind);
}

BigqueryTableStorageFunction::BigqueryTableStorageFunction()
    : TableFunction("bigquery_table_storage",
                    {LogicalType::VARCHAR},
                    BigqueryTableStorageExecute,
                    BigqueryTableStorageBind,
                    BigqueryTableStorageInitGlobal,
                    BigqueryTableStorageInitLocal) {
    to_string = BigqueryTableStorageToString;
    cardinality = BigqueryTableStorageCardinality;
    table_scan_progress = BigqueryTableStorageProgress;
    get_bind_info = BigqueryTableStorageGetBindInfo;

    projection_pushdown = true;
    filter_pushdown = true;
    filter_prune = true;

    named_parameters["region"] = LogicalType::VARCHAR;
    named_parameters["dataset"] = LogicalType::VARCHAR;
    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["use_legacy_scan"] = LogicalType::BOOLEAN;
}

} // namespace bigquery
} // namespace duckdb
