#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <google/protobuf/util/json_util.h>

#include "bigquery_client.hpp"
#include "bigquery_jobs.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {
namespace bigquery {

struct ListJobsBindData : public TableFunctionData {
    explicit ListJobsBindData(BigqueryCatalog &bq_catalog, ListJobsParams &params)
        : bq_catalog(bq_catalog), params(std::move(params)) {
    }

public:
    BigqueryCatalog &bq_catalog;
    ListJobsParams params;
    bool finished = false;
    mutable mutex lock;
};

struct ListJobsGlobalFunctionState : public GlobalTableFunctionState {
    explicit ListJobsGlobalFunctionState() {
    }

public:
    atomic<idx_t> current_idx = 0;
    bool single_job = false;
    google::cloud::bigquery::v2::Job job_by_id;
    vector<google::cloud::bigquery::v2::ListFormatJob> jobs_list;
};

void InitializeNamesAndReturnTypes(vector<LogicalType> &return_types, vector<string> &names) {
    struct ColumnInfo {
        ColumnInfo(const std::string &name, LogicalTypeId type) : name(name), type(type) {
        }

        string name;
        LogicalTypeId type;
    };

    std::vector<ColumnInfo> columns = {
        {"state", LogicalTypeId::VARCHAR},
        {"job_id", LogicalTypeId::VARCHAR},
        {"project", LogicalTypeId::VARCHAR},
        {"location", LogicalTypeId::VARCHAR},
        {"creation_time", LogicalTypeId::TIMESTAMP},
        {"start_time", LogicalTypeId::TIMESTAMP},
        {"end_time", LogicalTypeId::TIMESTAMP},
        {"duration_ms", LogicalTypeId::INTERVAL},
        {"bytes_processed", LogicalTypeId::BIGINT},
        {"total_slot_time_ms", LogicalTypeId::BIGINT},
        {"user_email", LogicalTypeId::VARCHAR},
        {"principal_subject", LogicalTypeId::VARCHAR},
        {"job_type", LogicalTypeId::VARCHAR},
    };

    for (const auto &column : columns) {
        names.emplace_back(column.name);
        return_types.emplace_back(column.type);
    }

    names.emplace_back("statistics");
    return_types.emplace_back(LogicalType::JSON());

    names.emplace_back("configuration");
    return_types.emplace_back(LogicalType::JSON());

    names.emplace_back("status");
    return_types.emplace_back(LogicalType::JSON());
}

template <typename T>
static std::string GetJobState(const T &job) {
    google::cloud::bigquery::v2::JobStatus status = job.status();
    if (status.state() == "DONE") {
        if (status.has_error_result()) {
            return "Error";
        } else {
            return "Completed";
        }
    } else if (status.state() == "PENDING") {
        return "Queued";
    } else if (status.state() == "RUNNING") {
        return "Active";
    }
    return status.state();
}

template <typename T>
static void MapJobToRow(const T &job, DataChunk &output, idx_t &out_idx) {
    const auto &job_ref = job.job_reference();
    const auto &job_stats = job.statistics();

    // Fill the output
    const auto job_state = GetJobState(job);
    int value_idx = 0;

    // state
    output.SetValue(value_idx++, out_idx, Value(job_state));

    // job_id, project, location
    output.SetValue(value_idx++, out_idx, Value(job_ref.job_id()));
    output.SetValue(value_idx++, out_idx, Value(job_ref.project_id()));
    output.SetValue(value_idx++, out_idx, Value(job_ref.location().value()));

    // creation time
    if (job_stats.creation_time() > 0) {
        auto creation_time = Timestamp::FromEpochMs(job_stats.creation_time());
        output.SetValue(value_idx++, out_idx, Value::TIMESTAMP(creation_time));
    } else {
        output.SetValue(value_idx++, out_idx, Value());
    }

    // start time
    if (job_stats.start_time() > 0) {
        auto start_time = Timestamp::FromEpochMs(job_stats.start_time());
        output.SetValue(value_idx++, out_idx, Value::TIMESTAMP(start_time));
    } else {
        output.SetValue(value_idx++, out_idx, Value());
    }

    // end time
    if (job_stats.end_time() > 0) {
        auto end_time = Timestamp::FromEpochMs(job_stats.end_time());
        output.SetValue(value_idx++, out_idx, Value::TIMESTAMP(end_time));
    } else {
        output.SetValue(value_idx++, out_idx, Value());
    }

    // duration_ms
    if (job_stats.start_time() > 0 && job_stats.end_time() > 0) {
        auto duration_ms = job_stats.end_time() - job_stats.start_time();
        auto duration_us = duration_ms * 1000; // from milliseconds to microseconds as Value::INTERVAL expects it
        auto duration_interval = Interval::FromMicro(duration_us);
        output.SetValue(value_idx++, out_idx, Value::INTERVAL(duration_interval));
    } else {
        output.SetValue(value_idx++, out_idx, Value());
    }

    // bytes processed
    if (job_stats.query().has_total_bytes_processed()) {
        const auto total_bytes_processed = job_stats.query().total_bytes_processed().value();
        output.SetValue(value_idx++, out_idx, Value::BIGINT(total_bytes_processed));
    } else {
        output.SetValue(value_idx++, out_idx, Value());
    }

    // total slot time
    if (job_stats.query().has_total_slot_ms()) {
        const auto total_slot_ms = job_stats.query().total_slot_ms().value();
        output.SetValue(value_idx++, out_idx, Value::BIGINT(total_slot_ms));
    } else {
        output.SetValue(value_idx++, out_idx, Value());
    }

    // user email
    const auto &user_email = job.user_email();
    output.SetValue(value_idx++, out_idx, Value(user_email));

    // principal subject
    const auto &principal_subject = job.principal_subject();
    output.SetValue(value_idx++, out_idx, Value(principal_subject));

    // job type
    const auto &job_type = job.configuration().job_type();
    output.SetValue(value_idx++, out_idx, Value(job_type));

    // statistics
    std::string json_output;
    auto status = google::protobuf::util::MessageToJsonString(job_stats, &json_output);
    if (!status.ok()) {
        throw BinderException("Failed to convert job statistics to JSON: " + std::string(status.message()));
    }
    output.SetValue(value_idx++, out_idx, Value(json_output));

    // configuration
    std::string configuration_json;
    status = google::protobuf::util::MessageToJsonString(job.configuration(), &configuration_json);
    if (!status.ok()) {
        throw BinderException("Failed to convert job configuration to JSON: " + std::string(status.message()));
    }
    output.SetValue(value_idx++, out_idx, Value(configuration_json));

    // status
    std::string status_json;
    status = google::protobuf::util::MessageToJsonString(job.status(), &status_json);
    if (!status.ok()) {
        throw BinderException("Failed to convert job status to JSON: " + std::string(status.message()));
    }
    output.SetValue(value_idx++, out_idx, Value(status_json));
}

static unique_ptr<FunctionData> BigQueryListJobsBind(ClientContext &context,
                                                     TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types,
                                                     vector<string> &names) {
    auto database_name = input.inputs[0].GetValue<string>();
    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, database_name);
    if (!database) {
        throw BinderException("Failed to find attached database " + database_name);
    }

    auto &catalog = database->GetCatalog();
    if (catalog.GetCatalogType() != "bigquery") {
        throw BinderException("Database " + database_name + " is not a BigQuery database");
    }
    auto &bq_catalog = catalog.Cast<BigqueryCatalog>();

    ListJobsParams params;
    params.projection = "full"; // Full projection by default
    std::map<std::string, std::function<void(const Value &)>> param_map = {
        {"jobId", [&](const Value &val) { params.job_id = val.GetValue<string>(); }},
        {"allUsers", [&](const Value &val) { params.all_users = val.GetValue<bool>(); }},
        {"maxResults", [&](const Value &val) { params.max_results = val.GetValue<int>(); }},
        {"minCreationTime", [&](const Value &val) { params.min_creation_time = val.GetValue<timestamp_t>(); }},
        {"maxCreationTime", [&](const Value &val) { params.max_creation_time = val.GetValue<timestamp_t>(); }},
        {"stateFilter", [&](const Value &val) { params.state_filter = val.GetValue<string>(); }},
        {"parentJobId", [&](const Value &val) { params.parent_job_id = val.GetValue<string>(); }},
    };
    for (auto &param : input.named_parameters) {
        auto it = param_map.find(param.first);
        if (it != param_map.end()) {
            it->second(param.second);
        }
    }

    if (params.projection.has_value() && params.projection != "full" && params.projection != "minimal") {
        throw BinderException("Invalid value for projection parameter: " + params.projection.value());
    }

    // Initialize the names and return types
    InitializeNamesAndReturnTypes(return_types, names);

    return make_uniq<ListJobsBindData>(bq_catalog, params);
}

static unique_ptr<GlobalTableFunctionState> BigQueryListJobsInitGlobalState(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
    auto &data = input.bind_data->CastNoConst<ListJobsBindData>();
    auto &transaction = BigqueryTransaction::Get(context, data.bq_catalog);
    auto bq_client = transaction.GetBigqueryClient();

    auto state = make_uniq<ListJobsGlobalFunctionState>();
    if (data.params.job_id.has_value()) {
        auto job = bq_client->GetJob(data.params.job_id.value());
        state->single_job = true;
        state->job_by_id = job;
        std::cout << "Job ID: " << job.job_reference().job_id() << std::endl;
    } else {
        auto jobs = bq_client->ListJobs(data.params);
        state->jobs_list = jobs;
    }
    return state;
}

static void BigQueryListJobsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    std::cout << "BigQueryListJobsFunc" << std::endl;
    auto &bind_data = data_p.bind_data->CastNoConst<ListJobsBindData>();
    auto &gstate = data_p.global_state->Cast<ListJobsGlobalFunctionState>();

    lock_guard<mutex> glock(bind_data.lock);
    if (bind_data.finished) {
        return;
    }

    idx_t out_idx = 0;
    idx_t job_idx = gstate.current_idx;

    if (gstate.single_job) {
        MapJobToRow(gstate.job_by_id, output, out_idx);
        out_idx++;
        output.SetCardinality(out_idx);
        bind_data.finished = true;
        std::cout << "Finished" << std::endl;
    } else {
        while (out_idx < STANDARD_VECTOR_SIZE && job_idx < gstate.jobs_list.size()) {
            const auto &job = gstate.jobs_list[job_idx];
            MapJobToRow(job, output, out_idx);
            out_idx++;
            job_idx++;
        }

        gstate.current_idx += out_idx;
        output.SetCardinality(out_idx);

        if (gstate.current_idx >= gstate.jobs_list.size()) {
            bind_data.finished = true;
        }
    }
}

BigQueryListJobsFunction::BigQueryListJobsFunction()
    : TableFunction("bigquery_jobs",
                    {LogicalType::VARCHAR},
                    BigQueryListJobsFunc,
                    BigQueryListJobsBind,
                    BigQueryListJobsInitGlobalState) {
    named_parameters["jobId"] = LogicalType::VARCHAR;
    named_parameters["allUsers"] = LogicalType::BOOLEAN;
    named_parameters["maxResults"] = LogicalType::INTEGER;
    named_parameters["minCreationTime"] = LogicalType::VARCHAR;
    named_parameters["maxCreationTime"] = LogicalType::VARCHAR;
    named_parameters["stateFilter"] = LogicalType::VARCHAR;
    named_parameters["parentJobId"] = LogicalType::VARCHAR;
}


} // namespace bigquery
} // namespace duckdb
