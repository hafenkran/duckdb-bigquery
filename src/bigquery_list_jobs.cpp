#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "bigquery_client.hpp"
#include "bigquery_list_jobs.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {
namespace bigquery {

static constexpr const char *LIST_JOBS_DOC_DESCRIPTION = R"(
	Lists all jobs in the BigQuery project. Job information is available for a six month period after creation.
)";

static constexpr const char *LIST_JOBS_DOC_EXAMPLE = R"(
	-- List all jobs in the BigQuery project
	SELECT * FROM bigquery_list_jobs('my_project');
)";


struct ListJobsBindData : public TableFunctionData {
    explicit ListJobsBindData(BigqueryCatalog &bq_catalog, ListJobsParams &params)
        : bq_catalog(bq_catalog), params(std::move(params)) {
    }

public:
    BigqueryCatalog &bq_catalog;
    ListJobsParams params;
    bool finished = false;
};

struct ListJobsGlobalFunctionState : public GlobalTableFunctionState {
    explicit ListJobsGlobalFunctionState() {
    }

public:
    idx_t current_idx = 0;
    vector<google::cloud::bigquery::v2::ListFormatJob> jobs;
};

void InitializeNamesAndReturnTypes(vector<LogicalType> &return_types, vector<string> &names, string projection) {
    struct ColumnInfo {
        ColumnInfo(const std::string &name, LogicalTypeId type) : name(name), type(type) {
        }

        string name;
        LogicalTypeId type;
    };

    std::vector<ColumnInfo> columns = {
        {"state", LogicalTypeId::VARCHAR},
        {"jobId", LogicalTypeId::VARCHAR},
        {"project", LogicalTypeId::VARCHAR},
        {"location", LogicalTypeId::VARCHAR},
        {"creation time", LogicalTypeId::TIMESTAMP},
        {"start time", LogicalTypeId::TIMESTAMP},
        {"end time", LogicalTypeId::TIMESTAMP},
        {"duration (ms)", LogicalTypeId::TIME},
        {"error reason", LogicalTypeId::VARCHAR},
        {"error location", LogicalTypeId::VARCHAR},
        {"error message", LogicalTypeId::VARCHAR},
        {"bytes processed", LogicalTypeId::BIGINT},
    };

    if (projection == "full") {
		std::vector<ColumnInfo> additional_columns = {
            {"total slot time (ms)", LogicalTypeId::BIGINT},
            {"user email", LogicalTypeId::VARCHAR},
            {"principal subject", LogicalTypeId::VARCHAR},
            {"job type", LogicalTypeId::VARCHAR},
            // {"statement type", LogicalTypeId::VARCHAR},
            // {"query", LogicalTypeId::VARCHAR},
            // {"destination table", LogicalTypeId::VARCHAR},
            // {"labels", LogicalType::MAP(LogicalTypeId::VARCHAR, LogicalTypeId::VARCHAR)},
        };
        for (const auto &column : additional_columns) {
            columns.emplace_back(column.name, column.type);
        }
    }

    for (const auto &column : columns) {
        names.emplace_back(column.name);
        return_types.emplace_back(column.type);
    }
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
    std::map<std::string, std::function<void(const Value &)>> param_map = {
        {"allUsers", [&](const Value &val) { params.all_users = val.GetValue<bool>(); }},
        {"maxResults", [&](const Value &val) { params.max_results = val.GetValue<int>(); }},
        {"minCreationTime", [&](const Value &val) { params.min_creation_time = val.GetValue<string>(); }},
        {"maxCreationTime", [&](const Value &val) { params.max_creation_time = val.GetValue<string>(); }},
        // {"pageToken", [&](const Value &val) { params.page_token = val.GetValue<string>(); }},
        {"projection", [&](const Value &val) { params.projection = val.GetValue<string>(); }},
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
    InitializeNamesAndReturnTypes(return_types, names, params.projection.value_or("minimal"));

    return make_uniq<ListJobsBindData>(bq_catalog, params);
}

static unique_ptr<GlobalTableFunctionState> BigQueryListJobsInitGlobalState(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
    auto &data = input.bind_data->CastNoConst<ListJobsBindData>();
    auto &transaction = BigqueryTransaction::Get(context, data.bq_catalog);
    auto bq_client = transaction.GetBigqueryClient();
    auto jobs = bq_client->ListJobs(data.params);

    auto state = make_uniq<ListJobsGlobalFunctionState>();
    state->jobs = jobs;

    return state;
}

static string GetJobState(const google::cloud::bigquery::v2::ListFormatJob &job) {
    if (job.state() == "DONE") {
        if (job.has_error_result()) {
            return "Error";
        } else {
            return "Completed";
        }
    } else if (job.state() == "PENDING") {
        return "Queued";
    } else if (job.state() == "RUNNING") {
        return "Active";
    }
    return job.state();
}

static void BigQueryListJobsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->CastNoConst<ListJobsBindData>();
    auto &gstate = data_p.global_state->Cast<ListJobsGlobalFunctionState>();
    if (bind_data.finished) {
        return;
    }

    idx_t out_idx = 0;
    idx_t job_idx = gstate.current_idx;
    while (out_idx < STANDARD_VECTOR_SIZE && job_idx < gstate.jobs.size()) {
        const auto &job = gstate.jobs[job_idx];
        const auto &job_ref = job.job_reference();
        const auto &job_stats = job.statistics();

        // Fill the output
        const auto job_state = GetJobState(job);
        int value_idx = 0;
        output.SetValue(value_idx++, out_idx, Value(job_state));
        // jobId, project, location
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

        // duration
        if (job_stats.start_time() > 0 && job_stats.end_time() > 0) {
            auto duration = job_stats.end_time() - job_stats.start_time();
            // Value::TIME()
            output.SetValue(value_idx++, out_idx, Value()); // TODO
        } else {
            output.SetValue(value_idx++, out_idx, Value());
        }

        // error reason, location, message
        if (job.has_error_result()) {
            const auto &error_result = job.error_result();
            output.SetValue(value_idx++, out_idx, Value(error_result.reason()));
            output.SetValue(value_idx++, out_idx, Value(error_result.location()));
            output.SetValue(value_idx++, out_idx, Value(error_result.message()));
        } else {
            output.SetValue(value_idx++, out_idx, Value());
            output.SetValue(value_idx++, out_idx, Value());
            output.SetValue(value_idx++, out_idx, Value());
        }

        // bytes processed
        if (job_stats.query().has_total_bytes_processed()) {
            const auto total_bytes_processed = job_stats.query().total_bytes_processed().value();
            output.SetValue(value_idx++, out_idx, Value::BIGINT(total_bytes_processed));
        } else {
            output.SetValue(value_idx++, out_idx, Value());
        }

        if (bind_data.params.projection.has_value() && bind_data.params.projection.value() == "full") {
            // total slot time
            if (job_stats.query().has_total_slot_ms()) {
                const auto total_slot_ms = job_stats.query().total_slot_ms().value();
                output.SetValue(value_idx++, out_idx, Value::BIGINT(total_slot_ms));
            } else {
                output.SetValue(value_idx++, out_idx, Value());
            }

            // user email
            auto user_email = job.user_email();
            output.SetValue(value_idx++, out_idx, Value(user_email));

            // principal subject
            auto principal_subject = job.principal_subject();
            output.SetValue(value_idx++, out_idx, Value(principal_subject));

            // job type
            auto job_type = job.configuration().job_type();
            output.SetValue(value_idx++, out_idx, Value(job_type));

            // // labels
            // if (job.configuration().labels_size() > 0) {
            // 	std::unordered_map<std::string, std::string> labels;
            // 	for (const auto &label : job.configuration().labels()) {
            // 		labels[label.first] = label.second;
            // 	}
            // 	output.SetValue(value_idx++, out_idx, Value::MAP(labels));
            // } else {
            // 	output.SetValue(value_idx++, out_idx, Value());
            // }
        }

        out_idx++;
        job_idx++;
    }

    gstate.current_idx += out_idx;
    output.SetCardinality(out_idx);

    if (gstate.current_idx >= gstate.jobs.size()) {
        bind_data.finished = true;
    }
}

BigQueryListJobsFunction::BigQueryListJobsFunction()
    : TableFunction("bigquery_list_jobs",
                    {LogicalType::VARCHAR},
                    BigQueryListJobsFunc,
                    BigQueryListJobsBind,
                    BigQueryListJobsInitGlobalState) {
    named_parameters["allUsers"] = LogicalType::BOOLEAN;
    named_parameters["maxResults"] = LogicalType::INTEGER;
    named_parameters["minCreationTime"] = LogicalType::VARCHAR;
    named_parameters["maxCreationTime"] = LogicalType::VARCHAR;
    named_parameters["pageToken"] = LogicalType::VARCHAR;
    named_parameters["projection"] = LogicalType::VARCHAR;
    named_parameters["stateFilter"] = LogicalType::VARCHAR;
    named_parameters["parentJobId"] = LogicalType::VARCHAR;
}

} // namespace bigquery
} // namespace duckdb
