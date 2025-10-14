#include "bigquery_client.hpp"
#include "bigquery_arrow_reader.hpp"
#include "bigquery_proto_writer.hpp"
#include "bigquery_secrets.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/parser.hpp"

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include "google/cloud/bigquery/storage/v1/bigquery_read_options.h"
#include "google/cloud/bigquery/storage/v1/bigquery_write_options.h"
#include "google/cloud/bigquery/storage/v1/storage.pb.h"
#include "google/cloud/bigquery/storage/v1/stream.pb.h"

#include "google/cloud/bigquerycontrol/v2/dataset_client.h"
#include "google/cloud/bigquerycontrol/v2/job_client.h"
#include "google/cloud/bigquerycontrol/v2/table_client.h"

#include "google/cloud/common_options.h"
#include "google/cloud/credentials.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/idempotency.h"
#include "google/cloud/internal/curl_options.h"
#include "google/cloud/internal/oauth2_google_application_default_credentials_file.h"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/ipc/writer.h"
#include "grpcpp/grpcpp.h"

#include <cstring>
#include <fstream>
#include <iostream>


namespace duckdb {
namespace bigquery {

class CustomReadIdempotencyPolicy : public google::cloud::bigquery_storage_v1::BigQueryReadConnectionIdempotencyPolicy {
public:
    ~CustomReadIdempotencyPolicy() override = default;
    std::unique_ptr<google::cloud::bigquery_storage_v1::BigQueryReadConnectionIdempotencyPolicy> clone()
        const override {
        return std::make_unique<CustomReadIdempotencyPolicy>(*this);
    }

    google::cloud::Idempotency CreateReadSession(
        google::cloud::bigquery::storage::v1::CreateReadSessionRequest const &request) override {
        return google::cloud::Idempotency::kIdempotent;
    }

    google::cloud::Idempotency SplitReadStream(
        google::cloud::bigquery::storage::v1::SplitReadStreamRequest const &request) override {
        return google::cloud::Idempotency::kIdempotent;
    }
};

class CustomWriteIdempotencyPolicy
    : public google::cloud::bigquery_storage_v1::BigQueryWriteConnectionIdempotencyPolicy {
public:
    ~CustomWriteIdempotencyPolicy() override = default;
    std::unique_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteConnectionIdempotencyPolicy> clone()
        const override {
        return std::make_unique<CustomWriteIdempotencyPolicy>(*this);
    }

    google::cloud::Idempotency CreateWriteStream(
        google::cloud::bigquery::storage::v1::CreateWriteStreamRequest const &request) override {
        return google::cloud::Idempotency::kIdempotent;
    }

    google::cloud::Idempotency AppendRows(google::cloud::bigquery::storage::v1::AppendRowsRequest const &request) {
        return google::cloud::Idempotency::kIdempotent;
    }

    google::cloud::Idempotency GetWriteStream(
        google::cloud::bigquery::storage::v1::GetWriteStreamRequest const &request) override {
        return google::cloud::Idempotency::kIdempotent;
    }

    google::cloud::Idempotency BatchCommitWriteStreams(
        google::cloud::bigquery::storage::v1::BatchCommitWriteStreamsRequest const &request) override {
        return google::cloud::Idempotency::kIdempotent;
    }
};

BigqueryClient::BigqueryClient(ClientContext &context, const BigqueryConfig &config)
    : config(config), context(&context) {
    if (config.project_id.empty()) {
        throw std::runtime_error("BigqueryClient::BigqueryClient: project_id is empty");
    }
}

google::cloud::Options BigqueryClient::OptionsAPI() {
    auto options = google::cloud::Options{};
    if (!config.api_endpoint.empty()) {
        options.set<google::cloud::EndpointOption>(config.api_endpoint);
    }

    bool credentials_set = false;
	auto secret_match = LookupBigQuerySecret(*context, config.project_id);
	if (secret_match.HasMatch()) {
		auto &bq_secret = dynamic_cast<const BigquerySecret &>(secret_match.GetSecret());
		auto credentials = CreateGCPCredentialsFromSecret(bq_secret);
		if (credentials) {
			options.set<google::cloud::v2_38::UnifiedCredentialsOption>(credentials);
			credentials_set = true;
		}
	}

    // Set CA bundle path if provided
    auto ca_path = BigquerySettings::CurlCaBundlePath();
    if (!ca_path.empty()) {
        options.set<google::cloud::v2_38::CARootsFilePathOption>(ca_path);
    }

    // Set default credentials if no secret credentials were set
    if (!credentials_set) {
        options.set<google::cloud::v2_38::UnifiedCredentialsOption>(
            google::cloud::MakeGoogleDefaultCredentials(options));
    }

    return options;
}

google::cloud::Options BigqueryClient::OptionsGRPC() {
    auto options = google::cloud::Options{};
    if (!config.grpc_endpoint.empty()) {
        options.set<google::cloud::EndpointOption>(config.grpc_endpoint);
        if (config.IsDevEnv()) {
            options.set<google::cloud::GrpcCredentialOption>(grpc::InsecureChannelCredentials());
        }
    }

    bool credentials_set = false;
	auto secret_match = LookupBigQuerySecret(*context, config.project_id);
	if (secret_match.HasMatch()) {
		auto &bq_secret = dynamic_cast<const BigquerySecret &>(secret_match.GetSecret());
		auto credentials = CreateGCPCredentialsFromSecret(bq_secret);
		if (credentials) {
			options.set<google::cloud::v2_38::UnifiedCredentialsOption>(credentials);
			credentials_set = true;
		}
	}

    // Set default credentials if no secret credentials were set
    if (!credentials_set) {
        options.set<google::cloud::v2_38::UnifiedCredentialsOption>(
            google::cloud::MakeGoogleDefaultCredentials(options));
    }

    return options;
}

vector<BigqueryDatasetRef> BigqueryClient::GetDatasets() {
    // Check authentication before making any API calls to avoid blocking
    CheckAuthentication();

    auto request = google::cloud::bigquery::v2::ListDatasetsRequest();
    request.set_project_id(config.project_id);

    auto dataset_client = make_shared_ptr<google::cloud::bigquerycontrol_v2::DatasetServiceClient>(
        google::cloud::bigquerycontrol_v2::MakeDatasetServiceConnectionRest(OptionsAPI()));
    auto datasets = dataset_client->ListDatasets(request);

    vector<BigqueryDatasetRef> result;
    for (google::cloud::StatusOr<google::cloud::bigquery::v2::ListFormatDataset> const &dataset : datasets) {
        if (!dataset.ok()) {
            ThrowOnErrorStatus(dataset.status());
            if (CheckInvalidJsonError(dataset.status())) {
                return result;
            }
            if (CheckSSLError(dataset.status())) {
                return GetDatasets();
            }
            throw BinderException(dataset.status().message());
        }

        google::cloud::bigquery::v2::ListFormatDataset dataset_val = dataset.value();
        const auto &dataset_ref = dataset_val.dataset_reference();

        BigqueryDatasetRef info;
        info.project_id = dataset_ref.project_id();
        info.dataset_id = dataset_ref.dataset_id();
        info.location = dataset_val.location();
        result.push_back(info);
    }
    return result;
}

vector<BigqueryTableRef> BigqueryClient::GetTables(const string &dataset_id) {
    CheckAuthentication();

    auto request = google::cloud::bigquery::v2::ListTablesRequest();
    request.set_project_id(config.project_id);
    request.set_dataset_id(dataset_id);

    auto table_client = make_shared_ptr<google::cloud::bigquerycontrol_v2::TableServiceClient>(
        google::cloud::bigquerycontrol_v2::MakeTableServiceConnectionRest(OptionsAPI()));
    auto tables = table_client->ListTables(request);

    vector<BigqueryTableRef> table_names;
    for (google::cloud::StatusOr<google::cloud::bigquery::v2::ListFormatTable> const &table : tables) {
        if (!table.ok()) {
            ThrowOnErrorStatus(table.status());
            if (CheckInvalidJsonError(table.status())) {
                return table_names;
            }
            if (CheckSSLError(table.status())) {
                return GetTables(dataset_id);
            }
            throw InternalException(table.status().message());
        }

        google::cloud::bigquery::v2::ListFormatTable table_val = table.value();
        const auto &table_ref = table_val.table_reference();

        BigqueryTableRef info;
        info.project_id = table_ref.project_id();
        info.dataset_id = table_ref.dataset_id();
        info.table_id = table_ref.table_id();
        table_names.push_back(info);
    }
    return table_names;
}

BigqueryDatasetRef BigqueryClient::GetDataset(const string &dataset_id) {
    auto client = make_shared_ptr<google::cloud::bigquerycontrol_v2::DatasetServiceClient>(
        google::cloud::bigquerycontrol_v2::MakeDatasetServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::GetDatasetRequest();
    request.set_project_id(config.project_id);
    request.set_dataset_id(dataset_id);

    auto response = client->GetDataset(request);
    if (!response.ok()) {
        if (CheckSSLError(response.status())) {
            return GetDataset(dataset_id);
        }
        throw InternalException(response.status().message());
    }

    auto dataset = response.value();
    const auto &dataset_ref = dataset.dataset_reference();

    BigqueryDatasetRef info;
    info.project_id = dataset_ref.project_id();
    info.dataset_id = dataset_ref.dataset_id();
    info.location = dataset.location();
    return info;
}

vector<google::cloud::bigquery::v2::ListFormatJob> BigqueryClient::ListJobs(const ListJobsParams &params) {
    CheckAuthentication();

    auto client = google::cloud::bigquerycontrol_v2::JobServiceClient(
        google::cloud::bigquerycontrol_v2::MakeJobServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::ListJobsRequest();
    request.set_project_id(config.project_id);

    // Default is 1000
    std::int32_t max_results = 1000;
    if (params.max_results.has_value()) {
        max_results = params.max_results.value();
    }
    request.mutable_max_results()->set_value(max_results);

    if (params.all_users.has_value()) {
        auto all_users = params.all_users.value();
        request.set_all_users(all_users);
    }
    if (params.min_creation_time.has_value()) {
        auto min_creation_time = params.min_creation_time.value();
        auto timestamp_ms = Timestamp::GetEpochMs(min_creation_time);
        request.set_min_creation_time(timestamp_ms);
    }
    if (params.max_creation_time.has_value()) {
        auto max_creation_time = params.max_creation_time.value();
        auto timestamp_ms = Timestamp::GetEpochMs(max_creation_time);
        request.mutable_max_creation_time()->set_value(timestamp_ms);
    }
    if (params.projection.has_value()) {
        auto projection = params.projection.value();
        if (projection == "full") {
            auto mapped = google::cloud::bigquery::v2::ListJobsRequest_Projection::ListJobsRequest_Projection_FULL;
            request.set_projection(mapped);
        } else if (projection == "minimal") {
            auto mapped = google::cloud::bigquery::v2::ListJobsRequest_Projection::ListJobsRequest_Projection_MINIMAL;
            request.set_projection(mapped);
        } else {
            throw BinderException("Invalid projection value: %s", projection);
        }
    }
    if (params.state_filter.has_value()) {
        auto state_filter = params.state_filter.value();
        std::transform(state_filter.begin(), state_filter.end(), state_filter.begin(), ::tolower);

        if (state_filter == "done") {
            request.add_state_filter(
                google::cloud::bigquery::v2::ListJobsRequest_StateFilter::ListJobsRequest_StateFilter_DONE);
        } else if (state_filter == "pending") {
            request.add_state_filter(
                google::cloud::bigquery::v2::ListJobsRequest_StateFilter::ListJobsRequest_StateFilter_PENDING);
        } else if (state_filter == "running") {
            request.add_state_filter(
                google::cloud::bigquery::v2::ListJobsRequest_StateFilter::ListJobsRequest_StateFilter_RUNNING);
        } else {
            throw BinderException("Invalid state filter value: %s", state_filter);
        }
    }
    if (params.parent_job_id.has_value()) {
        auto parent_job_id = params.parent_job_id.value();
        request.set_parent_job_id(parent_job_id);
    }

    vector<google::cloud::bigquery::v2::ListFormatJob> result;
    google::cloud::v2_38::StreamRange<google::cloud::bigquery::v2::ListFormatJob> response = client.ListJobs(request);

    int num_results = 0;
    for (const auto &job : response) {
        if (!job.ok()) {
            if (CheckSSLError(job.status())) {
                return ListJobs(params);
            }
            throw BinderException(job.status().message());
        }
        auto job_val = job.value();
        result.push_back(job_val);

        num_results++;
        if (num_results >= max_results) {
            break;
        }
    }
    return result;
}

google::cloud::bigquery::v2::Job BigqueryClient::GetJobByReference(
    const google::cloud::bigquery::v2::JobReference &job_ref) {
    if (job_ref.job_id().empty()) {
        throw BinderException("Job ID cannot be empty");
    }

    auto client = google::cloud::bigquerycontrol_v2::JobServiceClient(
        google::cloud::bigquerycontrol_v2::MakeJobServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::GetJobRequest();
    request.set_project_id(job_ref.project_id());
    request.set_job_id(job_ref.job_id());
    if (job_ref.has_location()) {
        request.set_location(job_ref.location().value());
    }

    auto response = client.GetJob(request);
    if (!response.ok()) {
        if (CheckSSLError(response.status())) {
            return GetJobByReference(job_ref);
        }
        throw BinderException(response.status().message());
    }

    auto job = response.value();
    return job;
}

google::cloud::bigquery::v2::Job BigqueryClient::GetJob(const string &job_id, const string &location) {
    if (job_id.empty()) {
        throw BinderException("Job ID cannot be empty");
    }

    auto client = google::cloud::bigquerycontrol_v2::JobServiceClient(
        google::cloud::bigquerycontrol_v2::MakeJobServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::GetJobRequest();
    request.set_project_id(config.BillingProject());
    request.set_job_id(job_id);
    if (!location.empty()) {
        request.set_location(location);
    }

    auto response = client.GetJob(request);
    if (!response.ok()) {
        if (CheckSSLError(response.status())) {
            return GetJob(job_id, location);
        }
        throw BinderException(response.status().message());
    }

    auto job = response.value();
    return job;
}

BigqueryTableRef BigqueryClient::GetTable(const string &dataset_id, const string &table_id) {
    auto client = make_shared_ptr<google::cloud::bigquerycontrol_v2::TableServiceClient>(
        google::cloud::bigquerycontrol_v2::MakeTableServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::GetTableRequest();
    request.set_project_id(config.project_id);
    request.set_dataset_id(dataset_id);
    request.set_table_id(table_id);

    auto response = client->GetTable(request);
    if (!response.ok()) {
        if (CheckSSLError(response.status())) {
            return GetTable(dataset_id, table_id);
        }
        throw InternalException(response.status().message());
    }

    auto table = response.value();
    const auto &table_ref = table.table_reference();

    BigqueryTableRef info;
    info.project_id = table_ref.project_id();
    info.dataset_id = table_ref.dataset_id();
    info.table_id = table_ref.table_id();
    return info;
}

bool BigqueryClient::DatasetExists(const string &dataset_id) {
    auto client = google::cloud::bigquerycontrol_v2::DatasetServiceClient(
        google::cloud::bigquerycontrol_v2::MakeDatasetServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::GetDatasetRequest();
    request.set_project_id(config.project_id);
    request.set_dataset_id(dataset_id);

    auto response = client.GetDataset(request);
    if (!response.ok()) {
        std::cerr << "Error: " << response.status().message() << "\n";
        return false;
    }
    return true;
}

bool BigqueryClient::TableExists(const string &dataset_id, const string &table_id) {
    auto client = google::cloud::bigquerycontrol_v2::TableServiceClient(
        google::cloud::bigquerycontrol_v2::MakeTableServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::GetTableRequest();
    request.set_project_id(config.project_id);
    request.set_dataset_id(dataset_id);
    request.set_table_id(table_id);

    auto response = client.GetTable(request);
    if (!response.ok()) {
        std::cerr << "Error: " << response.status().message() << "\n";
        return false;
    }
    return true;
}

void BigqueryClient::CreateDataset(const CreateSchemaInfo &info, const BigqueryDatasetRef &dataset_ref) {
    auto query = BigquerySQL::CreateSchemaInfoToSQL(GetProjectID(), info);
    ExecuteQuery(query, dataset_ref.location);

    // Check if dataset exists with an exponential backoff retry
    auto op = [this, &info]() -> bool { return DatasetExists(info.schema); };
    auto success = RetryOperation(op, 10, 1000);
    if (!success) {
        throw InternalException("Failed to verify that \"%s\" has been successfully created", info.schema);
    }
}

void BigqueryClient::CreateTable(const CreateTableInfo &info, const BigqueryTableRef &table_ref) {
    auto query = BigquerySQL::CreateTableInfoToSQL(GetProjectID(), info);
    ExecuteQuery(query);

    // Check if the table exists with an exponential backoff retry
    auto op = [this, &info]() -> bool { return TableExists(info.schema, info.table); };
    auto success = RetryOperation(op, 10, 1000);
    if (!success) {
        throw InternalException("Failed to verify that \"%s\" has been successfully created", info.table);
    }
}

void BigqueryClient::CreateView(const CreateViewInfo &info) {
    auto query = BigquerySQL::CreateViewInfoToSQL(GetProjectID(), info);
    ExecuteQuery(query);
}

void BigqueryClient::DropTable(const DropInfo &info) {
    auto drop_query = BigquerySQL::DropInfoToSQL(GetProjectID(), info);
    ExecuteQuery(drop_query);
}

void BigqueryClient::DropView(const DropInfo &info) {
    auto drop_query = BigquerySQL::DropInfoToSQL(GetProjectID(), info);
    ExecuteQuery(drop_query);
}

void BigqueryClient::DropDataset(const DropInfo &info) {
    auto drop_query = BigquerySQL::DropInfoToSQL(GetProjectID(), info);
    ExecuteQuery(drop_query);
}


void BigqueryClient::GetTableInfosFromDataset(const BigqueryDatasetRef &dataset_ref,
                                              std::map<string, CreateTableInfo> &table_infos) {
    auto dataset_refs = vector<BigqueryDatasetRef>{dataset_ref};
    GetTableInfosFromDatasets(dataset_refs, table_infos);
}

void BigqueryClient::GetTableInfosFromDatasets(const vector<BigqueryDatasetRef> &dataset_refs,
                                               std::map<string, CreateTableInfo> &table_infos) {
    if (dataset_refs.empty()) {
        throw BinderException("No datasets provided.");
    }

    auto project_id = dataset_refs[0].project_id;
    std::map<string, vector<string>> datasets_by_location;
    for (const auto &dataset_ref : dataset_refs) {
        if (dataset_ref.project_id != project_id) {
            throw BinderException("Project ID mismatch: %s != %s", dataset_ref.project_id, project_id);
        }
        datasets_by_location[dataset_ref.location].push_back(dataset_ref.dataset_id);
    }

    auto job_client = google::cloud::bigquerycontrol_v2::JobServiceClient(
        google::cloud::bigquerycontrol_v2::MakeJobServiceConnectionRest(OptionsAPI()));

    for (const auto &datasets_in_loc : datasets_by_location) {
        const auto &location = datasets_in_loc.first;
        const auto &datasets = datasets_in_loc.second;

        const auto info_schema_query = BigquerySQL::ColumnsFromInformationSchemaQuery(project_id, datasets);

        auto query_response = ExecuteQuery(info_schema_query, location);
        google::protobuf::RepeatedPtrField<::google::protobuf::Struct> all_rows = query_response.rows();

        string page_token = query_response.page_token();
        auto job_ref = query_response.job_reference();
        while (!page_token.empty()) {
            auto next_page_response = GetQueryResults(job_ref, page_token);
            const auto &next_rows = next_page_response.rows();
            all_rows.MergeFrom(next_rows);
            page_token = next_page_response.page_token();
        }

        MapInformationSchemaRows(project_id, all_rows, table_infos);
    }
}

void BigqueryClient::MapTableSchema(const google::cloud::bigquery::v2::TableSchema &schema,
                                    ColumnList &res_columns,
                                    vector<unique_ptr<Constraint>> &res_constraints) {
    for (const google::cloud::bigquery::v2::TableFieldSchema &field : schema.fields()) {
        // Create the ColumnDefinition
        auto column_type = BigqueryUtils::FieldSchemaToLogicalType(field);

        ColumnDefinition column(field.name(), std::move(column_type));
        // column.SetComment(std::move(field.description));

        const auto &default_value_expr = field.default_value_expression();
        const auto &default_value = default_value_expr.value();
        if (!default_value.empty() && default_value != "\"\"") {
            auto expressions = Parser::ParseExpressionList(default_value);
            if (expressions.empty()) {
                throw InternalException("Expression list is empty");
            }
            column.SetDefaultValue(std::move(expressions[0]));
        }
        res_columns.AddColumn(std::move(column));

        // The field mode. Possible values include NULLABLE, REQUIRED and REPEATED.
        // The default value is NULLABLE.
        const auto &mode = field.mode();
        if (mode == "REQUIRED") {
            auto field_name = field.name();
            auto field_index = res_columns.GetColumnIndex(field_name);
            res_constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(field_index)));
        }

        std::regex string_length_regex(R"(STRING\((\d+)\))");
        std::smatch match;

        if (std::regex_match(field.type(), match, string_length_regex)) {
            int max_length = std::stoi(match[1]);

            auto constraint_str = "length(" + field.name() + ") <= " + std::to_string(max_length);
            auto parsed_expressions = Parser::ParseExpressionList(constraint_str);

            auto check_constraint = make_uniq<CheckConstraint>(std::move(parsed_expressions[0]));
            res_constraints.push_back(std::move(check_constraint));
        }
    }
}

void BigqueryClient::GetTableInfo(const string &dataset_id,
                                  const string &table_id,
                                  ColumnList &res_columns,
                                  vector<unique_ptr<Constraint>> &res_constraints) {
    CheckAuthentication();

    auto client = make_shared_ptr<google::cloud::bigquerycontrol_v2::TableServiceClient>(
        google::cloud::bigquerycontrol_v2::MakeTableServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::GetTableRequest();
    request.set_project_id(config.project_id);
    request.set_dataset_id(dataset_id);
    request.set_table_id(table_id);

    auto response = client->GetTable(request);
    if (!response.ok()) {
        if (CheckSSLError(response.status())) {
            return GetTableInfo(dataset_id, table_id, res_columns, res_constraints);
        }
        if (response.status().code() == google::cloud::StatusCode::kNotFound) {
            auto table_ref = BigqueryUtils::FormatTableString(config.project_id, dataset_id, table_id);
            throw BinderException("GetTableInfo - table \"%s\" not found", table_ref);
        }
        throw InternalException(response.status().message());
    }

    auto table = response.value();
    MapTableSchema(table.schema(), res_columns, res_constraints);
}

void BigqueryClient::GetTableInfoForQuery(const string &query,
                                          ColumnList &res_columns,
                                          vector<unique_ptr<Constraint>> &res_constraints) {
    auto query_response = ExecuteQuery(query, "", true);
    if (!query_response.has_schema()) {
        throw BinderException("Query response does not contain a result schema.");
    }
    auto schema = query_response.schema();
    MapTableSchema(schema, res_columns, res_constraints);
}

shared_ptr<BigqueryArrowReader> BigqueryClient::CreateArrowReader(const BigqueryTableRef &table_ref,
                                                                  const idx_t num_streams,
                                                                  const vector<string> &column_ids,
                                                                  const string &filter_cond) {
    CheckAuthentication();

    auto options =
        OptionsGRPC()
            .set<google::cloud::bigquery_storage_v1::BigQueryReadConnectionIdempotencyPolicyOption>(
                CustomReadIdempotencyPolicy().clone())
            .set<google::cloud::bigquery_storage_v1::BigQueryReadRetryPolicyOption>(
                google::cloud::bigquery_storage_v1::BigQueryReadLimitedTimeRetryPolicy(std::chrono::minutes(10))
                    .clone())
            .set<google::cloud::bigquery_storage_v1::BigQueryReadBackoffPolicyOption>(
                google::cloud::ExponentialBackoffPolicy(
                    /*initial_delay=*/std::chrono::milliseconds(200),
                    /*maximum_delay=*/std::chrono::seconds(60),
                    /*scaling=*/2.0)
                    .clone());

    return make_shared_ptr<BigqueryArrowReader>(table_ref,
                                                config.BillingProject(),
                                                num_streams,
                                                options,
                                                column_ids,
                                                filter_cond);
}

shared_ptr<BigqueryProtoWriter> BigqueryClient::CreateProtoWriter(BigqueryTableEntry *entry) {
    CheckAuthentication();

    if (entry == nullptr) {
        throw InternalException("Error while initializing proto writer: entry is null");
    }
    auto &bq_catalog = dynamic_cast<BigqueryCatalog &>(entry->catalog);
    if (bq_catalog.GetProjectID() != config.project_id) {
        throw InternalException("Error while initializing proto writer: project_id mismatch");
    }

    // Check if dataset exists with an exponential backoff retry
    auto op = [this, &entry]() -> bool { return TableExists(entry->schema.name, entry->name); };
    auto success = RetryOperation(op, 10, 1000);
    if (!success) {
        throw InternalException("Failed to verify that \"%s.%s\" exists.", entry->schema.name, entry->name);
    }

    auto options = OptionsGRPC()
                       .set<google::cloud::bigquery_storage_v1::BigQueryWriteConnectionIdempotencyPolicyOption>(
                           CustomWriteIdempotencyPolicy().clone())
                       .set<google::cloud::bigquery_storage_v1::BigQueryWriteRetryPolicyOption>(
                           google::cloud::bigquery_storage_v1::BigQueryWriteLimitedErrorCountRetryPolicy(5).clone())
                       .set<google::cloud::bigquery_storage_v1::BigQueryWriteBackoffPolicyOption>(
                           google::cloud::ExponentialBackoffPolicy(
                               /*initial_delay=*/std::chrono::milliseconds(200),
                               /*maximum_delay=*/std::chrono::seconds(45),
                               /*scaling=*/2.0)
                               .clone());

    return make_shared_ptr<BigqueryProtoWriter>(entry, options);
}

google::cloud::bigquery::v2::QueryResponse BigqueryClient::ExecuteQuery(const string &query,
                                                                        const string &location,
                                                                        const bool &dry_run) {
    CheckAuthentication();

    auto client = google::cloud::bigquerycontrol_v2::JobServiceClient(
        google::cloud::bigquerycontrol_v2::MakeJobServiceConnectionRest(OptionsAPI()));

    auto response = PostQueryJobInternal(client, query, location, dry_run);
    if (!response.ok()) {
        ThrowOnErrorStatus(response.status());

        if (CheckSSLError(response.status())) {
            return ExecuteQuery(query, location, dry_run);
        }

        throw BinderException("Query execution failed: " + response.status().message());
    }

    auto complete = response->job_complete().value();
    if (!complete) {
        auto job_id = response->job_reference().job_id();
        throw BinderException("Query execution exceeded the timeout. Job ID: " + job_id);
    }

    return *response;
}

google::cloud::bigquery::v2::GetQueryResultsResponse BigqueryClient::GetQueryResults(
    const google::cloud::bigquery::v2::JobReference &job_ref,
    const string &page_token) {
    auto client = google::cloud::bigquerycontrol_v2::JobServiceClient(
        google::cloud::bigquerycontrol_v2::MakeJobServiceConnectionRest(OptionsAPI()));

    auto response = GetQueryResultsInternal(client, job_ref, page_token);
    if (!response) {
        if (CheckSSLError(response.status())) {
            return GetQueryResults(job_ref, page_token);
        }
        throw BinderException("GetQueryResults failed: " + response.status().message());
    }
    return *response;
}

google::cloud::StatusOr<google::cloud::bigquery::v2::Job> BigqueryClient::GetJobInternal(
    google::cloud::bigquerycontrol_v2::JobServiceClient &job_client,
    const string &job_id,
    const string &location) {
    if (config.project_id.empty()) {
        throw BinderException("project_id config parameter is empty.");
    } else if (job_id.empty()) {
        throw BinderException("job_id config parameter is empty.");
    }

    auto client = google::cloud::bigquerycontrol_v2::JobServiceClient(
        google::cloud::bigquerycontrol_v2::MakeJobServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::GetJobRequest();
    request.set_project_id(config.BillingProject());
    request.set_job_id(job_id);
    if (!location.empty()) {
        request.set_location(location);
    }

    auto response = client.GetJob(request);
    if (!response.ok()) {
        throw BinderException(response.status().message());
    }

    auto job = response.value();
    return job;
}

google::cloud::StatusOr<google::cloud::bigquery::v2::QueryResponse> BigqueryClient::PostQueryJobInternal(
    google::cloud::bigquerycontrol_v2::JobServiceClient &job_client,
    const string &query,
    const string &location,
    const bool &dry_run) {

    if (!dry_run && BigquerySettings::DebugQueryPrint()) {
        std::cout << "query: " << query << std::endl;
    }

    google::cloud::bigquery::v2::DatasetReference dataset_ref;
    dataset_ref.set_project_id(config.project_id);
    dataset_ref.set_dataset_id("UNKNOWN");

    auto query_request = google::cloud::bigquery::v2::QueryRequest();
    *query_request.mutable_query() = query;
    *query_request.mutable_request_id() = GenerateJobId();
    *query_request.mutable_default_dataset() = dataset_ref;
    query_request.mutable_use_legacy_sql()->set_value(false);
    // query_request.mutable_max_results()->set_value(3);
    query_request.set_dry_run(dry_run);

    int timeout_ms = BigquerySettings::QueryTimeoutMs();
    query_request.mutable_timeout_ms()->set_value(timeout_ms);

    if (!location.empty()) {
        *query_request.mutable_location() = location;
    }

    auto request = google::cloud::bigquery::v2::PostQueryRequest();
    request.set_project_id(config.BillingProject());
    *request.mutable_query_request() = query_request;

    return job_client.Query(request);
}

google::cloud::StatusOr<google::cloud::bigquery::v2::GetQueryResultsResponse> BigqueryClient::GetQueryResultsInternal(
    google::cloud::bigquerycontrol_v2::JobServiceClient &job_client,
    const google::cloud::bigquery::v2::JobReference &job_ref,
    const string &page_token) {

    auto get_results_request = google::cloud::bigquery::v2::GetQueryResultsRequest();
    get_results_request.set_project_id(job_ref.project_id());
    get_results_request.set_job_id(job_ref.job_id());
    // get_results_request.mutable_max_results()->set_value(3);

    if (job_ref.has_location()) {
        const string &location_value = job_ref.location().value();
        get_results_request.set_location(location_value);
    }
    if (!page_token.empty()) {
        get_results_request.set_page_token(page_token);
    }

    auto response = job_client.GetQueryResults(get_results_request);
    if (!response.ok()) {
        throw BinderException(response.status().message());
    }
    return response;
}

string BigqueryClient::GenerateJobId(const string &prefix) {
    constexpr char kDefaultJobPrefix[] = "job_duckdb";
    auto rng = google::cloud::internal::MakeDefaultPRNG();
    string id = google::cloud::internal::Sample(rng, 12, "abcdefghijklmnopqrstuvwxyz");
    return kDefaultJobPrefix + (prefix.empty() ? "" : "_" + prefix) + "_" + id;
}

void BigqueryClient::MapInformationSchemaRows(
    const std::string &project_id,
    const ::google::protobuf::RepeatedPtrField<::google::protobuf::Struct> &rows,
    std::map<std::string, CreateTableInfo> &table_infos) {
    vector<string> tables_with_errornous_columns;

    for (auto &row : rows) {
        const auto &fields = row.fields();
        const auto &field_list = fields.at("f").list_value().values();

        if (field_list.size() < 6) {
            throw BinderException("Unexpected number of fields in the row.");
        }

        string dataset_name = field_list[0].struct_value().fields().at("v").string_value();
        string table_name = field_list[1].struct_value().fields().at("v").string_value();
        string column_name = field_list[2].struct_value().fields().at("v").string_value();
        string data_type = field_list[3].struct_value().fields().at("v").string_value();
        string is_nullable = field_list[4].struct_value().fields().at("v").string_value();
        string column_default = field_list[5].struct_value().fields().at("v").string_value();

        auto table_string = BigqueryUtils::FormatTableStringSimple(project_id, dataset_name, table_name);

        LogicalType column_type;
        try {
            column_type = BigqueryUtils::BigquerySQLToLogicalType(data_type);
        } catch (BinderException &ex) {
            ErrorData error(ex);
            std::ostringstream oss;
            oss << "Failed to map column type for table " << table_string << ". Error: " << error.RawMessage()
                << " - skipping table.";
            std::cout << oss.str() << std::endl;
            continue;
        }

        ColumnDefinition column(column_name, std::move(column_type));

        if (!column_default.empty() && column_default != "\"\"" && column_default != "NULL") {
            auto expressions = Parser::ParseExpressionList(column_default);
            if (expressions.empty()) {
                throw InternalException("Expression list is empty");
            }
            column.SetDefaultValue(std::move(expressions[0]));
        }

        if (table_infos.find(table_string) == table_infos.end()) {
            table_infos[table_string] = CreateTableInfo(project_id, dataset_name, table_name);
        }

        table_infos[table_string].columns.AddColumn(std::move(column));

        if (is_nullable == "NO") {
            auto field_index = table_infos[table_string].columns.GetColumnIndex(column_name);
            table_infos[table_string].constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(field_index)));
        }

        if (data_type.find("STRING(") != std::string::npos) {
            std::regex string_length_regex(R"(STRING\((\d+)\))");
            std::smatch match;

            if (std::regex_match(data_type, match, string_length_regex)) {
                int max_length = std::stoi(match[1]);

                auto constraint_str = "length(" + column_name + ") <= " + std::to_string(max_length);
                auto parsed_expressions = Parser::ParseExpressionList(constraint_str);

                auto check_constraint = make_uniq<CheckConstraint>(std::move(parsed_expressions[0]));
                table_infos[table_name].constraints.push_back(std::move(check_constraint));
            }
        }
    }

    // remove tables with errornous columns
    for (const auto &table_name : tables_with_errornous_columns) {
        table_infos.erase(table_name);
    }
}

void BigqueryClient::CheckAuthentication() {
	if (authentication_checked) {
		return;
	}

	// Check if we have a DuckDB secret for this project
	auto secret_match = LookupBigQuerySecret(*context, config.project_id);
	if (secret_match.HasMatch()) {
		authentication_checked = true;
		return; // If we have a secret, we assume being able to authenticate
	}

    // Check if ADC environment variables are set
	auto adc_credential = google::cloud::oauth2_internal::GoogleAdcFilePathFromEnvVarOrEmpty();
	if (!adc_credential.empty()) {
		std::ifstream creds_file(adc_credential);
		if (creds_file.good()) {
			authentication_checked = true;
			return; // ADC file exists
		}
	}

	// Check if gcloud CLI credentials exist
	auto gcloud_creds_path = google::cloud::oauth2_internal::GoogleAdcFilePathFromWellKnownPathOrEmpty();
	if (!gcloud_creds_path.empty()) {
		std::ifstream creds_file(gcloud_creds_path);
		if (creds_file.good()) {
			authentication_checked = true;
			return; // gcloud ADC file exists
		}
	}

	// Check if we're in a GCP environment (i.e., metadata server set)
    const char *gcp_project = std::getenv("GOOGLE_CLOUD_PROJECT");
    const char *gce_metadata = std::getenv("GCE_METADATA_ROOT");
    if (gcp_project || gce_metadata) {
		authentication_checked = true;
        return; // Likely running on GCP, should have automatic credentials
    }

	// No authentication method found
    throw InvalidInputException(
        "BigQuery Authentication Failed\n"
        "\n"
        "No authentication credentials found. Please configure one of the following:\n"
        "\n"
        "Authentication options:\n"
        "  1. User credentials via Application Default Credentials (ADC):\n"
        "     • Run: gcloud auth application-default login\n"
        "  2. Service account key file via environment variable:\n"
        "     • Set GOOGLE_APPLICATION_CREDENTIALS to your service account key file path\n"
        "  3. DuckDB secret for this project:\n"
        "     • Run: CREATE SECRET (TYPE bigquery, SCOPE 'bq://your-project', ACCESS_TOKEN 'your-token')");
}

void BigqueryClient::ThrowOnErrorStatus(const google::cloud::Status &status) {
    if (status.code() == google::cloud::StatusCode::kUnauthenticated) {
        throw IOException( //
            "BigQuery Authentication Failed\n"
            "\n"
            "The provided credentials are invalid or have expired.\n"
            "\n"
            "Possible solutions:\n"
            "  1. Verify you are using the correct credentials for this project\n"
            "  2. Refresh your credentials:\n"
            "     • For user credentials: gcloud auth application-default login\n"
            "     • For service accounts: Update your service account key\n"
            "     • For DuckDB secrets: Update with CREATE OR REPLACE SECRET\n"
            "  3. Check if your access token has expired\n"
            "\n"
            "Error details from BigQuery API:\n"
            "  %s",
            status.message().c_str());
    }

    if (status.code() == google::cloud::StatusCode::kPermissionDenied) {
        throw PermissionException( //
            "BigQuery Permission Denied\n"
            "\n"
            "Your credentials are valid, but lack the necessary permissions for this operation.\n"
            "\n"
            "Required actions:\n"
			"  1. Verify you are using the correct credentials for this project"
            "  2. Verify your credentials have the required BigQuery roles:\n"
            "     • BigQuery Data Viewer (roles/bigquery.dataViewer) - for read access\n"
            "     • BigQuery Data Editor (roles/bigquery.dataEditor) - for write access\n"
            "     • BigQuery Job User (roles/bigquery.jobUser) - for running queries\n"
            "     • BigQuery Read Session User (roles/bigquery.readSessionUser) - for creating read sessions\n"
            "  3. Check project-level and dataset-level permissions\n"
            "\n"
            "Error details from BigQuery API:\n"
            "  %s",
            status.message().c_str());
    }
}

bool BigqueryClient::CheckSSLError(const google::cloud::Status &status) {
    if (status.message().find("Problem with the SSL CA cert") != std::string::npos) {
        if (!uses_custom_ca_bundle_path && BigquerySettings::CurlCaBundlePath().empty()) {
            uses_custom_ca_bundle_path = true;
            BigquerySettings::TryDetectCurlCaBundlePath();
            return true;
        }
    }
    return false;
}

bool BigqueryClient::CheckInvalidJsonError(const google::cloud::Status &status) {
    if (status.message().find("Not a valid Json") != std::string::npos) {
        return true;
    }
    return false;
}


} // namespace bigquery
} // namespace duckdb
