#include "bigquery_client.hpp"
#include "bigquery_arrow_reader.hpp"
#include "bigquery_proto_writer.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
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

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/ipc/writer.h"
#include "grpcpp/grpcpp.h"


namespace duckdb {
namespace bigquery {

class CustomIdempotencyPolicy : public google::cloud::bigquery_storage_v1::BigQueryWriteConnectionIdempotencyPolicy {
public:
    ~CustomIdempotencyPolicy() override = default;
    std::unique_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteConnectionIdempotencyPolicy> clone()
        const override {
        return std::make_unique<CustomIdempotencyPolicy>(*this);
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

BigqueryClient::BigqueryClient(const BigqueryConfig &config) : config(config) {
    if (config.project_id.empty()) {
        throw std::runtime_error("BigqueryClient::BigqueryClient: project_id is empty");
    }
}

google::cloud::Options BigqueryClient::OptionsAPI() {
    auto options = google::cloud::Options{};
    if (!config.api_endpoint.empty()) {
        options.set<google::cloud::EndpointOption>(config.api_endpoint);
    }
    auto ca_path = BigquerySettings::CurlCaBundlePath();
    if (!ca_path.empty()) {
        options.set<google::cloud::v2_27::CARootsFilePathOption>(ca_path);
    }
    return options;
}

google::cloud::Options BigqueryClient::OptionsGRPC() {
    auto options = google::cloud::Options{};
    if (!config.grpc_endpoint.empty()) {
        options.set<google::cloud::EndpointOption>(config.grpc_endpoint);
		if (config.is_dev_env()) {
            options.set<google::cloud::GrpcCredentialOption>(grpc::InsecureChannelCredentials());
        }
    }
    return options;
}

vector<BigqueryDatasetRef> BigqueryClient::GetDatasets() {
    auto request = google::cloud::bigquery::v2::ListDatasetsRequest();
    request.set_project_id(config.project_id);

    auto dataset_client = make_shared_ptr<google::cloud::bigquerycontrol_v2::DatasetServiceClient>(
        google::cloud::bigquerycontrol_v2::MakeDatasetServiceConnectionRest(OptionsAPI()));
    auto datasets = dataset_client->ListDatasets(request);

    vector<BigqueryDatasetRef> result;
    for (google::cloud::StatusOr<google::cloud::bigquery::v2::ListFormatDataset> const &dataset : datasets) {
        if (!dataset.ok()) {
            // Special case for empty projects. The "empty" result object seems to be unparseable by the lib.
            if (dataset.status().message() ==
                "Permanent error, with a last message of Not a valid Json DatasetList object") {
                return result;
            }
            return vector<BigqueryDatasetRef>();
        }

        google::cloud::bigquery::v2::ListFormatDataset dataset_val = dataset.value();
        auto dataset_ref = dataset_val.dataset_reference();

        BigqueryDatasetRef info;
        info.project_id = dataset_ref.project_id();
        info.dataset_id = dataset_ref.dataset_id();
        info.location = dataset_val.location();
        result.push_back(info);
    }
    return result;
}

vector<BigqueryTableRef> BigqueryClient::GetTables(const string &dataset_id) {
    auto request = google::cloud::bigquery::v2::ListTablesRequest();
    request.set_project_id(config.project_id);
    request.set_dataset_id(dataset_id);

    auto table_client = make_shared_ptr<google::cloud::bigquerycontrol_v2::TableServiceClient>(
        google::cloud::bigquerycontrol_v2::MakeTableServiceConnectionRest(OptionsAPI()));
    auto tables = table_client->ListTables(request);

    vector<BigqueryTableRef> table_names;
    for (google::cloud::StatusOr<google::cloud::bigquery::v2::ListFormatTable> const &table : tables) {
        if (!table.ok()) {
            // Special case for empty datasets. The "empty" result object seems to be unparseable by the lib.
            if (table.status().message() ==
                "Permanent error, with a last message of Not a valid Json TableList object") {
                return table_names;
            }
            throw InternalException(table.status().message());
        }

        google::cloud::bigquery::v2::ListFormatTable table_val = table.value();
        auto table_ref = table_val.table_reference();

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
        throw InternalException(response.status().message());
    }

    auto dataset = response.value();
    auto dataset_ref = dataset.dataset_reference();

    BigqueryDatasetRef info;
    info.project_id = dataset_ref.project_id();
    info.dataset_id = dataset_ref.dataset_id();
    info.location = dataset.location();
    return info;
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
        throw InternalException(response.status().message());
    }

    auto tablae = response.value();
    auto table_ref = tablae.table_reference();

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

void BigqueryClient::GetTableInfo(const string &dataset_id,
                                  const string &table_id,
                                  ColumnList &res_columns,
                                  vector<unique_ptr<Constraint>> &res_constraints) {

    auto client = make_shared_ptr<google::cloud::bigquerycontrol_v2::TableServiceClient>(
        google::cloud::bigquerycontrol_v2::MakeTableServiceConnectionRest(OptionsAPI()));

    auto request = google::cloud::bigquery::v2::GetTableRequest();
    request.set_project_id(config.project_id);
    request.set_dataset_id(dataset_id);
    request.set_table_id(table_id);

    auto response = client->GetTable(request);
    if (!response.ok()) {
        if (response.status().code() == google::cloud::StatusCode::kNotFound) {
            auto table_ref = BigqueryUtils::FormatTableString(config.project_id, dataset_id, table_id);
            throw BinderException("GetTableInfo - table \"%s\" not found", table_ref);
        }
        throw InternalException(response.status().message());
    }

    auto table = response.value();
    for (const google::cloud::bigquery::v2::TableFieldSchema &field : table.schema().fields()) {
        // Create the ColumnDefinition
        auto column_type = BigqueryUtils::FieldSchemaToLogicalType(field);

        ColumnDefinition column(field.name(), std::move(column_type));
        // column.SetComment(std::move(field.description));

        auto default_value_expr = field.default_value_expression();
        auto default_value = default_value_expr.value();
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
        auto mode = field.mode();
        if (mode == "REQUIRED") {
            auto field_name = field.name();
            auto field_index = res_columns.GetColumnIndex(field_name);
            res_constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(field_index)));
        }
    }
}

shared_ptr<BigqueryArrowReader> BigqueryClient::CreateArrowReader(const string &dataset_id,
                                                                  const string &table_id,
                                                                  const idx_t num_streams,
                                                                  const vector<string> &column_ids,
                                                                  const string &filter_cond) {

    return make_shared_ptr<BigqueryArrowReader>(BigqueryTableRef(config.project_id, dataset_id, table_id),
                                                config.billing_project(),
                                                num_streams,
                                                OptionsGRPC(),
                                                column_ids,
                                                filter_cond);
}

shared_ptr<BigqueryProtoWriter> BigqueryClient::CreateProtoWriter(BigqueryTableEntry *entry) {
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
                           CustomIdempotencyPolicy().clone())
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


google::cloud::bigquery::v2::QueryResponse BigqueryClient::ExecuteQuery(const string &query, const string &location) {
    auto client = google::cloud::bigquerycontrol_v2::JobServiceClient(
        google::cloud::bigquerycontrol_v2::MakeJobServiceConnectionRest(OptionsAPI()));

    auto response = PostQueryJob(client, query, location);
    if (!response) {
        throw BinderException("Query execution failed: " + response.status().message());
    }

    if (!config.is_dev_env()) {
        int delay = 1;
        int max_retries = 3;
        for (int i = 0; i < max_retries; i++) {
            auto job_status =
                GetJob(client, response->job_reference().job_id(), response->job_reference().location().value());
            if (job_status.ok() && job_status->status().state() == "DONE") {
                if (!job_status->status().error_result().reason().empty()) {
                    throw BinderException(job_status->status().error_result().message());
                }
                return *response;
            }
            if (i < max_retries) {
                std::this_thread::sleep_for(std::chrono::seconds(delay));
                delay *= 2;
            }
        }
        throw BinderException("Max retries exceeded.");
    }
    return *response;
}


google::cloud::StatusOr<google::cloud::bigquery::v2::Job> BigqueryClient::GetJob(
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
    request.set_project_id(config.billing_project());
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

google::cloud::StatusOr<google::cloud::bigquery::v2::QueryResponse> BigqueryClient::PostQueryJob(
    google::cloud::bigquerycontrol_v2::JobServiceClient &job_client,
    const string &query,
    const string &location) {

    if (BigquerySettings::DebugQueryPrint()) {
        std::cout << "query: " << query << std::endl;
    }

    google::cloud::bigquery::v2::DatasetReference dataset_ref;
    dataset_ref.set_project_id(config.project_id);
    dataset_ref.set_dataset_id("UNKNOWN");

    auto query_request = google::cloud::bigquery::v2::QueryRequest();
    *query_request.mutable_query() = query;
    *query_request.mutable_request_id() = GenerateJobId();
    *query_request.mutable_default_dataset() = dataset_ref;
    query_request.set_dry_run(false);
    query_request.mutable_use_legacy_sql()->set_value(false);

    // query_request.mutable_set_preserve_nulls()->set_value(true);
    if (!location.empty()) {
        *query_request.mutable_location() = location;
    }

    auto request = google::cloud::bigquery::v2::PostQueryRequest();
    request.set_project_id(config.billing_project());
    *request.mutable_query_request() = query_request;

    return job_client.Query(request);
}

string BigqueryClient::GenerateJobId(const string &prefix) {
    constexpr char kDefaultJobPrefix[] = "job_duckdb";
    auto rng = google::cloud::internal::MakeDefaultPRNG();
    string id = google::cloud::internal::Sample(rng, 12, "abcdefghijklmnopqrstuvwxyz");
    return kDefaultJobPrefix + (prefix.empty() ? "" : "_" + prefix) + "_" + id;
}


} // namespace bigquery
} // namespace duckdb
