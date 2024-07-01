#include "bigquery_arrow_reader.hpp"
#include "bigquery_client.hpp"
#include "bigquery_proto_writer.hpp"
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
#include "google/cloud/bigquery/v2/minimal/internal/dataset_client.h"
#include "google/cloud/bigquery/v2/minimal/internal/dataset_request.h"
#include "google/cloud/bigquery/v2/minimal/internal/job_client.h"
#include "google/cloud/bigquery/v2/minimal/internal/job_request.h"
#include "google/cloud/bigquery/v2/minimal/internal/table_client.h"
#include "google/cloud/bigquery/v2/minimal/internal/table_request.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/idempotency.h"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/ipc/writer.h"
#include "grpcpp/grpcpp.h"

#include <chrono>
#include <thread>

// #include "storage/bigquery_table_entry.cpp"

using namespace google::cloud::bigquery_v2_minimal_internal;

namespace duckdb {
namespace bigquery {

static bool debug_bigquery_print_queries = true;
static string bigquery_default_location = "US";

class CustomIdempotencyPolicy : public google::cloud::bigquery_storage_v1::BigQueryWriteConnectionIdempotencyPolicy {
public:
    ~CustomIdempotencyPolicy() override = default;
    std::unique_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteConnectionIdempotencyPolicy> clone()
        const override {
        return std::make_unique<CustomIdempotencyPolicy>(*this);
    }

    google::cloud::Idempotency CreateWriteStream(
        google::cloud::bigquery::storage::v1::CreateWriteStreamRequest const &request) {
        return google::cloud::Idempotency::kIdempotent;
    }

    google::cloud::Idempotency AppendRows(google::cloud::bigquery::storage::v1::AppendRowsRequest const &request) {
        return google::cloud::Idempotency::kIdempotent;
    }

    google::cloud::Idempotency GetWriteStream(
        google::cloud::bigquery::storage::v1::GetWriteStreamRequest const &request) {
        return google::cloud::Idempotency::kIdempotent;
    }

    google::cloud::Idempotency BatchCommitWriteStreams(
        google::cloud::bigquery::storage::v1::BatchCommitWriteStreamsRequest const &request) {
        return google::cloud::Idempotency::kIdempotent;
    }
};

template <typename Func>
bool retryOperation(Func op, int maxAttempts, int initialDelayMs) {
    int attempts = 0;
    int delay = initialDelayMs;

    while (attempts < maxAttempts) {
        if (op()) {
            return true;
        }

        attempts++;
        if (attempts < maxAttempts) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            delay *= 2;
        }
    }
    return false;
}


BigqueryClient::BigqueryClient(string project_id, string dataset_id, string api_endpoint, string grpc_endpoint)
    : project_id(project_id), dataset_id(dataset_id), api_endpoint(api_endpoint), grpc_endpoint(grpc_endpoint) {

    if (project_id.empty()) {
        throw std::runtime_error("BigqueryClient::BigqueryClient: project_id is empty");
    }

    api_options = google::cloud::Options{};
    if (!api_endpoint.empty()) {
        api_options = api_options.set<google::cloud::EndpointOption>(api_endpoint);
        if (api_endpoint.find("localhost") != string::npos || api_endpoint.find("127.0.0.0") != string::npos) {
            is_dev_env = true;
        }
    }

    grpc_options = google::cloud::Options{};
    if (!grpc_endpoint.empty()) {
        grpc_options = grpc_options.set<google::cloud::EndpointOption>(grpc_endpoint);
        if (grpc_endpoint.find("localhost") != string::npos || grpc_endpoint.find("127.0.0.0") != string::npos) {
            grpc_options = grpc_options.set<google::cloud::GrpcCredentialOption>(grpc::InsecureChannelCredentials());
        }
    }
}

BigqueryClient::BigqueryClient(ConnectionDetails &conn)
    : BigqueryClient(conn.project_id, conn.dataset_id, conn.api_endpoint, conn.grpc_endpoint) {
}

BigqueryClient::~BigqueryClient() {
}

BigqueryClient::BigqueryClient(const BigqueryClient &other) {
    project_id = other.project_id;
    dataset_id = other.dataset_id;
    api_endpoint = other.api_endpoint;
    grpc_endpoint = other.grpc_endpoint;
    dsn = other.dsn;
}

BigqueryClient &BigqueryClient::operator=(const BigqueryClient &other) {
    if (this != &other) {
        project_id = other.project_id;
        dataset_id = other.dataset_id;
        api_endpoint = other.api_endpoint;
        grpc_endpoint = other.grpc_endpoint;
        dsn = other.dsn;
    }
    return *this;
}

BigqueryClient::BigqueryClient(BigqueryClient &&other) noexcept {
    std::swap(project_id, other.project_id);
    std::swap(dataset_id, other.dataset_id);
    std::swap(api_endpoint, other.api_endpoint);
    std::swap(dsn, other.dsn);
}

BigqueryClient &BigqueryClient::operator=(BigqueryClient &&other) noexcept {
    std::swap(project_id, other.project_id);
    std::swap(dataset_id, other.dataset_id);
    std::swap(api_endpoint, other.api_endpoint);
    std::swap(dsn, other.dsn);
    return *this;
}

BigqueryClient BigqueryClient::NewClient(const string &connection_str) {
    auto con = BigqueryUtils::ParseConnectionString(connection_str);
    if (con.is_valid()) {
        throw InternalException("Invalid connection string");
    }

    BigqueryClient result(con.project_id, con.dataset_id, con.api_endpoint, con.grpc_endpoint);
    result.dsn = connection_str;
    return result;
}

vector<BigqueryDatasetRef> BigqueryClient::GetDatasets() {
    ListDatasetsRequest request(project_id);

    auto dataset_client = make_shared_ptr<DatasetClient>(MakeDatasetConnection(api_options));
    auto datasets = dataset_client->ListDatasets(request);

    vector<BigqueryDatasetRef> dataset_names;
    for (google::cloud::StatusOr<ListFormatDataset> const &dataset : datasets) {
        if (!dataset.ok()) {
            // Special case for empty projects. The "empty" result object seems to be unparseable by the lib.
            if (dataset.status().message() ==
                "Permanent error, with a last message of Not a valid Json DatasetList object") {
                return dataset_names;
            }
            return vector<BigqueryDatasetRef>();
        }

        ListFormatDataset dataset_val = dataset.value();
        BigqueryDatasetRef info;
        info.project_id = dataset_val.dataset_reference.project_id;
        info.dataset_id = dataset_val.dataset_reference.dataset_id;
        info.location = dataset_val.location;
        dataset_names.push_back(info);
    }

    return dataset_names;
}

vector<BigqueryTableRef> BigqueryClient::GetTables(const string dataset_id) {
    auto table_client = make_shared_ptr<TableClient>(MakeTableConnection(api_options));

    ListTablesRequest request(project_id, dataset_id);
    auto tables = table_client->ListTables(request);

    vector<BigqueryTableRef> table_names;
    for (google::cloud::StatusOr<ListFormatTable> const &table : tables) {
        if (!table.ok()) {
            // Special case for empty datasets. The "empty" result object seems to be unparseable by the lib.
            if (table.status().message() ==
                "Permanent error, with a last message of Not a valid Json TableList object") {
                return table_names;
            }
            throw InternalException(table.status().message());
        }
        ListFormatTable table_val = table.value();
        BigqueryTableRef info;
        info.project_id = table_val.table_reference.project_id;
        info.dataset_id = table_val.table_reference.dataset_id;
        info.table_id = table_val.table_reference.table_id;
        table_names.push_back(info);
    }
    return table_names;
}

BigqueryDatasetRef BigqueryClient::GetDataset(const string dataset_id) {
    auto dataset_client = make_shared_ptr<DatasetClient>(MakeDatasetConnection(api_options));
    GetDatasetRequest request(project_id, dataset_id);
    auto dataset = dataset_client->GetDataset(request);
    if (!dataset) {
        throw InternalException(dataset.status().message());
    }

    Dataset dataset_val = dataset.value();
    BigqueryDatasetRef info;
    info.project_id = project_id;
    info.dataset_id = dataset_id;
    info.location = dataset_val.location;
    return info;
}

BigqueryTableRef BigqueryClient::GetTable(const string dataset_id, const string table_id) {
    auto table_client = make_shared_ptr<TableClient>(MakeTableConnection(api_options));

    GetTableRequest request(project_id, dataset_id, table_id);
    auto table = table_client->GetTable(request);
    if (!table) {
        throw InternalException(table.status().message());
    }

    Table table_val = table.value();
    BigqueryTableRef info;
    info.project_id = project_id;
    info.dataset_id = dataset_id;
    info.table_id = table_id;
    return info;
}

bool BigqueryClient::DatasetExists(const string dataset_id) {
    auto dataset_client = make_shared_ptr<DatasetClient>(MakeDatasetConnection(api_options));

    GetDatasetRequest request(project_id, dataset_id);
    auto dataset = dataset_client->GetDataset(request);

    if (!dataset) {
        std::cerr << "Error: " << dataset.status().message() << "\n";
        return false;
    }
    return true;
}

bool BigqueryClient::TableExists(const string dataset_id, const string table_id) {
    auto table_client = make_shared_ptr<TableClient>(MakeTableConnection(api_options));

    GetTableRequest request(project_id, dataset_id, table_id);
    auto table = table_client->GetTable(request);

    if (!table) {
        std::cerr << "Error: " << table.status().message() << "\n";
        return false;
    }
    return true;
}

void BigqueryClient::CreateDataset(const CreateSchemaInfo &info, const BigqueryDatasetRef &dataset_ref) {
    auto query = BigquerySQL::CreateSchemaInfoToSQL(GetProjectID(), info);
    ExecuteQuery(query, dataset_ref.location);

    // Check if dataset exists with an exponential backoff retry
    auto op = [this, &info]() -> bool { return DatasetExists(info.schema); };
    auto success = retryOperation(op, 10, 1000);
    if (!success) {
        throw InternalException("Failed to verify that \"%s\" has been successfully created", info.schema);
    }
}

void BigqueryClient::CreateTable(const CreateTableInfo &info, const BigqueryTableRef &table_ref) {
    auto query = BigquerySQL::CreateTableInfoToSQL(GetProjectID(), info);
    ExecuteQuery(query);

    // Check if the table exists with an exponential backoff retry
    auto op = [this, &info]() -> bool { return TableExists(info.schema, info.table); };
    auto success = retryOperation(op, 10, 1000);
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

void BigqueryClient::GetTableInfo(const string dataset_id,
                                  const string table_id,
                                  ColumnList &res_columns,
                                  vector<unique_ptr<Constraint>> &res_constraints) {

    auto table_client = make_shared_ptr<TableClient>(MakeTableConnection(api_options));
    GetTableRequest request(project_id, dataset_id, table_id);
    auto table = table_client->GetTable(request);

    if (!table) {
        std::cerr << "Error: " << table.status().message() << "\n";
        auto metadata = table.status().error_info().metadata();
        for (auto const &m : metadata) {
            std::cerr << "metadata: " << m.first << " " << m.second << "\n";
        }
        auto table_ref = BigqueryUtils::FormatTableString(project_id, dataset_id, table_id);
        throw InternalException("GetTableInfo - table \"%s\" not found", table_ref);
    }

    Table table_val = table.value();
    for (const TableFieldSchema &field : table_val.schema.fields) {
        // Create the ColumnDefinition
        auto column_type = BigqueryUtils::FieldSchemaToLogicalType(field);

        ColumnDefinition column(std::move(field.name), std::move(column_type));
        // column.SetComment(std::move(field.description));

        auto default_value = field.default_value_expression;
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
        auto mode = field.mode;
        if (mode == "REQUIRED") {
            auto field_name = field.name;
            auto field_index = res_columns.GetColumnIndex(field_name);
            res_constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(field_index)));
        }
    }
}


std::pair<google::cloud::bigquery_storage_v1::BigQueryReadClient, google::cloud::bigquery::storage::v1::ReadSession>
BigqueryClient::CreateReadSession(const string dataset_id,
                                  const string table_id,
                                  const idx_t num_streams,
                                  const vector<string> &selected,
                                  const string &filter_cond) {
    const string parent = BigqueryUtils::FormatParentString(project_id);
    const string table_ref = BigqueryUtils::FormatTableString(project_id, dataset_id, table_id);

    // Initialize the client.
    auto connection = google::cloud::bigquery_storage_v1::MakeBigQueryReadConnection(grpc_options);
    auto client = google::cloud::bigquery_storage_v1::BigQueryReadClient(connection);

    // Create the ReadSession.
    google::cloud::bigquery::storage::v1::ReadSession read_session;
    read_session.set_table(table_ref);
    read_session.set_data_format(google::cloud::bigquery::storage::v1::DataFormat::ARROW);

    // https://github.com/rocketechgroup/bigquery-storage-read-api-example/blob/master/main_simple.py
    auto *read_options = read_session.mutable_read_options();
    if (selected.size() > 0) {
        for (const auto &column : selected) {
            read_options->add_selected_fields(column);
        }
    }
    if (filter_cond != "") {
        read_options->set_row_restriction(filter_cond);
    }

    auto session = client.CreateReadSession(parent, read_session, num_streams);
    if (!session) {
        throw InternalException("some error occured: " + session.status().message());
    }

    return std::make_pair(client, *session);
}

std::pair<shared_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteClient>,
          shared_ptr<google::cloud::bigquery::storage::v1::WriteStream>>
BigqueryClient::CreateWriteStream(const string dataset_id, const string table_id) {
    const string parent = BigqueryUtils::FormatParentString(project_id);
    const string table_ref = BigqueryUtils::FormatTableString(project_id, dataset_id, table_id);

    // Set retry and backoff policies.
    auto options = grpc_options
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

    // Initialize the client.
    auto client = make_shared_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteClient>(
        google::cloud::bigquery_storage_v1::MakeBigQueryWriteConnection(options));

    google::cloud::bigquery::storage::v1::WriteStream write_stream;
    write_stream.set_type(google::cloud::bigquery::storage::v1::WriteStream_Type::WriteStream_Type_COMMITTED);
    auto real_write_stream = client->CreateWriteStream(table_ref, write_stream);
    if (!real_write_stream) {
        throw InternalException("some error occured: " + real_write_stream.status().message());
    }

    auto res_write_stream = make_shared_ptr<google::cloud::bigquery::storage::v1::WriteStream>(real_write_stream.value());
    return std::make_pair(client, res_write_stream);
}

shared_ptr<BigqueryArrowReader> BigqueryClient::CreateArrowReader(const string dataset_id,
                                                                  const string table_id,
                                                                  const idx_t num_streams,
                                                                  const vector<string> &column_ids,
                                                                  const string &filter_cond) {
    return make_shared_ptr<BigqueryArrowReader>(project_id,
                                            dataset_id,
                                            table_id,
                                            num_streams,
                                            grpc_options,
                                            column_ids,
                                            filter_cond);
}

shared_ptr<BigqueryProtoWriter> BigqueryClient::CreateProtoWriter(BigqueryTableEntry *entry) {
    if (entry == nullptr) {
        throw InternalException("Error while initializing proto writer: entry is null");
    }
	auto &bq_catalog = dynamic_cast<BigqueryCatalog &>(entry->catalog);
    if (bq_catalog.GetProjectID() != project_id) {
        throw InternalException("Error while initializing proto writer: project_id mismatch");
    }

    while (!TableExists(entry->schema.name, entry->name)) {
        std::cout << "Table does not exist yet" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cout << "Retrying..." << std::endl;
    }

    auto options = grpc_options
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


PostQueryResults BigqueryClient::ExecuteQuery(const string &query, const string &location) {
    auto job_client = JobClient(MakeBigQueryJobConnection(api_options));
    auto response = PostQueryJob(job_client, query, location);
    if (!response) {
        throw BinderException("Query execution failed: " + response.status().message());
    }

    if (!is_dev_env) {
        int delay = 1;
        int max_retries = 3;
        for (int i = 0; i < max_retries; i++) {
            auto job_status = GetJob(job_client, response->job_reference.job_id, response->job_reference.location);
            if (job_status.ok() && job_status->status.state == "DONE") {
                if (!job_status->status.error_result.reason.empty()) {
                    throw BinderException(job_status->status.error_result.message);
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


google::cloud::StatusOr<Job> BigqueryClient::GetJob(JobClient &job_client, const string job_id, const string location) {
    if (project_id.empty()) {
        throw BinderException("project_id config parameter is empty.");
    } else if (job_id.empty()) {
        throw BinderException("job_id config parameter is empty.");
    }
    GetJobRequest request;
    request.set_project_id(project_id);
    request.set_job_id(job_id);
    if (!location.empty()) {
        request.set_location(location);
    }
    return job_client.GetJob(request);
}


google::cloud::StatusOr<PostQueryResults> BigqueryClient::PostQueryJob(JobClient &job_client,
                                                                       const string &query,
                                                                       const string &location) {
    if (project_id.empty()) {
        throw BinderException("project_id config parameter is empty.");
    }
    if (debug_bigquery_print_queries) {
        std::cout << "query: " << query << std::endl;
    }

    DatasetReference dataset_ref;
    dataset_ref.project_id = project_id;
    dataset_ref.dataset_id = "UNKNOWN";

    auto query_request = QueryRequest(query);
    query_request.set_dry_run(false);
    query_request.set_use_legacy_sql(false);
    query_request.set_request_id(GenerateJobId());
    query_request.set_preserve_nulls(true);
    query_request.set_default_dataset(dataset_ref);
    if (!location.empty()) {
        query_request.set_location(location);
    }

    PostQueryRequest request;
    request = request.set_project_id(project_id);
    request = request.set_query_request(query_request);

    return job_client.Query(request);
}


void BigqueryClient::DebugSetPrintQueries(bool print) {
    debug_bigquery_print_queries = print;
}

bool BigqueryClient::DebugPrintQueries() {
    return debug_bigquery_print_queries;
}

void BigqueryClient::SetDefaultBigqueryLocation(const string &location) {
    bigquery_default_location = location;
}

string BigqueryClient::DefaultBigqueryLocation() {
    return bigquery_default_location;
}

string BigqueryClient::GenerateJobId(const string &prefix) {
    constexpr char kDefaultJobPrefix[] = "job_duckdb";
    auto rng = google::cloud::internal::MakeDefaultPRNG();
    string id = google::cloud::internal::Sample(rng, 12, "abcdefghijklmnopqrstuvwxyz");
    return kDefaultJobPrefix + (prefix.empty() ? "" : "_" + prefix) + "_" + id;
}


} // namespace bigquery
} // namespace duckdb
