#pragma once

#include <iostream>
#include <string>
#include <vector>

#include "bigquery_arrow_reader.hpp"
#include "bigquery_proto_writer.hpp"
#include "bigquery_utils.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include "google/cloud/bigquery/storage/v1/bigquery_write_client.h"
#include "google/cloud/bigquery/storage/v1/storage.pb.h"
#include "google/cloud/bigquery/storage/v1/stream.pb.h"

#include "google/cloud/bigquery/v2/minimal/internal/job_client.h"
#include "google/cloud/bigquery/v2/minimal/internal/job_request.h"


namespace duckdb {
namespace bigquery {

class BigqueryClient {
public:
    explicit BigqueryClient(const string &project_id,
                            const string &dataset_id = "",
                            const string &api_endpoint = "",
                            const string &grpc_endpoint = "");
    explicit BigqueryClient(ConnectionDetails &conn);
    ~BigqueryClient();

    BigqueryClient(const BigqueryClient &);
    BigqueryClient &operator=(const BigqueryClient &);

    BigqueryClient(BigqueryClient &&other) noexcept;
    BigqueryClient &operator=(BigqueryClient &&other) noexcept;

    static BigqueryClient NewClient(const string &connection_string);

public:
    bool DatasetExists(const string &dataset_id);
    bool TableExists(const string &dataset_id, const string &table_id);

    vector<BigqueryDatasetRef> GetDatasets();
    vector<BigqueryTableRef> GetTables(const string &dataset_id);
    BigqueryDatasetRef GetDataset(const string &dataset_id);
    BigqueryTableRef GetTable(const string &dataset_id, const string &table_id);

    void CreateDataset(const CreateSchemaInfo &info, const BigqueryDatasetRef &dataset_ref);
    void CreateTable(const CreateTableInfo &info, const BigqueryTableRef &table_ref);
    void CreateView(const CreateViewInfo &info);

    void DropDataset(const DropInfo &info);
    void DropTable(const DropInfo &info);
    void DropView(const DropInfo &info);

    void GetTableInfo(const string &dataset_id,
                      const string &table_id,
                      ColumnList &res_columns,
                      vector<unique_ptr<Constraint>> &res_constraints);

    PostQueryResults ExecuteQuery(const string &query, const string &location = "");

    void CreateReadSession(const string &project_id, const string &dataset_id, const string &table_id);
    // void ReadArrowResutls(google::cloud::bigquery::storage::v1::ArrowSchema const &schema,
    //                       google::cloud::bigquery::storage::v1::ArrowRecordBatch const &batch);

    std::pair<google::cloud::bigquery_storage_v1::BigQueryReadClient, //
              google::cloud::bigquery::storage::v1::ReadSession>
    CreateReadSession(const string &dataset_id,
                      const string &table_id,
                      const idx_t num_streams,
                      const vector<string> &column_ids = std::vector<string>(),
                      const string &filter_cond = "");

    std::pair<shared_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteClient>,
              shared_ptr<google::cloud::bigquery::storage::v1::WriteStream>>
    CreateWriteStream(const string &dataset_id, const string &table_id);


    shared_ptr<BigqueryArrowReader> CreateArrowReader(const string &dataset_id,
                                                      const string &table_id,
                                                      const idx_t num_streams,
                                                      const vector<string> &column_ids = std::vector<string>(),
                                                      const string &filter_cond = "");

    shared_ptr<BigqueryProtoWriter> CreateProtoWriter(BigqueryTableEntry *entry);


    string GetProjectID() const {
        return project_id;
    }

    // DuckDB Settings
    static bool DebugPrintQueries();
    static void DebugSetPrintQueries(bool print);

    static string DefaultBigqueryLocation();
    static void SetDefaultBigqueryLocation(const string &location);

private:
    string GenerateJobId(const string &prefix = "");
    google::cloud::StatusOr<Job> GetJob(JobClient &job_client, const string &job_id, const string &location = "");

    google::cloud::StatusOr<PostQueryResults> PostQueryJob(JobClient &job_client,
                                                           const string &query,
                                                           const string &location = "");

    string dsn;
    string project_id;
    string dataset_id;
    string default_location;
    string api_endpoint;
    string grpc_endpoint;

    google::cloud::Options api_options;
    google::cloud::Options grpc_options;

    bool is_dev_env = false;
};

} // namespace bigquery
} // namespace duckdb
