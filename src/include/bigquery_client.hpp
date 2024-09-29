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

#include "google/cloud/bigquerycontrol/v2/job_client.h"


namespace duckdb {
namespace bigquery {

class BigqueryClient {
public:
    explicit BigqueryClient(const BigqueryConfig &config);
    ~BigqueryClient() {};

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

    void GetTableInfosFromDataset(const BigqueryDatasetRef &dataset_ref, map<string, CreateTableInfo> &table_infos);
    void GetTableInfo(const string &dataset_id,
                      const string &table_id,
                      ColumnList &res_columns,
                      vector<unique_ptr<Constraint>> &res_constraints);

    google::cloud::bigquery::v2::QueryResponse ExecuteQuery(const string &query, const string &location = "");
    google::cloud::bigquery::v2::Job GetJob(google::cloud::bigquery::v2::QueryResponse &query_response);

    shared_ptr<BigqueryArrowReader> CreateArrowReader(const string &dataset_id,
                                                      const string &table_id,
                                                      const idx_t num_streams,
                                                      const vector<string> &column_ids = std::vector<string>(),
                                                      const string &filter_cond = "");

    shared_ptr<BigqueryProtoWriter> CreateProtoWriter(BigqueryTableEntry *entry);


    string GetProjectID() const {
        return config.project_id;
    }

private:
    string GenerateJobId(const string &prefix = "");

    google::cloud::StatusOr<google::cloud::bigquery::v2::Job> GetJob(
        google::cloud::bigquerycontrol_v2::JobServiceClient &job_client,
        const string &job_id,
        const string &location);

    google::cloud::StatusOr<google::cloud::bigquery::v2::QueryResponse> PostQueryJob(
        google::cloud::bigquerycontrol_v2::JobServiceClient &job_client,
        const string &query,
        const string &location = "");

    google::cloud::Options OptionsAPI();
    google::cloud::Options OptionsGRPC();

private:
    BigqueryConfig config;
};

} // namespace bigquery
} // namespace duckdb
