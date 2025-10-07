#pragma once

#include <iostream>
#include <string>
#include <vector>

#include "bigquery_arrow_reader.hpp"
#include "bigquery_proto_writer.hpp"
#include "bigquery_settings.hpp"
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

struct ListJobsParams {
    std::optional<std::string> job_id;
    std::optional<bool> all_users;
    std::optional<int> max_results;
    std::optional<timestamp_t> min_creation_time;
    std::optional<timestamp_t> max_creation_time;
    std::optional<std::string> projection;
    std::optional<std::string> state_filter;
    std::optional<std::string> parent_job_id;
};

class BigqueryClient {
public:
    explicit BigqueryClient(const BigqueryConfig &config, optional_ptr<ClientContext> context = nullptr);
    ~BigqueryClient() {};

public:
    vector<BigqueryDatasetRef> GetDatasets();
    vector<BigqueryTableRef> GetTables(const string &dataset_id);

    BigqueryDatasetRef GetDataset(const string &dataset_id);
    BigqueryTableRef GetTable(const string &dataset_id, const string &table_id);

    bool DatasetExists(const string &dataset_id);
    bool TableExists(const string &dataset_id, const string &table_id);

    void CreateDataset(const CreateSchemaInfo &info, const BigqueryDatasetRef &dataset_ref);
    void CreateTable(const CreateTableInfo &info, const BigqueryTableRef &table_ref);
    void CreateView(const CreateViewInfo &info);

    void DropDataset(const DropInfo &info);
    void DropTable(const DropInfo &info);
    void DropView(const DropInfo &info);

    void GetTableInfosFromDataset(const BigqueryDatasetRef &dataset_ref, map<string, CreateTableInfo> &table_infos);
    void GetTableInfosFromDatasets(const vector<BigqueryDatasetRef> &dataset_ref,
                                   map<string, CreateTableInfo> &table_infos);

    void GetTableInfo(const string &dataset_id,
                      const string &table_id,
                      ColumnList &res_columns,
                      vector<unique_ptr<Constraint>> &res_constraints);
    void GetTableInfoForQuery(const string &query,
                              ColumnList &res_columns,
                              vector<unique_ptr<Constraint>> &res_constraints);

    vector<google::cloud::bigquery::v2::ListFormatJob> ListJobs(const ListJobsParams &params);
    google::cloud::bigquery::v2::Job GetJob(const string &job_id, const string &location = "");
	google::cloud::bigquery::v2::Job GetJobByReference(const google::cloud::bigquery::v2::JobReference &job_ref);

    google::cloud::bigquery::v2::QueryResponse ExecuteQuery(const string &query,
                                                            const string &location = "",
                                                            const bool &dry_run = false);
    google::cloud::bigquery::v2::GetQueryResultsResponse GetQueryResults(
        const google::cloud::bigquery::v2::JobReference &job_ref,
        const string &page_token = "");

    shared_ptr<BigqueryArrowReader> CreateArrowReader(const BigqueryTableRef &table_ref,
                                                      const idx_t num_streams,
                                                      const vector<string> &column_ids = std::vector<string>(),
                                                      const string &filter_cond = "");

    shared_ptr<BigqueryProtoWriter> CreateProtoWriter(BigqueryTableEntry *entry);

    string GetProjectID() const {
        return config.project_id;
    }

private:
    string GenerateJobId(const string &prefix = "");

    google::cloud::StatusOr<google::cloud::bigquery::v2::Job> GetJobInternal(
        google::cloud::bigquerycontrol_v2::JobServiceClient &job_client,
        const string &job_id,
        const string &location);

    google::cloud::StatusOr<google::cloud::bigquery::v2::QueryResponse> PostQueryJobInternal(
        google::cloud::bigquerycontrol_v2::JobServiceClient &job_client,
        const string &query,
        const string &location = "",
        const bool &dry_run = false);

    google::cloud::StatusOr<google::cloud::bigquery::v2::GetQueryResultsResponse> GetQueryResultsInternal(
        google::cloud::bigquerycontrol_v2::JobServiceClient &job_client,
        const google::cloud::bigquery::v2::JobReference &job_ref,
        const string &page_token = "");

    google::cloud::Options OptionsAPI();
    google::cloud::Options OptionsGRPC();

    void MapTableSchema(const google::cloud::bigquery::v2::TableSchema &schema,
                        ColumnList &res_columns,
                        vector<unique_ptr<Constraint>> &res_constraints);

    void MapInformationSchemaRows(const std::string &project_id,
                                  const google::protobuf::RepeatedPtrField<::google::protobuf::Struct> &rows,
                                  std::map<std::string, CreateTableInfo> &table_infos);

    bool CheckSSLError(const google::cloud::Status &status) {
        if (status.message().find("Problem with the SSL CA cert") != std::string::npos) {
            if (!uses_custom_ca_bundle_path && BigquerySettings::CurlCaBundlePath().empty()) {
                uses_custom_ca_bundle_path = true;
                BigquerySettings::TryDetectCurlCaBundlePath();
                return true;
            }
        }
        return false;
    }

    bool CheckInvalidJsonError(const google::cloud::Status &status) {
        if (status.message().find("Not a valid Json") != std::string::npos) {
            return true;
        }
        return false;
    }

private:
    bool GetCredentialsFromSecret(google::cloud::Options &options);

    BigqueryConfig config;
    bool uses_custom_ca_bundle_path = false;
    optional_ptr<ClientContext> context;
};

} // namespace bigquery
} // namespace duckdb
