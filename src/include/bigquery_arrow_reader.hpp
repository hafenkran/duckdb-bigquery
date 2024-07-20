#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/column_list.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"


namespace duckdb {
namespace bigquery {


struct BigqueryArrowReader {
public:
    BigqueryArrowReader(const string &project_id,
                        const string &dataset_id,
                        const string &table_id,
                        idx_t num_streams,
                        const google::cloud::Options &options,
                        const vector<string> &column_ids = std::vector<string>(),
                        const string &filter_cond = "");

    std::shared_ptr<arrow::Schema> GetSchema();
    int64_t GetEstimatedRowCount();

    void MapTableInfo(ColumnList &res_columns, vector<unique_ptr<Constraint>> &res_constraints);

    shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> NextStream();

    google::cloud::v2_23::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse> ReadRows(
        const string &stream_name,
        int row_offset);
    void ReadColumn(const std::shared_ptr<arrow::Array> &column, Vector &out_vec);
    std::shared_ptr<arrow::Schema> ReadSchema(const google::cloud::bigquery::storage::v1::ArrowSchema &schema);
    std::shared_ptr<arrow::RecordBatch> ReadBatch(const google::cloud::bigquery::storage::v1::ArrowRecordBatch &batch);

private:
    void ReadSimpleColumn(const std::shared_ptr<arrow::Array> &column, Vector &out_vec);
    void ReadListColumn(const std::shared_ptr<arrow::ListArray> &list_array, Vector &out_vec);
    void ReadStructColumn(const std::shared_ptr<arrow::StructArray> &struct_array, Vector &out_vec);

    string project_id;
    string dataset_id;
    string table_id;

    idx_t num_streams;
    idx_t next_stream = 0;
    google::cloud::Options options;

    unique_ptr<google::cloud::bigquery_storage_v1::BigQueryReadClient> read_client;
    unique_ptr<google::cloud::bigquery::storage::v1::ReadSession> read_session;
    std::shared_ptr<arrow::Schema> arrow_schema;

    bool localhost_test_env;
};


} // namespace bigquery
} // namespace duckdb
