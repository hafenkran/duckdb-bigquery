#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"

#include "bigquery_utils.hpp"

namespace duckdb {
namespace bigquery {

struct BigqueryArrowReader;

class BigqueryStreamFactory {
public:
    explicit BigqueryStreamFactory(shared_ptr<BigqueryArrowReader> reader) : reader(std::move(reader)) {
    }

    static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr, ArrowStreamParameters &parameters);
    static void GetSchema(ArrowArrayStream *factory_ptr, ArrowSchema &schema);

    shared_ptr<BigqueryArrowReader> GetArrowReader() {
        return reader;
    }

private:
    shared_ptr<BigqueryArrowReader> reader;
    std::atomic<idx_t> next_stream{0};
};

struct BigqueryArrowReader {
public:
    BigqueryArrowReader(const BigqueryTableRef table_ref,
                        const string billing_project_id,
                        idx_t num_streams,
                        const google::cloud::Options &options,
                        const vector<string> &column_ids = std::vector<string>(),
                        const string &filter_cond = "");

    std::shared_ptr<arrow::Schema> GetSchema();
    int64_t GetEstimatedRowCount();
    BigqueryTableRef GetTableRef() const;

    shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> GetStream(idx_t stream_idx);
    google::cloud::v2_38::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse> ReadRows(
        const string &stream_name,
        int row_offset);

    std::shared_ptr<arrow::Schema> ReadSchema(const google::cloud::bigquery::storage::v1::ArrowSchema &schema);
    std::shared_ptr<arrow::RecordBatch> ReadBatch(const google::cloud::bigquery::storage::v1::ArrowRecordBatch &batch);

private:
    BigqueryTableRef table_ref;
    string billing_project_id;

    idx_t num_streams;
    google::cloud::Options options;

    unique_ptr<google::cloud::bigquery_storage_v1::BigQueryReadClient> read_client;
    unique_ptr<google::cloud::bigquery::storage::v1::ReadSession> read_session;
    std::shared_ptr<arrow::Schema> arrow_schema;

    bool localhost_test_env;
};

} // namespace bigquery
} // namespace duckdb
