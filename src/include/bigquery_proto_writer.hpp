#pragma once

#include <google/cloud/bigquery/bigquery_write_client.h>
#undef NO_DATA

#include <deque>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"
#include "storage/bigquery_table_entry.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryProtoWriter {
public:
    explicit BigqueryProtoWriter(BigqueryTableEntry *entry, const google::cloud::Options &options);
    ~BigqueryProtoWriter();

    void InitMessageDescriptor(BigqueryTableEntry *entry);
    string CreateNestedMessage(google::protobuf::DescriptorProto *desc_proto,
                               const string &field_name,
                               const std::vector<std::pair<std::string, LogicalType>> &child_types);

    void WriteChunk(DataChunk &chunk, const vector<idx_t> &target_column_idxs);
    void WriteMessageField(google::protobuf::Message *msg,
                           const google::protobuf::Reflection *reflection,
                           const google::protobuf::FieldDescriptor *field,
                           const duckdb::LogicalType &col_type,
                           const duckdb::Value &val);
    void WriteRepeatedField(google::protobuf::Message *msg,
                            const google::protobuf::Reflection *reflection,
                            const google::protobuf::FieldDescriptor *field,
                            const duckdb::LogicalType &col_type,
                            const duckdb::Value &val);
    void WriteField(google::protobuf::Message *msg,
                    const google::protobuf::Reflection *reflection,
                    const google::protobuf::FieldDescriptor *field,
                    const duckdb::LogicalType &col_type,
                    const duckdb::Value &val);

    void Finalize();

private:
    enum class BoundWriteKind { SCALAR, REPEATED, MESSAGE };

    struct ColumnBinding {
        idx_t source_col_idx;
        const google::protobuf::FieldDescriptor *field;
        LogicalType col_type;
        BoundWriteKind write_kind;
    };

    struct PendingAppend {
        google::cloud::bigquery::storage::v1::AppendRowsRequest request;
        idx_t row_count;
        size_t request_bytes;
        int64_t offset;
    };

    static constexpr idx_t DEFAULT_APPEND_ROWS_SOFT_LIMIT = 9728 * 1024; // 9.5MB
    static constexpr idx_t APPEND_ROWS_ROW_OVERHEAD = 32;
    static constexpr idx_t MAX_INFLIGHT_REQUESTS = 4;
    static constexpr idx_t MAX_INFLIGHT_BYTES = DEFAULT_APPEND_ROWS_SOFT_LIMIT * MAX_INFLIGHT_REQUESTS;

    void InitializeColumnBindings(const DataChunk &chunk, const vector<idx_t> &target_column_idxs);
    void EnsureRequestInitialized();
    void FlushBufferedRequest();
    void EnsureGrpcStreamWithReplay();
    void DrainInflightResponse();
    void DrainInflightRequestsToWindow();
    void SendAppendRequest(PendingAppend pending);

    string table_string;

    google::protobuf::DescriptorPool pool;
    const google::protobuf::Descriptor *msg_descriptor = nullptr;
    vector<ColumnBinding> column_bindings;
    bool column_bindings_initialized = false;

    google::cloud::bigquery::storage::v1::AppendRowsRequest buffered_request;
    idx_t buffered_rows = 0;
    size_t buffered_request_bytes = 0;
    bool buffered_request_initialized = false;
    std::deque<PendingAppend> inflight_requests;
    size_t inflight_request_bytes = 0;
    int64_t next_request_offset = 0;

    unique_ptr<google::protobuf::DynamicMessageFactory> msg_factory;
    const google::protobuf::Message *msg_prototype = nullptr;
    unique_ptr<google::protobuf::Message> row_message;
    const google::protobuf::Reflection *row_reflection = nullptr;
    bool enable_inflight_request_windowing = true;

    unique_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteClient> write_client;
    google::cloud::bigquery::storage::v1::WriteStream write_stream;
    std::unique_ptr<google::cloud::AsyncStreamingReadWriteRpc<google::cloud::bigquery::storage::v1::AppendRowsRequest,
                                                              google::cloud::bigquery::storage::v1::AppendRowsResponse>>
        grpc_stream;
};

} // namespace bigquery
} // namespace duckdb
