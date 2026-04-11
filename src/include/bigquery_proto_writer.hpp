#pragma once

#include <functional>
#include <google/cloud/bigquery/bigquery_write_client.h>
#undef NO_DATA

#include "google/protobuf/descriptor.h"
#include "storage/bigquery_table_entry.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryProtoWriter {
public:
    using ConnectionRefreshCallback = std::function<std::shared_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteConnection>()>;

    explicit BigqueryProtoWriter(BigqueryTableEntry *entry,
                                 std::shared_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteConnection> connection,
                                 ConnectionRefreshCallback refresh_connection = nullptr);
    ~BigqueryProtoWriter();

    void InitMessageDescriptor(BigqueryTableEntry *entry);
    string CreateNestedMessage(google::protobuf::DescriptorProto *desc_proto,
                               const string &field_name,
                               const std::vector<std::pair<std::string, LogicalType>> &child_types);

    void WriteChunk(DataChunk &chunk, const std::map<std::string, idx_t> &column_idxs);
    void WriteMessageField(google::protobuf::Message *msg,
                           const google::protobuf::Reflection *reflection,
                           const google::protobuf::FieldDescriptor *field,
                           const duckdb::LogicalType &col_type,
                           duckdb::Value &val);
    void WriteRepeatedField(google::protobuf::Message *msg,
                            const google::protobuf::Reflection *reflection,
                            const google::protobuf::FieldDescriptor *field,
                            const duckdb::LogicalType &col_type,
                            duckdb::Value &val);
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

    static constexpr idx_t DEFAULT_APPEND_ROWS_SOFT_LIMIT = 9728 * 1024; // 9.5MB
    static constexpr idx_t APPEND_ROWS_ROW_OVERHEAD = 32;

    void InitializeColumnBindings(const DataChunk &chunk, const std::map<std::string, idx_t> &column_idxs);
    void EnsureRequestInitialized();
    void FlushBufferedRequest();
    void SendAppendRequest(const google::cloud::bigquery::storage::v1::AppendRowsRequest &request);

    string table_string;

    google::protobuf::DescriptorPool pool;
    const google::protobuf::Descriptor *msg_descriptor = nullptr;
    vector<ColumnBinding> column_bindings;
    bool column_bindings_initialized = false;

    google::cloud::bigquery::storage::v1::AppendRowsRequest buffered_request;
    idx_t buffered_rows = 0;
    size_t buffered_request_bytes = 0;
    bool buffered_request_initialized = false;

    unique_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteClient> write_client;
    google::cloud::bigquery::storage::v1::WriteStream write_stream;
    std::unique_ptr<google::cloud::AsyncStreamingReadWriteRpc<google::cloud::bigquery::storage::v1::AppendRowsRequest,
                                                              google::cloud::bigquery::storage::v1::AppendRowsResponse>>
        grpc_stream;
};

} // namespace bigquery
} // namespace duckdb
