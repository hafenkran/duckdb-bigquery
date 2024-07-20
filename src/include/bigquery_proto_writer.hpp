#pragma once

#include <google/cloud/bigquery/bigquery_write_client.h>

#include "google/protobuf/descriptor.h"
#include "storage/bigquery_table_entry.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryProtoWriter {
public:
    explicit BigqueryProtoWriter(BigqueryTableEntry *entry, const google::cloud::Options &options);
    ~BigqueryProtoWriter();

    void InitMessageDescriptor(BigqueryTableEntry *entry);

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


private:
    string table_string;

    google::protobuf::DescriptorPool pool;
    unique_ptr<google::protobuf::DynamicMessageFactory> msg_factory;
    const google::protobuf::Descriptor *msg_descriptor = nullptr;
    const google::protobuf::Message *msg_prototype = nullptr;


    unique_ptr<google::cloud::bigquery_storage_v1::BigQueryWriteClient> write_client;
    google::cloud::bigquery::storage::v1::WriteStream write_stream;
    // google::cloud::bigquery::storage::v1::AppendRowsRequest append_request;
};

} // namespace bigquery
} // namespace duckdb
