#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "duckdb.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"

#include "bigquery_proto_writer.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_table_entry.hpp"

#include <cctype>
#include <chrono>
#include <iostream>
#include <thread>


namespace duckdb {
namespace bigquery {

void ValidateDateRange(const duckdb::date_t &value) {
    // Range: 0001-01-01 to 9999-12-31
    auto constexpr kDateRangeMin = -719162;
    auto constexpr kDateRangeMax = 2932896;
    auto days = Date::EpochDays(value);
    if (days < kDateRangeMin || days > kDateRangeMax) {
        throw InternalException("Date is out of the valid BigQuery range (0001-01-01 to 9999-12-31)");
    }
}

void ValidateTimeRange(const duckdb::dtime_t &value) {
    auto constexpr kTimeRangeMin = 0;
    auto constexpr kTimeRangeMax = 86399999999999;
    if (value.micros < kTimeRangeMin || value.micros > kTimeRangeMax) {
        throw InternalException("Time is out of the valid BigQuery range (00:00:00 to 23:59:59.999999)");
    }
}

void ValidateTimestampRange(const duckdb::timestamp_t &value) {
    // Range: 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999 UTC
    auto constexpr kMinTimestampMicros = -62135596800000000;
    auto constexpr kMaxTimestampMicros = 253402300799999999;
    auto epoch_microseconds = Timestamp::GetEpochMicroSeconds(value);
    if (epoch_microseconds < kMinTimestampMicros || epoch_microseconds > kMaxTimestampMicros) {
        throw InternalException(
            "Timestamp is out of the valid BigQuery range (0001-01-01 00:00:00 to 9999-12-31 23:59:59");
    }
}

void ValidateIntervalRange(const duckdb::interval_t &value) { // TODO
    // Range: -10000-0 -3660000 -87840000:0:0 to 10000-0 3660000 87840000:0:0
    auto constexpr kIntervalRangeMin = -315576000000000;
    auto constexpr kIntervalRangeMax = 315576000000000;
    if (value.micros < kIntervalRangeMin || value.micros > kIntervalRangeMax) {
        throw InternalException("Interval is out of the valid BigQuery range (-315576000000000 to 315576000000000)");
    }
}

BigqueryProtoWriter::BigqueryProtoWriter(BigqueryTableEntry *entry, const google::cloud::Options &options) {
    auto &bq_catalog = dynamic_cast<BigqueryCatalog &>(entry->catalog);
    auto project_id = bq_catalog.GetProjectID();
    auto dataset_id = entry->schema.name;
    auto table_id = entry->name;
    table_string = BigqueryUtils::FormatTableString(project_id, dataset_id, table_id);

    // Create the message descriptor and prototype
    InitMessageDescriptor(entry);

    int max_retries = 100;
    bool created_successfully = false;
    for (int attempt = 0; attempt < max_retries; attempt++) {
        // Initialize the BigQuery write client
        write_client = make_uniq<google::cloud::bigquery_storage_v1::BigQueryWriteClient>(
            google::cloud::bigquery_storage_v1::MakeBigQueryWriteConnection(options));

        // Create the write stream
        auto stream = google::cloud::bigquery::storage::v1::WriteStream();
        stream.set_type(google::cloud::bigquery::storage::v1::WriteStream_Type::WriteStream_Type_PENDING);

        auto write_stream_status = write_client->CreateWriteStream(table_string, stream);
        if (write_stream_status) {
            write_stream = write_stream_status.value();
            created_successfully = true;
            break;
        } else {
            std::cout << "Failed to create write stream: " << write_stream_status.status() << std::endl
                      << write_stream_status.status().message() << std::endl;
            if (attempt < max_retries - 1) {
                std::cout << "Retrying..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
    }

    if (!created_successfully) {
        throw BinderException("Cannot create BigQuery write stream to " + table_string);
    }
}

BigqueryProtoWriter::~BigqueryProtoWriter() {
}

void BigqueryProtoWriter::InitMessageDescriptor(BigqueryTableEntry *entry) {
    this->pool.~DescriptorPool();
    new (&this->pool) google::protobuf::DescriptorPool();
    // msg_factory = make_uniq<google::protobuf::DynamicMessageFactory>();

    google::protobuf::FileDescriptorProto file_desc_proto;
    file_desc_proto.set_syntax("proto2");
    file_desc_proto.set_name("bigquery.proto"); // TODO table name

    google::protobuf::DescriptorProto *desc_proto = file_desc_proto.add_message_type();
    desc_proto->set_name("BigQueryMsg");

    int32_t num = 1;
    auto &column_list = entry->GetColumns();
    for (auto &column : column_list.Logical()) {
        const auto &column_type = column.GetType();

        switch (column_type.id()) {
        case LogicalTypeId::LIST:
        case LogicalTypeId::ARRAY: {
            const auto &child_type = column_type.id() == LogicalTypeId::LIST //
                                         ? ListType::GetChildType(column_type)
                                         : ArrayType::GetChildType(column_type);

            if (child_type.id() == LogicalTypeId::STRUCT) {
                auto &child_types = StructType::GetChildTypes(child_type);
                auto nested_type_name = CreateNestedMessage(desc_proto, column.GetName(), child_types);

                auto *field_desc_proto = desc_proto->add_field();
                field_desc_proto->set_name(column.GetName());
                field_desc_proto->set_number(num++);
                field_desc_proto->set_type(google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
                field_desc_proto->set_type_name(nested_type_name);
                field_desc_proto->set_label(google::protobuf::FieldDescriptorProto::LABEL_REPEATED);
            } else {
                // For other types within a LIST, handle normally
                // Set the field as a repeated message for the LIST of STRUCTS
                auto proto_type = BigqueryUtils::LogicalTypeToProtoType(child_type);
                auto *field_desc_proto = desc_proto->add_field();
                field_desc_proto->set_name(column.GetName());
                field_desc_proto->set_number(num++);
                field_desc_proto->set_type(proto_type);
                field_desc_proto->set_label(google::protobuf::FieldDescriptorProto::LABEL_REPEATED);
            }
            break;
        }
        case LogicalTypeId::STRUCT: {
            auto &child_types = StructType::GetChildTypes(column_type);
            auto nested_type_name = CreateNestedMessage(desc_proto, column.GetName(), child_types);

            auto *field_desc_proto = desc_proto->add_field();
            field_desc_proto->set_name(column.GetName());
            field_desc_proto->set_number(num++);
            field_desc_proto->set_type(google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
            field_desc_proto->set_type_name(nested_type_name);
            field_desc_proto->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

            break;
        }
        default: {
            auto proto_type = BigqueryUtils::LogicalTypeToProtoType(column.GetType());
            auto *field_desc_proto = desc_proto->add_field();
            field_desc_proto->set_name(column.GetName());
            field_desc_proto->set_number(num++);
            field_desc_proto->set_type(proto_type);
            field_desc_proto->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);

            if (column.HasDefaultValue()) {
                const auto &default_expr = column.DefaultValue();
                auto default_value = default_expr.ToString();
                if (column.GetType().id() == LogicalTypeId::VARCHAR) {
                    default_value = default_value.substr(1, default_value.size() - 2);
                }
                field_desc_proto->set_default_value(default_value);
            }
            break;
        }
        }
    }

    // std::cout << "Raw Descriptor:\n" << file_desc_proto.DebugString() << std::endl;
    auto *file_desc = pool.BuildFile(file_desc_proto);
    if (file_desc == nullptr) {
        throw BinderException("Cannot get file descriptor from file descriptor proto");
    }
    msg_descriptor = file_desc->message_type(0);
    if (msg_descriptor == nullptr) {
        throw BinderException("Cannot get message descriptor from file descriptor");
    }
}

string BigqueryProtoWriter::CreateNestedMessage(google::protobuf::DescriptorProto *parent_proto,
                                                const std::string &field_name,
                                                const std::vector<std::pair<std::string, LogicalType>> &child_types) {
    std::string nested_type_name = field_name + "Msg";
    nested_type_name[0] = std::toupper(nested_type_name[0]);

    auto *nested_desc = parent_proto->add_nested_type();
    nested_desc->set_name(nested_type_name);

    int nested_field_num = 1;
    for (const auto &child : child_types) {
        const std::string &child_name = child.first;
        const LogicalType &child_type = child.second;

        auto *child_field = nested_desc->add_field();
        child_field->set_name(child_name);
        child_field->set_number(nested_field_num++);

        if (child_type.id() == LogicalTypeId::STRUCT) {
            auto &grandchildren = StructType::GetChildTypes(child_type);
            std::string nested_child_type = CreateNestedMessage(nested_desc, child_name, grandchildren);
            child_field->set_type(google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
            child_field->set_type_name(nested_child_type);
        } else {
            child_field->set_type(BigqueryUtils::LogicalTypeToProtoType(child_type));
        }

        child_field->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
    }

    return nested_type_name; // damit der Aufrufer das Feld korrekt setzen kann
}

void BigqueryProtoWriter::WriteChunk(DataChunk &chunk, const std::map<std::string, idx_t> &column_idxs) {
    auto msg_factory = google::protobuf::DynamicMessageFactory();
    msg_prototype = msg_factory.GetPrototype(msg_descriptor);
    if (msg_prototype == nullptr) {
        throw BinderException("Cannot get message prototype from message descriptor");
    }

    // Create the append request
    google::cloud::bigquery::storage::v1::AppendRowsRequest request;
    request.set_write_stream(write_stream.name());
    msg_descriptor->CopyTo(request.mutable_proto_rows()->mutable_writer_schema()->mutable_proto_descriptor());

    vector<idx_t> column_indexes;
    if (column_idxs.empty()) {
        column_indexes.resize(chunk.ColumnCount());
        for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
            column_indexes[i] = i;
        }
    } else {
        for (auto &kv : column_idxs) {
            column_indexes.push_back(kv.second);
        }
    }

    auto *rows = request.mutable_proto_rows()->mutable_rows();
    for (idx_t i = 0; i < chunk.size(); i++) {
        google::protobuf::Message *msg = msg_prototype->New();
        const google::protobuf::Reflection *reflection = msg->GetReflection();

        for (idx_t idx = 0; idx < column_indexes.size(); idx++) {
            auto col_idx = column_indexes[idx];
            auto &col = chunk.data[idx];
            auto &col_type = col.GetType();
            auto *field = msg_descriptor->field(col_idx);
            if (col.GetValue(i).IsNull()) {
                continue;
            }
            auto val = col.GetValue(i);

            switch (col_type.id()) {
            case LogicalTypeId::ARRAY:
            case LogicalTypeId::LIST: {
                WriteRepeatedField(msg, reflection, field, col_type, val);
                break;
            }
            case LogicalTypeId::STRUCT: {
                WriteMessageField(msg, reflection, field, col_type, val);
                break;
            }
            default:
                WriteField(msg, reflection, field, col_type, val);
                break;
            }
        }

        string serialized_msg;
        if (!msg->SerializeToString(&serialized_msg)) {
            throw std::runtime_error("Failed to serialize message");
        }
        rows->add_serialized_rows(serialized_msg);
        delete msg;
    }

    int max_retries = 100;
    for (int attempt = 0; attempt < max_retries; attempt++) {
        auto handle_broken_stream = [this](char const *where) {
            auto status = grpc_stream->Finish().get();
            throw IOException("Unexpected streaming RPC error in %s: %s", where, status.message());
        };

        if (!grpc_stream) {
            grpc_stream = write_client->AsyncAppendRows();

            if (!grpc_stream->Start().get()) {
                handle_broken_stream("Start");
            }
        }

        auto write = grpc_stream->Write(request, grpc::WriteOptions()).get();
        if (!write) {
            if (attempt < max_retries - 1) {
                grpc_stream.reset();
                std::cout << "Retrying..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            } else {
                handle_broken_stream("Write");
                throw std::runtime_error("Write failed");
            }
        }

        // GET THE RESPONSE AND ERROR HANDLING
        auto response = grpc_stream->Read().get();
        if (!response) {
            if (attempt < max_retries - 1) {
                grpc_stream.reset();
                std::cout << "Retrying..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            } else {
                handle_broken_stream("Read");
                throw std::runtime_error("Read failed");
            }
        }

        if (response && response->has_error()) {
            for (const auto &error : response->row_errors()) {
                std::cerr << "Row " << error.index() << " failed: " << error.message() << "\n";
            }

            throw IOException("Failed to write chunk: %s", response->error().message());
        }

        break;
    }
}

void BigqueryProtoWriter::Finalize() {
    if (!grpc_stream) {
        return;
    }

    grpc_stream->WritesDone().get();
    auto finish = grpc_stream->Finish().get();
    if (!finish.ok()) {
        throw IOException("Unexpected streaming RPC error: %s", finish.message());
    }

    auto finalize = write_client->FinalizeWriteStream(write_stream.name());
    if (!finalize) {
        throw IOException("Unexpected error finalizing write stream: %s", finalize.status().message());
    }

    auto commit_request = google::cloud::bigquery::storage::v1::BatchCommitWriteStreamsRequest();
    commit_request.set_parent(table_string);
    commit_request.add_write_streams(write_stream.name());
    auto commit = write_client->BatchCommitWriteStreams(commit_request);
    if (!commit) {
        throw IOException("Unexpected error commiting write streams: %s", commit.status().message());
    }
}

void BigqueryProtoWriter::WriteMessageField(google::protobuf::Message *msg,
                                            const google::protobuf::Reflection *reflection,
                                            const google::protobuf::FieldDescriptor *field,
                                            const duckdb::LogicalType &col_type,
                                            duckdb::Value &val) {
    // Get the children vals/types
    auto &child_types = StructType::GetChildTypes(col_type);
    auto &child_values = StructValue::GetChildren(val);

    google::protobuf::Message *nested_msg = nullptr;
    if (field->is_repeated()) {
        nested_msg = reflection->AddMessage(msg, field);
    } else {
        nested_msg = reflection->MutableMessage(msg, field);
    }

    // Iterate through child types and values
    const google::protobuf::Reflection *nested_reflection = nested_msg->GetReflection();
    for (idx_t j = 0; j < child_types.size(); j++) {
        auto &child_type = child_types[j].second;
        auto &child_value = child_values[j];
        if (child_value.IsNull()) {
            continue;
        }

        const auto *nested_field = nested_msg->GetDescriptor()->field(j);
        if (child_type.id() == LogicalTypeId::STRUCT) {
            // Handle nested STRUCT types
            WriteMessageField(nested_msg,
                              nested_reflection,
                              nested_field,
                              child_type,
                              const_cast<duckdb::Value &>(child_value));
        } else {
            WriteField(nested_msg, nested_reflection, nested_field, child_type, child_value);
        }
    }
}

void BigqueryProtoWriter::WriteRepeatedField(google::protobuf::Message *msg,
                                             const google::protobuf::Reflection *reflection,
                                             const google::protobuf::FieldDescriptor *field,
                                             const duckdb::LogicalType &col_type,
                                             duckdb::Value &val) {
    // Get the children vals/types
    duckdb::LogicalType child_type;
    duckdb::vector<duckdb::Value> children;
    if (col_type.id() == LogicalTypeId::ARRAY) {
        child_type = ArrayType::GetChildType(col_type);
        children = ArrayValue::GetChildren(val);
    } else {
        child_type = ListType::GetChildType(col_type);
        children = ListValue::GetChildren(val);
    }
    if (children.empty()) {
        return;
    }

    switch (child_type.id()) {
    case LogicalTypeId::BIGINT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddInt64(msg, field, item.GetValueUnsafe<int64_t>());
            }
        }
        break;
    }
    case LogicalTypeId::BIT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, item.GetValueUnsafe<string>());
            }
        }
        break;
    }
    case LogicalTypeId::BLOB: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, item.GetValueUnsafe<string>());
            }
        }
        break;
    }
    case LogicalTypeId::BOOLEAN: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddBool(msg, field, item.GetValueUnsafe<bool>());
            }
        }
        break;
    }
    case LogicalTypeId::DATE: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                auto value = item.GetValueUnsafe<duckdb::date_t>();
                ValidateDateRange(item.GetValueUnsafe<duckdb::date_t>());
                reflection->AddInt32(msg, field, value.days);
            }
        }
        break;
    }
    case LogicalTypeId::DECIMAL: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                const auto &value = item.GetValueUnsafe<hugeint_t>();
                string decimal_str = BigqueryUtils::DecimalToString(value, child_type);
                reflection->AddString(msg, field, decimal_str);
            }
        }
        break;
    }
    case LogicalTypeId::DOUBLE: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddDouble(msg, field, item.GetValueUnsafe<double>());
            }
        }
        break;
    }
    case LogicalTypeId::FLOAT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddFloat(msg, field, item.GetValueUnsafe<float>());
            }
        }
        break;
    }
    case LogicalTypeId::HUGEINT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, item.GetValueUnsafe<hugeint_t>().ToString());
            }
        }
        break;
    }
    case LogicalTypeId::INTEGER: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddInt32(msg, field, item.GetValueUnsafe<int32_t>());
            }
        }
        break;
    }
    case LogicalTypeId::INTERVAL: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, Interval::ToString(item.GetValueUnsafe<interval_t>()));
            }
        }
        break;
    }
    case LogicalTypeId::SMALLINT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddInt32(msg, field, item.GetValueUnsafe<int16_t>());
            }
        }
        break;
    }
    case LogicalTypeId::TIME: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, Time::ToString(item.GetValueUnsafe<duckdb::dtime_t>()));
            }
        }
        break;
    }
    case LogicalTypeId::TIMESTAMP: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddInt64(msg,
                                     field,
                                     Timestamp::GetEpochMicroSeconds(val.GetValueUnsafe<duckdb::timestamp_t>()));
            }
        }
        break;
    }
    case LogicalTypeId::TIMESTAMP_MS: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, Timestamp::ToString(item.GetValueUnsafe<duckdb::timestamp_t>()));
            }
        }
        break;
    }
    case LogicalTypeId::TIMESTAMP_NS: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, Timestamp::ToString(item.GetValueUnsafe<duckdb::timestamp_t>()));
            }
        }
        break;
    }
    case LogicalTypeId::TIMESTAMP_SEC: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, Timestamp::ToString(item.GetValueUnsafe<duckdb::timestamp_t>()));
            }
        }
        break;
    }
    case LogicalTypeId::TIMESTAMP_TZ: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, Timestamp::ToString(item.GetValueUnsafe<duckdb::timestamp_t>()));
            }
        }
        break;
    }
    case LogicalTypeId::TINYINT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddInt32(msg, field, item.GetValueUnsafe<int8_t>());
            }
        }
        break;
    }
    case LogicalTypeId::UBIGINT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, std::to_string(item.GetValueUnsafe<uint64_t>()));
            }
        }
        break;
    }
    case LogicalTypeId::UHUGEINT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, item.GetValueUnsafe<uhugeint_t>().ToString());
            }
        }
        break;
    }
    case LogicalTypeId::UINTEGER: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddUInt32(msg, field, item.GetValueUnsafe<uint32_t>());
            }
        }
        break;
    }
    case LogicalTypeId::USMALLINT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddUInt32(msg, field, item.GetValueUnsafe<uint16_t>());
            }
        }
        break;
    }
    case LogicalTypeId::UTINYINT: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddUInt32(msg, field, item.GetValueUnsafe<uint8_t>());
            }
        }
        break;
    }
    case LogicalTypeId::UUID: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                if (!item.IsNull()) {
                    reflection->AddString(msg, field, UUID::ToString(item.GetValueUnsafe<hugeint_t>()));
                }
            }
        }
        break;
    }
    case LogicalTypeId::VARCHAR: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                reflection->AddString(msg, field, item.GetValueUnsafe<string>());
            }
        }
        break;
    }
    case LogicalTypeId::STRUCT: {
        for (auto &item : children) {
            if (!item.IsNull()) {
                WriteMessageField(msg, reflection, field, child_type, item);
            }
        }
        break;
    }
    default:
        throw BinderException("Unsupported list type: " + child_type.ToString());
    }
}

void BigqueryProtoWriter::WriteField(google::protobuf::Message *msg,
                                     const google::protobuf::Reflection *reflection,
                                     const google::protobuf::FieldDescriptor *field,
                                     const duckdb::LogicalType &col_type,
                                     const duckdb::Value &val) {
    if (val.IsNull()) {
        return;
    }
    switch (col_type.id()) {
    case LogicalTypeId::BIGINT: {
        auto value = val.GetValueUnsafe<int64_t>();
        reflection->SetInt64(msg, field, value);
        break;
    }
    case LogicalTypeId::BIT: {
        //! TODO: bool?
        auto value = val.GetValueUnsafe<string>();
        reflection->SetString(msg, field, value);
        break;
    }
    case LogicalTypeId::BLOB: {
        // auto value = val;
        reflection->SetString(msg, field, val.GetValueUnsafe<string>());
        break;
    }
    case LogicalTypeId::BOOLEAN: {
        auto value = val.GetValueUnsafe<bool>();
        reflection->SetBool(msg, field, value);
        break;
    }
    case LogicalTypeId::DATE: {
        auto value = val.GetValueUnsafe<duckdb::date_t>();
        ValidateDateRange(value);
        reflection->SetInt32(msg, field, Date::EpochDays(value));
        break;
    }
    case LogicalTypeId::DECIMAL: {
        auto value = val.GetValueUnsafe<hugeint_t>();
        std::string decimal_value = BigqueryUtils::DecimalToString(value, col_type);
        reflection->SetString(msg, field, decimal_value);
        break;
    }
    case LogicalTypeId::DOUBLE: {
        auto value = val.GetValueUnsafe<double>();
        reflection->SetDouble(msg, field, value);
        break;
    }
    case LogicalTypeId::HUGEINT: {
        //! TODO: Validate
        auto value = val.GetValueUnsafe<hugeint_t>();
        reflection->SetString(msg, field, value.ToString());
        break;
    }
    case LogicalTypeId::INTEGER: {
        auto value = val.GetValueUnsafe<int32_t>();
        reflection->SetInt32(msg, field, value);
        break;
    }
    case LogicalTypeId::INTERVAL: {
        auto value = val.GetValueUnsafe<interval_t>();
        reflection->SetString(msg, field, BigqueryUtils::IntervalToBigqueryIntervalString(value));
        break;
    }
    case LogicalTypeId::FLOAT: {
        auto value = val.GetValueUnsafe<float>();
        reflection->SetFloat(msg, field, value);
        break;
    }
    case LogicalTypeId::SMALLINT: {
        auto value = val.GetValueUnsafe<int16_t>();
        reflection->SetInt32(msg, field, value);
        break;
    }
    case LogicalTypeId::TIME: {
        auto value = val.GetValueUnsafe<duckdb::dtime_t>();
        ValidateTimeRange(value);
        reflection->SetString(msg, field, Time::ToString(value));
        break;
    }
    case LogicalTypeId::TIMESTAMP: {
        auto value = val.GetValueUnsafe<duckdb::timestamp_t>();
        ValidateTimestampRange(value);
        reflection->SetInt64(msg, field, Timestamp::GetEpochMicroSeconds(val.GetValueUnsafe<duckdb::timestamp_t>()));
        break;
    }
    case LogicalTypeId::TIMESTAMP_MS: {
        auto value = val.GetValueUnsafe<timestamp_t>();
        reflection->SetInt64(msg, field, Timestamp::GetEpochNanoSeconds(value));
        break;
    }
    case LogicalTypeId::TIMESTAMP_NS: {
        auto value = val.DefaultCastAs(LogicalType::TIMESTAMP).GetValueUnsafe<timestamp_t>();
        reflection->SetInt64(msg, field, Timestamp::GetEpochNanoSeconds(value));
        break;
    }
    case LogicalTypeId::TIMESTAMP_SEC: {
        // There doesn't seem to exist a cast type? (-> * 1000)
        auto value = val.GetValueUnsafe<timestamp_t>();
        reflection->SetInt64(msg, field, Timestamp::GetEpochNanoSeconds(value) * 1000);
        break;
    }
    case LogicalTypeId::TIMESTAMP_TZ: {
        auto value = val.GetValueUnsafe<duckdb::timestamp_t>();
        reflection->SetString(msg, field, Timestamp::ToString(value));
        break;
    }
    case LogicalTypeId::TINYINT: {
        auto value = val.GetValueUnsafe<int8_t>();
        reflection->SetInt32(msg, field, value);
        break;
    }
    case LogicalTypeId::UBIGINT: {
        auto value = val.GetValueUnsafe<uint64_t>();
        reflection->SetString(msg, field, std::to_string(value));
        break;
    }
    case LogicalTypeId::UHUGEINT: {
        auto value = val.GetValueUnsafe<uhugeint_t>();
        reflection->SetString(msg, field, value.ToString());
        break;
    }
    case LogicalTypeId::UINTEGER: {
        auto value = val.GetValueUnsafe<uint32_t>();
        reflection->SetUInt32(msg, field, value);
        break;
    }
    case LogicalTypeId::USMALLINT: {
        auto value = val.GetValueUnsafe<uint16_t>();
        reflection->SetUInt32(msg, field, value);
        break;
    }
    case LogicalTypeId::UTINYINT: {
        auto value = val.GetValueUnsafe<uint8_t>();
        reflection->SetUInt32(msg, field, value);
        break;
    }
    case LogicalTypeId::UUID: {
        auto value = val.GetValueUnsafe<hugeint_t>();
        reflection->SetString(msg, field, UUID::ToString(value));
        break;
    }
    case LogicalTypeId::VARCHAR: {
        auto value = val.GetValueUnsafe<string>();
        reflection->SetString(msg, field, value);
        break;
    }
    default:
        throw BinderException("Unsupported type: " + col_type.ToString());
    }
}


} // namespace bigquery
} // namespace duckdb
