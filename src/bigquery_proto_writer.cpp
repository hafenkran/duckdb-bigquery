#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/rpc/code.pb.h>

#include "duckdb.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"

#include "bigquery_geography.hpp"
#include "bigquery_proto_writer.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_table_entry.hpp"

#include <cctype>
#include <chrono>
#include <iostream>
#include <thread>


namespace duckdb {
namespace bigquery {

void ValidateDateRange(const duckdb::date_t &value);
void ValidateTimeRange(const duckdb::dtime_t &value);
void ValidateTimestampRange(const duckdb::timestamp_t &value);

namespace {

void SetProtoString(google::protobuf::Message *msg,
                    const google::protobuf::Reflection *reflection,
                    const google::protobuf::FieldDescriptor *field,
                    const string_t &value) {
    reflection->SetString(msg, field, string(value.GetData(), value.GetSize()));
}

string GeometryToBigqueryText(const string_t &input_geom) {
    string normalized_geom_storage;
    string_t normalized_geom;
    NormalizeGeography(input_geom, normalized_geom, normalized_geom_storage);

    auto text_vector = Vector(LogicalType::VARCHAR);
    auto text = Geometry::ToString(text_vector, normalized_geom);
    return text.GetString();
}

void ConvertGeometryVectorToText(Vector &source, idx_t count, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](const string_t &input_geom) {
        string normalized_geom_storage;
        string_t normalized_geom;
        NormalizeGeography(input_geom, normalized_geom, normalized_geom_storage);
        return Geometry::ToString(result, normalized_geom);
    });
}

bool SupportsScalarUnifiedWrite(const LogicalType &col_type) {
    switch (col_type.id()) {
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::BIT:
    case LogicalTypeId::BLOB:
    case LogicalTypeId::BOOLEAN:
    case LogicalTypeId::DATE:
    case LogicalTypeId::DECIMAL:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::INTERVAL:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::TIME:
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::UBIGINT:
    case LogicalTypeId::UHUGEINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::UUID:
    case LogicalTypeId::VARCHAR:
        return true;
    default:
        return false;
    }
}

string DecimalToBigqueryString(const UnifiedVectorFormat &format, const LogicalType &col_type, idx_t source_idx) {
    auto width = DecimalType::GetWidth(col_type);
    auto scale = DecimalType::GetScale(col_type);
    switch (format.physical_type) {
    case PhysicalType::INT16:
        return Decimal::ToString(UnifiedVectorFormat::GetData<int16_t>(format)[source_idx], width, scale);
    case PhysicalType::INT32:
        return Decimal::ToString(UnifiedVectorFormat::GetData<int32_t>(format)[source_idx], width, scale);
    case PhysicalType::INT64:
        return Decimal::ToString(UnifiedVectorFormat::GetData<int64_t>(format)[source_idx], width, scale);
    case PhysicalType::INT128:
        return Decimal::ToString(UnifiedVectorFormat::GetData<hugeint_t>(format)[source_idx], width, scale);
    default:
        throw InternalException("Unsupported decimal physical type in BigQuery protobuf writer: %s",
                                TypeIdToString(format.physical_type));
    }
}

void WriteScalarUnifiedField(google::protobuf::Message *msg,
                             const google::protobuf::Reflection *reflection,
                             const google::protobuf::FieldDescriptor *field,
                             const LogicalType &col_type,
                             const UnifiedVectorFormat &format,
                             idx_t row_idx) {
    auto source_idx = format.sel->get_index(row_idx);
    if (!format.validity.RowIsValid(source_idx)) {
        return;
    }

    switch (col_type.id()) {
    case LogicalTypeId::BIGINT:
        reflection->SetInt64(msg, field, UnifiedVectorFormat::GetData<int64_t>(format)[source_idx]);
        return;
    case LogicalTypeId::BIT:
    case LogicalTypeId::BLOB:
    case LogicalTypeId::VARCHAR:
        SetProtoString(msg, reflection, field, UnifiedVectorFormat::GetData<string_t>(format)[source_idx]);
        return;
    case LogicalTypeId::BOOLEAN:
        reflection->SetBool(msg, field, UnifiedVectorFormat::GetData<bool>(format)[source_idx]);
        return;
    case LogicalTypeId::DATE: {
        auto value = UnifiedVectorFormat::GetData<date_t>(format)[source_idx];
        ValidateDateRange(value);
        reflection->SetInt32(msg, field, Date::EpochDays(value));
        return;
    }
    case LogicalTypeId::DECIMAL:
        reflection->SetString(msg, field, DecimalToBigqueryString(format, col_type, source_idx));
        return;
    case LogicalTypeId::DOUBLE:
        reflection->SetDouble(msg, field, UnifiedVectorFormat::GetData<double>(format)[source_idx]);
        return;
    case LogicalTypeId::FLOAT:
        reflection->SetFloat(msg, field, UnifiedVectorFormat::GetData<float>(format)[source_idx]);
        return;
    case LogicalTypeId::HUGEINT:
        reflection->SetString(msg, field, UnifiedVectorFormat::GetData<hugeint_t>(format)[source_idx].ToString());
        return;
    case LogicalTypeId::INTEGER:
        reflection->SetInt32(msg, field, UnifiedVectorFormat::GetData<int32_t>(format)[source_idx]);
        return;
    case LogicalTypeId::INTERVAL:
        reflection->SetString(msg,
                              field,
                              BigqueryUtils::IntervalToBigqueryIntervalString(
                                  UnifiedVectorFormat::GetData<interval_t>(format)[source_idx]));
        return;
    case LogicalTypeId::SMALLINT:
        reflection->SetInt32(msg, field, UnifiedVectorFormat::GetData<int16_t>(format)[source_idx]);
        return;
    case LogicalTypeId::TIME: {
        auto value = UnifiedVectorFormat::GetData<dtime_t>(format)[source_idx];
        ValidateTimeRange(value);
        reflection->SetString(msg, field, Time::ToString(value));
        return;
    }
    case LogicalTypeId::TIMESTAMP: {
        auto value = UnifiedVectorFormat::GetData<timestamp_t>(format)[source_idx];
        ValidateTimestampRange(value);
        reflection->SetInt64(msg, field, Timestamp::GetEpochMicroSeconds(value));
        return;
    }
    case LogicalTypeId::TINYINT:
        reflection->SetInt32(msg, field, UnifiedVectorFormat::GetData<int8_t>(format)[source_idx]);
        return;
    case LogicalTypeId::UBIGINT:
        reflection->SetString(msg, field, std::to_string(UnifiedVectorFormat::GetData<uint64_t>(format)[source_idx]));
        return;
    case LogicalTypeId::UHUGEINT:
        reflection->SetString(msg, field, UnifiedVectorFormat::GetData<uhugeint_t>(format)[source_idx].ToString());
        return;
    case LogicalTypeId::UINTEGER:
        reflection->SetUInt32(msg, field, UnifiedVectorFormat::GetData<uint32_t>(format)[source_idx]);
        return;
    case LogicalTypeId::USMALLINT:
        reflection->SetUInt32(msg, field, UnifiedVectorFormat::GetData<uint16_t>(format)[source_idx]);
        return;
    case LogicalTypeId::UTINYINT:
        reflection->SetUInt32(msg, field, UnifiedVectorFormat::GetData<uint8_t>(format)[source_idx]);
        return;
    case LogicalTypeId::UUID:
        reflection->SetString(msg, field, UUID::ToString(UnifiedVectorFormat::GetData<hugeint_t>(format)[source_idx]));
        return;
    default:
        throw InternalException("Unsupported scalar fast path type in BigQuery protobuf writer: %s",
                                col_type.ToString());
    }
}

} // namespace

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
            auto status = write_stream_status.status();

            // Fail immediately on non-retryable errors instead of retrying
            if (status.code() == google::cloud::StatusCode::kPermissionDenied) {
                throw PermissionException(
                    "BigQuery Storage Write API permission denied for %s.\n"
                    "\n"
                    "The BigQuery Storage Write API requires additional permissions beyond standard\n"
                    "BigQuery access. Ensure your credentials have:\n"
                    "  - bigquery.tables.updateData\n"
                    "  - bigquery.readsessions.create (required by the Storage API)\n"
                    "\n"
                    "Alternatively, grant the BigQuery Data Editor role (roles/bigquery.dataEditor)\n"
                    "and the BigQuery Read Session User role (roles/bigquery.readSessionUser).\n"
                    "\n"
                    "Error details: %s",
                    table_string,
                    status.message());
            }
            if (status.code() == google::cloud::StatusCode::kUnauthenticated) {
                throw IOException("BigQuery Storage Write API authentication failed for %s.\n"
                                  "\n"
                                  "The provided credentials are invalid or have expired.\n"
                                  "  - For user credentials: gcloud auth application-default login\n"
                                  "  - For service accounts: verify your service account key is valid\n"
                                  "\n"
                                  "Error details: %s",
                                  table_string,
                                  status.message());
            }
            if (status.code() == google::cloud::StatusCode::kNotFound) {
                throw BinderException("BigQuery table not found: %s.\n"
                                      "\n"
                                      "Error details: %s",
                                      table_string,
                                      status.message());
            }
            if (status.code() == google::cloud::StatusCode::kInvalidArgument) {
                throw BinderException("BigQuery Storage Write API invalid argument for %s.\n"
                                      "\n"
                                      "Error details: %s",
                                      table_string,
                                      status.message());
            }

            std::cout << "Failed to create write stream: " << status << std::endl << status.message() << std::endl;
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
    // Ensure gRPC stream is properly closed to avoid connection leaks
    // when the writer is destroyed without calling Finalize() (e.g., on exception paths).
    if (grpc_stream) {
        try {
            grpc_stream->WritesDone().get();
            grpc_stream->Finish().get();
        } catch (...) {
            // Best-effort cleanup during destruction
        }
        grpc_stream.reset();
    }
}

void BigqueryProtoWriter::InitMessageDescriptor(BigqueryTableEntry *entry) {
    row_message.reset();
    msg_prototype = nullptr;
    row_reflection = nullptr;
    msg_factory.reset();
    this->pool.~DescriptorPool();
    new (&this->pool) google::protobuf::DescriptorPool();
    column_bindings.clear();
    column_bindings_initialized = false;

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

    msg_factory = make_uniq<google::protobuf::DynamicMessageFactory>();
    msg_prototype = msg_factory->GetPrototype(msg_descriptor);
    if (msg_prototype == nullptr) {
        throw BinderException("Cannot get message prototype from message descriptor");
    }

    row_message.reset(msg_prototype->New());
    if (!row_message) {
        throw InternalException("Cannot allocate protobuf message");
    }
    row_reflection = row_message->GetReflection();
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

void BigqueryProtoWriter::InitializeColumnBindings(const DataChunk &chunk, const vector<idx_t> &target_column_idxs) {
    if (column_bindings_initialized) {
        if (column_bindings.size() != chunk.ColumnCount()) {
            throw InternalException("Unexpected column count change in BigQuery protobuf writer");
        }
        return;
    }

    auto get_write_kind = [](const LogicalType &type) {
        if (type.id() == LogicalTypeId::ARRAY || type.id() == LogicalTypeId::LIST) {
            return BoundWriteKind::REPEATED;
        }
        if (type.id() == LogicalTypeId::STRUCT) {
            return BoundWriteKind::MESSAGE;
        }
        return BoundWriteKind::SCALAR;
    };

    column_bindings.reserve(chunk.ColumnCount());
    if (target_column_idxs.empty()) {
        for (idx_t source_col_idx = 0; source_col_idx < chunk.ColumnCount(); source_col_idx++) {
            auto *field = msg_descriptor->field(source_col_idx);
            if (field == nullptr) {
                throw BinderException("Cannot get field descriptor for BigQuery message");
            }
            const auto &col_type = chunk.data[source_col_idx].GetType();
            column_bindings.push_back({source_col_idx, field, col_type, get_write_kind(col_type)});
        }
    } else {
        if (target_column_idxs.size() != chunk.ColumnCount()) {
            throw InternalException("Unexpected column binding count in BigQuery protobuf writer");
        }

        for (idx_t source_col_idx = 0; source_col_idx < target_column_idxs.size(); source_col_idx++) {
            auto target_col_idx = target_column_idxs[source_col_idx];
            auto *field = msg_descriptor->field(target_col_idx);
            if (field == nullptr) {
                throw BinderException("Cannot get field descriptor for BigQuery message");
            }
            const auto &col_type = chunk.data[source_col_idx].GetType();
            column_bindings.push_back({source_col_idx, field, col_type, get_write_kind(col_type)});
        }
    }
    column_bindings_initialized = true;
}

void BigqueryProtoWriter::EnsureRequestInitialized() {
    if (buffered_request_initialized) {
        return;
    }

    buffered_request = google::cloud::bigquery::storage::v1::AppendRowsRequest();
    buffered_request.set_write_stream(write_stream.name());
    msg_descriptor->CopyTo(buffered_request.mutable_proto_rows()->mutable_writer_schema()->mutable_proto_descriptor());

    buffered_rows = 0;
    buffered_request_bytes = buffered_request.ByteSizeLong();
    buffered_request_initialized = true;
}

void BigqueryProtoWriter::FlushBufferedRequest() {
    if (!buffered_request_initialized || buffered_rows == 0) {
        return;
    }

    PendingAppend pending;
    pending.request = std::move(buffered_request);
    pending.row_count = buffered_rows;
    pending.request_bytes = buffered_request_bytes;
    pending.offset = next_request_offset;
    pending.request.mutable_offset()->set_value(pending.offset);
    next_request_offset += UnsafeNumericCast<int64_t>(pending.row_count);

    SendAppendRequest(std::move(pending));
    buffered_request = google::cloud::bigquery::storage::v1::AppendRowsRequest();
    buffered_rows = 0;
    buffered_request_bytes = 0;
    buffered_request_initialized = false;
}

void BigqueryProtoWriter::WriteChunk(DataChunk &chunk, const vector<idx_t> &target_column_idxs) {
    InitializeColumnBindings(chunk, target_column_idxs);
    auto *msg = row_message.get();
    const google::protobuf::Reflection *reflection = row_reflection;
    vector<UnifiedVectorFormat> scalar_formats(column_bindings.size());
    vector<bool> has_scalar_fast_path(column_bindings.size(), false);
    vector<unique_ptr<Vector>> geometry_text_vectors(column_bindings.size());
    vector<UnifiedVectorFormat> geometry_text_formats(column_bindings.size());
    vector<bool> has_geometry_text(column_bindings.size(), false);

    for (idx_t binding_idx = 0; binding_idx < column_bindings.size(); binding_idx++) {
        const auto &binding = column_bindings[binding_idx];
        if (binding.write_kind != BoundWriteKind::SCALAR || !BigqueryUtils::IsGeometryType(binding.col_type)) {
            if (binding.write_kind == BoundWriteKind::SCALAR && SupportsScalarUnifiedWrite(binding.col_type)) {
                chunk.data[binding.source_col_idx].ToUnifiedFormat(chunk.size(), scalar_formats[binding_idx]);
                has_scalar_fast_path[binding_idx] = true;
            }
            continue;
        }

        geometry_text_vectors[binding_idx] = make_uniq<Vector>(LogicalType::VARCHAR);
        ConvertGeometryVectorToText(chunk.data[binding.source_col_idx],
                                    chunk.size(),
                                    *geometry_text_vectors[binding_idx]);
        geometry_text_vectors[binding_idx]->ToUnifiedFormat(chunk.size(), geometry_text_formats[binding_idx]);
        has_geometry_text[binding_idx] = true;
    }

    for (idx_t i = 0; i < chunk.size(); i++) {
        msg->Clear();
        for (idx_t binding_idx = 0; binding_idx < column_bindings.size(); binding_idx++) {
            const auto &binding = column_bindings[binding_idx];
            if (has_geometry_text[binding_idx]) {
                const auto &geometry_text_format = geometry_text_formats[binding_idx];
                auto source_idx = geometry_text_format.sel->get_index(i);
                if (!geometry_text_format.validity.RowIsValid(source_idx)) {
                    continue;
                }

                auto text_value = UnifiedVectorFormat::GetData<string_t>(geometry_text_format)[source_idx];
                SetProtoString(msg, reflection, binding.field, text_value);
                continue;
            }

            if (has_scalar_fast_path[binding_idx]) {
                WriteScalarUnifiedField(msg,
                                        reflection,
                                        binding.field,
                                        binding.col_type,
                                        scalar_formats[binding_idx],
                                        i);
                continue;
            }

            auto &col = chunk.data[binding.source_col_idx];
            auto val = col.GetValue(i);
            if (val.IsNull()) {
                continue;
            }

            switch (binding.write_kind) {
            case BoundWriteKind::REPEATED:
                WriteRepeatedField(msg, reflection, binding.field, binding.col_type, val);
                break;
            case BoundWriteKind::MESSAGE:
                WriteMessageField(msg, reflection, binding.field, binding.col_type, val);
                break;
            case BoundWriteKind::SCALAR:
                WriteField(msg, reflection, binding.field, binding.col_type, val);
                break;
            }
        }

        auto serialized_size = msg->ByteSizeLong();
        auto estimated_size_increase = serialized_size + APPEND_ROWS_ROW_OVERHEAD;

        EnsureRequestInitialized();
        if (buffered_rows == 0 && buffered_request_bytes + estimated_size_increase > DEFAULT_APPEND_ROWS_SOFT_LIMIT) {
            throw IOException("Single row exceeds BigQuery AppendRows request size limit");
        }

        if (buffered_rows > 0 && buffered_request_bytes + estimated_size_increase > DEFAULT_APPEND_ROWS_SOFT_LIMIT) {
            FlushBufferedRequest();
            EnsureRequestInitialized();
        }

        auto *serialized_row = buffered_request.mutable_proto_rows()->mutable_rows()->add_serialized_rows();
        serialized_row->clear();
        serialized_row->reserve(serialized_size);
        if (!msg->SerializeToString(serialized_row)) {
            throw std::runtime_error("Failed to serialize message");
        }
        buffered_rows++;
        buffered_request_bytes += estimated_size_increase;

        if (buffered_request_bytes >= DEFAULT_APPEND_ROWS_SOFT_LIMIT) {
            FlushBufferedRequest();
        }
    }
}

void BigqueryProtoWriter::EnsureGrpcStreamWithReplay() {
    if (grpc_stream) {
        return;
    }

    int max_retries = 100;
    for (int attempt = 0; attempt < max_retries; attempt++) {
        grpc_stream = write_client->AsyncAppendRows();
        if (!grpc_stream->Start().get()) {
            grpc_stream.reset();
            if (attempt < max_retries - 1) {
                std::cout << "Retrying..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
            throw IOException("Unexpected streaming RPC error in Start: failed to start AppendRows stream");
        }

        bool replay_ok = true;
        for (const auto &pending : inflight_requests) {
            if (!grpc_stream->Write(pending.request, grpc::WriteOptions()).get()) {
                replay_ok = false;
                break;
            }
        }

        if (replay_ok) {
            return;
        }

        grpc_stream.reset();
        if (attempt < max_retries - 1) {
            std::cout << "Retrying..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        throw IOException("Unexpected streaming RPC error in Replay: failed to restore inflight AppendRows requests");
    }
}

void BigqueryProtoWriter::DrainInflightResponse() {
    if (inflight_requests.empty()) {
        return;
    }

    std::optional<google::cloud::bigquery::storage::v1::AppendRowsResponse> response;
    int max_retries = 100;
    for (int attempt = 0; attempt < max_retries; attempt++) {
        EnsureGrpcStreamWithReplay();

        response = grpc_stream->Read().get();
        if (response) {
            break;
        }

        grpc_stream.reset();
        if (attempt < max_retries - 1) {
            std::cout << "Retrying..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        throw std::runtime_error("Read failed");
    }

    const auto &pending = inflight_requests.front();
    if (response->has_error()) {
        if (response->error().code() != google::rpc::ALREADY_EXISTS) {
            for (const auto &error : response->row_errors()) {
                std::cerr << "Row " << error.index() << " failed: " << error.message() << "\n";
            }

            throw IOException("Failed to write chunk: %s", response->error().message());
        }
    }

    if (response->has_append_result() && response->append_result().has_offset() &&
        response->append_result().offset().value() != pending.offset) {
        throw IOException("Unexpected AppendRows response offset: expected %lld, got %lld",
                          static_cast<long long>(pending.offset),
                          static_cast<long long>(response->append_result().offset().value()));
    }

    inflight_request_bytes -= pending.request_bytes;
    inflight_requests.pop_front();
}

void BigqueryProtoWriter::DrainInflightRequestsToWindow() {
    while (inflight_requests.size() > MAX_INFLIGHT_REQUESTS || inflight_request_bytes > MAX_INFLIGHT_BYTES) {
        DrainInflightResponse();
    }
}

void BigqueryProtoWriter::SendAppendRequest(PendingAppend pending) {
    if (!pending.request.has_proto_rows() || pending.request.proto_rows().rows().serialized_rows_size() == 0) {
        return;
    }

    inflight_request_bytes += pending.request_bytes;
    inflight_requests.push_back(std::move(pending));

    if (!grpc_stream) {
        EnsureGrpcStreamWithReplay();
    } else {
        auto write = grpc_stream->Write(inflight_requests.back().request, grpc::WriteOptions()).get();
        if (!write) {
            grpc_stream.reset();
            EnsureGrpcStreamWithReplay();
        }
    }

    DrainInflightRequestsToWindow();
}

void BigqueryProtoWriter::Finalize() {
    FlushBufferedRequest();
    while (!inflight_requests.empty()) {
        DrainInflightResponse();
    }

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

    // Mark stream as consumed so destructor doesn't try to finalize again
    grpc_stream.reset();
}

void BigqueryProtoWriter::WriteMessageField(google::protobuf::Message *msg,
                                            const google::protobuf::Reflection *reflection,
                                            const google::protobuf::FieldDescriptor *field,
                                            const duckdb::LogicalType &col_type,
                                            const duckdb::Value &val) {
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
            WriteMessageField(nested_msg, nested_reflection, nested_field, child_type, child_value);
        } else {
            WriteField(nested_msg, nested_reflection, nested_field, child_type, child_value);
        }
    }
}

void BigqueryProtoWriter::WriteRepeatedField(google::protobuf::Message *msg,
                                             const google::protobuf::Reflection *reflection,
                                             const google::protobuf::FieldDescriptor *field,
                                             const duckdb::LogicalType &col_type,
                                             const duckdb::Value &val) {
    // Get the children vals/types
    duckdb::LogicalType child_type;
    const duckdb::vector<duckdb::Value> *children_ptr = nullptr;
    if (col_type.id() == LogicalTypeId::ARRAY) {
        child_type = ArrayType::GetChildType(col_type);
        children_ptr = &ArrayValue::GetChildren(val);
    } else {
        child_type = ListType::GetChildType(col_type);
        children_ptr = &ListValue::GetChildren(val);
    }
    const auto &children = *children_ptr;
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
                auto value = item.GetValueUnsafe<duckdb::timestamp_t>();
                ValidateTimestampRange(value);
                reflection->AddInt64(msg, field, Timestamp::GetEpochMicroSeconds(value));
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
    case LogicalTypeId::GEOMETRY: {
        for (const auto &item : children) {
            if (!item.IsNull()) {
                auto geometry_value = string_t(StringValue::Get(item));
                reflection->AddString(msg, field, GeometryToBigqueryText(geometry_value));
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
    case LogicalTypeId::GEOMETRY: {
        auto geometry_value = string_t(StringValue::Get(val));
        reflection->SetString(msg, field, GeometryToBigqueryText(geometry_value));
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
