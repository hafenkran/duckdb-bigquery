#include "duckdb.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/parser/column_list.hpp"

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/visit_type_inline.h>
#include <chrono>
#include <ctime>
#include <iostream>
#include <stdexcept>

#include "bigquery_arrow_reader.hpp"
#include "bigquery_utils.hpp"


namespace duckdb {
namespace bigquery {

BigqueryArrowReader::BigqueryArrowReader(const BigqueryTableRef table_ref,
                                         const string billing_project_id,
                                         idx_t num_streams,
                                         const google::cloud::Options &options,
                                         const vector<string> &selected_columns,
                                         const string &filter_condition)
    : table_ref(table_ref), billing_project_id(billing_project_id), num_streams(num_streams),
      options(options), localhost_test_env(false) {

    if (options.has<google::cloud::EndpointOption>()) {
        localhost_test_env = true; // TODO
    }

    const string parent = BigqueryUtils::FormatParentString(billing_project_id);
    const string table_string = table_ref.TableString();

    // Initialize the Client
    auto connection = google::cloud::bigquery_storage_v1::MakeBigQueryReadConnection(options);
    read_client = make_uniq<google::cloud::bigquery_storage_v1::BigQueryReadClient>(connection);

    // Create the ReadSession
    auto session = google::cloud::bigquery::storage::v1::ReadSession();
    session.set_table(table_string);
    session.set_data_format(google::cloud::bigquery::storage::v1::DataFormat::ARROW);

    auto *read_options = session.mutable_read_options();
    if (!selected_columns.empty()) {
        for (const auto &column : selected_columns) {
            read_options->add_selected_fields(column);
        }
    }
    if (!filter_condition.empty()) {
        read_options->set_row_restriction(filter_condition);
    }

    auto new_session = read_client->CreateReadSession(parent, session, num_streams);
    if (!new_session) {
        throw BinderException("Error while creating read session: " +
                              new_session.status().message());
    }

    read_session = make_uniq<google::cloud::bigquery::storage::v1::ReadSession>(
        std::move(new_session.value()));
}

shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> BigqueryArrowReader::NextStream() {
    if (!read_session) {
        throw BinderException("Read session is not initialized.");
    }
    auto next_stream_idx = next_stream++;
    if (static_cast<int>(next_stream_idx) >= read_session->streams_size()) {
        return nullptr;
    }
    auto stream = make_shared_ptr<google::cloud::bigquery::storage::v1::ReadStream>( //
        read_session->streams(next_stream_idx));
    return stream;
}

std::shared_ptr<arrow::Schema> BigqueryArrowReader::GetSchema() {
    if (!arrow_schema) {
        if (!read_session) {
            throw BinderException("Read session is not initialized.");
        }
        arrow_schema = ReadSchema(read_session->arrow_schema());
    }
    return arrow_schema;
}

void BigqueryArrowReader::MapTableInfo(ColumnList &res_columns,
                                       vector<unique_ptr<Constraint>> &res_constraints) {
    // Get the schema of the table
    auto schema = read_session->arrow_schema();
    auto table_schema = GetSchema();

    // Create the columns
    for (const auto &field : table_schema->fields()) {
        auto field_name = field->name();
        auto arrow_type = field->type();
        auto is_nullable = field->nullable();

        auto duckdb_type = BigqueryUtils::ArrowTypeToLogicalType(arrow_type);
        ColumnDefinition column(std::move(field_name), std::move(duckdb_type));
        res_columns.AddColumn(std::move(column));

        if (!is_nullable) {
            // TODO
            // auto constraint_ptr = make_uniq<NotNullConstraint>(field_name);
            // auto base_class_ptr = make_uniq<Constraint>(std::move(constraint_ptr));
            // res_constraints.push_back(std::move(base_class_ptr));
        }
    }
}

google::cloud::v2_33::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse>
BigqueryArrowReader::ReadRows(const string &stream_name, int row_offset) {
    return read_client->ReadRows(stream_name, row_offset);
}

std::shared_ptr<arrow::Schema> BigqueryArrowReader::ReadSchema(
    const google::cloud::bigquery::storage::v1::ArrowSchema &schema) {
    auto buffer_ptr = arrow::Buffer::FromString(schema.serialized_schema());
    arrow::io::BufferReader buffer_reader(buffer_ptr);
    auto arrow_schema_res = arrow::ipc::ReadSchema(&buffer_reader, nullptr);
    if (!arrow_schema_res.ok()) {
        throw BinderException("Error reading Arrow schema: " + arrow_schema_res.status().message());
    }
    return arrow_schema_res.ValueOrDie();
}

std::shared_ptr<arrow::RecordBatch> BigqueryArrowReader::ReadBatch(
    const google::cloud::bigquery::storage::v1::ArrowRecordBatch &batch) {

    std::shared_ptr<arrow::RecordBatch> arrow_batch;
    auto schema = GetSchema();

    if (localhost_test_env) {
        // Step 1: Convert serialized Arrow data (batch.serialized_record_batch()) into an Arrow Buffer
        auto arrow_buffer = arrow::Buffer::FromString(batch.serialized_record_batch());

        // Step 2: Create a RecordBatch from the Arrow Buffer
        // Note: This example assumes the schema is already known and provided.
        arrow::io::BufferReader buffer_reader(arrow_buffer);
        auto arrow_reader_result = arrow::ipc::RecordBatchStreamReader::Open(&buffer_reader);
        if (!arrow_reader_result.ok()) {
            throw BinderException("Failed to create RecordBatchStreamReader: " +
                                  arrow_reader_result.status().ToString());
        }
        std::shared_ptr<arrow::ipc::RecordBatchStreamReader> arrow_reader =
            arrow_reader_result.ValueOrDie();

        // Step 3: Read the RecordBatch
        auto arrow_read_result = arrow_reader->ReadNext(&arrow_batch);
        if (!arrow_read_result.ok() || arrow_batch == nullptr) {
            // Handle error: Failed to read Arrow RecordBatch
            throw BinderException("Failed to read Arrow RecordBatch");
        }
    } else {
        // Read the Arrow Record Batch from the serialized data
        auto arrow_buffer = arrow::Buffer::FromString(batch.serialized_record_batch());
        arrow::io::BufferReader buffer_reader(arrow_buffer);

        auto options = arrow::ipc::IpcReadOptions::Defaults();
        arrow::Result<std::shared_ptr<arrow::RecordBatch>> arrow_record_batch_res =
            arrow::ipc::ReadRecordBatch(schema, nullptr, options, &buffer_reader);
        if (!arrow_record_batch_res.ok()) {
            throw BinderException("Failed to read Arrow RecordBatch: " +
                                  arrow_record_batch_res.status().message());
        }

        arrow_batch = arrow_record_batch_res.ValueOrDie();
    }
    return arrow_batch;
}

void BigqueryArrowReader::ReadColumn(const std::shared_ptr<arrow::Array> &column, Vector &out_vec) {
    switch (column->type_id()) {
    case arrow::Type::STRUCT: {
        ConvertStructArrayToVector(std::static_pointer_cast<arrow::StructArray>(column), out_vec);
        break;
    }
    case arrow::Type::LIST: {
        ConvertListArrayToVector(std::static_pointer_cast<arrow::ListArray>(column), out_vec);
        break;
    }
    default: {
        ConvertFlatArrayToVector(column, out_vec);
        break;
    }
    }
}

void BigqueryArrowReader::ConvertFlatArrayToVector(const std::shared_ptr<arrow::Array> &array,
                                                   Vector &out_vec) {

    const auto type_id = array->type_id();
    const auto row_count = array->length();

    switch (type_id) {
    case arrow::Type::BOOL: {
        ConvertPrimitiveArray<arrow::BooleanArray>(array, out_vec, [](bool val) {
            return Value(val);
        });
        break;
    }
    case arrow::Type::BINARY: {
        auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
        for (int64_t row = 0; row < row_count; ++row) {
            if (!binary_array->IsNull(row)) {
                int32_t length;
                const uint8_t *data = binary_array->GetValue(row, &length);
                out_vec.SetValue(row, Value::BLOB(data, length));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::DATE32: {
        auto date32_array = std::static_pointer_cast<arrow::Date32Array>(array);
        for (int64_t row = 0; row < row_count; ++row) {
            if (!date32_array->IsNull(row)) {
                int32_t days_since_epoch = date32_array->Value(row);
                out_vec.SetValue(row, Value::DATE(Date::EpochDaysToDate(days_since_epoch)));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::TIME64: {
        auto time_array = std::static_pointer_cast<arrow::Time64Array>(array);
        auto time_unit = std::static_pointer_cast<arrow::Time64Type>(array->type())->unit();
        for (int64_t row = 0; row < row_count; ++row) {
            if (time_array->IsNull(row)) {
                out_vec.SetValue(row, Value());
                continue;
            }
            int64_t time_value = time_array->Value(row);
            switch (time_unit) {
            case arrow::TimeUnit::MICRO:
                out_vec.SetValue(row, Value::TIME(Time::FromTimeMs(time_value / 1000)));
                break;
            case arrow::TimeUnit::NANO:
                out_vec.SetValue(row, Value::TIME(Time::FromTimeMs(time_value / 1000000)));
                break;
            default:
                throw InternalException("Unsupported time unit in TIME64 array.");
            }
        }
        break;
    }
    case arrow::Type::TIMESTAMP: {
        auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(array);
        for (int64_t row = 0; row < row_count; ++row) {
            if (!timestamp_array->IsNull(row)) {
                int64_t micros = timestamp_array->Value(row);
                out_vec.SetValue(row, Value::TIMESTAMP(Timestamp::FromEpochMicroSeconds(micros)));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::INTERVAL_MONTH_DAY_NANO: {
        auto interval_array = std::static_pointer_cast<arrow::MonthDayNanoIntervalArray>(array);
        for (int64_t row = 0; row < row_count; ++row) {
            if (!interval_array->IsNull(row)) {
                auto interval = interval_array->Value(row);
                out_vec.SetValue(
                    row,
                    Value::INTERVAL(interval.months, interval.days, interval.nanoseconds / 1000));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::INT64: {
        ConvertPrimitiveArray<arrow::Int64Array>(array, out_vec, [](int64_t val) {
            return Value::BIGINT(val);
        });
        break;
    }
    case arrow::Type::FLOAT: {
        ConvertPrimitiveArray<arrow::FloatArray>(array, out_vec, [](float val) {
            return Value(val);
        });
        break;
    }
    case arrow::Type::DOUBLE: {
        ConvertPrimitiveArray<arrow::DoubleArray>(array, out_vec, [](double val) {
            return Value(val);
        });
        break;
    }
    case arrow::Type::STRING: {
        auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
        for (int64_t row = 0; row < row_count; ++row) {
            if (!string_array->IsNull(row)) {
                out_vec.SetValue(row, Value(string_array->GetString(row)));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::DECIMAL128: {
        auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(array);
        auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(array->type());
        auto scale = decimal_type->scale();
        auto precision = decimal_type->precision();

        for (int64_t row = 0; row < row_count; ++row) {
            if (!decimal_array->IsNull(row)) {
                arrow::Decimal128 decimal_value(decimal_array->Value(row));
                hugeint_t hugeint_value;
                hugeint_value.upper = static_cast<int64_t>(decimal_value.high_bits());
                hugeint_value.lower = static_cast<uint64_t>(decimal_value.low_bits());
                out_vec.SetValue(row, Value::DECIMAL(hugeint_value, precision, scale));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::DECIMAL256: {
        auto decimal_array = std::static_pointer_cast<arrow::Decimal256Array>(array);
        auto decimal_type = std::static_pointer_cast<arrow::Decimal256Type>(array->type());
        auto scale = decimal_type->scale();
        auto precision = decimal_type->precision();

        if (out_vec.GetType() == LogicalType::VARCHAR) {
            for (int64_t row = 0; row < row_count; ++row) {
                if (!decimal_array->IsNull(row)) {
                    out_vec.SetValue(row, Value(decimal_array->FormatValue(row)));
                } else {
                    out_vec.SetValue(row, Value());
                }
            }
            break;
        }

        if (precision > 38) {
            throw BinderException(
                "BIGDECIMAL precision of " + std::to_string(precision) +
                " exceeds the maximum supported precision of 38 in DuckDB. Consider enabling "
                "'bq_bignumeric_as_varchar' to read them as VARCHAR instead.");
        }

        for (int64_t row = 0; row < row_count; ++row) {
            if (decimal_array->IsNull(row)) {
                out_vec.SetValue(row, Value());
                continue;
            }
            const uint8_t *value_ptr = decimal_array->Value(row);

            uint64_t parts[4] = {0};
            for (int i = 0; i < 4; ++i) {
                parts[i] = *reinterpret_cast<const uint64_t *>(value_ptr + i * sizeof(uint64_t));
            }

            if (parts[2] != 0 || parts[3] != 0) {
                throw BinderException(
                    "BIGDECIMAL value exceeds the range of 128-bit Decimal supported by DuckDB.");
            }

            hugeint_t hugeint_value;
            hugeint_value.lower = parts[0];
            hugeint_value.upper = static_cast<int64_t>(parts[1]);
            out_vec.SetValue(row, Value::DECIMAL(hugeint_value, precision, scale));
        }
        break;
    }
    default:
        throw InternalException("Unsupported Arrow type: " + array->type()->name());
    }
}

void BigqueryArrowReader::ConvertListArrayToVector(
    const std::shared_ptr<arrow::ListArray> &list_array,
    Vector &out_vec) {
    for (int64_t row = 0; row < list_array->length(); ++row) {
        auto list_value = ConvertListElementToValue(list_array, row, out_vec.GetType());
        out_vec.SetValue(row, list_value);
    }
}

Value BigqueryArrowReader::ConvertListElementToValue(
    const std::shared_ptr<arrow::ListArray> &list_array,
    int64_t row,
    const LogicalType &target_type) {
    if (list_array->IsNull(row)) {
        return Value();
    }

    auto values = list_array->values();
    auto value_type = values->type_id();
    auto child_type = BigqueryUtils::ArrowTypeToLogicalType(values->type());

    int32_t start_offset = list_array->value_offset(row);
    int32_t end_offset = list_array->value_offset(row + 1);

    vector<Value> list_values;
    list_values.reserve(end_offset - start_offset);

    // clang-format off
    switch (value_type) {
	case arrow::Type::BOOL: {
        ReadPrimitiveListElements<arrow::BooleanArray>(values, start_offset, end_offset, list_values,
            [](bool val) { return Value(val); });
        break;
	}
    case arrow::Type::BINARY: {
        auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(values);
        for (int32_t i = start_offset; i < end_offset; ++i) {
            if (binary_array->IsNull(i)) {
                list_values.push_back(Value());
            } else {
                int32_t length;
                const uint8_t *data = binary_array->GetValue(i, &length);
                list_values.push_back(Value::BLOB(data, length));
            }
        }
		break;
    }
    case arrow::Type::DATE32: {
        ReadPrimitiveListElements<arrow::Date32Array>(values, start_offset, end_offset, list_values,
            [](int32_t days) { return Value::DATE(Date::EpochDaysToDate(days)); });
        break;
    }
    case arrow::Type::TIMESTAMP: {
        auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(values);
        for (int32_t i = start_offset; i < end_offset; ++i) {
            if (timestamp_array->IsNull(i)) {
                list_values.push_back(Value());
            } else {
                list_values.push_back(Value::TIMESTAMP(Timestamp::FromEpochMicroSeconds(timestamp_array->Value(i))));
            }
        }
        break;
    }
    case arrow::Type::INT32: {
        ReadPrimitiveListElements<arrow::Int32Array>(values, start_offset, end_offset, list_values,
            [](int32_t val) { return Value(val); });
        break;
    }
    case arrow::Type::INT64: {
        ReadPrimitiveListElements<arrow::Int64Array>(values, start_offset, end_offset, list_values,
            [](int64_t val) { return Value(val); });
        break;
    }
    case arrow::Type::FLOAT: {
        ReadPrimitiveListElements<arrow::FloatArray>(values, start_offset, end_offset, list_values,
            [](float val) { return Value(val); });
        break;
    }
    case arrow::Type::DOUBLE: {
        ReadPrimitiveListElements<arrow::DoubleArray>(values, start_offset, end_offset, list_values,
            [](double val) { return Value(val); });
        break;
    }
    case arrow::Type::STRING: {
		ReadPrimitiveListElements<arrow::StringArray>(values, start_offset, end_offset, list_values,
			[](std::string_view val) { return Value(string(val)); });
        break;
    }
    case arrow::Type::DECIMAL128: {
        auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(values);
        auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(values->type());
        int32_t scale = decimal_type->scale();
        int32_t precision = decimal_type->precision();

        for (int32_t i = start_offset; i < end_offset; ++i) {
            if (decimal_array->IsNull(i)) {
                list_values.push_back(Value());
            } else {
                arrow::Decimal128 decimal_value(decimal_array->Value(i));
                hugeint_t hugeint_value;
                hugeint_value.upper = static_cast<int64_t>(decimal_value.high_bits());
                hugeint_value.lower = static_cast<uint64_t>(decimal_value.low_bits());
                list_values.push_back(Value::DECIMAL(hugeint_value, precision, scale));
            }
        }
        break;
    }
    case arrow::Type::DECIMAL256: {
        auto decimal_array = std::static_pointer_cast<arrow::Decimal256Array>(values);
        auto decimal_type = std::static_pointer_cast<arrow::Decimal256Type>(values->type());
        auto scale = decimal_type->scale();
        auto precision = decimal_type->precision();

        // bq_bignumeric_as_varchar setting
        if (target_type == LogicalType::VARCHAR) {
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!decimal_array->IsNull(i)) {
                    auto value_str = decimal_array->FormatValue(i);
                    list_values.push_back(Value(value_str));
                    continue;
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }

        if (precision > 38) {
            throw BinderException("BIGDECIMAL precision of " + std::to_string(precision) +
                                  " exceeds the maximum supported precision of 38 in DuckDB. Consider enabling "
                                  "'bq_bignumeric_as_varchar' to read them as VARCHAR instead.");
        }

        for (int32_t i = start_offset; i < end_offset; ++i) {
            if (!decimal_array->IsNull(i)) {
                const uint8_t *value_ptr = decimal_array->Value(i);

                uint64_t parts[4] = {0};
                for (int i = 0; i < 4; i++) {
                    parts[i] = *reinterpret_cast<const uint64_t *>(value_ptr + i * sizeof(uint64_t));
                }

                hugeint_t hugeint_value;
                hugeint_value.lower = parts[0];                       // lower 64 Bits
                hugeint_value.upper = static_cast<int64_t>(parts[1]); // next 64 Bits

                if (parts[2] != 0 || parts[3] != 0) {
                    throw BinderException("BIGDECIMAL value exceeds the range of 128-bit Decimal supported by "
                                          "DuckDB. Consider enabling "
                                          "'bq_bignumeric_as_varchar' to read them as VARCHAR instead.");
                }

                list_values.push_back(Value::DECIMAL(hugeint_value, precision, scale));
            } else {
                list_values.push_back(Value());
            }
        }
        break;
    }
	case arrow::Type::LIST: {
        auto list_array_nested = std::static_pointer_cast<arrow::ListArray>(values);
        for (int32_t i = start_offset; i < end_offset; ++i) {
            if (list_array_nested->IsNull(i)) {
                list_values.push_back(Value());
            } else {
                list_values.push_back(ConvertListElementToValue(list_array_nested, i, child_type));
            }
        }
        break;
    }
    case arrow::Type::STRUCT: {
        auto struct_array = std::static_pointer_cast<arrow::StructArray>(values);
        for (int32_t i = start_offset; i < end_offset; ++i) {
            if (struct_array->IsNull(i)) {
                list_values.push_back(Value());
            } else {
                list_values.push_back(ConvertStructFieldToValue(struct_array, i, child_type));
            }
        }
        break;
    }
    default:
        throw InternalException("Unsupported Arrow type in list: " + values->type()->name());
    }
    // clang-format on

    return Value::LIST(child_type, std::move(list_values));
}


void BigqueryArrowReader::ConvertStructArrayToVector(
    const std::shared_ptr<arrow::StructArray> &struct_array,
    Vector &out_vec) {

    auto struct_fields = struct_array->type()->fields();

    // Get target LogicalTypes for every field in advance
    vector<LogicalType> target_types;
    target_types.reserve(struct_fields.size());
    for (const auto &field : struct_fields) {
        target_types.push_back(BigqueryUtils::ArrowTypeToLogicalType(field->type()));
    }

    for (int64_t row = 0; row < struct_array->length(); ++row) {
        if (struct_array->IsNull(row)) {
            out_vec.SetValue(row, Value());
            continue;
        }
        auto row_value = ConvertStructRowToValue(struct_array, row, target_types);
        out_vec.SetValue(row, std::move(row_value));
    }
}

Value BigqueryArrowReader::ConvertStructRowToValue(
    const std::shared_ptr<arrow::StructArray> &struct_array,
    int64_t row,
    const vector<LogicalType> &target_types) {

    child_list_t<Value> struct_values;

    auto field_arrays = struct_array->fields();
    auto struct_fields = struct_array->type()->fields();

    D_ASSERT(field_arrays.size() == struct_fields.size());
    D_ASSERT(field_arrays.size() == target_types.size());

    for (idx_t i = 0; i < field_arrays.size(); ++i) {
        const auto &field_array = field_arrays[i];
        const auto &field_name = struct_fields[i]->name();
        const auto &logical_type = target_types[i];

        if (field_array->IsNull(row)) {
            struct_values.push_back(make_pair(field_name, Value()));
            continue;
        }

        struct_values.push_back(
            make_pair(field_name, ConvertStructFieldToValue(field_array, row, logical_type)));
    }

    return Value::STRUCT(std::move(struct_values));
}

Value BigqueryArrowReader::ConvertStructFieldToValue(
    const std::shared_ptr<arrow::Array> &field_array,
    int64_t row,
    const LogicalType &target_type) {
    if (field_array->IsNull(row)) {
        return Value();
    }

    auto field_type = field_array->type();

    switch (field_type->id()) {
    case arrow::Type::BOOL: {
        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(field_array);
        return Value(bool_array->Value(row));
    }
    case arrow::Type::BINARY: {
        auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(field_array);
        int32_t length;
        const uint8_t *value = binary_array->GetValue(row, &length);
        return Value::BLOB(value, length);
    }
    case arrow::Type::DATE32: {
        auto date32_array = std::static_pointer_cast<arrow::Date32Array>(field_array);
        int32_t days_since_epoch = date32_array->Value(row);
        return Value::DATE(Date::EpochDaysToDate(days_since_epoch));
    }
    case arrow::Type::TIME64: {
        auto time64_array = std::static_pointer_cast<arrow::Time64Array>(field_array);
        int64_t time_value = time64_array->Value(row);
        auto time_unit = std::static_pointer_cast<arrow::Time64Type>(field_array->type())->unit();
        switch (time_unit) {
        case arrow::TimeUnit::MICRO:
            return Value::TIME(Time::FromTimeMs(time_value / 1000));
        case arrow::TimeUnit::NANO:
            return Value::TIME(Time::FromTimeMs(time_value / 1000 / 1000));
        default:
            throw InternalException("Unsupported time unit in TIME64 array.");
        }
    }
    case arrow::Type::TIMESTAMP: {
        auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(field_array);
        int64_t micros = timestamp_array->Value(row);
        return Value::TIMESTAMP(Timestamp::FromEpochMicroSeconds(micros));
    }
    case arrow::Type::INTERVAL_MONTH_DAY_NANO: {
        auto interval_array =
            std::static_pointer_cast<arrow::MonthDayNanoIntervalArray>(field_array);
        auto interval = interval_array->Value(row);
        return Value::INTERVAL(interval.months, interval.days, interval.nanoseconds / 1000);
    }
    case arrow::Type::INT64: {
        auto int64_array = std::static_pointer_cast<arrow::Int64Array>(field_array);
        return Value(int64_array->Value(row));
    }
    case arrow::Type::FLOAT: {
        auto float_array = std::static_pointer_cast<arrow::FloatArray>(field_array);
        return Value(float_array->Value(row));
    }
    case arrow::Type::DOUBLE: {
        auto double_array = std::static_pointer_cast<arrow::DoubleArray>(field_array);
        return Value(double_array->Value(row));
    }
    case arrow::Type::STRING: {
        auto string_array = std::static_pointer_cast<arrow::StringArray>(field_array);
        return Value(string_array->GetString(row));
    }
    case arrow::Type::DECIMAL128: {
        auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(field_array);
        auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(field_array->type());
        int32_t scale = decimal_type->scale();
        int32_t precision = decimal_type->precision();

        arrow::Decimal128 decimal_value(decimal_array->Value(row));
        hugeint_t hugeint_value;
        hugeint_value.upper = static_cast<int64_t>(decimal_value.high_bits());
        hugeint_value.lower = static_cast<uint64_t>(decimal_value.low_bits());
        return Value::DECIMAL(hugeint_value, precision, scale);
    }
    case arrow::Type::DECIMAL256: {
        auto decimal_array = std::static_pointer_cast<arrow::Decimal256Array>(field_array);
        auto decimal_type = std::static_pointer_cast<arrow::Decimal256Type>(field_array->type());
        int32_t scale = decimal_type->scale();
        int32_t precision = decimal_type->precision();

        if (target_type.id() == LogicalTypeId::VARCHAR) {
            auto value_str = decimal_array->FormatValue(row);
            return Value(value_str);
        }

        if (precision > 38) {
            throw BinderException("BIGDECIMAL precision of " + std::to_string(precision) +
                                  " exceeds maximum supported precision of 38 in DuckDB.");
        }

        const uint8_t *value_ptr = decimal_array->Value(row);
        uint64_t parts[4] = {0};
        for (int i = 0; i < 4; i++) {
            parts[i] = *reinterpret_cast<const uint64_t *>(value_ptr + i * sizeof(uint64_t));
        }
        if (parts[2] != 0 || parts[3] != 0) {
            throw BinderException(
                "BIGDECIMAL value exceeds the range of 128-bit Decimal supported by DuckDB.");
        }

        hugeint_t hugeint_value;
        hugeint_value.lower = parts[0];
        hugeint_value.upper = static_cast<int64_t>(parts[1]);
        return Value::DECIMAL(hugeint_value, precision, scale);
    }
    case arrow::Type::LIST: {
        auto list_array = std::static_pointer_cast<arrow::ListArray>(field_array);
        return ConvertListElementToValue(list_array, row, target_type);
    }
    case arrow::Type::STRUCT: {
        auto struct_array = std::static_pointer_cast<arrow::StructArray>(field_array);

        vector<LogicalType> child_types;
        auto struct_fields = struct_array->type()->fields();
        child_types.reserve(struct_fields.size());
        for (const auto &child_field : struct_fields) {
            child_types.push_back(BigqueryUtils::ArrowTypeToLogicalType(child_field->type()));
        }

        return ConvertStructRowToValue(struct_array, row, child_types);
    }
    default:
        throw InternalException("Unsupported Arrow type in struct field: " + field_type->name());
    }
}

int64_t BigqueryArrowReader::GetEstimatedRowCount() {
    if (!read_session) {
        throw BinderException("Read session is not initialized.");
    }
    return read_session->estimated_row_count();
}

BigqueryTableRef BigqueryArrowReader::GetTableRef() const {
    return table_ref;
}

} // namespace bigquery
} // namespace duckdb
