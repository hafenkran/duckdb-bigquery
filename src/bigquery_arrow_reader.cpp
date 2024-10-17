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
    : table_ref(table_ref), billing_project_id(billing_project_id), num_streams(num_streams), options(options),
      localhost_test_env(false) {

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
        throw BinderException("Error while creating read session: " + new_session.status().message());
    }

    read_session = make_uniq<google::cloud::bigquery::storage::v1::ReadSession>(std::move(new_session.value()));
}

shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> BigqueryArrowReader::NextStream() {
    if (!read_session) {
        throw BinderException("Read session is not initialized.");
    }
    auto next_stream_idx = next_stream++;
    if (next_stream_idx >= read_session->streams_size()) {
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

void BigqueryArrowReader::MapTableInfo(ColumnList &res_columns, vector<unique_ptr<Constraint>> &res_constraints) {
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

google::cloud::v2_27::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse> BigqueryArrowReader::ReadRows(
    const string &stream_name,
    int row_offset) {
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
        std::shared_ptr<arrow::ipc::RecordBatchStreamReader> arrow_reader = arrow_reader_result.ValueOrDie();

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
            throw BinderException("Failed to read Arrow RecordBatch: " + arrow_record_batch_res.status().message());
        }

        arrow_batch = arrow_record_batch_res.ValueOrDie();
    }
    return arrow_batch;
}

void BigqueryArrowReader::ReadColumn(const std::shared_ptr<arrow::Array> &column, Vector &out_vec) {
    switch (column->type_id()) {
    case arrow::Type::STRUCT: {
        ReadStructColumn(std::static_pointer_cast<arrow::StructArray>(column), out_vec);
        break;
    }
    case arrow::Type::LIST: {
        ReadListColumn(std::static_pointer_cast<arrow::ListArray>(column), out_vec);
        break;
    }
    default: {
        ReadSimpleColumn(column, out_vec);
        break;
    }
    }
}

void BigqueryArrowReader::ReadSimpleColumn(const std::shared_ptr<arrow::Array> &column, Vector &out_vec) {
    switch (column->type_id()) {
    case arrow::Type::BOOL: {
        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(column);
        for (int64_t row = 0; row < bool_array->length(); ++row) {
            if (!bool_array->IsNull(row)) {
                auto value = bool_array->Value(row);
                out_vec.SetValue(row, Value(value));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::BINARY: {
        auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(column);
        for (int64_t row = 0; row < binary_array->length(); ++row) {
            if (!binary_array->IsNull(row)) {
                int32_t length;
                const uint8_t *value = binary_array->GetValue(row, &length);
                auto value_blob = Value::BLOB(value, length);
                out_vec.SetValue(row, value_blob);
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::DATE32: {
        auto date32_array = std::static_pointer_cast<arrow::Date32Array>(column);
        for (int64_t row = 0; row < date32_array->length(); ++row) {
            if (!date32_array->IsNull(row)) {
                int32_t days_since_epoch = date32_array->Value(row);
                auto value = Date::EpochDaysToDate(days_since_epoch);
                out_vec.SetValue(row, Value::DATE(value));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::TIME64: {
        auto time_array = std::static_pointer_cast<arrow::Time64Array>(column);
        for (int64_t row = 0; row < time_array->length(); ++row) {
            if (!time_array->IsNull(row)) {
                // Extract the time value
                int64_t time_value = time_array->Value(row);

                // Convert the time value to the appropriate unit
                auto time_unit = std::static_pointer_cast<arrow::Time64Type>(time_array->type())->unit();
                Value value;
                switch (time_unit) {
                case arrow::TimeUnit::MICRO:
                    value = Value::TIME(Time::FromTimeMs(time_value / 1000));
                    break;
                case arrow::TimeUnit::NANO:
                    value = Value::TIME(Time::FromTimeMs(time_value / 1000 / 1000));
                    break;
                default:
                    std::cerr << "Unsupported time unit in TIME64 array." << std::endl;
                    continue;
                }
                out_vec.SetValue(row, value);
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::TIMESTAMP: {
        auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(column);
        for (int64_t row = 0; row < timestamp_array->length(); ++row) {
            if (!timestamp_array->IsNull(row)) {
                // Extract the timestamp value (number of seconds since epoch)
                int64_t seconds_since_epoch = timestamp_array->Value(row);

                // Convert the timestamp to a DuckDB timestamp
                auto ts_value = Timestamp::FromEpochMicroSeconds(seconds_since_epoch);
                auto value = Value::TIMESTAMP(ts_value);
                out_vec.SetValue(row, value);
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::INTERVAL_MONTH_DAY_NANO: {
        auto interval_array = std::static_pointer_cast<arrow::MonthDayNanoIntervalArray>(column);
        for (int64_t row = 0; row < interval_array->length(); ++row) {
            if (!interval_array->IsNull(row)) {
                // Extract the interval value
                auto interval = interval_array->Value(row);

                // Convert the interval to a DuckDB interval
                auto value = Value::INTERVAL(interval.months, interval.days, interval.nanoseconds / 1000);
                out_vec.SetValue(row, value);
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::INT64: {
        auto int64_array = std::static_pointer_cast<arrow::Int64Array>(column);
        for (int64_t row = 0; row < int64_array->length(); ++row) {
            if (!int64_array->IsNull(row)) {
                int64_t value = int64_array->Value(row);
                out_vec.SetValue(row, Value::BIGINT(value));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::FLOAT: {
        auto float_array = std::static_pointer_cast<arrow::FloatArray>(column);
        for (int64_t row = 0; row < float_array->length(); ++row) {
            if (!float_array->IsNull(row)) {
                auto value = float_array->Value(row);
                out_vec.SetValue(row, Value(value));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::DOUBLE: {
        auto double_array = std::static_pointer_cast<arrow::DoubleArray>(column);
        for (int64_t row = 0; row < double_array->length(); ++row) {
            if (!double_array->IsNull(row)) {
                auto value = double_array->Value(row);
                out_vec.SetValue(row, Value(value));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    case arrow::Type::STRING: {
        auto string_array = std::static_pointer_cast<arrow::StringArray>(column);
        for (int64_t row = 0; row < string_array->length(); ++row) {
            if (!string_array->IsNull(row)) {
                auto value = string_array->GetString(row);
                out_vec.SetValue(row, Value(value));
            } else {
                out_vec.SetValue(row, Value());
            }
        }
        break;
    }
    default: {
        throw InternalException("Unsupported Arrow type: " + column->type()->name());
    }
    }
}

void BigqueryArrowReader::ReadListColumn(const std::shared_ptr<arrow::ListArray> &list_array, Vector &out_vec) {
    auto values = list_array->values();
    auto value_type = values->type_id();
    auto child_type = BigqueryUtils::ArrowTypeToLogicalType(values->type());

    for (int64_t row = 0; row < list_array->length(); ++row) {
        if (list_array->IsNull(row)) {
            continue;
        }
        int32_t start_offset = list_array->value_offset(row);
        int32_t end_offset = list_array->value_offset(row + 1);

        // Creating a DuckDB Value Arrays for the lists
        vector<Value> list_values;
        switch (value_type) {
        case arrow::Type::BOOL: {
            auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(values);
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!bool_array->IsNull(i)) {
                    auto value = bool_array->Value(i);
                    list_values.push_back(Value(value));
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }
        case arrow::Type::BINARY: {
            auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(values);
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!binary_array->IsNull(i)) {
                    int32_t length;
                    const uint8_t *value = binary_array->GetValue(i, &length);
                    auto value_blob = Value::BLOB(value, length);
                    list_values.push_back(value_blob);
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }
        case arrow::Type::DATE32: {
            auto date32_array = std::static_pointer_cast<arrow::Date32Array>(values);
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!date32_array->IsNull(i)) {
                    int32_t days_since_epoch = date32_array->Value(i);
                    auto value = Date::EpochDaysToDate(days_since_epoch);
                    list_values.push_back(Value::DATE(value));
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }
        case arrow::Type::TIMESTAMP: {
            auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(values);
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!timestamp_array->IsNull(i)) {
                    int64_t seconds_since_epoch = timestamp_array->Value(i);
                    auto ts_value = Timestamp::FromEpochMicroSeconds(seconds_since_epoch);
                    auto value = Value::TIMESTAMP(ts_value);
                    list_values.push_back(value);
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }
        case arrow::Type::INT32: {
            auto int_array = std::static_pointer_cast<arrow::Int32Array>(values);
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!int_array->IsNull(i)) {
                    list_values.push_back(Value(int_array->Value(i)));
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }
        case arrow::Type::INT64: {
            auto int_array = std::static_pointer_cast<arrow::Int64Array>(values);
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!int_array->IsNull(i)) {
                    list_values.push_back(Value(int_array->Value(i)));
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }
        case arrow::Type::FLOAT: {
            auto float_array = std::static_pointer_cast<arrow::FloatArray>(values);
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!float_array->IsNull(i)) {
                    list_values.push_back(Value(float_array->Value(i)));
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }
        case arrow::Type::DOUBLE: {
            auto double_array = std::static_pointer_cast<arrow::DoubleArray>(values);
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!double_array->IsNull(i)) {
                    list_values.push_back(Value(double_array->Value(i)));
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }
        case arrow::Type::STRING: {
            auto string_array = std::static_pointer_cast<arrow::StringArray>(values);
            for (int32_t i = start_offset; i < end_offset; ++i) {
                if (!string_array->IsNull(i)) {
                    list_values.push_back(Value(string_array->GetString(i)));
                } else {
                    list_values.push_back(Value());
                }
            }
            break;
        }
        case arrow::Type::STRUCT: {
			auto struct_array = std::static_pointer_cast<arrow::StructArray>(values);

			auto struct_type = BigqueryUtils::ArrowTypeToLogicalType(struct_array->type());
			duckdb::Vector struct_vector(struct_type);

			ReadStructColumn(struct_array, struct_vector);
			for (int32_t i = start_offset; i < end_offset; ++i) {
				if (!struct_array->IsNull(i)) {
					list_values.push_back(struct_vector.GetValue(i));
				} else {
					list_values.push_back(Value());
				}
			}
			break;
        }
        default:
            throw InternalException("Unsupported Arrow type: " + values->type()->name());
        }

        if (list_values.empty()) {
            out_vec.SetValue(row, Value::LIST(child_type, list_values));
        } else {
            out_vec.SetValue(row, Value::LIST(list_values));
        }
    }
}

void BigqueryArrowReader::ReadStructColumn(const std::shared_ptr<arrow::StructArray> &struct_array, Vector &out_vec) {
    for (int64_t row = 0; row < struct_array->length(); ++row) {
        if (struct_array->IsNull(row)) {
            out_vec.SetValue(row, Value());
            continue;
        }
        // Access the schema of the StructArray to get field names
        auto field_arrays = struct_array->fields();
        auto struct_fields = struct_array->type()->fields();

        // Iterate over each field in the struct
        child_list_t<Value> struct_values;
        for (size_t i = 0; i < field_arrays.size(); ++i) {
            auto field_name = struct_fields[i]->name();
            auto field_array = field_arrays[i];
            auto field_type = field_array->type();

            // Process each field based on its type
            switch (field_type->id()) {
            case arrow::Type::BOOL: {
                auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(field_array);
                if (!bool_array->IsNull(row)) {
                    struct_values.push_back(make_pair(field_name, Value(bool_array->Value(row))));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            case arrow::Type::BINARY: {
                auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(field_array);
                if (!binary_array->IsNull(row)) {
                    int32_t length;
                    const uint8_t *value = binary_array->GetValue(row, &length);
                    struct_values.push_back(make_pair(field_name, Value::BLOB(value, length)));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            case arrow::Type::DATE32: {
                auto date32_array = std::static_pointer_cast<arrow::Date32Array>(field_array);
                if (!date32_array->IsNull(row)) {
                    int32_t days_since_epoch = date32_array->Value(row);
                    struct_values.push_back(
                        make_pair(field_name, Value::DATE(Date::EpochDaysToDate(days_since_epoch))));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            case arrow::Type::TIME64: {
                auto time64_array = std::static_pointer_cast<arrow::Time64Array>(field_array);
                if (!time64_array->IsNull(row)) {
                    int64_t time_value = time64_array->Value(row);
                    auto time_unit = std::static_pointer_cast<arrow::Time64Type>(field_array->type())->unit();
                    Value value;
                    switch (time_unit) {
                    case arrow::TimeUnit::MICRO:
                        value = Value::TIME(Time::FromTimeMs(time_value / 1000));
                        break;
                    case arrow::TimeUnit::NANO:
                        value = Value::TIME(Time::FromTimeMs(time_value / 1000 / 1000));
                        break;
                    default:
                        throw InternalException("Unsupported time unit in TIME64 array.");
                    }
                    struct_values.push_back(make_pair(field_name, value));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            case arrow::Type::TIMESTAMP: {
                auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(field_array);
                if (!timestamp_array->IsNull(row)) {
                    int64_t seconds_since_epoch = timestamp_array->Value(row);
                    auto ts_value = Timestamp::FromEpochMicroSeconds(seconds_since_epoch);
                    struct_values.push_back(make_pair(field_name, Value::TIMESTAMP(ts_value)));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            case arrow::Type::INTERVAL_MONTH_DAY_NANO: {
                auto interval_array = std::static_pointer_cast<arrow::MonthDayNanoIntervalArray>(field_array);
                if (!interval_array->IsNull(row)) {
                    auto interval = interval_array->Value(row);
                    struct_values.push_back(
                        make_pair(field_name,
                                  Value::INTERVAL(interval.months, interval.days, interval.nanoseconds / 1000)));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            case arrow::Type::INT64: {
                auto int64_array = std::static_pointer_cast<arrow::Int64Array>(field_array);
                if (!int64_array->IsNull(row)) {
                    struct_values.push_back(make_pair(field_name, Value(int64_array->Value(row))));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            case arrow::Type::FLOAT: {
                auto float_array = std::static_pointer_cast<arrow::FloatArray>(field_array);
                if (!float_array->IsNull(row)) {
                    struct_values.push_back(make_pair(field_name, Value(float_array->Value(row))));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            case arrow::Type::DOUBLE: {
                auto double_array = std::static_pointer_cast<arrow::DoubleArray>(field_array);
                if (!double_array->IsNull(row)) {
                    struct_values.push_back(make_pair(field_name, Value(double_array->Value(row))));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            case arrow::Type::STRING: {
                auto string_array = std::static_pointer_cast<arrow::StringArray>(field_array);
                if (!string_array->IsNull(row)) {
                    struct_values.push_back(make_pair(field_name, Value(string_array->GetString(row))));
                } else {
                    struct_values.push_back(make_pair(field_name, Value()));
                }
                break;
            }
            default:
                throw InternalException("Unsupported Arrow type: " + field_type->name());
            }
        }
        out_vec.SetValue(row, Value::STRUCT(std::move(struct_values)));
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
