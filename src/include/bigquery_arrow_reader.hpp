#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/column_list.hpp"
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

//  BigQuery Stream Factory – provides ArrowArrayStreams from BigQuery Read Streams
class BigQueryStreamFactory {
public:
	explicit BigQueryStreamFactory(shared_ptr<BigqueryArrowReader> reader) //
		: reader(std::move(reader)) {
	}

	//! DuckDB calls this via the function pointer in the bind object
	static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr, ArrowStreamParameters & /*params*/);

	//! Called once in the bind step to get the schema
	static void GetSchema(ArrowArrayStream *factory_ptr, ArrowSchema &schema);

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

    void MapTableInfo(ColumnList &res_columns, vector<unique_ptr<Constraint>> &res_constraints);
    shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> GetStream(idx_t stream_idx);
    google::cloud::v2_33::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse> ReadRows(
        const string &stream_name,
        int row_offset);

    void ReadColumn(const std::shared_ptr<arrow::Array> &column, Vector &out_vec);
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

    // Convert simple types
    void ConvertFlatArrayToVector(const std::shared_ptr<arrow::Array> &column, Vector &out_vec);

    // Convert Lists
    void ConvertListArrayToVector(const std::shared_ptr<arrow::ListArray> &list_array, Vector &out_vec);
    Value ConvertListElementToValue(const std::shared_ptr<arrow::ListArray> &list_array,
                                    int64_t row,
                                    const LogicalType &target_type);

    // Convert Structs
    void ConvertStructArrayToVector(const std::shared_ptr<arrow::StructArray> &struct_array, Vector &out_vec);
    Value ConvertStructRowToValue(const std::shared_ptr<arrow::StructArray> &struct_array,
                                  int64_t row,
                                  const vector<LogicalType> &target_types);
    Value ConvertStructFieldToValue(const std::shared_ptr<arrow::Array> &field_array,
                                    int64_t row,
                                    const LogicalType &target_type);
};

template <class ArrowArrayType, class ValueCreator>
void ConvertPrimitiveArray(const std::shared_ptr<arrow::Array> &array, Vector &out_vec, ValueCreator create_value) {
    auto typed_array = std::static_pointer_cast<ArrowArrayType>(array);
    for (int64_t row = 0; row < typed_array->length(); ++row) {
        out_vec.SetValue(row, typed_array->IsNull(row) ? Value() : create_value(typed_array->Value(row)));
    }
}

template <class ArrowArrayType, class ValueCreator>
void ReadPrimitiveListElements(const std::shared_ptr<arrow::Array> &values,
                               int32_t start_offset,
                               int32_t end_offset,
                               vector<Value> &list_values,
                               ValueCreator &&create_value) {
    auto array = std::static_pointer_cast<ArrowArrayType>(values);
    for (int32_t i = start_offset; i < end_offset; ++i) {
        if (array->IsNull(i)) {
            list_values.push_back(Value());
        } else {
            list_values.push_back(create_value(array->Value(i)));
        }
    }
}

} // namespace bigquery
} // namespace duckdb
