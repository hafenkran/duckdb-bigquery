#include "duckdb.hpp"

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/iterator.h>

#include <iostream>

#include "bigquery_arrow_reader.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_utils.hpp"

namespace duckdb {
namespace bigquery {

using BatchPtr = std::shared_ptr<arrow::RecordBatch>;
using BatchIterator = arrow::Iterator<BatchPtr>;

class IteratorBatchReader : public arrow::RecordBatchReader {
public:
    explicit IteratorBatchReader(BatchIterator iterator, std::shared_ptr<arrow::Schema> schema)
        : iterator_(std::move(iterator)), schema_(std::move(schema)) {
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        return schema_;
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *out) override {
        ARROW_ASSIGN_OR_RAISE(auto batch, iterator_.Next());
        *out = std::move(batch);
        return arrow::Status::OK();
    }

private:
    BatchIterator iterator_;
    std::shared_ptr<arrow::Schema> schema_;
};

struct BigqueryStreamState {
    shared_ptr<BigqueryArrowReader> reader;
    shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> stream;
    BigqueryStreamFactory *factory;

    google::cloud::v2_38::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse> range;
    google::cloud::v2_38::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse>::iterator it;

    bool range_open = false;
    int64_t rows_delivered = 0;
};

static std::shared_ptr<arrow::Schema> SchemaWithDecimal256AsString(const std::shared_ptr<arrow::Schema> &schema) {
    arrow::FieldVector new_fields;
    bool changed = false;
    for (const auto &field : schema->fields()) {
        if (field->type()->id() == arrow::Type::DECIMAL256) {
            new_fields.push_back(arrow::field(field->name(), arrow::utf8(), field->nullable()));
            changed = true;
        } else {
            new_fields.push_back(field);
        }
    }
    return changed ? arrow::schema(std::move(new_fields)) : schema;
}

static std::shared_ptr<arrow::RecordBatch> ConvertDecimal256ToString(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const std::shared_ptr<arrow::Schema> &target_schema) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    bool changed = false;
    for (int i = 0; i < batch->num_columns(); ++i) {
        auto col = batch->column(i);
        if (col->type_id() == arrow::Type::DECIMAL256) {
            auto dec = std::static_pointer_cast<arrow::Decimal256Array>(col);
            arrow::StringBuilder builder;
            for (int64_t row = 0; row < col->length(); ++row) {
                if (dec->IsNull(row)) {
                    (void)builder.AppendNull();
                } else {
                    (void)builder.Append(dec->FormatValue(row));
                }
            }
            std::shared_ptr<arrow::Array> str_array;
            (void)builder.Finish(&str_array);
            columns.push_back(std::move(str_array));
            changed = true;
        } else {
            columns.push_back(col);
        }
    }
    return changed ? arrow::RecordBatch::Make(target_schema, batch->num_rows(), std::move(columns)) : batch;
}

unique_ptr<ArrowArrayStreamWrapper> BigqueryStreamFactory::Produce(uintptr_t factory_ptr, ArrowStreamParameters &) {
    auto *factory = reinterpret_cast<BigqueryStreamFactory *>(factory_ptr);
    auto &reader = factory->reader;

    const idx_t first_idx = factory->next_stream.fetch_add(1);
    auto bq_stream = reader->GetStream(first_idx);

    auto modified_schema = SchemaWithDecimal256AsString(reader->GetSchema());

    auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
    if (!bq_stream) {
        auto empty_iter = arrow::MakeEmptyIterator<std::shared_ptr<arrow::RecordBatch>>();
        auto empty_reader = std::make_shared<IteratorBatchReader>(std::move(empty_iter), modified_schema);
        auto status = arrow::ExportRecordBatchReader(empty_reader, &wrapper->arrow_array_stream);
        if (!status.ok()) {
            throw BinderException("Arrow export (empty stream) failed: " + status.ToString());
        }
        return wrapper;
    }

    auto st = std::make_shared<BigqueryStreamState>();
    st->reader = reader;
    st->factory = factory;
    st->stream = bq_stream;

    auto iter = arrow::MakeFunctionIterator([st, modified_schema]() -> arrow::Result<BatchPtr> {
        while (true) {
            if (!st->range_open) {
                st->range = st->reader->ReadRows(st->stream->name(), 0);
                st->it = st->range.begin();
                st->range_open = true;
            }

            if (st->it == st->range.end()) {
                st->range = st->reader->ReadRows(st->stream->name(), st->rows_delivered);
                st->it = st->range.begin();

                if (st->it == st->range.end()) {
                    const idx_t next = st->factory->next_stream.fetch_add(1);
                    st->stream = st->reader->GetStream(next);
                    if (!st->stream) {
                        return nullptr;
                    }
                    st->range_open = false;
                    st->rows_delivered = 0;
                    continue;
                }
            }

            auto &resp_or = *st->it;
            if (!resp_or) {
                return arrow::Status::IOError("ReadRows error: " + resp_or.status().message());
            }
            if (!resp_or->has_arrow_record_batch()) {
                st->it++;
                continue;
            }

            const auto &arrow_msg = resp_or->arrow_record_batch();
            auto batch = st->reader->ReadBatch(arrow_msg);
            st->it++;
            if (!batch) {
                return arrow::Status::IOError("Arrow deserialization failed");
            }

            st->rows_delivered += batch->num_rows();
            return ConvertDecimal256ToString(batch, modified_schema);
        }
    });

    auto rb_reader = std::make_shared<IteratorBatchReader>(std::move(iter), modified_schema);

    auto export_status = arrow::ExportRecordBatchReader(rb_reader, &wrapper->arrow_array_stream);
    if (!export_status.ok()) {
        throw BinderException("Arrow export failed: " + export_status.ToString());
    }

    return wrapper;
}

void BigqueryStreamFactory::GetSchema(ArrowArrayStream *factory_ptr, ArrowSchema &schema) {
    auto *factory = reinterpret_cast<BigqueryStreamFactory *>(factory_ptr);
    auto &reader = factory->reader;

    auto arrow_schema = SchemaWithDecimal256AsString(reader->GetSchema());
    auto status = arrow::ExportSchema(*arrow_schema, &schema);
    if (!status.ok()) {
        throw BinderException("Arrow export failed: " + status.ToString());
    }
}

BigqueryArrowReader::BigqueryArrowReader(const BigqueryTableRef table_ref,
                                         const string billing_project_id,
                                         idx_t num_streams,
                                         const google::cloud::Options &options,
                                         const vector<string> &selected_columns,
                                         const string &filter_condition)
    : table_ref(table_ref), billing_project_id(billing_project_id), num_streams(num_streams), options(options),
      localhost_test_env(false) {
    if (options.has<google::cloud::EndpointOption>()) {
        localhost_test_env = true;
    }

    const string parent = BigqueryUtils::FormatParentString(billing_project_id);
    const string table_string = table_ref.TableString();

    auto connection = google::cloud::bigquery_storage_v1::MakeBigQueryReadConnection(options);
    read_client = make_uniq<google::cloud::bigquery_storage_v1::BigQueryReadClient>(connection);

    auto session = google::cloud::bigquery::storage::v1::ReadSession();
    session.set_table(table_string);
    session.set_data_format(google::cloud::bigquery::storage::v1::DataFormat::ARROW);

    auto *read_options = session.mutable_read_options();
    auto arrow_options = read_options->mutable_arrow_serialization_options();
    arrow_options->set_buffer_compression(BigquerySettings::GetArrowCompressionCodec());

    if (!selected_columns.empty()) {
        if (BigquerySettings::DebugQueryPrint()) {
            std::cout << "BigQuery selected fields: " << StringUtil::Join(selected_columns, ", ") << std::endl;
        }
        for (const auto &column : selected_columns) {
            read_options->add_selected_fields(column);
        }
    }
    if (!filter_condition.empty()) {
        if (BigquerySettings::DebugQueryPrint()) {
            std::cout << "BigQuery row restrictions: " << filter_condition << std::endl;
        }
        read_options->set_row_restriction(filter_condition);
    }

    auto new_session = read_client->CreateReadSession(parent, session, num_streams);
    if (!new_session) {
        throw BinderException("Error while creating read session: " + new_session.status().message());
    }

    read_session = make_uniq<google::cloud::bigquery::storage::v1::ReadSession>(std::move(new_session.value()));
}

shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> BigqueryArrowReader::GetStream(idx_t stream_idx) {
    if (!read_session) {
        throw BinderException("Read session is not initialized.");
    }
    if (stream_idx >= static_cast<duckdb::idx_t>(read_session->streams_size())) {
        return nullptr;
    }
    return make_shared_ptr<google::cloud::bigquery::storage::v1::ReadStream>(read_session->streams(stream_idx));
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

google::cloud::v2_38::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse> BigqueryArrowReader::ReadRows(
    const string &stream_name,
    int row_offset) {
    return read_client->ReadRows(stream_name, row_offset);
}

std::shared_ptr<arrow::Schema> BigqueryArrowReader::ReadSchema(
    const google::cloud::bigquery::storage::v1::ArrowSchema &schema) {
    const auto &serialized = schema.serialized_schema();
    auto buffer_ptr = arrow::Buffer::Wrap(serialized.data(), serialized.size());
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
        auto arrow_buffer = arrow::Buffer::FromString(batch.serialized_record_batch());
        arrow::io::BufferReader buffer_reader(arrow_buffer);
        auto arrow_reader_result = arrow::ipc::RecordBatchStreamReader::Open(&buffer_reader);
        if (!arrow_reader_result.ok()) {
            throw BinderException("Failed to create RecordBatchStreamReader: " +
                                  arrow_reader_result.status().ToString());
        }
        std::shared_ptr<arrow::ipc::RecordBatchStreamReader> arrow_reader = arrow_reader_result.ValueOrDie();

        auto arrow_read_result = arrow_reader->ReadNext(&arrow_batch);
        if (!arrow_read_result.ok() || arrow_batch == nullptr) {
            throw BinderException("Failed to read Arrow RecordBatch");
        }
    } else {
        auto arrow_buffer =
            arrow::Buffer::Wrap(batch.serialized_record_batch().data(), batch.serialized_record_batch().size());
        arrow::io::BufferReader buffer_reader(arrow_buffer);

        auto options = arrow::ipc::IpcReadOptions::Defaults();
        options.use_threads = true;

        arrow::Result<std::shared_ptr<arrow::RecordBatch>> arrow_record_batch_res =
            arrow::ipc::ReadRecordBatch(schema, nullptr, options, &buffer_reader);
        if (!arrow_record_batch_res.ok()) {
            throw BinderException("Failed to read Arrow RecordBatch: " + arrow_record_batch_res.status().message());
        }

        arrow_batch = arrow_record_batch_res.ValueOrDie();
    }
    return arrow_batch;
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
