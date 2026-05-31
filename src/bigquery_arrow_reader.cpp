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
#include <arrow/util/byte_size.h>

#include <condition_variable>
#include <deque>
#include <exception>
#include <iostream>
#include <mutex>
#include <thread>

#include "bigquery_arrow_reader.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_utils.hpp"

namespace duckdb {
namespace bigquery {

using BatchPtr = std::shared_ptr<arrow::RecordBatch>;
using BatchIterator = arrow::Iterator<BatchPtr>;
using ReadRowsResponse = google::cloud::bigquery::storage::v1::ReadRowsResponse;
using ReadRowsRange = google::cloud::StreamRange<ReadRowsResponse>;

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
    unique_ptr<google::cloud::bigquery_storage_v1::BigQueryReadClient> read_client;
    shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> stream;
    BigqueryStreamFactory *factory;

    ReadRowsRange range;
    ReadRowsRange::iterator it;

    bool range_open = false;
    int64_t rows_delivered = 0;
    shared_ptr<ReadRowsResponse> last_response_owner;
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

static idx_t EstimateRecordBatchBytes(const BatchPtr &batch, idx_t serialized_size) {
    if (!batch) {
        return serialized_size;
    }
    const auto decoded_size = arrow::util::TotalBufferSize(*batch);
    if (decoded_size <= 0) {
        return serialized_size;
    }
    return MaxValue<idx_t>(serialized_size, static_cast<idx_t>(decoded_size));
}

static arrow::Status ReadRowsErrorStatus(const BigqueryArrowReader &reader, const google::cloud::Status &status) {
    if (status.code() == google::cloud::StatusCode::kPermissionDenied) {
        return arrow::Status::IOError(
            "BigQuery Storage Read API permission denied while reading rows for " +
            reader.GetTableRef().TableString() +
            ".\n"
            "\n"
            "This path requires both table/view read access and permission to create and use read "
            "sessions.\n"
            "\n"
            "Required permissions usually include:\n"
            "  - bigquery.tables.getData on the referenced table or view\n"
            "  - bigquery.readsessions.create on the project\n"
            "  - bigquery.readsessions.getData and bigquery.readsessions.update on the table or higher\n"
            "\n"
            "Error details: " +
            status.message());
    }
    return arrow::Status::IOError("ReadRows error: " + status.message());
}

struct BigqueryBatchResult {
    BatchPtr batch;
    shared_ptr<ReadRowsResponse> response_owner;
    arrow::Status status = arrow::Status::OK();
    idx_t estimated_bytes = 0;
    bool finished = false;

    static BigqueryBatchResult Finished() {
        BigqueryBatchResult result;
        result.finished = true;
        return result;
    }

    static BigqueryBatchResult Error(arrow::Status status) {
        BigqueryBatchResult result;
        result.status = std::move(status);
        return result;
    }
};

class BigqueryReadCursor {
public:
    BigqueryReadCursor(shared_ptr<BigqueryArrowReader> reader,
                       BigqueryStreamFactory *factory,
                       shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> stream,
                       std::shared_ptr<arrow::Schema> modified_schema)
        : state(std::make_shared<BigqueryStreamState>()), modified_schema(std::move(modified_schema)) {
        state->reader = std::move(reader);
        state->read_client = state->reader->CreateReadClient();
        state->factory = factory;
        state->stream = std::move(stream);
    }

    BigqueryBatchResult Next() {
        while (true) {
            if (!state->stream) {
                return BigqueryBatchResult::Finished();
            }

            if (!state->range_open) {
                state->range = state->read_client->ReadRows(state->stream->name(), 0);
                state->it = state->range.begin();
                state->range_open = true;
            }

            if (state->it == state->range.end()) {
                state->range = state->read_client->ReadRows(state->stream->name(), state->rows_delivered);
                state->it = state->range.begin();

                if (state->it == state->range.end()) {
                    state->stream = state->factory->GetNextStream();
                    if (!state->stream) {
                        return BigqueryBatchResult::Finished();
                    }
                    state->range_open = false;
                    state->rows_delivered = 0;
                    continue;
                }
            }

            auto &resp_or = *state->it;
            if (!resp_or) {
                return BigqueryBatchResult::Error(ReadRowsErrorStatus(*state->reader, resp_or.status()));
            }

            auto response_owner = make_shared_ptr<ReadRowsResponse>(std::move(*resp_or));
            state->it++;

            if (!response_owner->has_arrow_record_batch()) {
                continue;
            }

            const auto &arrow_msg = response_owner->arrow_record_batch();
            const auto serialized_size = static_cast<idx_t>(arrow_msg.serialized_record_batch().size());
            auto batch = state->reader->ReadBatch(arrow_msg);
            if (!batch) {
                return BigqueryBatchResult::Error(arrow::Status::IOError("Arrow deserialization failed"));
            }

            state->rows_delivered += batch->num_rows();
            batch = ConvertDecimal256ToString(batch, modified_schema);
            state->last_response_owner = response_owner;

            BigqueryBatchResult result;
            result.batch = std::move(batch);
            result.response_owner = std::move(response_owner);
            result.estimated_bytes = EstimateRecordBatchBytes(result.batch, serialized_size);
            return result;
        }
    }

private:
    shared_ptr<BigqueryStreamState> state;
    std::shared_ptr<arrow::Schema> modified_schema;
};

struct PrefetchMemoryBudget {
    explicit PrefetchMemoryBudget(idx_t max_bytes) : max_bytes(max_bytes) {
    }

    bool Reserve(idx_t bytes, const std::atomic<bool> &stop_requested) {
        if (max_bytes == 0 || bytes == 0) {
            return true;
        }

        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&]() { return stop_requested.load() || CanReserve(bytes); });
        if (stop_requested.load()) {
            return false;
        }
        used_bytes += bytes;
        return true;
    }

    void Release(idx_t bytes) {
        if (max_bytes == 0 || bytes == 0) {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(mutex);
            if (bytes > used_bytes) {
                used_bytes = 0;
            } else {
                used_bytes -= bytes;
            }
        }
        cv.notify_all();
    }

    void NotifyAll() {
        cv.notify_all();
    }

private:
    bool CanReserve(idx_t bytes) const {
        if (bytes > max_bytes) {
            return used_bytes == 0;
        }
        if (used_bytes >= max_bytes) {
            return false;
        }
        return bytes <= max_bytes - used_bytes;
    }

    idx_t max_bytes;
    idx_t used_bytes = 0;
    std::mutex mutex;
    std::condition_variable cv;
};

class PrefetchingRecordBatchReader : public arrow::RecordBatchReader {
public:
    PrefetchingRecordBatchReader(unique_ptr<BigqueryReadCursor> cursor,
                                 std::shared_ptr<arrow::Schema> schema,
                                 idx_t queue_size,
                                 shared_ptr<PrefetchMemoryBudget> memory_budget)
        : cursor(std::move(cursor)), schema_(std::move(schema)), queue_size(queue_size),
          memory_budget(std::move(memory_budget)) {
        worker = std::thread([this]() { WorkerLoop(); });
    }

    ~PrefetchingRecordBatchReader() override {
        Stop();
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        return schema_;
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *out) override {
        BigqueryBatchResult entry;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [&]() { return stop_requested.load() || !queue.empty(); });
            if (queue.empty()) {
                *out = nullptr;
                return arrow::Status::OK();
            }
            entry = std::move(queue.front());
            queue.pop_front();
        }
        queue_cv.notify_all();

        if (entry.estimated_bytes > 0) {
            memory_budget->Release(entry.estimated_bytes);
        }

        if (!entry.status.ok()) {
            return entry.status;
        }
        if (entry.finished) {
            last_response_owner.reset();
            *out = nullptr;
            return arrow::Status::OK();
        }

        last_response_owner = std::move(entry.response_owner);
        *out = std::move(entry.batch);
        return arrow::Status::OK();
    }

private:
    void Stop() {
        if (!stop_requested.exchange(true)) {
            queue_cv.notify_all();
            memory_budget->NotifyAll();
        }
        if (worker.joinable()) {
            worker.join();
        }
    }

    bool Enqueue(BigqueryBatchResult result) {
        const bool has_batch = !result.finished && result.status.ok();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [&]() { return stop_requested.load() || queue.size() < queue_size; });
            if (stop_requested.load()) {
                return false;
            }
        }

        bool reserved_memory = false;
        if (has_batch && result.estimated_bytes > 0) {
            if (!memory_budget->Reserve(result.estimated_bytes, stop_requested)) {
                return false;
            }
            reserved_memory = true;
        }

        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if (stop_requested.load()) {
                if (reserved_memory) {
                    memory_budget->Release(result.estimated_bytes);
                }
                return false;
            }
            queue.push_back(std::move(result));
        }
        queue_cv.notify_all();
        return true;
    }

    void WorkerLoop() {
        try {
            while (!stop_requested.load()) {
                auto result = cursor->Next();
                const bool terminal = result.finished || !result.status.ok();
                if (!Enqueue(std::move(result)) || terminal) {
                    return;
                }
            }
        } catch (std::exception &ex) {
            (void)Enqueue(BigqueryBatchResult::Error(arrow::Status::IOError(ex.what())));
        } catch (...) {
            (void)Enqueue(BigqueryBatchResult::Error(arrow::Status::IOError("Unknown BigQuery read prefetch error")));
        }
    }

    unique_ptr<BigqueryReadCursor> cursor;
    std::shared_ptr<arrow::Schema> schema_;
    idx_t queue_size;
    shared_ptr<PrefetchMemoryBudget> memory_budget;

    std::deque<BigqueryBatchResult> queue;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    std::thread worker;
    std::atomic<bool> stop_requested{false};
    shared_ptr<ReadRowsResponse> last_response_owner;
};

BigqueryStreamFactory::BigqueryStreamFactory(shared_ptr<BigqueryArrowReader> reader)
    : reader(std::move(reader)), prefetch_queue_size(BigquerySettings::ReadPrefetchQueueSize()),
      prefetch_memory_budget(
          make_shared_ptr<PrefetchMemoryBudget>(static_cast<idx_t>(BigquerySettings::ReadPrefetchMaxMemory()))),
      modified_schema(SchemaWithDecimal256AsString(this->reader->GetSchema())) {
}

shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> BigqueryStreamFactory::GetNextStream() {
    const idx_t stream_idx = next_stream.fetch_add(1);
    return reader->GetStream(stream_idx);
}

unique_ptr<ArrowArrayStreamWrapper> BigqueryStreamFactory::Produce(uintptr_t factory_ptr, ArrowStreamParameters &) {
    auto *factory = reinterpret_cast<BigqueryStreamFactory *>(factory_ptr);
    auto &reader = factory->reader;

    auto bq_stream = factory->GetNextStream();
    auto modified_schema = factory->GetModifiedSchema();

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

    std::shared_ptr<arrow::RecordBatchReader> rb_reader;
    if (factory->GetPrefetchQueueSize() == 0) {
        auto cursor = std::make_shared<BigqueryReadCursor>(reader, factory, std::move(bq_stream), modified_schema);
        auto iter = arrow::MakeFunctionIterator([cursor]() -> arrow::Result<BatchPtr> {
            auto result = cursor->Next();
            if (!result.status.ok()) {
                return result.status;
            }
            if (result.finished) {
                return nullptr;
            }
            return result.batch;
        });
        rb_reader = std::make_shared<IteratorBatchReader>(std::move(iter), modified_schema);
    } else {
        auto cursor = make_uniq<BigqueryReadCursor>(reader, factory, std::move(bq_stream), modified_schema);
        rb_reader = std::make_shared<PrefetchingRecordBatchReader>(std::move(cursor),
                                                                   modified_schema,
                                                                   factory->GetPrefetchQueueSize(),
                                                                   factory->GetPrefetchMemoryBudget());
    }

    auto export_status = arrow::ExportRecordBatchReader(rb_reader, &wrapper->arrow_array_stream);
    if (!export_status.ok()) {
        throw BinderException("Arrow export failed: " + export_status.ToString());
    }

    return wrapper;
}

void BigqueryStreamFactory::GetSchema(ArrowArrayStream *factory_ptr, ArrowSchema &schema) {
    auto *factory = reinterpret_cast<BigqueryStreamFactory *>(factory_ptr);

    auto arrow_schema = factory->GetModifiedSchema();
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

    read_connection = google::cloud::bigquery_storage_v1::MakeBigQueryReadConnection(options);
    read_client = make_uniq<google::cloud::bigquery_storage_v1::BigQueryReadClient>(read_connection);

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
        auto status = new_session.status();
        if (status.code() == google::cloud::StatusCode::kPermissionDenied) {
            throw PermissionException(
                "BigQuery Storage Read API permission denied while creating a read session for %s.\n"
                "\n"
                "This path requires both table/view read access and permission to create and use read sessions.\n"
                "\n"
                "Required permissions usually include:\n"
                "  - bigquery.tables.getData on the referenced table or view\n"
                "  - bigquery.readsessions.create on the project\n"
                "  - bigquery.readsessions.getData and bigquery.readsessions.update on the table or higher\n"
                "\n"
                "Common predefined roles:\n"
                "  - roles/bigquery.dataViewer on the referenced dataset, table, or view\n"
                "  - roles/bigquery.readSessionUser on the project that creates the read session\n"
                "\n"
                "Error details: %s",
                table_string,
                status.message());
        }
        throw BinderException("Error while creating read session: " + status.message());
    }

    read_session = make_uniq<google::cloud::bigquery::storage::v1::ReadSession>(std::move(new_session.value()));
    streams.reserve(read_session->streams_size());
    for (int stream_idx = 0; stream_idx < read_session->streams_size(); stream_idx++) {
        streams.push_back(read_session->streams(stream_idx));
    }
}

shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> BigqueryArrowReader::GetStream(idx_t stream_idx) {
    if (!read_session) {
        throw BinderException("Read session is not initialized.");
    }
    if (stream_idx >= static_cast<idx_t>(streams.size())) {
        return nullptr;
    }
    return make_shared_ptr<google::cloud::bigquery::storage::v1::ReadStream>(streams[stream_idx]);
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

idx_t BigqueryArrowReader::GetStreamCount() const {
    if (!read_session) {
        throw BinderException("Read session is not initialized.");
    }
    return static_cast<idx_t>(streams.size());
}

unique_ptr<google::cloud::bigquery_storage_v1::BigQueryReadClient> BigqueryArrowReader::CreateReadClient() const {
    if (!read_connection) {
        throw BinderException("Read connection is not initialized.");
    }
    return make_uniq<google::cloud::bigquery_storage_v1::BigQueryReadClient>(read_connection);
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
