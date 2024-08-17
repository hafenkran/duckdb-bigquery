#include "bigquery_scan.hpp"
#include "bigquery_arrow_reader.hpp"
#include "bigquery_client.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include "google/cloud/bigquery/storage/v1/storage.pb.h"
#include "google/cloud/bigquery/storage/v1/stream.pb.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>

#include "duckdb.hpp"
// #include "duckdb/common/types.hpp"


namespace duckdb {
namespace bigquery {


struct BigqueryGlobalFunctionState : public GlobalTableFunctionState {
    explicit BigqueryGlobalFunctionState(shared_ptr<BigqueryArrowReader> arrow_reader)
        : arrow_reader(std::move(arrow_reader)) {
    }

    mutable mutex lock;
    idx_t position = 0;
    idx_t max_threads;

    // The index of the next stream to read (i.e., current file + 1)
    atomic<idx_t> next_stream = 0;
    shared_ptr<BigqueryArrowReader> arrow_reader;

    idx_t MaxThreads() const override {
        return max_threads;
    }
};


struct BigqueryLocalFunctionState : public LocalTableFunctionState {
    BigqueryLocalFunctionState() : row_offset(0), done(false) {
    }

    vector<column_t> column_ids;
    vector<column_t> column_ids_ranked;

    int row_offset;
    bool done;

    void ScanNextChunk(DataChunk &output) {
        if (current_batch == nullptr || row_offset >= current_batch->num_rows()) {
            if (done || !ReadNextBatch()) {
                done = true;
                return;
            }
            row_offset = 0;
        }

        auto slice = current_batch->Slice(row_offset, STANDARD_VECTOR_SIZE);
        for (idx_t i = 0; i < output.ColumnCount(); i++) {
            auto col_idx = column_ids[i];
            if (COLUMN_IDENTIFIER_ROW_ID == col_idx) {
                continue;
            }
            auto rank_idx = column_ids_ranked[i];
            auto &out_vec = output.data[i];
            arrow_reader->ReadColumn(slice->column(rank_idx), out_vec);
        }
        output.SetCardinality(slice->num_rows());

        row_offset += slice->num_rows();
        if (row_offset >= current_batch->num_rows()) {
            current_batch = nullptr;
        }
    }

    shared_ptr<BigqueryArrowReader> arrow_reader;
    shared_ptr<google::cloud::bigquery::storage::v1::ReadStream> read_stream;

private:
    bool rows_read = false;
    google::cloud::v2_27::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse> read_rows;
    google::cloud::v2_27::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse>::iterator read_rows_it;
    google::cloud::v2_27::StatusOr<google::cloud::bigquery::storage::v1::ReadRowsResponse> rows;
    std::shared_ptr<arrow::RecordBatch> current_batch;

    bool ReadNextBatch() {
        if (!rows_read) {
            read_rows = arrow_reader->ReadRows(read_stream->name(), row_offset);
            read_rows_it = read_rows.begin();
            rows_read = true;
        }

        if (read_rows_it == read_rows.end()) {
            done = true;
            return false;
        }

        rows = *read_rows_it;
        if (!rows.ok()) {
            std::cerr << "Error reading rows: " << rows.status() << std::endl;
            done = true;
            return false;
        }
        current_batch = arrow_reader->ReadBatch(rows->arrow_record_batch());
        read_rows_it++;
        return true;
    }
};


static unique_ptr<FunctionData> BigqueryBind(ClientContext &context,
                                             TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types,
                                             vector<string> &names) {
    auto table_string = input.inputs[0].GetValue<string>();
    auto table_ref = BigqueryUtils::ParseTableString(table_string);
    if (!table_ref.has_dataset_id() || !table_ref.has_table_id()) {
        throw ParserException("Invalid table string: %s", table_string);
    }

    auto result = make_uniq<BigqueryBindData>();
    result->project_id = table_ref.project_id;
    result->dataset_id = table_ref.dataset_id;
    result->table_id = table_ref.table_id;
    result->bq_client = make_shared_ptr<BigqueryClient>(result->project_id, result->execution_project_id);

    auto arrow_reader = result->bq_client->CreateArrowReader(result->dataset_id, result->table_id, 1);

    // Get the table info (columns, constraints, etc.)
    ColumnList columns;
    vector<unique_ptr<Constraint>> constraints;
    arrow_reader->MapTableInfo(columns, constraints);

    for (auto &column : columns.Logical()) {
        names.push_back(column.GetName());
        return_types.push_back(column.GetType());
    }
    if (names.empty()) {
        throw std::runtime_error("no columns for table " + result->table_id);
    }

    // TODO GetMaxRowId
    result->estimated_row_count = idx_t(arrow_reader->GetEstimatedRowCount());
    result->names = names;
    result->types = return_types;
    return std::move(result);
}


static unique_ptr<GlobalTableFunctionState> BigqueryInitGlobalState(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    auto &bind_data = (BigqueryBindData &)*input.bind_data;

    // selected fields
    vector<string> selected_fields;
    for (auto &column_id : input.column_ids) {
        if (COLUMN_IDENTIFIER_ROW_ID == column_id) {
            continue;
        }
        selected_fields.push_back(bind_data.names[column_id]);
    }

    // filters
    string filter_string;
    auto filters = input.filters;
    if (filters && !filters->filters.empty()) {
        for (auto &filter : filters->filters) {
            if (!filter_string.empty()) {
                filter_string += " AND ";
            }
            string column_name = selected_fields[filter.first];
            // auto  = KeywordHelper::WriteQuoted(column_name, '`');
            auto &filter_cond = *filter.second;

            filter_string += BigquerySQL::TransformFilter(column_name, filter_cond);
        }
    }

    idx_t k_max_read_streams = 1;
    auto arrow_reader = bind_data.bq_client->CreateArrowReader(bind_data.dataset_id,
                                                               bind_data.table_id,
                                                               k_max_read_streams,
                                                               selected_fields,
                                                               filter_string);
    bind_data.estimated_row_count = idx_t(arrow_reader->GetEstimatedRowCount());
    auto result = make_uniq<BigqueryGlobalFunctionState>(arrow_reader);
    result->position = 0;
    result->max_threads = k_max_read_streams;
    return std::move(result);
}


std::vector<column_t> CalculateRanks(const std::vector<column_t> &nums) {
    size_t n = nums.size();
    std::vector<std::pair<column_t, column_t>> value_index_pairs(n);
    for (size_t i = 0; i < n; ++i) {
        value_index_pairs[i] = {nums[i], i};
    }
    std::sort(value_index_pairs.begin(), value_index_pairs.end());
    std::vector<column_t> ranks(n);
    for (column_t i = 0; i < n; ++i) {
        ranks[value_index_pairs[i].second] = i;
    }
    return ranks;
}

static unique_ptr<LocalTableFunctionState> BigqueryInitLocalState(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
    auto &gstate = global_state->Cast<BigqueryGlobalFunctionState>();
    auto lstate = make_uniq<BigqueryLocalFunctionState>();
    lstate->arrow_reader = gstate.arrow_reader;
    lstate->read_stream = gstate.arrow_reader->NextStream();
    lstate->column_ids = input.column_ids;
    lstate->column_ids_ranked = CalculateRanks(input.column_ids);
    if (lstate->read_stream == nullptr) {
        lstate->done = true;
    }
    return std::move(lstate);
}

static void BigqueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &lstate = data.local_state->Cast<BigqueryLocalFunctionState>();
    auto &gstate = data.global_state->Cast<BigqueryGlobalFunctionState>();
    if (lstate.done) {
        return;
    }
    output.Reset();
    lstate.ScanNextChunk(output);
    lock_guard<mutex> glock(gstate.lock);
    gstate.position += output.size();
}

static string BigqueryToString(const FunctionData *bind_data_p) {
    D_ASSERT(bind_data_p);
    auto &bind_data = bind_data_p->Cast<BigqueryBindData>();
    return bind_data.TableString();
}

unique_ptr<NodeStatistics> BigqueryScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BigqueryBindData>();
    return make_uniq<NodeStatistics>(bind_data.estimated_row_count);
}

double BigqueryScanProgress(ClientContext &context,
                            const FunctionData *bind_data_p,
                            const GlobalTableFunctionState *global_state) {
    auto &bind_data = bind_data_p->Cast<BigqueryBindData>();
    auto &gstate = global_state->Cast<BigqueryGlobalFunctionState>();
    double progress = 0.0;
    if (bind_data.estimated_row_count > 0) {
        lock_guard<mutex> glock(gstate.lock);
        progress = 100.0 * double(gstate.position) / double(bind_data.estimated_row_count);
    }
    return MinValue<double>(100, progress);
}

BigqueryScanFunction::BigqueryScanFunction()
    : TableFunction("bigquery_scan",
                    {LogicalType::VARCHAR},
                    BigqueryScan,
                    BigqueryBind,
                    BigqueryInitGlobalState,
                    BigqueryInitLocalState) {
    to_string = BigqueryToString;
    cardinality = BigqueryScanCardinality;
    table_scan_progress = BigqueryScanProgress;
    projection_pushdown = true;
    filter_pushdown = true;
}


} // namespace bigquery
} // namespace duckdb
