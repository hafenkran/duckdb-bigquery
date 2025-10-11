#include "bigquery_scan.hpp"
#include "bigquery_arrow_reader.hpp"
#include "bigquery_arrow_scan.hpp"
#include "bigquery_client.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include "google/cloud/bigquery/storage/v1/storage.pb.h"
#include "google/cloud/bigquery/storage/v1/stream.pb.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {
namespace bigquery {


struct BigqueryLocalFunctionState : public LocalTableFunctionState {
    BigqueryLocalFunctionState() : row_offset(0), done(false) {
    }

    vector<column_t> column_ids;
    vector<column_t> column_ids_ranked;

    int row_offset;
    bool done;

    void ScanNextChunk(DataChunk &output, BigqueryGlobalFunctionState &gstate) {
        if (current_batch == nullptr || row_offset >= current_batch->num_rows()) {
            if (!ReadNextBatch()) {
                // Current stream is exhausted, check if we have more streams to read
                auto streamidx = gstate.NextStreamIndex();
                read_stream = arrow_reader->GetStream(streamidx);
                if (read_stream == nullptr) {
                    done = true;
                    return;
                }

                // Reset the row offset for the new stream
                rows_read = false;
                row_offset = 0;

                // Try to read from a new stream
                if (!ReadNextBatch()) {
                    done = true;
                    return;
                }
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
    google::cloud::v2_38::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse> read_rows;
    google::cloud::v2_38::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse>::iterator read_rows_it;
    std::shared_ptr<arrow::RecordBatch> current_batch;

    bool ReadNextBatch() {
        if (!rows_read) {
            read_rows = arrow_reader->ReadRows(read_stream->name(), row_offset);
            read_rows_it = read_rows.begin();
            rows_read = true;
        }

        if (read_rows_it == read_rows.end()) {
            return false;
        }

        if (!read_rows_it->ok()) {
            std::cerr << "Error reading rows: " << read_rows_it->status() << std::endl;
            done = true;
            return false;
        }
        current_batch = arrow_reader->ReadBatch(read_rows_it->value().arrow_record_batch());
        read_rows_it++;
        return true;
    }
};

unique_ptr<FunctionData> BigqueryLegacyScanFunction::BigqueryLegacyScanBind(ClientContext &context,
                                                                            TableFunctionBindInput &input,
                                                                            vector<LogicalType> &return_types,
                                                                            vector<string> &names) {
    auto table_string = input.inputs[0].GetValue<string>();
    auto table_ref = BigqueryUtils::ParseTableString(table_string);
    if (!table_ref.has_dataset_id() || !table_ref.has_table_id()) {
        throw ParserException("Invalid table string: %s", table_string);
    }

    // Parse named parameters using centralized function
    auto params = BigQueryCommonParameters::ParseFromNamedParameters(input.named_parameters);

    auto result = make_uniq<BigqueryLegacyScanBindData>();
    result->table_ref = table_ref;
    result->filter_condition = params.filter;
    result->config = BigqueryConfig(table_ref.project_id)
                         .SetDatasetId(table_ref.dataset_id)
                         .SetBillingProjectId(params.billing_project)
                         .SetApiEndpoint(params.api_endpoint)
                         .SetGrpcEndpoint(params.grpc_endpoint);
    result->bq_client = make_shared_ptr<BigqueryClient>(context, result->config);

    ColumnList columns;
    vector<unique_ptr<Constraint>> constraints;

    string filter_cond = "";
    if (!params.filter.empty()) {
        filter_cond = params.filter;
    }

    auto arrow_reader = result->bq_client->CreateArrowReader(table_ref, 1, std::vector<string>(), filter_cond);
    arrow_reader->MapTableInfo(columns, constraints);

    for (auto &column : columns.Logical()) {
        names.push_back(column.GetName());
        return_types.push_back(column.GetType());
    }
    if (names.empty()) {
        auto table_ref = arrow_reader->GetTableRef();
        throw std::runtime_error("no columns for table " + table_ref.table_id);
    }

    if (BigquerySettings::GeographyAsGeometry()) {
        for (const auto &column : columns.Logical()) {
            if (BigqueryUtils::IsGeographyType(column.GetType())) {
                throw BinderException(
                    "BigQuery GEOGRAPHY columns with geography_as_geometry=true are not supported in legacy scan. "
                    "Please either set bq_use_legacy_scan=false (recommended) or set bq_geography_as_geometry=false.");
            }
        }
    }

    // TODO GetMaxRowId
    result->estimated_row_count = idx_t(arrow_reader->GetEstimatedRowCount());
    result->names = names;
    result->types = return_types;
    return std::move(result);
}

unique_ptr<FunctionData> BigqueryScanFunction::BigqueryScanBind(ClientContext &context,
                                                                TableFunctionBindInput &input,
                                                                vector<LogicalType> &return_types,
                                                                vector<string> &names) {
    // Parse named parameters to check use_legacy_scan
    auto params = BigQueryCommonParameters::ParseFromNamedParameters(input.named_parameters);

    if (params.use_legacy_scan) {
        return BigqueryLegacyScanFunction::BigqueryLegacyScanBind(context, input, return_types, names);
    } else {
        return BigqueryArrowScanFunction::BigqueryArrowScanBind(context, input, return_types, names);
    }
}

unique_ptr<GlobalTableFunctionState> BigqueryLegacyScanFunction::BigqueryLegacyScanInitGlobalState(
    ClientContext &context,
    TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<BigqueryLegacyScanBindData>();

    // selected fields
    vector<string> selected_fields;
    for (auto &column_id : input.column_ids) {
        if (COLUMN_IDENTIFIER_ROW_ID == column_id) {
            continue;
        }
        selected_fields.push_back(bind_data.names[column_id]);
    }

    // filters
    bool enable_filter_pushdown = BigquerySettings::ExperimentalFilterPushdown();
    string filter_string;
    auto filters = input.filters;
    if (!bind_data.filter_condition.empty()) {
        if (!filter_string.empty()) {
            filter_string += " AND ";
        }
        filter_string += bind_data.filter_condition;
    } else if (enable_filter_pushdown && filters && !filters->filters.empty()) {
        for (auto &filter : filters->filters) {
            if (!filter_string.empty()) {
                filter_string += " AND ";
            }
            string column_name = selected_fields[filter.first];
            auto &filter_cond = *filter.second;
            filter_string += BigquerySQL::TransformFilter(column_name, filter_cond);
        }
    }

    // when preserve_insertion_order=FALSE, we can use multiple streams for parallelization; defaults to maximum_threads
    // when preserve_insertion_order=TRUE, we use only 1 stream as there won't be any parallelization from DuckDB
    idx_t k_max_read_streams = BigquerySettings::GetMaxReadStreams(context);
    auto arrow_reader =
        bind_data.bq_client->CreateArrowReader(bind_data.table_ref, k_max_read_streams, selected_fields, filter_string);

    auto &mutable_bind_data = input.bind_data->CastNoConst<BigqueryLegacyScanBindData>();
    mutable_bind_data.estimated_row_count = idx_t(arrow_reader->GetEstimatedRowCount());
    auto result = make_uniq<BigqueryGlobalFunctionState>(arrow_reader, k_max_read_streams);
    return std::move(result);
}

unique_ptr<LocalTableFunctionState> BigqueryLegacyScanFunction::BigqueryLegacyScanInitLocalState(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state) {
    auto &gstate = global_state->Cast<BigqueryGlobalFunctionState>();
    auto lstate = make_uniq<BigqueryLocalFunctionState>();
    lstate->arrow_reader = gstate.arrow_reader;

    lstate->column_ids = input.column_ids;
    lstate->column_ids_ranked = CalculateRanks(input.column_ids);

    auto streamidx = gstate.NextStreamIndex();
    lstate->read_stream = gstate.arrow_reader->GetStream(streamidx);
    if (lstate->read_stream == nullptr) {
        lstate->done = true;
    }
    return std::move(lstate);
}

void BigqueryLegacyScanFunction::BigqueryLegacyScanExecute(ClientContext &context,
                                                           TableFunctionInput &data,
                                                           DataChunk &output) {
    auto &lstate = data.local_state->Cast<BigqueryLocalFunctionState>();
    auto &gstate = data.global_state->Cast<BigqueryGlobalFunctionState>();
    if (lstate.done) {
        return;
    }
    output.Reset();
    lstate.ScanNextChunk(output, gstate);
    lock_guard<mutex> glock(gstate.lock);
    gstate.position += output.size();
}

unique_ptr<GlobalTableFunctionState> BigqueryScanFunction::BigqueryScanInitGlobalState(ClientContext &context,
                                                                                       TableFunctionInitInput &input) {
    // Check bind data type and forward to appropriate implementation
    if (dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        return BigqueryArrowScanFunction::BigqueryArrowScanInitGlobal(context, input);
    } else {
        return BigqueryLegacyScanFunction::BigqueryLegacyScanInitGlobalState(context, input);
    }
}

unique_ptr<LocalTableFunctionState> BigqueryScanFunction::BigqueryScanInitLocalState(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state) {
    // Check bind data type and forward to appropriate implementation
    if (dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        return BigqueryArrowScanFunction::BigqueryArrowScanInitLocal(context, input, global_state);
    } else {
        return BigqueryLegacyScanFunction::BigqueryLegacyScanInitLocalState(context, input, global_state);
    }
}

void BigqueryScanFunction::BigqueryScanExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    // Check bind data type and forward to appropriate implementation
    if (dynamic_cast<const BigqueryArrowScanBindData *>(data.bind_data.get())) {
        BigqueryArrowScanFunction::BigqueryArrowScanExecute(context, data, output);
    } else {
        BigqueryLegacyScanFunction::BigqueryLegacyScanExecute(context, data, output);
    }
}

static InsertionOrderPreservingMap<string> BigqueryScanToString(TableFunctionToStringInput &input) {
    D_ASSERT(input.bind_data);
    InsertionOrderPreservingMap<string> result;

    // Check bind data type and forward to appropriate implementation
    if (auto *arrow_bind_data = dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        result["Table"] = arrow_bind_data->TableString();
    } else {
        auto &bind_data = input.bind_data->Cast<BigqueryLegacyScanBindData>();
        result["Table"] = bind_data.TableString();
    }
    return result;
}

unique_ptr<NodeStatistics> BigqueryScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    // Check bind data type and forward to appropriate implementation
    if (auto *arrow_bind_data = dynamic_cast<const BigqueryArrowScanBindData *>(bind_data_p)) {
        return make_uniq<NodeStatistics>(arrow_bind_data->estimated_row_count, arrow_bind_data->estimated_row_count);
    } else {
        auto &bind_data = bind_data_p->Cast<BigqueryLegacyScanBindData>();
        return make_uniq<NodeStatistics>(bind_data.estimated_row_count, bind_data.estimated_row_count);
    }
}

double BigqueryScanProgress(ClientContext &context,
                            const FunctionData *bind_data_p,
                            const GlobalTableFunctionState *global_state) {
    // Check bind data type and forward to appropriate implementation
    if (auto *arrow_bind_data = dynamic_cast<const BigqueryArrowScanBindData *>(bind_data_p)) {
        auto &gstate = global_state->Cast<BigqueryArrowScanGlobalState>();
        double progress = 0.0;
        if (arrow_bind_data->estimated_row_count > 0) {
            lock_guard<mutex> glock(gstate.lock);
            progress = 100.0 * double(gstate.position) / double(arrow_bind_data->estimated_row_count);
        }
        return MinValue<double>(100, progress);
    } else {
        auto &bind_data = bind_data_p->Cast<BigqueryLegacyScanBindData>();
        auto &gstate = global_state->Cast<BigqueryGlobalFunctionState>();
        double progress = 0.0;
        if (bind_data.estimated_row_count > 0) {
            lock_guard<mutex> glock(gstate.lock);
            progress = 100.0 * double(gstate.position) / double(bind_data.estimated_row_count);
        }
        return MinValue<double>(100, progress);
    }
}

static BindInfo BigqueryScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    // Try to cast to BigqueryArrowScanBindData first (optimized scan)
    if (auto *arrow_bind_data = dynamic_cast<const BigqueryArrowScanBindData *>(bind_data_p.get())) {
        BindInfo info(ScanType::EXTERNAL);
        if (arrow_bind_data->bq_table_entry) {
            info.table = arrow_bind_data->bq_table_entry.get_mutable();
        }
        return info;
    } else {
        auto &bind_data = bind_data_p->Cast<BigqueryLegacyScanBindData>();
        BindInfo info(ScanType::EXTERNAL);
        if (bind_data.bq_table_entry) {
            info.table = bind_data.bq_table_entry.get_mutable();
        }
        return info;
    }
}

BigqueryScanFunction::BigqueryScanFunction()
    : TableFunction("bigquery_scan",
                    {LogicalType::VARCHAR},
                    BigqueryScanFunction::BigqueryScanExecute,
                    BigqueryScanFunction::BigqueryScanBind,
                    BigqueryScanFunction::BigqueryScanInitGlobalState,
                    BigqueryScanFunction::BigqueryScanInitLocalState) {
    to_string = BigqueryScanToString;
    cardinality = BigqueryScanCardinality;
    table_scan_progress = BigqueryScanProgress;
    get_bind_info = BigqueryScanGetBindInfo;

    projection_pushdown = true;
    filter_pushdown = true;

    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["filter"] = LogicalType::VARCHAR;
    named_parameters["use_legacy_scan"] = LogicalType::BOOLEAN;
}

} // namespace bigquery
} // namespace duckdb
