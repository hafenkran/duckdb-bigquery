#include "bigquery_scan.hpp"
#include "bigquery_arrow_reader.hpp"
#include "bigquery_client.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include "google/cloud/bigquery/storage/v1/storage.pb.h"
#include "google/cloud/bigquery/storage/v1/stream.pb.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"


namespace duckdb {
namespace bigquery {

struct BigqueryGlobalFunctionState : public GlobalTableFunctionState {
    explicit BigqueryGlobalFunctionState(shared_ptr<BigqueryArrowReader> arrow_reader, idx_t max_threads)
        : arrow_reader(std::move(arrow_reader)), position(0), max_threads(max_threads) {
    }

    mutable mutex lock;
    atomic<idx_t> position;
    idx_t max_threads;

    // The index of the next stream to read (i.e., current file + 1)
    atomic<idx_t> next_stream = 0;
    shared_ptr<BigqueryArrowReader> arrow_reader;

    idx_t MaxThreads() const override {
        return max_threads;
    }

    idx_t NextStreamIndex() {
        return next_stream.fetch_add(1);
    }
};


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
    google::cloud::v2_33::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse> read_rows;
    google::cloud::v2_33::StreamRange<google::cloud::bigquery::storage::v1::ReadRowsResponse>::iterator read_rows_it;
    google::cloud::v2_33::StatusOr<google::cloud::bigquery::storage::v1::ReadRowsResponse> rows;
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

static void SetFromNamedParameters(const TableFunctionBindInput &input,
                                   string &billing_project_id,
                                   string &api_endpoint,
                                   string &grpc_endpoint,
                                   string &filter_condition) {
    for (auto &kv : input.named_parameters) {
        auto loption = StringUtil::Lower(kv.first);
        if (loption == "billing_project") {
            billing_project_id = kv.second.GetValue<string>();
        } else if (loption == "api_endpoint") {
            api_endpoint = kv.second.GetValue<string>();
        } else if (loption == "grpc_endpoint") {
            grpc_endpoint = kv.second.GetValue<string>();
        } else if (loption == "filter") {
            filter_condition = kv.second.GetValue<string>();
        }
    }
}

static unique_ptr<FunctionData> BigqueryScanBind(ClientContext &context,
                                                 TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types,
                                                 vector<string> &names) {
    auto table_string = input.inputs[0].GetValue<string>();
    auto table_ref = BigqueryUtils::ParseTableString(table_string);
    if (!table_ref.has_dataset_id() || !table_ref.has_table_id()) {
        throw ParserException("Invalid table string: %s", table_string);
    }

    string billing_project_id, api_endpoint, grpc_endpoint, filter_condition;
    SetFromNamedParameters(input, billing_project_id, api_endpoint, grpc_endpoint, filter_condition);

    auto result = make_uniq<BigqueryBindData>();
    result->table_ref = table_ref;
    result->filter_condition = filter_condition;
    result->config = BigqueryConfig(table_ref.project_id)
                         .SetDatasetId(table_ref.dataset_id)
                         .SetBillingProjectId(billing_project_id)
                         .SetApiEndpoint(api_endpoint)
                         .SetGrpcEndpoint(grpc_endpoint);
    result->bq_client = make_shared_ptr<BigqueryClient>(result->config);

    ColumnList columns;
    vector<unique_ptr<Constraint>> constraints;

    string filter_cond = "";
    if (!filter_condition.empty()) {
        filter_cond = filter_condition;
    }

    auto arrow_reader = result->bq_client->CreateArrowReader(table_ref.dataset_id,
                                                             table_ref.table_id,
                                                             1,
                                                             std::vector<string>(),
                                                             filter_cond);
    arrow_reader->MapTableInfo(columns, constraints);

    for (auto &column : columns.Logical()) {
        names.push_back(column.GetName());
        return_types.push_back(column.GetType());
    }
    if (names.empty()) {
        auto table_ref = arrow_reader->GetTableRef();
        throw std::runtime_error("no columns for table " + table_ref.table_id);
    }

    // TODO GetMaxRowId
    result->estimated_row_count = idx_t(arrow_reader->GetEstimatedRowCount());
    result->names = names;
    result->types = return_types;
    return std::move(result);
}


static unique_ptr<FunctionData> BigqueryQueryBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto query_string = input.inputs[1].GetValue<string>();

    string billing_project_id, api_endpoint, grpc_endpoint, filter_condition;
    SetFromNamedParameters(input, billing_project_id, api_endpoint, grpc_endpoint, filter_condition);

    auto bind_data = make_uniq<BigqueryBindData>();
    bind_data->query = query_string;
    bind_data->estimated_row_count = 1;

    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, dbname_or_project_id);
    if (database) {
        // Use attached database for this operation
        auto &catalog = database->GetCatalog();
        if (catalog.GetCatalogType() != "bigquery") {
            throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
        }
        if (!billing_project_id.empty() || !api_endpoint.empty() || !grpc_endpoint.empty()) {
            throw BinderException("Named parameters are not supported for attached databases");
        }

        auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
        auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);

        bind_data->config = bigquery_catalog.config;
        bind_data->bq_client = transaction.GetBigqueryClient();
    } else {
        // Use the provided project_id of the gcp project
        auto bq_config = BigqueryConfig(dbname_or_project_id)
                             .SetBillingProjectId(billing_project_id)
                             .SetApiEndpoint(api_endpoint)
                             .SetGrpcEndpoint(api_endpoint);
        auto bq_client = make_shared_ptr<BigqueryClient>(bq_config);

        bind_data->config = bq_config;
        bind_data->bq_client = bq_client;
    }

    ColumnList columns;
    vector<unique_ptr<Constraint>> constraints;
    bind_data->bq_client->GetTableInfoForQuery(query_string, columns, constraints);

    for (auto &column : columns.Logical()) {
        names.push_back(column.GetName());
        return_types.push_back(column.GetType());
    }
    if (names.empty()) {
        throw std::runtime_error("no columns for query: " + query_string);
    }

    bind_data->names = names;
    bind_data->types = return_types;
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> BigqueryInitGlobalState(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    auto &bind_data = (BigqueryBindData &)*input.bind_data;

    if (bind_data.RequiresQueryExec()) {
        auto query_response = bind_data.bq_client->ExecuteQuery(bind_data.query);
        auto job = bind_data.bq_client->GetJob(query_response.job_reference().job_id(),
                                               query_response.job_reference().location().value());
        if (job.status().has_error_result()) {
            throw BinderException(job.status().error_result().message());
        }

        auto destination_table = job.configuration().query().destination_table();
        auto table_ref = BigqueryTableRef(destination_table.project_id(),
                                          destination_table.dataset_id(),
                                          destination_table.table_id());
        bind_data.table_ref = table_ref;
    }

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
    auto arrow_reader = bind_data.bq_client->CreateArrowReader(bind_data.table_ref.dataset_id,
                                                               bind_data.table_ref.table_id,
                                                               k_max_read_streams,
                                                               selected_fields,
                                                               filter_string);

    bind_data.estimated_row_count = idx_t(arrow_reader->GetEstimatedRowCount());
    auto result = make_uniq<BigqueryGlobalFunctionState>(arrow_reader, k_max_read_streams);
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

    lstate->column_ids = input.column_ids;
    lstate->column_ids_ranked = CalculateRanks(input.column_ids);

    auto streamidx = gstate.NextStreamIndex();
    lstate->read_stream = gstate.arrow_reader->GetStream(streamidx);
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
    lstate.ScanNextChunk(output, gstate);
    lock_guard<mutex> glock(gstate.lock);
    gstate.position += output.size();
}

static InsertionOrderPreservingMap<string> BigqueryToString(TableFunctionToStringInput &input) {
    D_ASSERT(input.bind_data);
    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<BigqueryBindData>();
    result["Table"] = bind_data.TableString();
    return result;
}

unique_ptr<NodeStatistics> BigqueryScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BigqueryBindData>();
    return make_uniq<NodeStatistics>(bind_data.estimated_row_count, bind_data.estimated_row_count);
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

static BindInfo BigqueryGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BigqueryBindData>();
    BindInfo info(ScanType::EXTERNAL);
    if (bind_data.bq_table_entry) {
        info.table = bind_data.bq_table_entry.get_mutable();
    }
    return info;
}

BigqueryScanFunction::BigqueryScanFunction()
    : TableFunction("bigquery_scan",
                    {LogicalType::VARCHAR},
                    BigqueryScan,
                    BigqueryScanBind,
                    BigqueryInitGlobalState,
                    BigqueryInitLocalState) {
    to_string = BigqueryToString;
    cardinality = BigqueryScanCardinality;
    table_scan_progress = BigqueryScanProgress;
    get_bind_info = BigqueryGetBindInfo;

    projection_pushdown = true;
    filter_pushdown = true;

    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["filter"] = LogicalType::VARCHAR;
}

BigqueryQueryFunction::BigqueryQueryFunction()
    : TableFunction("bigquery_query",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    BigqueryScan,
                    BigqueryQueryBind,
                    BigqueryInitGlobalState,
                    BigqueryInitLocalState) {
    to_string = BigqueryToString;
    cardinality = BigqueryScanCardinality;
    table_scan_progress = BigqueryScanProgress;
    get_bind_info = BigqueryGetBindInfo;

    projection_pushdown = true;
    filter_pushdown = true;

    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
}

} // namespace bigquery
} // namespace duckdb
