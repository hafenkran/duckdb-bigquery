#include "bigquery_scan.hpp"
#include "bigquery_arrow_scan.hpp"
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

bool DetermineLegacyScan(ClientContext &context, const TableFunctionBindInput &input) {
    // Check for explicit use_legacy_scan parameter
    for (auto &kv : input.named_parameters) {
        auto lower_key = StringUtil::Lower(kv.first);
        if (lower_key == "use_legacy_scan") {
            return BooleanValue::Get(kv.second);
        }
    }

    // Fall back to global setting
    return BigquerySettings::UseLegacyScan();
}

static void SetFromNamedParameters(const TableFunctionBindInput &input,
                                   string &billing_project_id,
                                   string &api_endpoint,
                                   string &grpc_endpoint,
                                   string &filter_condition) {
    for (auto &kv : input.named_parameters) {
        auto lower_option = StringUtil::Lower(kv.first);
        if (lower_option == "billing_project") {
            billing_project_id = kv.second.GetValue<string>();
        } else if (lower_option == "api_endpoint") {
            api_endpoint = kv.second.GetValue<string>();
        } else if (lower_option == "grpc_endpoint") {
            grpc_endpoint = kv.second.GetValue<string>();
        } else if (lower_option == "filter") {
            filter_condition = kv.second.GetValue<string>();
        }
    }
}

struct BigqueryGlobalFunctionState : public GlobalTableFunctionState {
    explicit BigqueryGlobalFunctionState(shared_ptr<BigqueryArrowReader> arrow_reader, idx_t max_threads)
        : position(0), max_threads(max_threads), arrow_reader(std::move(arrow_reader)) {
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

static unique_ptr<FunctionData> BigqueryLegacyScanBind(ClientContext &context,
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

    auto result = make_uniq<BigqueryLegacyScanBindData>();
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

    auto arrow_reader = result->bq_client->CreateArrowReader(table_ref,
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

static unique_ptr<FunctionData> BigqueryScanBind(ClientContext &context,
                                                 TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types,
                                                 vector<string> &names) {
    bool use_legacy = DetermineLegacyScan(context, input);

    if (use_legacy) {
        return BigqueryLegacyScanBind(context, input, return_types, names);
    } else {
        return BigqueryArrowScanFunction::BigqueryArrowScanBind(context, input, return_types, names);
    }
}

static unique_ptr<GlobalTableFunctionState> BigqueryLegacyScanInitGlobalState(ClientContext &context,
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
    auto arrow_reader = bind_data.bq_client->CreateArrowReader(bind_data.table_ref,
                                                               k_max_read_streams,
                                                               selected_fields,
                                                               filter_string);

    auto &mutable_bind_data = input.bind_data->CastNoConst<BigqueryLegacyScanBindData>();
    mutable_bind_data.estimated_row_count = idx_t(arrow_reader->GetEstimatedRowCount());
    auto result = make_uniq<BigqueryGlobalFunctionState>(arrow_reader, k_max_read_streams);
    return std::move(result);
}

static unique_ptr<LocalTableFunctionState> BigqueryLegacyScanInitLocalState(ExecutionContext &context,
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

static void BigqueryLegacyScanExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
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

static unique_ptr<GlobalTableFunctionState> BigqueryScanInitGlobalState(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
    // Check bind data type and forward to appropriate implementation
    if (dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        return BigqueryArrowScanFunction::BigqueryArrowScanInitGlobal(context, input);
    } else {
        return BigqueryLegacyScanInitGlobalState(context, input);
    }
}

static unique_ptr<LocalTableFunctionState> BigqueryScanInitLocalState(ExecutionContext &context,
                                                                      TableFunctionInitInput &input,
                                                                      GlobalTableFunctionState *global_state) {
    // Check bind data type and forward to appropriate implementation
    if (dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        return BigqueryArrowScanFunction::BigqueryArrowScanInitLocal(context, input, global_state);
    } else {
        return BigqueryLegacyScanInitLocalState(context, input, global_state);
    }
}

static void BigqueryScanExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    // Check bind data type and forward to appropriate implementation
    if (dynamic_cast<const BigqueryArrowScanBindData *>(data.bind_data.get())) {
        BigqueryArrowScanFunction::BigqueryArrowScanExecute(context, data, output);
    } else {
        BigqueryLegacyScanExecute(context, data, output);
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
                    BigqueryScanExecute,
                    BigqueryScanBind,
                    BigqueryScanInitGlobalState,
                    BigqueryScanInitLocalState) {
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

static unique_ptr<FunctionData> BigqueryQueryBind(ClientContext &context,
                                                  TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types,
                                                  vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto query_string = input.inputs[1].GetValue<string>();

    bool use_legacy = DetermineLegacyScan(context, input);

    string billing_project_id, api_endpoint, grpc_endpoint, filter_condition;
    SetFromNamedParameters(input, billing_project_id, api_endpoint, grpc_endpoint, filter_condition);

    if (!use_legacy) {
        auto bind_data = make_uniq<BigqueryArrowScanBindData>();
        bind_data->query = query_string;
        bind_data->estimated_row_count = 1;

        auto &database_manager = DatabaseManager::Get(context);
        auto database = database_manager.GetDatabase(context, dbname_or_project_id);
        if (database) {
            auto &catalog = database->GetCatalog();
            if (catalog.GetCatalogType() != "bigquery") {
                throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
            }
            if (!billing_project_id.empty() || !api_endpoint.empty() || !grpc_endpoint.empty()) {
                throw BinderException("Named parameters are not supported for attached databases");
            }

            auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
            auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);

            bind_data->bq_config = bigquery_catalog.config;
            bind_data->bq_client = transaction.GetBigqueryClient();
        } else {
            auto bq_config = BigqueryConfig(dbname_or_project_id)
                                 .SetBillingProjectId(billing_project_id)
                                 .SetApiEndpoint(api_endpoint)
                                 .SetGrpcEndpoint(grpc_endpoint);
            auto bq_client = make_shared_ptr<BigqueryClient>(bq_config);

            bind_data->bq_config = bq_config;
            bind_data->bq_client = bq_client;
        }

        ColumnList columns;
        vector<unique_ptr<Constraint>> constraints;
        bind_data->bq_client->GetTableInfoForQuery(query_string, columns, constraints);

        auto arrow_schema_ptr = BigqueryUtils::BuildArrowSchema(columns);
        auto status = arrow::ExportSchema(*std::move(arrow_schema_ptr), &bind_data->schema_root.arrow_schema);
        if (!status.ok()) {
            throw BinderException("Arrow schema export failed: " + status.ToString());
        }

        ArrowTableFunction::PopulateArrowTableType(DBConfig::GetConfig(context),
                                                   bind_data->arrow_table,
                                                   bind_data->schema_root,
                                                   names,
                                                   return_types);

        if (return_types.empty()) {
            throw BinderException("BigQuery query has no columns: " + query_string);
        }

        bind_data->names = names;
        bind_data->all_types = return_types;
        return std::move(bind_data);
    } else {
        // Legacy implementation (V1)
        auto bind_data = make_uniq<BigqueryLegacyScanBindData>();
        bind_data->query = query_string;
        bind_data->estimated_row_count = 1;

        auto &database_manager = DatabaseManager::Get(context);
        auto database = database_manager.GetDatabase(context, dbname_or_project_id);
        if (database) {
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
                                 .SetGrpcEndpoint(grpc_endpoint);
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
}

static unique_ptr<GlobalTableFunctionState> BigqueryQueryInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    // Execute query first - this is common for both implementations
    if (dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        // Arrow scan implementation
        auto &mutable_bind_data = input.bind_data->CastNoConst<BigqueryArrowScanBindData>();

        // Execute the query and get destination table
        auto query_response = mutable_bind_data.bq_client->ExecuteQuery(mutable_bind_data.query);
        auto job = mutable_bind_data.bq_client->GetJobByReference(query_response.job_reference());

        if (job.status().has_error_result()) {
            throw BinderException(job.status().error_result().message());
        }

        auto destination_table = job.configuration().query().destination_table();
        auto table_ref = BigqueryTableRef(destination_table.project_id(),
                                          destination_table.dataset_id(),
                                          destination_table.table_id());
        mutable_bind_data.table_ref = table_ref;
        return BigqueryArrowScanFunction::BigqueryArrowScanInitGlobal(context, input);
    } else {
        // Legacy scan implementation
        auto &bind_data = input.bind_data->CastNoConst<BigqueryLegacyScanBindData>();

        // Execute the query and get destination table
        auto query_response = bind_data.bq_client->ExecuteQuery(bind_data.query);
        auto job = bind_data.bq_client->GetJobByReference(query_response.job_reference());

        if (job.status().has_error_result()) {
            throw BinderException(job.status().error_result().message());
        }

        auto destination_table = job.configuration().query().destination_table();
        auto table_ref = BigqueryTableRef(destination_table.project_id(),
                                          destination_table.dataset_id(),
                                          destination_table.table_id());
        bind_data.table_ref = table_ref;
        return BigqueryLegacyScanInitGlobalState(context, input);
    }
}

static unique_ptr<LocalTableFunctionState> BigqueryQueryInitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
    if (dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        return BigqueryArrowScanFunction::BigqueryArrowScanInitLocal(context, input, global_state);
    } else {
        return BigqueryLegacyScanInitLocalState(context, input, global_state);
    }
}

static void BigqueryQueryExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    if (dynamic_cast<const BigqueryArrowScanBindData *>(data.bind_data.get())) {
        BigqueryArrowScanFunction::BigqueryArrowScanExecute(context, data, output);
    } else {
        BigqueryLegacyScanExecute(context, data, output);
    }
}

static BindInfo BigqueryQueryGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    // Try to cast to BigqueryArrowScanBindData first (experimental scan)
    if (auto arrow_bind_data = dynamic_cast<const BigqueryArrowScanBindData *>(bind_data_p.get())) {
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

static InsertionOrderPreservingMap<string> BigqueryQueryToString(TableFunctionToStringInput &input) {
    D_ASSERT(input.bind_data);
    InsertionOrderPreservingMap<string> result;

    // Try to cast to BigqueryArrowScanBindData first (experimental scan)
    if (auto *arrow_bind_data = dynamic_cast<const BigqueryArrowScanBindData *>(input.bind_data.get())) {
        result["Query"] = arrow_bind_data->query;
        result["Table"] = arrow_bind_data->TableString();
    } else {
        auto &bind_data = input.bind_data->Cast<BigqueryLegacyScanBindData>();
        result["Query"] = bind_data.query;
        result["Table"] = bind_data.TableString();
    }
    return result;
}

static unique_ptr<NodeStatistics> BigqueryQueryCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    // Try to cast to BigqueryArrowScanBindData first (experimental scan)
    if (auto *arrow_bind_data = dynamic_cast<const BigqueryArrowScanBindData *>(bind_data_p)) {
        return make_uniq<NodeStatistics>(arrow_bind_data->estimated_row_count, arrow_bind_data->estimated_row_count);
    } else {
        auto &bind_data = bind_data_p->Cast<BigqueryLegacyScanBindData>();
        return make_uniq<NodeStatistics>(bind_data.estimated_row_count, bind_data.estimated_row_count);
    }
}

static double BigqueryQueryProgress(ClientContext &context,
                                    const FunctionData *bind_data_p,
                                    const GlobalTableFunctionState *global_state) {
    // Try to cast to BigqueryArrowScanBindData first (experimental scan)
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

BigqueryQueryFunction::BigqueryQueryFunction()
    : TableFunction("bigquery_query",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    BigqueryQueryExecute,
                    BigqueryQueryBind,
                    BigqueryQueryInitGlobal,
                    BigqueryQueryInitLocal) {
    to_string = BigqueryQueryToString;
    cardinality = BigqueryQueryCardinality;
    table_scan_progress = BigqueryQueryProgress;
    get_bind_info = BigqueryQueryGetBindInfo;

    projection_pushdown = true;
    filter_pushdown = true;
    filter_prune = true;

    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["use_legacy_scan"] = LogicalType::BOOLEAN;
}

} // namespace bigquery
} // namespace duckdb
