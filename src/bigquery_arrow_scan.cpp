#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include "bigquery_arrow_reader.hpp"
#include "bigquery_arrow_scan.hpp"
#include "bigquery_client.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"

#include <arrow/c/bridge.h>
#include <arrow/util/iterator.h>
#include <iostream>
#include <limits>

namespace duckdb {
namespace bigquery {

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

unique_ptr<FunctionData> BigqueryArrowScanFunction::BigqueryArrowScanBind(ClientContext &context,
                                                                          TableFunctionBindInput &input,
                                                                          vector<LogicalType> &return_types,
                                                                          vector<string> &names) {
    // Parse table name parameter
    if (input.inputs.empty()) {
        throw BinderException("bigquery_arrow_scan: table name must be provided");
    }

    auto table_string = input.inputs[0].GetValue<string>();
    auto table_ref = BigqueryUtils::ParseTableString(table_string);
    if (!table_ref.has_dataset_id() || !table_ref.has_table_id()) {
        throw BinderException("Invalid table string: %s", table_string);
    }

    // Parse named parameters
    string billing_project_id, api_endpoint, grpc_endpoint, filter_condition;
    SetFromNamedParameters(input, billing_project_id, api_endpoint, grpc_endpoint, filter_condition);

    // Initialize bind data
    auto bind_data = make_uniq<BigqueryArrowScanBindData>();
    bind_data->table_ref = table_ref;
    bind_data->filter_condition = filter_condition;
    bind_data->bq_config = BigqueryConfig(table_ref.project_id)
                               .SetDatasetId(table_ref.dataset_id)
                               .SetBillingProjectId(billing_project_id)
                               .SetApiEndpoint(api_endpoint)
                               .SetGrpcEndpoint(grpc_endpoint);
    bind_data->bq_client = make_shared_ptr<BigqueryClient>(bind_data->bq_config);

    ColumnList columns;
    vector<unique_ptr<Constraint>> constraints;
    bind_data->bq_client->GetTableInfo(table_ref.dataset_id, table_ref.table_id, columns, constraints);

    auto arrow_schema_ptr = BigqueryUtils::BuildArrowSchema(columns);
    auto status = arrow::ExportSchema(*std::move(arrow_schema_ptr), &bind_data->schema_root.arrow_schema);
    if (!status.ok()) {
        throw BinderException("Arrow schema export failed: " + status.ToString());
    }

    // Convert Arrow schema to DuckDB types and names
    ArrowTableFunction::PopulateArrowTableType(DBConfig::GetConfig(context),
                                               bind_data->arrow_table,
                                               bind_data->schema_root,
                                               names,
                                               return_types);

    if (return_types.empty()) {
        throw BinderException("BigQuery table has no columns");
    }

    // Store schema information
    bind_data->names = names;
    bind_data->all_types = return_types;
    return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> BigqueryArrowScanFunction::BigqueryArrowScanInitGlobal(
    ClientContext &context,
    TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->CastNoConst<BigqueryArrowScanBindData>();

    // Build selected fields for BigQuery (exclude ROWID columns)
    vector<string> selected_fields;
    selected_fields.reserve(input.column_ids.size());

    for (auto col_id : input.column_ids) {
        if (col_id == COLUMN_IDENTIFIER_ROW_ID || col_id < 0) {
            continue;
        }
        selected_fields.emplace_back(bind_data.names[col_id]);
    }

    // Build filter condition for BigQuery
    string filter_string = bind_data.filter_condition;
    if (BigquerySettings::ExperimentalFilterPushdown() && filter_string.empty() && input.filters &&
        !input.filters->filters.empty()) {

        for (auto &filter : input.filters->filters) {
            column_t logical_col = input.column_ids[filter.first];
            if (logical_col == COLUMN_IDENTIFIER_ROW_ID || logical_col < 0) {
                throw InvalidInputException("ROWID cannot be referenced in a WHERE clause for BigQuery tables");
            }

            if (!filter_string.empty()) {
                filter_string += " AND ";
            }
            const string &column_name = bind_data.names[logical_col];
            filter_string += BigquerySQL::TransformFilter(column_name, *filter.second);
        }
    }

    // Initialize the BigQuery arrow reader
    idx_t max_read_streams = BigquerySettings::GetMaxReadStreams(context);
    auto bq_arrow_reader = bind_data.bq_client->CreateArrowReader( //
        bind_data.table_ref.dataset_id,
        bind_data.table_ref.table_id,
        max_read_streams,
        selected_fields,
        filter_string //
    );

    // Wrap reader in a factory so every thread can open its own stream
    auto factory = make_shared_ptr<BigqueryStreamFactory>(bq_arrow_reader);
    bind_data.factory_dep()->factory = factory;
    bind_data.stream_factory_ptr = reinterpret_cast<uintptr_t>(factory.get());

    // Initialize global scan state
    auto gstate = make_uniq<BigqueryArrowScanGlobalState>();
    gstate->max_threads = max_read_streams;
    bind_data.estimated_row_count = bq_arrow_reader->GetEstimatedRowCount();

    // Set up type mapping from physical to logical columns
    auto arrow_schema = bq_arrow_reader->GetSchema();
    const idx_t phys_col_count = arrow_schema->num_fields();

    // Map column name → logical col_id for fast lookup
    unordered_map<string, column_t> name_to_bq_physical;
    name_to_bq_physical.reserve(bind_data.names.size());
    for (idx_t i = 0; i < bind_data.names.size(); ++i) {
        name_to_bq_physical.emplace(bind_data.names[i], i);
    }

    // Set up scanned types in physical column order
    gstate->scanned_types.clear();
    gstate->scanned_types.reserve(phys_col_count);
    for (idx_t phys_idx = 0; phys_idx < phys_col_count; ++phys_idx) {
        const string &field_name = arrow_schema->field(phys_idx)->name();
        auto col_id = name_to_bq_physical.at(field_name);
        if (bind_data.mapped_bq_types.empty()) {
            gstate->scanned_types.emplace_back(bind_data.all_types[col_id]);
        } else {
            gstate->scanned_types.emplace_back(bind_data.mapped_bq_types[col_id]);
        }
    }

    bool needs_projection = false;
    for (idx_t out_idx = 0; out_idx < input.projection_ids.size(); ++out_idx) {
        idx_t proj_id = input.projection_ids[out_idx];
        idx_t col_id = input.column_ids[proj_id];
        if (col_id == COLUMN_IDENTIFIER_ROW_ID || col_id < 0) {
            continue;
        }

        const string &name = bind_data.names[col_id];
        idx_t phys_idx = static_cast<idx_t>(arrow_schema->GetFieldIndex(name));
        if (phys_idx == static_cast<idx_t>(-1)) {
            throw InternalException("Column '" + name + "' not found in Arrow schema");
        }

        gstate->projection_ids.push_back(phys_idx);
        if (phys_idx != out_idx) {
            needs_projection = true;
        }
    }

    // Clear projection IDs if no reordering is needed
    if (!needs_projection) {
        gstate->projection_ids.clear();
    }

    // Create the Arrow scan stream
    gstate->stream = ::duckdb::ProduceArrowScan(bind_data, input.column_ids, input.filters.get());
    return std::move(gstate);
}

unique_ptr<LocalTableFunctionState> BigqueryArrowScanFunction::BigqueryArrowScanInitLocal(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state_p) {
    auto &client_context = context.client;
    auto &bind_data = input.bind_data->CastNoConst<BigqueryArrowScanBindData>();
    auto &global_state = global_state_p->Cast<ArrowScanGlobalState>();

    auto current_chunk = make_uniq<ArrowArrayWrapper>();
    auto result = make_uniq<BigqueryArrowScanLocalState>(std::move(current_chunk), client_context);

    auto sorted_column_ids = input.column_ids;
    std::sort(sorted_column_ids.begin(), sorted_column_ids.end());

    result->column_ids = sorted_column_ids;
    result->filters = input.filters.get();

    if (!bind_data.projection_pushdown_enabled) {
        result->column_ids.clear();
    } else if (!input.projection_ids.empty() || bind_data.requires_cast) {
        auto &asgs = global_state_p->Cast<ArrowScanGlobalState>();
        result->all_columns.Initialize(client_context, asgs.scanned_types);
    }
    if (!ArrowTableFunction::ArrowScanParallelStateNext(client_context, input.bind_data.get(), *result, global_state)) {
        return nullptr;
    }

    return std::move(result);
}

void BigqueryArrowScanFunction::BigqueryArrowScanExecute(ClientContext &ctx,
                                                         TableFunctionInput &data_p,
                                                         DataChunk &output) {
    if (!data_p.local_state) {
        return;
    }

    auto &data = data_p.bind_data->CastNoConst<BigqueryArrowScanBindData>();
    auto &state = data_p.local_state->Cast<BigqueryArrowScanLocalState>();
    auto &gstate = data_p.global_state->Cast<BigqueryArrowScanGlobalState>();

    //! Out of tuples in this chunk
    if (state.chunk_offset >= static_cast<idx_t>(state.chunk->arrow_array.length)) {
        if (!ArrowTableFunction::ArrowScanParallelStateNext(ctx, data_p.bind_data.get(), state, gstate)) {
            return;
        }
    }

    auto output_size = MinValue<idx_t>( //
        STANDARD_VECTOR_SIZE,
        NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
    data.lines_read += output_size;

    if (gstate.CanRemoveFilterColumns()) {
        state.all_columns.Reset();
        state.all_columns.SetCardinality(output_size);

        ArrowTableFunction::ArrowToDuckDB(state,
                                          data.arrow_table.GetColumns(),
                                          state.all_columns,
                                          data.lines_read - output_size);

        // If no projection is needed, we can directly reference the columns
        // or cast them if required
        if (!data.requires_cast) {
            // Reference columns directly
            output.ReferenceColumns(state.all_columns, gstate.projection_ids);
        } else {
            for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
                auto proj_id = gstate.projection_ids[col_idx];
                VectorOperations::Cast(ctx,
                                       state.all_columns.data[proj_id],
                                       output.data[col_idx],
                                       state.all_columns.size());
            }
        }
    } else {
        output.SetCardinality(output_size);
        if (!data.requires_cast) {
            // Direct write to output
            ArrowTableFunction::ArrowToDuckDB(state,
                                              data.arrow_table.GetColumns(),
                                              output,
                                              data.lines_read - output_size);
        } else {
            state.all_columns.Reset();
            state.all_columns.SetCardinality(output_size);
            ArrowTableFunction::ArrowToDuckDB(state,
                                              data.arrow_table.GetColumns(),
                                              state.all_columns,
                                              data.lines_read - output_size);

            // Cast each column to the output type
            for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
                VectorOperations::Cast(ctx,
                                       state.all_columns.data[col_idx],
                                       output.data[col_idx],
                                       state.all_columns.size());
            }
        }
    }

    output.SetCardinality(output_size);
    output.Verify();
    state.chunk_offset += output.size();

    lock_guard<mutex> glock(gstate.lock);
    gstate.position += output.size();
}

static InsertionOrderPreservingMap<string> BigqueryArrowScanToString(TableFunctionToStringInput &input) {
    D_ASSERT(input.bind_data);
    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<BigqueryArrowScanBindData>();
    result["Table"] = bind_data.TableString();
    return result;
}

static BindInfo BigqueryArrowScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BigqueryArrowScanBindData>();
    BindInfo info(ScanType::EXTERNAL);
    if (bind_data.bq_table_entry) {
        info.table = bind_data.bq_table_entry.get_mutable();
    }
    return info;
}

unique_ptr<NodeStatistics> BigqueryArrowScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BigqueryArrowScanBindData>();
    return make_uniq<NodeStatistics>(bind_data.estimated_row_count, bind_data.estimated_row_count);
}

double BigqueryArrowScanProgress(ClientContext &context,
                                 const FunctionData *bind_data_p,
                                 const GlobalTableFunctionState *global_state) {
    auto &bind_data = bind_data_p->Cast<BigqueryArrowScanBindData>();
    auto &gstate = global_state->Cast<BigqueryArrowScanGlobalState>();
    double progress = 0.0;
    if (bind_data.estimated_row_count > 0) {
        lock_guard<mutex> glock(gstate.lock);
        progress = 100.0 * double(gstate.position) / double(bind_data.estimated_row_count);
    }
    return MinValue<double>(100, progress);
}

BigqueryArrowScanFunction::BigqueryArrowScanFunction()
    : TableFunction("bigquery_arrow_scan",
                    {LogicalType::VARCHAR},
                    BigqueryArrowScanFunction::BigqueryArrowScanExecute,
                    BigqueryArrowScanFunction::BigqueryArrowScanBind,
                    BigqueryArrowScanFunction::BigqueryArrowScanInitGlobal,
                    BigqueryArrowScanFunction::BigqueryArrowScanInitLocal) {
    projection_pushdown = true;
    filter_pushdown = true;
    filter_prune = true;

    to_string = BigqueryArrowScanToString;
    cardinality = BigqueryArrowScanCardinality;
    table_scan_progress = BigqueryArrowScanProgress;
    get_bind_info = BigqueryArrowScanGetBindInfo;

    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["filter"] = LogicalType::VARCHAR;
}

} // namespace bigquery
} // namespace duckdb
