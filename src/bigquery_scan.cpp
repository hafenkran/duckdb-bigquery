#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include "bigquery_arrow_reader.hpp"
#include "bigquery_client.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"

#include <arrow/c/bridge.h>
#include <arrow/util/iterator.h>
#include <unordered_map>

namespace duckdb {

unique_ptr<ArrowArrayStreamWrapper> ProduceArrowScan(const ArrowScanFunctionData &function,
                                                     const vector<column_t> &column_ids,
                                                     TableFilterSet *filters);

namespace bigquery {

unique_ptr<FunctionData> BigqueryScanFunction::BigqueryScanBind(ClientContext &context,
                                                                TableFunctionBindInput &input,
                                                                vector<LogicalType> &return_types,
                                                                vector<string> &names) {
    if (input.inputs.empty()) {
        throw BinderException("bigquery_scan: table name must be provided");
    }

    auto table_string = input.inputs[0].GetValue<string>();
    auto table_ref = BigqueryUtils::ParseTableString(table_string);
    if (!table_ref.HasDatasetId() || !table_ref.HasTableId()) {
        throw BinderException("Invalid table string: %s", table_string);
    }

    auto params = BigQueryCommonParameters::ParseFromNamedParameters(input.named_parameters);

    auto bind_data = make_uniq<BigqueryScanBindData>();
    bind_data->table_ref = table_ref;
    bind_data->filter_condition = params.filter;
    bind_data->bq_config = BigqueryConfig(table_ref.project_id)
                               .SetDatasetId(table_ref.dataset_id)
                               .SetBillingProjectId(params.billing_project)
                               .SetApiEndpoint(params.api_endpoint)
                               .SetGrpcEndpoint(params.grpc_endpoint);
    bind_data->bq_client = make_shared_ptr<BigqueryClient>(context, bind_data->bq_config);

    ColumnList columns;
    vector<unique_ptr<Constraint>> constraints;
    bind_data->bq_client->GetTableInfo(table_ref.dataset_id, table_ref.table_id, columns, constraints);

    auto arrow_schema_ptr = BigqueryUtils::BuildArrowSchema(columns);
    auto status = arrow::ExportSchema(*std::move(arrow_schema_ptr), &bind_data->schema_root.arrow_schema);
    if (!status.ok()) {
        throw BinderException("Arrow schema export failed: " + status.ToString());
    }

    vector<LogicalType> mapped_bq_types;
    BigqueryUtils::PopulateAndMapArrowTableTypes(context,
                                                 bind_data->arrow_table,
                                                 bind_data->schema_root,
                                                 names,
                                                 return_types,
                                                 mapped_bq_types,
                                                 &columns);

    bind_data->names = names;
    bind_data->all_types = return_types;
    if (!mapped_bq_types.empty()) {
        bind_data->mapped_bq_types = std::move(mapped_bq_types);
        bind_data->requires_cast = true;
    } else {
        bind_data->requires_cast = false;
    }

    return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> BigqueryScanFunction::BigqueryScanInitGlobalState(ClientContext &context,
                                                                                       TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->CastNoConst<BigqueryScanBindData>();

    vector<string> selected_fields;
    selected_fields.reserve(input.column_ids.size());
    for (auto col_id : input.column_ids) {
        if (col_id == COLUMN_IDENTIFIER_ROW_ID || col_id < 0) {
            continue;
        }
        selected_fields.emplace_back(bind_data.names[col_id]);
    }

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

    idx_t max_read_streams = BigquerySettings::GetMaxReadStreams(context);
    auto bq_arrow_reader =
        bind_data.bq_client->CreateArrowReader(bind_data.table_ref, max_read_streams, selected_fields, filter_string);

    auto factory = make_shared_ptr<BigqueryStreamFactory>(bq_arrow_reader);
    auto factory_dependency = bind_data.GetFactoryDependency();
    if (!factory_dependency) {
        throw InternalException("Factory dependency not initialized");
    }
    factory_dependency->factory = factory;
    bind_data.stream_factory_ptr = reinterpret_cast<uintptr_t>(factory.get());

    auto gstate = make_uniq<BigqueryScanGlobalState>();
    gstate->max_threads = max_read_streams;
    bind_data.estimated_row_count = bq_arrow_reader->GetEstimatedRowCount();

    auto arrow_schema = bq_arrow_reader->GetSchema();
    const idx_t phys_col_count = arrow_schema->num_fields();

    unordered_map<string, column_t> name_to_bq_physical;
    name_to_bq_physical.reserve(bind_data.names.size());
    for (idx_t i = 0; i < bind_data.names.size(); ++i) {
        name_to_bq_physical.emplace(bind_data.names[i], i);
    }

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

    vector<idx_t> column_physical_positions;
    column_physical_positions.reserve(input.column_ids.size());
    const idx_t invalid_phys_idx = NumericLimits<idx_t>::Maximum();
    bool contains_rowid = false;
    for (idx_t col_pos = 0; col_pos < input.column_ids.size(); ++col_pos) {
        idx_t col_id = input.column_ids[col_pos];
        if (col_id == COLUMN_IDENTIFIER_ROW_ID || col_id < 0) {
            contains_rowid = true;
            column_physical_positions.push_back(invalid_phys_idx);
            continue;
        }
        const string &name = bind_data.names[col_id];
        idx_t phys_idx = static_cast<idx_t>(arrow_schema->GetFieldIndex(name));
        if (phys_idx == static_cast<idx_t>(-1)) {
            throw InternalException("Column '" + name + "' not found in Arrow schema");
        }
        column_physical_positions.push_back(phys_idx);
    }

    bool requires_physical_reorder = false;
    vector<idx_t> projection_mapping;
    if (!input.projection_ids.empty()) {
        projection_mapping.reserve(input.projection_ids.size());
        for (idx_t out_idx = 0; out_idx < input.projection_ids.size(); ++out_idx) {
            idx_t col_pos = input.projection_ids[out_idx];
            D_ASSERT(col_pos < column_physical_positions.size());
            idx_t phys_idx = column_physical_positions[col_pos];
            if (phys_idx == invalid_phys_idx) {
                contains_rowid = true;
                requires_physical_reorder = true;
                break;
            }
            projection_mapping.push_back(phys_idx);
            if (phys_idx != col_pos) {
                requires_physical_reorder = true;
            }
        }
        if (!contains_rowid) {
            gstate->projection_ids = std::move(projection_mapping);
        } else {
            gstate->projection_ids.clear();
        }
    } else {
        projection_mapping.reserve(column_physical_positions.size());
        for (idx_t out_idx = 0; out_idx < column_physical_positions.size(); ++out_idx) {
            idx_t phys_idx = column_physical_positions[out_idx];
            if (phys_idx == invalid_phys_idx) {
                contains_rowid = true;
                requires_physical_reorder = true;
                break;
            }
            projection_mapping.push_back(phys_idx);
            if (phys_idx != out_idx && phys_idx != COLUMN_IDENTIFIER_ROW_ID && phys_idx >= 0) {
                requires_physical_reorder = true;
            }
        }
        if (!contains_rowid && requires_physical_reorder) {
            gstate->projection_ids = std::move(projection_mapping);
        } else {
            gstate->projection_ids.clear();
        }
    }

    gstate->stream = ::duckdb::ProduceArrowScan(bind_data, input.column_ids, input.filters.get());
    return std::move(gstate);
}

unique_ptr<LocalTableFunctionState> BigqueryScanFunction::BigqueryScanInitLocalState(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state_p) {
    auto &client_context = context.client;
    auto &bind_data = input.bind_data->CastNoConst<BigqueryScanBindData>();
    auto &global_state = global_state_p->Cast<ArrowScanGlobalState>();

    auto current_chunk = make_uniq<ArrowArrayWrapper>();
    auto result = make_uniq<BigqueryScanLocalState>(std::move(current_chunk), client_context);

    auto sorted_column_ids = input.column_ids;
    std::sort(sorted_column_ids.begin(), sorted_column_ids.end());

    result->column_ids = sorted_column_ids;
    result->filters = input.filters.get();
    if (!bind_data.projection_pushdown_enabled) {
        result->column_ids.clear();
    } else if (!input.projection_ids.empty() || bind_data.requires_cast || !global_state.projection_ids.empty()) {
        auto &asgs = global_state_p->Cast<ArrowScanGlobalState>();
        result->all_columns.Initialize(client_context, asgs.scanned_types);
    }
    if (!ArrowTableFunction::ArrowScanParallelStateNext(client_context, input.bind_data.get(), *result, global_state)) {
        return nullptr;
    }

    return std::move(result);
}

void BigqueryScanFunction::BigqueryScanExecute(ClientContext &ctx, TableFunctionInput &data_p, DataChunk &output) {
    if (!data_p.local_state) {
        return;
    }

    auto &data = data_p.bind_data->CastNoConst<BigqueryScanBindData>();
    auto &state = data_p.local_state->Cast<BigqueryScanLocalState>();
    auto &gstate = data_p.global_state->Cast<BigqueryScanGlobalState>();

    if (state.chunk_offset >= static_cast<idx_t>(state.chunk->arrow_array.length)) {
        if (!ArrowTableFunction::ArrowScanParallelStateNext(ctx, data_p.bind_data.get(), state, gstate)) {
            return;
        }
    }

    auto output_size =
        MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
    data.lines_read += output_size;

    auto ensure_column_id_coverage = [&](DataChunk &chunk) {
        if (!state.column_ids.empty() && state.column_ids.size() != chunk.ColumnCount()) {
            state.column_ids.clear();
        }
    };

    bool only_rowid_columns = !state.column_ids.empty();
    for (auto col_id : state.column_ids) {
        if (col_id != COLUMN_IDENTIFIER_ROW_ID && col_id >= 0) {
            only_rowid_columns = false;
            break;
        }
    }

    if (gstate.CanRemoveFilterColumns()) {
        state.all_columns.Reset();
        state.all_columns.SetCardinality(output_size);

        ensure_column_id_coverage(state.all_columns);
        bool arrow_scan_is_projected = !state.column_ids.empty() && state.column_ids.size() < data.names.size();
        ArrowTableFunction::ArrowToDuckDB(state,
                                          data.arrow_table.GetColumns(),
                                          state.all_columns,
                                          arrow_scan_is_projected,
                                          COLUMN_IDENTIFIER_ROW_ID);

        bool geometry_cast_needed = false;
        if (!data.requires_cast) {
            if (gstate.projection_ids.empty()) {
                for (idx_t i = 0; i < output.ColumnCount(); i++) {
                    auto &src_vec = state.all_columns.data[i];
                    auto &dst_type = output.data[i].GetType();
                    if (dst_type.id() == LogicalTypeId::GEOMETRY && src_vec.GetType().id() == LogicalTypeId::VARCHAR) {
                        geometry_cast_needed = true;
                        break;
                    }
                }
            } else {
                for (idx_t i = 0; i < output.ColumnCount(); i++) {
                    auto proj_id = gstate.projection_ids[i];
                    auto &src_vec = state.all_columns.data[proj_id];
                    auto &dst_type = output.data[i].GetType();
                    if (dst_type.id() == LogicalTypeId::GEOMETRY && src_vec.GetType().id() == LogicalTypeId::VARCHAR) {
                        geometry_cast_needed = true;
                        break;
                    }
                }
            }
        }

        bool do_cast = data.requires_cast || geometry_cast_needed;
        if (!do_cast) {
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

        bool geometry_cast_needed = false;
        if (!data.requires_cast) {
            for (idx_t i = 0; i < output.ColumnCount(); i++) {
                auto &dst_type = output.data[i].GetType();
                const LogicalType &src_type = gstate.scanned_types[i];
                if (dst_type.id() == LogicalTypeId::GEOMETRY && src_type.id() == LogicalTypeId::VARCHAR) {
                    geometry_cast_needed = true;
                    break;
                }
            }
        }

        bool do_cast = data.requires_cast || geometry_cast_needed;
        if (!do_cast) {
            ensure_column_id_coverage(output);
            bool arrow_scan_is_projected = !state.column_ids.empty() && state.column_ids.size() < data.names.size();
            ArrowTableFunction::ArrowToDuckDB(state,
                                              data.arrow_table.GetColumns(),
                                              output,
                                              arrow_scan_is_projected,
                                              COLUMN_IDENTIFIER_ROW_ID);
        } else {
            if (only_rowid_columns) {
                for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
                    output.data[col_idx].Sequence(NumericCast<int64_t>(gstate.position.load()), 1, output_size);
                }
                output.SetCardinality(output_size);
                output.Verify();
                state.chunk_offset += output.size();

                lock_guard<mutex> glock(gstate.lock);
                gstate.position += output.size();
                return;
            }

            state.all_columns.Reset();
            state.all_columns.SetCardinality(output_size);
            ensure_column_id_coverage(state.all_columns);
            bool arrow_scan_is_projected = !state.column_ids.empty() && state.column_ids.size() < data.names.size();
            ArrowTableFunction::ArrowToDuckDB(state,
                                              data.arrow_table.GetColumns(),
                                              state.all_columns,
                                              arrow_scan_is_projected,
                                              COLUMN_IDENTIFIER_ROW_ID);
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

static InsertionOrderPreservingMap<string> BigqueryScanToString(TableFunctionToStringInput &input) {
    D_ASSERT(input.bind_data);

    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<BigqueryScanBindData>();
    result["Table"] = bind_data.TableString();
    return result;
}

static BindInfo BigqueryScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BigqueryScanBindData>();
    BindInfo info(ScanType::EXTERNAL);
    if (bind_data.bq_table_entry) {
        info.table = bind_data.bq_table_entry.get_mutable();
    }
    return info;
}

static unique_ptr<NodeStatistics> BigqueryScanCardinality(ClientContext &, const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<BigqueryScanBindData>();
    return make_uniq<NodeStatistics>(bind_data.estimated_row_count, bind_data.estimated_row_count);
}

static double BigqueryScanProgress(ClientContext &,
                                   const FunctionData *bind_data_p,
                                   const GlobalTableFunctionState *global_state) {
    auto &bind_data = bind_data_p->Cast<BigqueryScanBindData>();
    auto &gstate = global_state->Cast<BigqueryScanGlobalState>();
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
                    BigqueryScanFunction::BigqueryScanExecute,
                    BigqueryScanFunction::BigqueryScanBind,
                    BigqueryScanFunction::BigqueryScanInitGlobalState,
                    BigqueryScanFunction::BigqueryScanInitLocalState) {
    projection_pushdown = true;
    filter_pushdown = true;
    filter_prune = true;

    to_string = BigqueryScanToString;
    cardinality = BigqueryScanCardinality;
    table_scan_progress = BigqueryScanProgress;
    get_bind_info = BigqueryScanGetBindInfo;

    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["filter"] = LogicalType::VARCHAR;
}

} // namespace bigquery
} // namespace duckdb
