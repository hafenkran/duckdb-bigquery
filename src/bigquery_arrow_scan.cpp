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

unique_ptr<FunctionData> BigQueryArrowScanBind(ClientContext &context,
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
	auto bind_data = make_uniq<BigQueryArrowScanBindData>();
	bind_data->table_ref = table_ref;
	bind_data->filter_condition = filter_condition;
	bind_data->bq_config = BigqueryConfig(table_ref.project_id)
							   .SetDatasetId(table_ref.dataset_id)
							   .SetBillingProjectId(billing_project_id)
							   .SetApiEndpoint(api_endpoint)
							   .SetGrpcEndpoint(grpc_endpoint);
	bind_data->bq_client = make_shared_ptr<BigqueryClient>(bind_data->bq_config);

	// Create schema reader to fetch table metadata
	auto schema_reader = bind_data->bq_client->CreateArrowReader(table_ref.dataset_id,
																 table_ref.table_id,
																 1,
																 vector<string>(),
																 filter_condition);

	// Export Arrow schema for DuckDB integration
	auto status = arrow::ExportSchema(*schema_reader->GetSchema(), &bind_data->schema_root.arrow_schema);
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
	bind_data->column_mapping.resize(names.size());
	for (idx_t i = 0; i < names.size(); i++) {
		bind_data->column_mapping[i] = i;
	}

	return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> BigQueryArrowScanInitGlobal(ClientContext &context,
																 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<BigQueryArrowScanBindData>();

	// Determine columns required by the query
	vector<string> selected_fields;
	selected_fields.reserve(input.column_ids.size());
	for (auto col_id : input.column_ids) {
		if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
			throw BinderException("BigQuery tables do not support ROWID");
		}
		selected_fields.push_back(bind_data.names[col_id]);
	}

	// Apply filter pushdown if enabled
	string filter_string = bind_data.filter_condition;
	bool enable_filter_pushdown = BigquerySettings::ExperimentalFilterPushdown();
	if (enable_filter_pushdown && filter_string.empty() && input.filters && !input.filters->filters.empty()) {
		for (const auto &filter : input.filters->filters) {
			if (!filter_string.empty()) {
				filter_string += " AND ";
			}
			const string &column_name = selected_fields[filter.first];
			filter_string += BigquerySQL::TransformFilter(column_name, *filter.second);
		}
	}

	// Create BigQuery Arrow reader with selected columns and filters
	idx_t max_read_streams = BigquerySettings::GetMaxReadStreams(context);
	auto bq_arrow_reader = bind_data.bq_client->CreateArrowReader(
		bind_data.table_ref.dataset_id,
		bind_data.table_ref.table_id,
		max_read_streams,
		selected_fields,
		filter_string
	);

	// Create and store stream factory
	auto factory = make_shared_ptr<BigQueryStreamFactory>(bq_arrow_reader);
	bind_data.factory_dep()->factory = factory;
	bind_data.stream_factory_ptr = reinterpret_cast<uintptr_t>(factory.get());

	// Initialize global scan state
	auto gstate = make_uniq<ArrowScanGlobalState>();
	gstate->max_threads = max_read_streams;

	// Analyze Arrow schema (contains only selected fields)
	auto arrow_schema = bq_arrow_reader->GetSchema();
	const idx_t physical_column_count = arrow_schema->num_fields();

	// Build mapping from column name to logical column ID
	unordered_map<string, column_t> name_to_column_id;
	name_to_column_id.reserve(bind_data.names.size());
	for (idx_t col_id = 0; col_id < bind_data.names.size(); ++col_id) {
		name_to_column_id[bind_data.names[col_id]] = col_id;
	}

	// Set up scanned types in physical column order
	gstate->scanned_types.clear();
	gstate->scanned_types.reserve(physical_column_count);
	for (idx_t phys_idx = 0; phys_idx < physical_column_count; ++phys_idx) {
		const string &field_name = arrow_schema->field(phys_idx)->name();
		auto col_id = name_to_column_id.at(field_name);
		gstate->scanned_types.emplace_back(bind_data.all_types[col_id]);
	}

	// Set up column projection mapping (logical to physical)
	gstate->projection_ids.resize(input.column_ids.size());
	bool needs_projection = false;
	for (idx_t output_idx = 0; output_idx < input.column_ids.size(); ++output_idx) {
		auto col_id = input.column_ids[output_idx];
		const string &column_name = bind_data.names[col_id];
		idx_t physical_idx = arrow_schema->GetFieldIndex(column_name);
		gstate->projection_ids[output_idx] = physical_idx;
		if (physical_idx != output_idx) {
			needs_projection = true;
		}
	}

	// Clear projection if no reordering is needed (1:1 mapping)
	if (!needs_projection) {
		gstate->projection_ids.clear();
	}

	// Create and attach Arrow scan stream
	gstate->stream = ::duckdb::ProduceArrowScan(bind_data, input.column_ids, input.filters.get());
	return std::move(gstate);
}

BigQueryArrowScanFunction::BigQueryArrowScanFunction()
    : TableFunction("bigquery_arrow_scan",
                    {LogicalType::VARCHAR},
                    ArrowTableFunction::ArrowScanFunction,
                    BigQueryArrowScanBind,
                    BigQueryArrowScanInitGlobal,
                    ArrowTableFunction::ArrowScanInitLocal) {
    projection_pushdown = true;
    filter_pushdown = true;
    filter_prune = true;

    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["filter"] = LogicalType::VARCHAR;
}

} // namespace bigquery
} // namespace duckdb
