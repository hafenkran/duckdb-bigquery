#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "bigquery_execute.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {
namespace bigquery {

struct BigQueryExecuteBindData : public TableFunctionData {
public:
    explicit BigQueryExecuteBindData(BigqueryCatalog &bq_catalog, string query)
        : bq_catalog(bq_catalog), query(std::move(query)) {
    }

public:
    BigqueryCatalog &bq_catalog;
    string query;
    bool finished = false;
};


static duckdb::unique_ptr<FunctionData> BigQueryExecuteBind(ClientContext &context,
                                                            TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<string> &names) {
    return_types.emplace_back(LogicalTypeId::BOOLEAN);
    names.emplace_back("success");
	return_types.emplace_back(LogicalTypeId::VARCHAR);
	names.emplace_back("job_id");
	return_types.emplace_back(LogicalTypeId::VARCHAR);
	names.emplace_back("project_id");
	return_types.emplace_back(LogicalTypeId::VARCHAR);
	names.emplace_back("location");
	return_types.emplace_back(LogicalTypeId::UBIGINT);
	names.emplace_back("total_rows");
	return_types.emplace_back(LogicalTypeId::BIGINT);
	names.emplace_back("total_bytes_processed");
	return_types.emplace_back(LogicalTypeId::VARCHAR);
	names.emplace_back("num_dml_affected_rows");

    auto database_name = input.inputs[0].GetValue<string>();
    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, database_name);
    if (!database) {
        throw BinderException("Failed to find attached database " + database_name);
    }

    auto &catalog = database->GetCatalog();
    if (catalog.GetCatalogType() != "bigquery") {
        throw BinderException("Database " + database_name + " is not a BigQuery database");
    }

    auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
    auto query = input.inputs[1].GetValue<string>();
    return make_uniq<BigQueryExecuteBindData>(bigquery_catalog, query);
}

static void BigQueryExecuteFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<BigQueryExecuteBindData>();
    if (data.finished) {
        return;
    }

    auto &transaction = BigqueryTransaction::Get(context, data.bq_catalog);
    if (transaction.GetAccessMode() == AccessMode::READ_ONLY) {
        throw BinderException("Cannot execute BigQuery query in read-only transaction");
    }
    auto bqclient = transaction.GetBigqueryClient();
    auto response = bqclient->ExecuteQuery(data.query);

	data.finished = true;
	output.SetValue(0, 0, true);
	output.SetValue(1, 0, response.job_reference().job_id());
	output.SetValue(2, 0, response.job_reference().project_id());
	output.SetValue(3, 0, response.job_reference().location().value());
	output.SetValue(4, 0, Value::UBIGINT(response.total_rows().value()));
	output.SetValue(5, 0, Value::BIGINT(response.total_bytes_processed().value()));
	output.SetValue(6, 0, Value::BIGINT(response.num_dml_affected_rows().value()));
	output.SetCardinality(1);
}

BigQueryExecuteFunction::BigQueryExecuteFunction()
    : TableFunction("bigquery_execute",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    BigQueryExecuteFunc,
                    BigQueryExecuteBind) {
}

} // namespace bigquery
} // namespace duckdb
