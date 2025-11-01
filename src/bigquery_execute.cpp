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
    BigqueryConfig config;
    shared_ptr<BigqueryClient> bq_client;
    string query;
    bool dry_run = false;
    bool finished = false;
};

static duckdb::unique_ptr<FunctionData> BigQueryExecuteBind(ClientContext &context,
                                                            TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto query_string = input.inputs[1].GetValue<string>();

    // Parse named parameters using centralized function
    auto params = BigQueryCommonParameters::ParseFromNamedParameters(input.named_parameters);

    auto result = make_uniq<BigQueryExecuteBindData>();
    result->query = query_string;
    result->dry_run = params.dry_run;

    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, dbname_or_project_id);
    if (database) {
        // Use attached database for this operation
        auto &catalog = database->GetCatalog();
        if (catalog.GetCatalogType() != "bigquery") {
            throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
        }
        if (!params.api_endpoint.empty() || !params.grpc_endpoint.empty()) {
            throw BinderException("Named parameters are not supported for attached databases");
        }

        auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
        auto &transaction = BigqueryTransaction::Get(context, bigquery_catalog);
        if (transaction.GetAccessMode() == AccessMode::READ_ONLY) {
            throw BinderException("Cannot execute BigQuery query in read-only transaction");
        }

        result->config = bigquery_catalog.config;
        result->bq_client = transaction.GetBigqueryClient();
    } else {
        // Use the provided project_id of the gcp project
        result->config = BigqueryConfig(dbname_or_project_id) //
                             .SetApiEndpoint(params.api_endpoint)
                             .SetGrpcEndpoint(params.grpc_endpoint);
        result->bq_client = make_shared_ptr<BigqueryClient>(context, result->config);
    }

    if (!params.dry_run) {
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
    } else {
        return_types.emplace_back(LogicalTypeId::BIGINT);
        names.emplace_back("total_bytes_processed");
        return_types.emplace_back(LogicalTypeId::BOOLEAN);
        names.emplace_back("cache_hit");
        return_types.emplace_back(LogicalTypeId::VARCHAR);
        names.emplace_back("location");
    }

    return result;
}

static void BigQueryExecuteFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<BigQueryExecuteBindData>();
    if (data.finished) {
        return;
    }
    auto response = data.bq_client->ExecuteQuery(data.query, "", data.dry_run);
    data.finished = true;

    if (!data.dry_run) {
        output.SetValue(0, 0, true);
        output.SetValue(1, 0, response.job_reference().job_id());
        output.SetValue(2, 0, response.job_reference().project_id());
        output.SetValue(3, 0, response.job_reference().location().value());
        output.SetValue(4, 0, Value::UBIGINT(response.total_rows().value()));
        output.SetValue(5, 0, Value::BIGINT(response.total_bytes_processed().value()));
        output.SetValue(6, 0, Value::BIGINT(response.num_dml_affected_rows().value()));
    } else {
        output.SetValue(0, 0, Value::BIGINT(response.total_bytes_processed().value()));
        output.SetValue(1, 0, Value::BOOLEAN(response.cache_hit().value()));
        output.SetValue(2, 0, response.job_reference().location().value());
    }
    output.SetCardinality(1);
}

BigQueryExecuteFunction::BigQueryExecuteFunction()
    : TableFunction("bigquery_execute",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    BigQueryExecuteFunc,
                    BigQueryExecuteBind) {

    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["dry_run"] = LogicalType::BOOLEAN;
}

} // namespace bigquery
} // namespace duckdb
