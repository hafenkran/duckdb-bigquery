#include "bigquery_table_storage.hpp"
#include "bigquery_query.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include <optional>

namespace duckdb {
namespace bigquery {

static string ResolveProjectId(ClientContext &context, const string &dbname_or_project_id) {
    auto &database_manager = DatabaseManager::Get(context);
    auto database = database_manager.GetDatabase(context, dbname_or_project_id);
    if (database) {
        auto &catalog = database->GetCatalog();
        if (catalog.GetCatalogType() != "bigquery") {
            throw BinderException("Database " + dbname_or_project_id + " is not a BigQuery database");
        }
        auto &bigquery_catalog = catalog.Cast<BigqueryCatalog>();
        return bigquery_catalog.config.project_id;
    }
    return dbname_or_project_id;
}

static unique_ptr<FunctionData> BigqueryTableStorageBind(ClientContext &context,
                                                         TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types,
                                                         vector<string> &names) {
    auto dbname_or_project_id = input.inputs[0].GetValue<string>();
    auto params = BigQueryCommonParameters::ParseFromNamedParameters(input.named_parameters);

    std::optional<string> region;
    std::optional<string> dataset;
    for (auto &param : input.named_parameters) {
        auto key = StringUtil::Lower(param.first);
        if (key == "region") {
            region = param.second.GetValue<string>();
        } else if (key == "dataset") {
            dataset = param.second.GetValue<string>();
        }
    }

    if (region.has_value() == dataset.has_value()) {
        throw BinderException("Provide exactly one of 'region' or 'dataset'");
    }

    string dataset_id;
    if (region.has_value()) {
        if (region->empty()) {
            throw BinderException("'region' cannot be empty");
        }
        if (StringUtil::StartsWith(StringUtil::Lower(*region), "region-")) {
            dataset_id = *region;
        } else {
            dataset_id = "region-" + *region;
        }
    } else {
        if (dataset->empty()) {
            throw BinderException("'dataset' cannot be empty");
        }
        dataset_id = *dataset;
    }

    auto project_id = ResolveProjectId(context, dbname_or_project_id);
    auto table_string =
        BigqueryUtils::FormatTableStringSimple(project_id, dataset_id, "INFORMATION_SCHEMA.TABLE_STORAGE");
    auto query_string = "SELECT * FROM `" + table_string + "`";

    return BigqueryQueryBindInternal(context, dbname_or_project_id, query_string, params, return_types, names, false);
}

BigqueryTableStorageFunction::BigqueryTableStorageFunction()
    : TableFunction("bigquery_table_storage",
                    {LogicalType::VARCHAR},
                    BigqueryQueryExecute,
                    BigqueryTableStorageBind,
                    BigqueryQueryInitGlobal,
                    BigqueryQueryInitLocal) {
    to_string = BigqueryQueryToString;
    cardinality = BigqueryQueryCardinality;
    table_scan_progress = BigqueryQueryProgress;
    get_bind_info = BigqueryQueryGetBindInfo;

    projection_pushdown = true;
    filter_pushdown = true;
    filter_prune = true;

    named_parameters["region"] = LogicalType::VARCHAR;
    named_parameters["dataset"] = LogicalType::VARCHAR;
    named_parameters["billing_project"] = LogicalType::VARCHAR;
    named_parameters["api_endpoint"] = LogicalType::VARCHAR;
    named_parameters["grpc_endpoint"] = LogicalType::VARCHAR;
    named_parameters["use_legacy_scan"] = LogicalType::BOOLEAN;
}

} // namespace bigquery
} // namespace duckdb
