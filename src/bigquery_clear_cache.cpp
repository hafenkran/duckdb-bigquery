#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include <iostream>

#include "bigquery_clear_cache.hpp"
#include "storage/bigquery_catalog.hpp"


namespace duckdb {
namespace bigquery {

struct BigqueryClearCacheFunctionData : public TableFunctionData {
    bool finished = false;
};

static unique_ptr<FunctionData> BigQueryClearCacheBind(ClientContext &context,
                                                       TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types,
                                                       vector<string> &names) {
    auto result = make_uniq<BigqueryClearCacheFunctionData>();
    return_types.push_back(LogicalType::BOOLEAN);
    names.push_back("success");
    return std::move(result);
}

static void ClearBigqueryCaches(ClientContext &context) {
    auto databases = DatabaseManager::Get(context).GetDatabases(context);
    for (auto &db_ptr : databases) {
        if (!db_ptr) {
            continue;
        }
        auto &db = *db_ptr;
        auto &catalog = db.GetCatalog();
        if (catalog.GetCatalogType() != "bigquery") {
            continue;
        }
        std::cout << "Clearing BigQuery Cache for Database: " << db.GetName() << std::endl;
        catalog.Cast<BigqueryCatalog>().ClearCache();
    }
}

static void ClearBigqueryCachesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<BigqueryClearCacheFunctionData>();
    if (data.finished) {
        return;
    }
    ClearBigqueryCaches(context);
    // Emit a single row indicating success
    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BOOLEAN(true));
    data.finished = true;
}

void BigqueryClearCacheFunction::ClearCache(ClientContext &context, SetScope scope, Value &parameter) {
    ClearBigqueryCaches(context);
}

BigqueryClearCacheFunction::BigqueryClearCacheFunction()
    : TableFunction("bigquery_clear_cache", {}, ClearBigqueryCachesFunction, BigQueryClearCacheBind) {
}

} // namespace bigquery
} // namespace duckdb
