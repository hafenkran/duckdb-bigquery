#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"

#include "storage/bigquery_options.hpp"
#include "storage/bigquery_schema_set.hpp"

namespace duckdb {
namespace bigquery {
class BigquerySchemaEntry;

class BigqueryCatalog : public Catalog {
public:
	explicit BigqueryCatalog(AttachedDatabase &db_p, const BigqueryConfig &config, BigqueryOptions options_p);
    explicit BigqueryCatalog(AttachedDatabase &db_p, const string &connection_str, BigqueryOptions options_p);
    ~BigqueryCatalog() = default;

    BigqueryConfig config;
    BigqueryOptions options;

public:
    string GetCatalogType() override {
        return "bigquery";
    }

    void Initialize(bool load_builtin) override;

    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

    optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction,
                                               const string &schema_name,
                                               OnEntryNotFound if_not_found,
                                               QueryErrorContext error_context = QueryErrorContext()) override;

    void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

    void DropSchema(ClientContext &context, DropInfo &info) override;

    unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context,
                                                   LogicalCreateTable &op,
                                                   unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context,
                                            LogicalInsert &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context,
                                            LogicalDelete &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context,
                                            LogicalUpdate &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder,
                                                CreateStatement &stmt,
                                                TableCatalogEntry &table,
                                                unique_ptr<LogicalOperator> plan) override;


    DatabaseSize GetDatabaseSize(ClientContext &context) override;
    vector<MetadataBlockInfo> GetMetadataInfo(ClientContext &context) override;

    bool InMemory() override;
    string GetDBPath() override;

    const string GetProjectID() {
        return config.project_id;
    }

    const string GetDefaultDatasetID() {
        return config.dataset_id;
    }

    void ClearCache();

private:
    BigquerySchemaSet schemas;
    unique_ptr<BigquerySchemaEntry> default_dataset;
    mutex default_dataset_lock;
};

} // namespace bigquery
} // namespace duckdb
