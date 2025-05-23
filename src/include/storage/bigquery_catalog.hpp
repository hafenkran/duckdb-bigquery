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

    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
                                                  const EntryLookupInfo &schema_lookup,
                                                  OnEntryNotFound if_not_found) override;

    void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

    void DropSchema(ClientContext &context, DropInfo &info) override;

    PhysicalOperator &PlanCreateTableAs(ClientContext &context,
                                        PhysicalPlanGenerator &planner,
                                        LogicalCreateTable &op,
                                        PhysicalOperator &plan) override;
    PhysicalOperator &PlanInsert(ClientContext &context,
                                 PhysicalPlanGenerator &planner,
                                 LogicalInsert &op,
                                 optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context,
                                 PhysicalPlanGenerator &planner,
                                 LogicalDelete &op,
                                 PhysicalOperator &plan) override;
    PhysicalOperator &PlanUpdate(ClientContext &context,
                                 PhysicalPlanGenerator &planner,
                                 LogicalUpdate &op,
                                 PhysicalOperator &plan) override;
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

    //! Whether or not this catalog should search a specific type with the standard priority
    CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
        switch (type) {
        // case CatalogType::INDEX_ENTRY:
        case CatalogType::TABLE_ENTRY:
        // case CatalogType::TYPE_ENTRY:
        case CatalogType::VIEW_ENTRY:
            return CatalogLookupBehavior::STANDARD;
        default:
            // unsupported type (e.g. scalar functions, aggregates, ...)
            return CatalogLookupBehavior::NEVER_LOOKUP;
        }
    }

private:
    BigquerySchemaSet schemas;
    unique_ptr<BigquerySchemaEntry> default_dataset;
    mutex default_dataset_lock;
};

} // namespace bigquery
} // namespace duckdb
