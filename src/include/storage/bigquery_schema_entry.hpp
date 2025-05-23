#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "storage/bigquery_schema_set.hpp"
#include "storage/bigquery_table_set.hpp"

namespace duckdb {
struct DropInfo;

namespace bigquery {
class BigQueryTransacation;

class BigquerySchemaEntry : public SchemaCatalogEntry {
public:
    BigquerySchemaEntry(Catalog &catalog, CreateSchemaInfo &info, BigqueryDatasetRef &bq_dataset_ref);
    BigquerySchemaEntry(Catalog &catalog,
                        CreateSchemaInfo &info,
                        BigqueryDatasetRef &bq_dataset_ref,
                        vector<CreateTableInfo> &table_infos);

public:
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction,
                                           CreateIndexInfo &info,
                                           TableCatalogEntry &table) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
                                                   CreateTableFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
                                                  CreateCopyFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
                                                    CreatePragmaFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
    void Alter(CatalogTransaction transaction, AlterInfo &info) override;
    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    void DropEntry(ClientContext &context, DropInfo &info) override;
    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

    BigqueryCatalogSet &GetCatalogSet(CatalogType type);

public:
    string GetBigqueryLocation() const {
        return bq_dataset_ref.location;
    }

    BigqueryDatasetRef &GetBigqueryDatasetRef() {
        return bq_dataset_ref;
    }

    std::optional<vector<CreateTableInfo>> &GetTableInfos() {
        return prefetched_table_infos;
    }

private:
    void TryDropEntry(ClientContext &context, CatalogType type, const string &name);

private:
    BigqueryTableSet tables;
    BigqueryDatasetRef bq_dataset_ref;
    std::optional<vector<CreateTableInfo>> prefetched_table_infos;
};


} // namespace bigquery
} // namespace duckdb
