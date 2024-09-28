#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
struct DropInfo;

namespace bigquery {
class BigquerySchemaEntry;
class BigqueryTransaction;


class BigqueryCatalogSet {
public:
    BigqueryCatalogSet(Catalog &catalog);

    virtual optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);
    virtual void DropEntry(ClientContext &context, DropInfo &info);
    void ClearEntries();

    void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
    optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);

protected:
    virtual void LoadEntries(ClientContext &context) = 0;
    void EraseEntryInternal(const string &name);

    Catalog &catalog;

private:
    void TryLoadEntries(ClientContext &context);

    atomic<bool> is_loaded = false;
    mutex entry_lock;
    mutex load_lock;
    case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
};


class BigqueryInSchemaSet : public BigqueryCatalogSet {
public:
    BigqueryInSchemaSet(BigquerySchemaEntry &schema);

    optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry) override;

protected:
    BigquerySchemaEntry &schema;
};

} // namespace bigquery
} // namespace duckdb
