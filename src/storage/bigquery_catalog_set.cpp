#include "duckdb/parser/parsed_data/drop_info.hpp"

#include "storage/bigquery_catalog_set.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {
namespace bigquery {

BigqueryCatalogSet::BigqueryCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false) {
}

optional_ptr<CatalogEntry> BigqueryCatalogSet::CreateEntry(BigqueryTransaction &transaction,
                                                           shared_ptr<CatalogEntry> entry) {
    lock_guard<mutex> lock(entry_lock);
    if (!entry || entry->name.empty()) {
        throw InternalException("Cannot create entry with empty name");
    }
    auto result = transaction.ReferenceEntry(entry);
    entries[result->name] = std::move(entry);
    return result;
}

void BigqueryCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    auto bqclient = transaction.GetBigqueryClient();

    if (info.type == CatalogType::SCHEMA_ENTRY) {
        bqclient->DropDataset(info);
    } else if (info.type == CatalogType::TABLE_ENTRY) {
        bqclient->DropTable(info);
    } else if (info.type == CatalogType::VIEW_ENTRY) {
        bqclient->DropView(info);
    } else {
        throw BinderException("Cannot drop entry of type " + CatalogTypeToString(info.type));
    }

    EraseEntryInternal(info.name);
}

void BigqueryCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    TryLoadEntries(context, transaction);

    vector<shared_ptr<CatalogEntry>> entry_refs;
    {
        lock_guard<mutex> lock(entry_lock);
        for (auto &entry : entries) {
            entry_refs.push_back(entry.second);
        }
    }

    for (auto &entry : entry_refs) {
        auto pinned_entry = transaction.ReferenceEntry(entry);
        callback(*pinned_entry);
    }
}

optional_ptr<CatalogEntry> BigqueryCatalogSet::GetEntry(ClientContext &context, const string &name) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    TryLoadEntries(context, transaction);

    {
        lock_guard<mutex> lock(entry_lock);
        auto entry = entries.find(name);
        if (entry != entries.end()) {
            return transaction.ReferenceEntry(entry->second);
        }
    }

    if (SupportReload()) {
        lock_guard<mutex> lock(load_lock);
        {
            lock_guard<mutex> entry_guard(entry_lock);
            auto entry = entries.find(name);
            if (entry != entries.end()) {
                return transaction.ReferenceEntry(entry->second);
            }
        }
        return ReloadEntry(context, transaction, name);
    }
    return nullptr;
}

optional_ptr<CatalogEntry> BigqueryCatalogSet::GetFirstEntry(ClientContext &context) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    TryLoadEntries(context, transaction);

    lock_guard<mutex> lock(entry_lock);
    if (entries.empty()) {
        return nullptr;
    }
    return transaction.ReferenceEntry(entries.begin()->second);
}

void BigqueryCatalogSet::ClearEntries() {
    lock_guard<mutex> lock(entry_lock);
    entries.clear();
    is_loaded = false;
}

void BigqueryCatalogSet::EraseEntryInternal(const string &name) {
    lock_guard<mutex> lock(entry_lock);
    entries.erase(name);
}

optional_ptr<CatalogEntry> BigqueryCatalogSet::ReloadEntry(ClientContext &, BigqueryTransaction &, const string &) {
    return nullptr;
}

void BigqueryCatalogSet::TryLoadEntries(ClientContext &context, BigqueryTransaction &transaction) {
    lock_guard<mutex> lock(load_lock);
    if (is_loaded) {
        return;
    }

    try {
        LoadEntries(context, transaction);
        is_loaded = true;
    } catch (...) {
        is_loaded = false;
        throw;
    }
}

BigqueryInSchemaSet::BigqueryInSchemaSet(BigquerySchemaEntry &schema)
    : BigqueryCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> BigqueryInSchemaSet::CreateEntry(BigqueryTransaction &transaction,
                                                            shared_ptr<CatalogEntry> entry) {
    return BigqueryCatalogSet::CreateEntry(transaction, std::move(entry));
}

} // namespace bigquery
} // namespace duckdb
