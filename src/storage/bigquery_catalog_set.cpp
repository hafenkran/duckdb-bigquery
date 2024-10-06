#include "duckdb/parser/parsed_data/drop_info.hpp"

#include "storage/bigquery_catalog_set.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_transaction.hpp"

#include <iostream>

namespace duckdb {
namespace bigquery {

BigqueryCatalogSet::BigqueryCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false) {
}

optional_ptr<CatalogEntry> BigqueryCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
    lock_guard<mutex> lock(entry_lock);
    auto result = entry.get();
    if (result->name.empty()) {
        throw InternalException("Cannot create entry with empty name");
    }
    entries.insert(std::make_pair(result->name, std::move(entry)));
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
    TryLoadEntries(context);
    lock_guard<mutex> lock(entry_lock);
    for (auto &entry : entries) {
        callback(*entry.second);
    }
}

optional_ptr<CatalogEntry> BigqueryCatalogSet::GetEntry(ClientContext &context, const string &name) {
    TryLoadEntries(context);

    lock_guard<mutex> lock(entry_lock);
    auto entry = entries.find(name);
    if (entry == entries.end()) {
        return nullptr;
    }
    return entry->second.get();
}

optional_ptr<CatalogEntry> BigqueryCatalogSet::GetFirstEntry(ClientContext &context) {
	TryLoadEntries(context);

	lock_guard<mutex> lock(entry_lock);
	if (entries.empty()) {
		return nullptr;
	}
	return entries.begin()->second.get();
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

void BigqueryCatalogSet::TryLoadEntries(ClientContext &context) {
    lock_guard<mutex> lock(load_lock);
    if (is_loaded) {
        return;
    }

    is_loaded = true;
    LoadEntries(context);
}

BigqueryInSchemaSet::BigqueryInSchemaSet(BigquerySchemaEntry &schema)
    : BigqueryCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> BigqueryInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
    return BigqueryCatalogSet::CreateEntry(std::move(entry));
}

} // namespace bigquery
} // namespace duckdb
