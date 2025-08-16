#pragma once

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "storage/bigquery_catalog_set.hpp"


namespace duckdb {

namespace bigquery {

class BigquerySchemaEntry;

class BigqueryViewSet : public BigqueryInSchemaSet {
public:
    explicit BigqueryViewSet(BigquerySchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateView(ClientContext &context, CreateViewInfo &info);
	optional_ptr<CatalogEntry> RefreshView(ClientContext &context, const string &view_name);

protected:
	void LoadEntries(ClientContext &context) override;
	void ClearEntries() override;
};

} // namespace bigquery
} // namespace duckdb
