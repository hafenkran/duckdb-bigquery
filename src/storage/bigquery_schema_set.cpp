#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "storage/bigquery_schema_set.hpp"
#include "storage/bigquery_transaction.hpp"
#include "bigquery_settings.hpp"

namespace duckdb {
namespace bigquery {

BigquerySchemaSet::BigquerySchemaSet(Catalog &catalog) : BigqueryCatalogSet(catalog) {
}

optional_ptr<CatalogEntry> BigquerySchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
	auto &bq_catalog = dynamic_cast<BigqueryCatalog &>(catalog);
    auto bqclient = transaction.GetBigqueryClient();

    BigqueryDatasetRef dataset_ref;
	dataset_ref.project_id = bq_catalog.config.project_id;
    dataset_ref.dataset_id = info.schema;
    dataset_ref.location = BigquerySettings::DefaultLocation();

    bqclient->CreateDataset(info, dataset_ref);
    auto schema_entry = make_uniq<BigquerySchemaEntry>(catalog, info, dataset_ref);
    return CreateEntry(std::move(schema_entry));
}

void BigquerySchemaSet::LoadEntries(ClientContext &context) {
    auto &transaction = BigqueryTransaction::Get(context, catalog);
    auto bqclient = transaction.GetBigqueryClient();

    auto datasets = bqclient->GetDatasets();
    for (auto dataset : datasets) {
        CreateSchemaInfo info;
        info.catalog = dataset.project_id;
        info.schema = dataset.dataset_id;

        auto schema = make_uniq<BigquerySchemaEntry>(catalog, info, dataset);
        CreateEntry(std::move(schema));
    }
}


} // namespace bigquery
} // namespace duckdb
