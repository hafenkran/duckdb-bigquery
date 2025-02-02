#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "bigquery_settings.hpp"
#include "storage/bigquery_schema_set.hpp"
#include "storage/bigquery_transaction.hpp"

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

    if (BigquerySettings::ExperimentalFetchCatalogFromInformationSchema()) {
		auto &bq_catalog = dynamic_cast<BigqueryCatalog &>(catalog);

		vector<BigqueryDatasetRef> datasets;
		if (bq_catalog.config.HasDatasetId()){
			auto dataset = bqclient->GetDataset(bq_catalog.config.dataset_id);
			datasets.push_back(dataset);
		} else {
			datasets = bqclient->GetDatasets();
		}

		std::map<std::string, CreateTableInfo> table_infos;
        bqclient->GetTableInfosFromDatasets(datasets, table_infos);

        std::map<std::string, vector<CreateTableInfo>> tables_by_schema;
        for (auto &table_info : table_infos) {
            tables_by_schema[table_info.second.schema].push_back(std::move(table_info.second));
        }

        for (auto &dataset : datasets) {
            CreateSchemaInfo info;
            info.catalog = dataset.project_id;
            info.schema = dataset.dataset_id;

            vector<CreateTableInfo> &table_infos = tables_by_schema[dataset.dataset_id];
            auto schema = make_uniq<BigquerySchemaEntry>(catalog, info, dataset, table_infos);
            CreateEntry(std::move(schema));
        }
    } else {
        auto datasets = bqclient->GetDatasets();
        for (auto dataset : datasets) {
            CreateSchemaInfo info;
            info.catalog = dataset.project_id;
            info.schema = dataset.dataset_id;

            auto schema = make_uniq<BigquerySchemaEntry>(catalog, info, dataset);
            CreateEntry(std::move(schema));
        }
    }
}


} // namespace bigquery
} // namespace duckdb
