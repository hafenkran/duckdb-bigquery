#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include "storage/bigquery_catalog.hpp"

#include "bigquery_client.hpp"


namespace duckdb {
namespace bigquery {

class BigqueryCatalog;
class BigquerySchemaEntry;
class BigqueryTableEntry;


class BigqueryTransaction : public Transaction {
public:
    BigqueryTransaction(BigqueryCatalog &bigquery_catalog, TransactionManager &manager, ClientContext &context);
    ~BigqueryTransaction() override;

    shared_ptr<BigqueryClient> GetBigqueryClient();
    static BigqueryTransaction &Get(ClientContext &context, Catalog &catalog);

    AccessMode GetAccessMode() {
        return access_mode;
    }

private:
    BigqueryCatalog &bigquery_catalog;
    shared_ptr<BigqueryClient> client;
    case_insensitive_map_t<unique_ptr<CatalogEntry>> catalog_entries;
    AccessMode access_mode;
};


class BigqueryTransactionManager : public TransactionManager {
public:
    BigqueryTransactionManager(AttachedDatabase &db_p, BigqueryCatalog &bigquery_catalog);

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;

    void Checkpoint(ClientContext &context, bool force = false) override;

private:
    BigqueryCatalog &bigquery_catalog;
    mutex transaction_lock;
    reference_map_t<Transaction, unique_ptr<BigqueryTransaction>> transactions;
};


} // namespace bigquery
} // namespace duckdb
