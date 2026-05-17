#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/shared_ptr.hpp"
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

    shared_ptr<duckdb::bigquery::BigqueryClient> GetBigqueryClient();
    static BigqueryTransaction &Get(ClientContext &context, Catalog &catalog);
    optional_ptr<CatalogEntry> ReferenceEntry(shared_ptr<CatalogEntry> &entry);

    AccessMode GetAccessMode() const {
        return access_mode;
    }

    static void CheckReadWrite(ClientContext &context, Catalog &catalog, const string &operation);

private:
    BigqueryCatalog &bigquery_catalog;
    shared_ptr<duckdb::bigquery::BigqueryClient> client;
    reference_map_t<CatalogEntry, shared_ptr<CatalogEntry>> catalog_entries;
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
