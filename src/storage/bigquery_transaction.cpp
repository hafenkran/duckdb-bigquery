#include "storage/bigquery_transaction.hpp"
#include "storage/bigquery_catalog.hpp"

#include <iostream>


namespace duckdb {
namespace bigquery {

// ########################################################################
// # Transaction
// ########################################################################

BigqueryTransaction::BigqueryTransaction(BigqueryCatalog &bigquery_catalog,
                                         TransactionManager &manager,
                                         ClientContext &context)
    : Transaction(manager, context),      //
      bigquery_catalog(bigquery_catalog), //
      access_mode(bigquery_catalog.options.access_mode) {
    client = make_shared_ptr<BigqueryClient>(context, bigquery_catalog.config);
}

BigqueryTransaction::~BigqueryTransaction() {
}

shared_ptr<BigqueryClient> BigqueryTransaction::GetBigqueryClient() {
    return client;
}

BigqueryTransaction &BigqueryTransaction::Get(ClientContext &context, Catalog &catalog) {
    return Transaction::Get(context, catalog).Cast<BigqueryTransaction>();
}

// ########################################################################
// # TransactionManager
// ########################################################################

BigqueryTransactionManager::BigqueryTransactionManager(AttachedDatabase &db_p, BigqueryCatalog &bigquery_catalog)
    : TransactionManager(db_p), bigquery_catalog(bigquery_catalog) {
}

Transaction &BigqueryTransactionManager::StartTransaction(ClientContext &context) {
    auto transaction = make_uniq<BigqueryTransaction>(bigquery_catalog, *this, context);
    auto &result = *transaction;
    lock_guard<mutex> lock(transaction_lock);
    transactions[result] = std::move(transaction);
    return result;
}

ErrorData BigqueryTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
    lock_guard<mutex> lock(transaction_lock);
    transactions.erase(transaction);
    return ErrorData();
}

void BigqueryTransactionManager::RollbackTransaction(Transaction &transaction) {
    lock_guard<mutex> lock(transaction_lock);
    transactions.erase(transaction);
}

void BigqueryTransactionManager::Checkpoint(ClientContext &context, bool force) {
    throw NotImplementedException("BigQuery does not support checkpoints");
}


} // namespace bigquery
} // namespace duckdb
