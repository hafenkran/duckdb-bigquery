#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include "storage/bigquery_catalog.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryStorageExtension : public StorageExtension {
public:
    BigqueryStorageExtension();
};

} // namespace bigquery
} // namespace duckdb
