#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {
namespace bigquery {

class BigQuerySecretFunctions {
public:
	static vector<SecretType> GetSecretTypes();
	static vector<CreateSecretFunction> GetSecretFunctions();

private:
	static unique_ptr<BaseSecret> CreateBigQuerySecretFromConfig(ClientContext &context, CreateSecretInput &input);
	static unique_ptr<BaseSecret> CreateBigQuerySecretFromServiceAccount(ClientContext &context,
	                                                                      CreateSecretInput &input);
};

} // namespace bigquery
} // namespace duckdb


