#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {
namespace bigquery {

	class AuthenticationInfo {
		public:
		 string service_account_json;
	};

	CreateSecretFunction BuildCreateBigQuerySecretFunction();
	unique_ptr<AuthenticationInfo> BuildAuthenticationInfo(ClientContext &context, const AttachInfo &info);

} // namespace bigquery
} // namespace duckdb
