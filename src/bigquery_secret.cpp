#include "duckdb.hpp"
#include "bigquery_secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {
namespace bigquery {


void SetBigQuerySecretParameters(CreateSecretFunction &function) {
	function.named_parameters["service_account_json"] = LogicalType::VARCHAR;
}

unique_ptr<BaseSecret> CreateBigQuerySecret(ClientContext &context, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "bigquery", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "service_account_json") {
			result->secret_map["service_account_json"] = named_param.second.ToString();
		}
		else {
			throw InternalException("Unknown named parameter passed to BigQuery extension secret management: " + lower_name);
		}
	}

	//! Set redact keys
	// TODO support different login method and redact keys
	 result->redact_keys = {"service_account_json"};
	return std::move(result);
}

/**
 * Create the secret function for the BigQuery extension
 * This function is called by the extension to create the secret function
 * It validates the input parameters and creates the secret
 */
CreateSecretFunction BuildCreateBigQuerySecretFunction(){
	CreateSecretFunction bigquery_secret_function = {"bigquery", "config", CreateBigQuerySecret};
	SetBigQuerySecretParameters(bigquery_secret_function);
	return bigquery_secret_function;
}

/**
 * Get the secret with the given name, or nullptr if the secret is not found
 */
unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// FIXME: this should be adjusted once the `GetSecretByName` API supports this
	// use case
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}
	return nullptr;
}

/**
 * Get the value of the secret with the given name, or an empty string if the secret is not provided
 */
string SecretValueOrEmpty(const KeyValueSecret &kv_secret, const string &name) {
	Value input_val = kv_secret.TryGetValue(name);
	if (input_val.IsNull()) {
		// not provided
		return string();
	}
	return input_val.ToString();
}

unique_ptr<AuthenticationInfo> BuildAuthenticationInfo(ClientContext &context, const AttachInfo &info) {
	unique_ptr<AuthenticationInfo> result = make_uniq<AuthenticationInfo>();

	string service_account_json = "";
	// check if we have a secret provided
	string secret_name;
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			// already handled
		} else if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for BigQuery attach: %s", entry.first);
		}
	}

	// if no secret is specified we default to the unnamed bigquery secret, if it
	// exists
	bool explicit_secret = !secret_name.empty();
	if (!explicit_secret) {
		// look up settings from the default unnamed bigquery secret if none is
		// provided
		secret_name = "__default_bigquery";
	}

	auto secret_entry = GetSecret(context, secret_name);
	if (secret_entry) {
		// secret found - read data
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		service_account_json = SecretValueOrEmpty(kv_secret, "service_account_json");
	} else if (explicit_secret) {
		// secret not found and one was explicitly provided - throw an error
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}

	if (!service_account_json.empty()) {
		result->service_account_json = service_account_json;
		return result;
	}

	return nullptr;
}

} // namespace bigquery
} // namespace duckdb
