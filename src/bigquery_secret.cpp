#include "bigquery_secret.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace bigquery {

vector<SecretType> BigQuerySecretFunctions::GetSecretTypes() {
	vector<SecretType> result;

	SecretType secret_type;
	secret_type.name = "bigquery";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	result.push_back(std::move(secret_type));

	return result;
}

vector<CreateSecretFunction> BigQuerySecretFunctions::GetSecretFunctions() {
	vector<CreateSecretFunction> result;

	// CONFIG provider - for explicit key/value parameters
	CreateSecretFunction config_function;
	config_function.secret_type = "bigquery";
	config_function.provider = "config";
	config_function.function = CreateBigQuerySecretFromConfig;

	// Parameters
	config_function.named_parameters["project_id"] = LogicalType::VARCHAR;
	config_function.named_parameters["dataset_id"] = LogicalType::VARCHAR;
	config_function.named_parameters["key_id"] = LogicalType::VARCHAR;   // client_email
	config_function.named_parameters["secret"] = LogicalType::VARCHAR;   // private_key
	config_function.named_parameters["account_name"] = LogicalType::VARCHAR; // optional service account name

	result.push_back(std::move(config_function));

	// SERVICE_ACCOUNT provider - for full JSON content
	CreateSecretFunction sa_function;
	sa_function.secret_type = "bigquery";
	sa_function.provider = "service_account";
	sa_function.function = CreateBigQuerySecretFromServiceAccount;

	// Parameters
	sa_function.named_parameters["json"] = LogicalType::VARCHAR;       // Full service account JSON
	sa_function.named_parameters["project_id"] = LogicalType::VARCHAR; // Optional override

	result.push_back(std::move(sa_function));

	return result;
}

unique_ptr<BaseSecret> BigQuerySecretFunctions::CreateBigQuerySecretFromConfig(ClientContext &context,
                                                                                CreateSecretInput &input) {

	auto result = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	// Copy parameters from input
	for (const auto &param : input.options) {
		result->secret_map[param.first] = param.second;
	}

	// Mark sensitive keys for redaction
	result->redact_keys.insert("secret");
	result->redact_keys.insert("private_key");

	// Validate required fields
	Value project_id_value;
	if (!result->TryGetValue("project_id", project_id_value)) {
		throw InvalidInputException("BigQuery secret requires 'project_id' parameter");
	}

	return std::move(result);
}

unique_ptr<BaseSecret> BigQuerySecretFunctions::CreateBigQuerySecretFromServiceAccount(ClientContext &context,
                                                                                        CreateSecretInput &input) {

	auto result = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	// Get the JSON parameter
	auto json_lookup = input.options.find("json");
	if (json_lookup == input.options.end()) {
		throw InvalidInputException(
		    "BigQuery service_account secret requires 'json' parameter containing service account JSON");
	}

	string json_content = json_lookup->second.ToString();

	// Store the full JSON
	result->secret_map["json"] = Value(json_content);

	// Optionally allow project_id override
	auto project_lookup = input.options.find("project_id");
	if (project_lookup != input.options.end()) {
		result->secret_map["project_id"] = project_lookup->second;
	}

	// Mark as redacted
	result->redact_keys.insert("json");

	return std::move(result);
}

} // namespace bigquery
} // namespace duckdb


