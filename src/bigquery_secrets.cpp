#include "bigquery_secrets.hpp"

#include "duckdb/main/secret/secret_manager.hpp"

#include "google/cloud/internal/oauth2_access_token_credentials.h"
#include "google/cloud/internal/oauth2_external_account_credentials.h"
#include "google/cloud/internal/oauth2_service_account_credentials.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <sys/stat.h>

namespace oauth2 = google::cloud::oauth2_internal;

namespace duckdb {
namespace bigquery {

//===--------------------------------------------------------------------===//
// BigquerySecret Implementation
//===--------------------------------------------------------------------===//

BigquerySecret::BigquerySecret(const vector<string> &prefix_paths, const string &provider, const string &name)
    : KeyValueSecret(prefix_paths, "bigquery", provider, name) {
    serializable = true;
    redact_keys = {kAccessToken, kServiceAccountJson, kServiceAccountPath, kExternalAccountJson, kExternalAccountPath};
}

BigquerySecret::BigquerySecret(const BaseSecret &base_secret)
    : KeyValueSecret(base_secret.GetScope(), base_secret.GetType(), base_secret.GetProvider(), base_secret.GetName()) {
    serializable = true;
    redact_keys = {kAccessToken, kServiceAccountJson, kServiceAccountPath, kExternalAccountJson, kExternalAccountPath};
    if (auto kv_secret = dynamic_cast<const KeyValueSecret *>(&base_secret)) {
        secret_map = kv_secret->secret_map;
    }
}

string BigquerySecret::GetAccessToken() const {
    auto it = secret_map.find(kAccessToken);
    if (it != secret_map.end()) {
        return it->second.GetValue<string>();
    }
    return "";
}

string BigquerySecret::GetServiceAccountKeyJson() const {
    auto it = secret_map.find(kServiceAccountJson);
    if (it != secret_map.end()) {
        return it->second.GetValue<string>();
    }
    return "";
}

string BigquerySecret::GetServiceAccountKeyPath() const {
    auto it = secret_map.find(kServiceAccountPath);
    if (it != secret_map.end()) {
        return it->second.GetValue<string>();
    }
    return "";
}

string BigquerySecret::GetExternalAccountCredsJson() const {
    auto it = secret_map.find(kExternalAccountJson);
    if (it != secret_map.end()) {
        return it->second.GetValue<string>();
    }
    return "";
}

string BigquerySecret::GetExternalAccountCredsPath() const {
    auto it = secret_map.find(kExternalAccountPath);
    if (it != secret_map.end()) {
        return it->second.GetValue<string>();
    }
    return "";
}

void BigquerySecret::Serialize(Serializer &serializer) const {
    SerializeBaseSecret(serializer);

    // Write CUSTOM serialization type BEFORE the secret_map (property 201)
    // This ensures properties are in ascending order: 100, 101, 102, 103, 104, 201, 202
    serializer.WriteProperty(104, "serialization_type", SecretSerializationType::CUSTOM);

    vector<Value> map_values;
    for (const auto &kv : secret_map) {
        child_list_t<Value> map_struct;
        map_struct.push_back(make_pair("key", Value(kv.first)));
        map_struct.push_back(make_pair("value", kv.second));
        map_values.push_back(Value::STRUCT(map_struct));
    }
    auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::ANY);
    serializer.WriteProperty(201, "secret_map", Value::MAP(ListType::GetChildType(map_type), map_values));

    vector<Value> redact_values;
    for (const auto &key : redact_keys) {
        redact_values.push_back(Value(key));
    }
    serializer.WriteProperty(202, "redact_keys", Value::LIST(LogicalType::VARCHAR, redact_values));
}

unique_ptr<BaseSecret> BigquerySecret::Deserialize(Deserializer &deserializer, BaseSecret base_secret) {
    return KeyValueSecret::Deserialize<BigquerySecret>(deserializer, std::move(base_secret));
}

unique_ptr<const BaseSecret> BigquerySecret::Clone() const {
    return make_uniq<BigquerySecret>(*this);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

static string ReadJsonFile(const string &param, const string &file_path, const string &example_path) {
    string error_msg = "The '" + param + "' parameter must be a valid file path (e.g., '" + example_path + "')";

	if (!std::filesystem::exists(std::filesystem::path(file_path))) {
        throw InvalidInputException(error_msg);
    }

    std::ifstream json_file(file_path);
    if (!json_file.is_open()) {
        throw InvalidInputException(error_msg);
    }

    std::string json_content((std::istreambuf_iterator<char>(json_file)), std::istreambuf_iterator<char>());
    json_file.close();
    return json_content;
}

void ValidateCredentialInput(const string &param, const string &value) {
    bool is_service_account = (param == kServiceAccountJson || param == kServiceAccountPath);
    bool is_path = (param == kServiceAccountPath || param == kExternalAccountPath);

    string json_content = value;
    if (is_path) {
        string example_path = is_service_account ? "/path/to/key.json" : "/path/to/credentials.json";
        json_content = ReadJsonFile(param, value, example_path);
    }

    if (is_service_account) {
        auto result = oauth2::ParseServiceAccountCredentials(json_content, "duckdb_secret");
        if (result.ok()) return;

        if (is_path) {
            string err = "The '" + param + "' parameter points to a file with invalid service account JSON.";
            throw InvalidInputException(err);
        } else {
            string err = "The '" + param + "' parameter must be valid JSON content for service account credentials.";
            throw InvalidInputException(err);
        }
    } else {
        auto result = oauth2::ParseExternalAccountConfiguration(json_content, google::cloud::internal::ErrorContext{});
        if (result.ok()) return;

        if (is_path) {
            string err =
                "The '" + param + "' parameter points to a file with invalid external account JSON: '" + value + "'";
            throw InvalidInputException(err);
        } else {
            string err = "The '" + param + "' parameter must be valid JSON content for external account credentials.";
            throw InvalidInputException(err);
        }
    }
}

std::shared_ptr<google::cloud::Credentials> CreateGCPCredentialsFromSecret(const BigquerySecret &secret) {
    auto access_token = secret.GetAccessToken();
    if (!access_token.empty()) {
        return google::cloud::MakeAccessTokenCredentials(access_token, {});
    }

    auto service_account_path = secret.GetServiceAccountKeyPath();
    if (!service_account_path.empty()) {
        std::ifstream json_file(service_account_path);
        if (!json_file.is_open()) {
            std::cerr << "Failed to open service account key file: " << service_account_path << std::endl;
            return nullptr;
        }
        std::string json_content((std::istreambuf_iterator<char>(json_file)), std::istreambuf_iterator<char>());
        json_file.close();
        return google::cloud::MakeServiceAccountCredentials(json_content);
    }

    auto service_account_json = secret.GetServiceAccountKeyJson();
    if (!service_account_json.empty()) {
        return google::cloud::MakeServiceAccountCredentials(service_account_json);
    }

    auto external_account_path = secret.GetExternalAccountCredsPath();
    if (!external_account_path.empty()) {
        std::ifstream json_file(external_account_path);
        if (!json_file.is_open()) {
            std::cerr << "Failed to open external account credentials file: " << external_account_path << std::endl;
            return nullptr;
        }
        std::string json_content((std::istreambuf_iterator<char>(json_file)), std::istreambuf_iterator<char>());
        json_file.close();
        return google::cloud::MakeExternalAccountCredentials(json_content);
    }

    auto external_account_json = secret.GetExternalAccountCredsJson();
    if (!external_account_json.empty()) {
        return google::cloud::MakeExternalAccountCredentials(external_account_json);
    }

    return nullptr;
}

SecretMatch LookupBigQuerySecret(ClientContext &context, const string &project_id) {
    auto &secret_manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

    vector<string> scope_prefixes = {"bq://", "bigquery://"};
    for (const auto &prefix : scope_prefixes) {
        string scope = prefix + project_id;
        auto secret_match = secret_manager.LookupSecret(transaction, scope, "bigquery");

        if (secret_match.HasMatch()) {
            auto &secret = secret_match.GetSecret();
            if (secret.GetType() == "bigquery") {
                return secret_match;
            }
        }
    }

    return SecretMatch();
}

//===--------------------------------------------------------------------===//
// Secret Registration
//===--------------------------------------------------------------------===//

unique_ptr<BaseSecret> CreateBigquerySecretFunction(ClientContext &context, CreateSecretInput &input) {
    auto bigquery_secret = make_uniq<BigquerySecret>(input.scope, input.provider, input.name);

    int auth_methods_count = 0;
    vector<string> auth_methods = {kAccessToken,
                                   kServiceAccountPath,
                                   kServiceAccountJson,
                                   kExternalAccountPath,
                                   kExternalAccountJson};

    for (const auto &method : auth_methods) {
        if (bigquery_secret->TrySetValue(method, input)) {
            auth_methods_count++;
        }
    }

    if (auth_methods_count == 0) {
        throw InvalidInputException( //
            "BigQuery secret must contain one of: 'access_token', 'service_account_path', "
            "'service_account_json', 'external_account_path', or 'external_account_json'");
    } else if (auth_methods_count > 1) {
        throw InvalidInputException( //
            "BigQuery secret must contain exactly one authentication method. Please provide "
            "only one of: 'access_token', 'service_account_path', 'service_account_json', 'external_account_path', "
            "or 'external_account_json'");
    }

    // Validate all credential parameters (only if not empty)
    auto service_account_json = bigquery_secret->GetServiceAccountKeyJson();
    if (!service_account_json.empty()) {
        ValidateCredentialInput(kServiceAccountJson, service_account_json);
    }

    auto service_account_path = bigquery_secret->GetServiceAccountKeyPath();
    if (!service_account_path.empty()) {
        ValidateCredentialInput(kServiceAccountPath, service_account_path);
    }

    auto external_account_json = bigquery_secret->GetExternalAccountCredsJson();
    if (!external_account_json.empty()) {
        ValidateCredentialInput(kExternalAccountJson, external_account_json);
    }

    auto external_account_path = bigquery_secret->GetExternalAccountCredsPath();
    if (!external_account_path.empty()) {
        ValidateCredentialInput(kExternalAccountPath, external_account_path);
    }

    return std::move(bigquery_secret);
}

void RegisterBigquerySecretType(DatabaseInstance &db) {
    auto &secret_mgr = SecretManager::Get(db);

    SecretType bq_secret_type;
    bq_secret_type.name = "bigquery";
    bq_secret_type.default_provider = "config";
    bq_secret_type.deserializer = BigquerySecret::Deserialize;
    bq_secret_type.extension = "bigquery";
    secret_mgr.RegisterSecretType(bq_secret_type);

    CreateSecretFunction create_func;
    create_func.secret_type = "bigquery";
    create_func.provider = "config";
    create_func.function = CreateBigquerySecretFunction;
    create_func.named_parameters[kAccessToken] = LogicalType::VARCHAR;
    create_func.named_parameters[kServiceAccountJson] = LogicalType::VARCHAR;
    create_func.named_parameters[kServiceAccountPath] = LogicalType::VARCHAR;
    create_func.named_parameters[kExternalAccountJson] = LogicalType::VARCHAR;
    create_func.named_parameters[kExternalAccountPath] = LogicalType::VARCHAR;

    secret_mgr.RegisterSecretFunction(create_func, OnCreateConflict::ERROR_ON_CONFLICT);
}

} // namespace bigquery
} // namespace duckdb
