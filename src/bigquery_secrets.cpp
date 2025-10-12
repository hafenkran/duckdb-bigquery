#include "bigquery_secrets.hpp"

#include "duckdb/main/secret/secret_manager.hpp"

#include "google/cloud/internal/oauth2_access_token_credentials.h"
#include "google/cloud/internal/oauth2_external_account_credentials.h"
#include "google/cloud/internal/oauth2_service_account_credentials.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <sys/stat.h>

namespace duckdb {
namespace bigquery {

namespace oauth2 = google::cloud::oauth2_internal;


//===--------------------------------------------------------------------===//
// BigquerySecret Implementation
//===--------------------------------------------------------------------===//

BigquerySecret::BigquerySecret(const vector<string> &prefix_paths, const string &provider, const string &name)
    : KeyValueSecret(prefix_paths, "bigquery", provider, name) {
    serializable = true;
    redact_keys = {"access_token", "sa_key_json", "sa_key_path", "ea_config_json", "ea_config_path"};
}

BigquerySecret::BigquerySecret(const BaseSecret &base_secret)
    : KeyValueSecret(base_secret.GetScope(), base_secret.GetType(), base_secret.GetProvider(), base_secret.GetName()) {
    serializable = true;
    redact_keys = {"access_token", "sa_key_json", "sa_key_path", "ea_config_json", "ea_config_path"};
    if (auto kv_secret = dynamic_cast<const KeyValueSecret *>(&base_secret)) {
        secret_map = kv_secret->secret_map;
    }
}

string BigquerySecret::GetAccessToken() const {
    auto it = secret_map.find("access_token");
    if (it != secret_map.end()) {
        return it->second.GetValue<string>();
    }
    return "";
}

string BigquerySecret::GetServiceAccountKeyJson() const {
    auto it = secret_map.find("sa_key_json");
    if (it != secret_map.end()) {
        return it->second.GetValue<string>();
    }
    return "";
}

string BigquerySecret::GetServiceAccountKeyPath() const {
    auto it = secret_map.find("sa_key_path");
    if (it != secret_map.end()) {
        return it->second.GetValue<string>();
    }
    return "";
}

string BigquerySecret::GetExternalAccountCredsJson() const {
    auto it = secret_map.find("ea_config_json");
    if (it != secret_map.end()) {
        return it->second.GetValue<string>();
    }
    return "";
}

string BigquerySecret::GetExternalAccountCredsPath() const {
    auto it = secret_map.find("ea_config_path");
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

KeyKind ClassifyCredentialInput(const string &input) {
    auto sa = oauth2::ParseServiceAccountCredentials(input, "duckdb_secret");
    if (sa.ok()) return KeyKind::kServiceAccount;

    auto ext = oauth2::ParseExternalAccountConfiguration(input, google::cloud::internal::ErrorContext{});
    if (ext.ok()) return KeyKind::kExternalAccount;

    using std::filesystem::exists;
    if (exists(std::filesystem::path(input))) {
        // Try to load and parse the file
        std::ifstream json_file(input);
        if (json_file.is_open()) {
            std::string json_content((std::istreambuf_iterator<char>(json_file)), std::istreambuf_iterator<char>());
            json_file.close();

            // Try to parse the loaded content as service account credentials
            auto sa_file = oauth2::ParseServiceAccountCredentials(json_content, "duckdb_secret");
            if (sa_file.ok()) return KeyKind::kServiceAccountPath;

            // Try to parse the loaded content as external account credentials
            auto ext_file = oauth2::ParseExternalAccountConfiguration(json_content, google::cloud::internal::ErrorContext{});
            if (ext_file.ok()) return KeyKind::kExternalAccountPath;

            return KeyKind::kInvalidFile;
        }
    }

    return KeyKind::kInvalid;
}

std::shared_ptr<google::cloud::Credentials> CreateGCPCredentialsFromSecret(const BigquerySecret &secret) {
    auto access_token = secret.GetAccessToken();
    if (!access_token.empty()) {
        return google::cloud::MakeAccessTokenCredentials(access_token, {});
    }

    auto sa_key_path = secret.GetServiceAccountKeyPath();
    if (!sa_key_path.empty()) {
        std::ifstream json_file(sa_key_path);
        if (!json_file.is_open()) {
            std::cerr << "Failed to open service account key file: " << sa_key_path << std::endl;
            return nullptr;
        }
        std::string json_content((std::istreambuf_iterator<char>(json_file)), std::istreambuf_iterator<char>());
        json_file.close();
        return google::cloud::MakeServiceAccountCredentials(json_content);
    }

    auto sa_key_json = secret.GetServiceAccountKeyJson();
    if (!sa_key_json.empty()) {
        return google::cloud::MakeServiceAccountCredentials(sa_key_json);
    }

    auto ea_config_path = secret.GetExternalAccountCredsPath();
    if (!ea_config_path.empty()) {
        std::ifstream json_file(ea_config_path);
        if (!json_file.is_open()) {
            std::cerr << "Failed to open external account credentials file: " << ea_config_path << std::endl;
            return nullptr;
        }
        std::string json_content((std::istreambuf_iterator<char>(json_file)), std::istreambuf_iterator<char>());
        json_file.close();
        return google::cloud::MakeExternalAccountCredentials(json_content);
    }

    auto ea_config_json = secret.GetExternalAccountCredsJson();
    if (!ea_config_json.empty()) {
        return google::cloud::MakeExternalAccountCredentials(ea_config_json);
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
    vector<string> auth_methods = {"access_token", "sa_key_path", "sa_key_json", "ea_config_path", "ea_config_json"};

    for (const auto &method : auth_methods) {
        if (bigquery_secret->TrySetValue(method, input)) {
            auth_methods_count++;
        }
    }

    if (auth_methods_count == 0) {
        throw InvalidInputException( //
            "BigQuery secret must contain one of: 'access_token', 'sa_key_path', "
            "'sa_key_json', 'ea_config_path', or 'ea_config_json'");
    } else if (auth_methods_count > 1) {
        throw InvalidInputException( //
            "BigQuery secret must contain exactly one authentication method. Please provide "
            "only one of: 'access_token', 'sa_key_path', 'sa_key_json', 'ea_config_path', "
            "or 'ea_config_json'");
    }

    // Validate sa_key_path
    auto sa_key_path = bigquery_secret->GetServiceAccountKeyPath();
    if (!sa_key_path.empty()) {
        switch (ClassifyCredentialInput(sa_key_path)) {
        case KeyKind::kServiceAccountPath:
            break;
        case KeyKind::kServiceAccount:
            throw InvalidInputException( //
                "The 'sa_key_path' parameter must be a file path, not JSON content. "
                "Use 'sa_key_json' for inline JSON credentials.");
        case KeyKind::kExternalAccount:
            throw InvalidInputException( //
                "The 'sa_key_path' parameter must be a file path, not external account JSON "
                "content. Use 'sa_key_json' for inline JSON credentials.");
        case KeyKind::kExternalAccountPath:
            throw InvalidInputException( //
                "The 'sa_key_path' parameter points to an external account credentials file. "
                "Use 'ea_config_path' for external account credentials.");
        case KeyKind::kInvalidFile:
            throw InvalidInputException( //
                "The service account key file is not valid JSON or has an invalid format: '" + sa_key_path + "'");
        case KeyKind::kInvalid:
            throw InvalidInputException( //
                "The 'sa_key_path' parameter must be a valid file path (e.g., "
                "'/path/to/key.json')");
        }
    }

    // Validate sa_key_json
    auto sa_key_json = bigquery_secret->GetServiceAccountKeyJson();
    if (!sa_key_json.empty()) {
        switch (ClassifyCredentialInput(sa_key_json)) {
        case KeyKind::kServiceAccount:
            break;
        case KeyKind::kServiceAccountPath:
            throw InvalidInputException( //
                "The 'sa_key_json' parameter must be JSON content, not a file path. "
                "Use 'sa_key_path' for file paths.");
        case KeyKind::kExternalAccount:
            throw InvalidInputException(
                "The 'sa_key_json' parameter must be JSON content for service account, not "
                "external account credentials. Use 'ea_config_json' for external account credentials.");
        case KeyKind::kExternalAccountPath:
            throw InvalidInputException( //
                "The 'sa_key_json' parameter must be JSON content, not a file path. "
                "Use 'sa_key_path' for file paths.");
        case KeyKind::kInvalidFile:
            throw InvalidInputException( //
                "The 'sa_key_json' parameter contains a file path, but the file is not "
                "valid JSON or has an invalid format. Use 'sa_key_path' for file paths.");
        case KeyKind::kInvalid:
            throw InvalidInputException(
                "The 'sa_key_json' parameter must be valid JSON content for service account credentials.");
        }
    }

    // Validate ea_config_path
    auto ea_config_path = bigquery_secret->GetExternalAccountCredsPath();
    if (!ea_config_path.empty()) {
        switch (ClassifyCredentialInput(ea_config_path)) {
        case KeyKind::kExternalAccountPath:
            break;
        case KeyKind::kServiceAccountPath:
            throw InvalidInputException( //
                "The 'ea_config_path' parameter points to a service account key file. "
                "Use 'sa_key_path' for service account credentials.");
        case KeyKind::kServiceAccount:
            throw InvalidInputException( //
                "The 'ea_config_path' parameter must be a file path, not JSON content. "
                "Use 'ea_config_json' for inline JSON credentials.");
        case KeyKind::kExternalAccount:
            throw InvalidInputException( //
                "The 'ea_config_path' parameter must be a file path, not JSON content. "
                "Use 'ea_config_json' for inline JSON credentials.");
        case KeyKind::kInvalidFile:
            throw InvalidInputException(
                "The external account credentials file is not valid JSON or has an invalid format: '" + ea_config_path +
                "'");
        case KeyKind::kInvalid:
            throw InvalidInputException( //
                "The 'ea_config_path' parameter must be a valid file path (e.g., "
                "'/path/to/credentials.json')");
        }
    }

    // Validate ea_config_json
    auto ea_config_json = bigquery_secret->GetExternalAccountCredsJson();
    if (!ea_config_json.empty()) {
        switch (ClassifyCredentialInput(ea_config_json)) {
        case KeyKind::kExternalAccount:
            break;
        case KeyKind::kServiceAccount:
            throw InvalidInputException( //
                "The 'ea_config_json' parameter must be JSON content for external account, not "
                "service account credentials. Use 'sa_key_json' for service account credentials.");
        case KeyKind::kServiceAccountPath:
            throw InvalidInputException( //
                "The 'ea_config_json' parameter must be JSON content, not a file path. Use "
                "'ea_config_path' for file paths.");
        case KeyKind::kExternalAccountPath:
            throw InvalidInputException( //
                "The 'ea_config_json' parameter must be JSON content, not a file path. Use "
                "'ea_config_path' for file paths.");
        case KeyKind::kInvalidFile:
            throw InvalidInputException( //
                "The 'ea_config_json' parameter contains a file path, but the file is not "
                "valid JSON or has an invalid format. Use 'ea_config_path' for file paths.");
        case KeyKind::kInvalid:
            throw InvalidInputException(
                "The 'ea_config_json' parameter must be valid JSON content for external account credentials.");
        }
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
    create_func.named_parameters["access_token"] = LogicalType::VARCHAR;
    create_func.named_parameters["sa_key_json"] = LogicalType::VARCHAR;
    create_func.named_parameters["sa_key_path"] = LogicalType::VARCHAR;
    create_func.named_parameters["ea_config_json"] = LogicalType::VARCHAR;
    create_func.named_parameters["ea_config_path"] = LogicalType::VARCHAR;

    secret_mgr.RegisterSecretFunction(create_func, OnCreateConflict::ERROR_ON_CONFLICT);
}

} // namespace bigquery
} // namespace duckdb
