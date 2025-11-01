#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "google/cloud/credentials.h"

namespace duckdb {
namespace bigquery {

// Secret parameter name constants
static constexpr const char *kAccessToken = "access_token";
static constexpr const char *kServiceAccountJson = "service_account_json";
static constexpr const char *kServiceAccountPath = "service_account_path";
static constexpr const char *kExternalAccountJson = "external_account_json";
static constexpr const char *kExternalAccountPath = "external_account_path";

class BigquerySecret : public KeyValueSecret {
public:
    //! Constructor for creating a new BigQuery secret
    BigquerySecret(const vector<string> &prefix_paths, const string &provider, const string &name);

    //! Constructor for deserialization
    BigquerySecret(const BaseSecret &base_secret);

public:
    //! Get the access_token from the secret
    string GetAccessToken() const;

    //! Get the service account key JSON from the secret
    string GetServiceAccountKeyJson() const;

    //! Get the service account key file path from the secret
    string GetServiceAccountKeyPath() const;

    //! Get the external account credentials JSON from the secret
    string GetExternalAccountCredsJson() const;

    //! Get the external account credentials file path from the secret
    string GetExternalAccountCredsPath() const;

    //! Serialize method
    void Serialize(Serializer &serializer) const override;

    //! Deserializer function for BigquerySecret
    static unique_ptr<BaseSecret> Deserialize(Deserializer &deserializer, BaseSecret base_secret);

    //! Clone method to preserve the BigquerySecret type
    unique_ptr<const BaseSecret> Clone() const override;
};

//! Validate credential input based on parameter name
//! Throws InvalidInputException with detailed error message if validation fails
void ValidateCredentialInput(const string &param, const string &value);

//! Helper function to create Google Cloud credentials from a BigQuery secret
//! Returns nullptr if no credentials could be created
std::shared_ptr<google::cloud::Credentials> CreateGCPCredentialsFromSecret(const BigquerySecret &secret);

//! Helper function to lookup a secret for a specific project_id using scope bq://project_id or bigquery://project_id
//! Returns a SecretMatch that owns the secret to prevent use-after-free
SecretMatch LookupBigQuerySecret(ClientContext &context, const string &project_id);

//! Register the BigQuery secret type and creation functions with the SecretManager
void RegisterBigquerySecretType(DatabaseInstance &db);

} // namespace bigquery
} // namespace duckdb
