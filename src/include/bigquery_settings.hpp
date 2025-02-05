#pragma once

#include "duckdb.hpp"

#include <cstdlib>
#include <fstream>
#include <iostream>

namespace duckdb {
namespace bigquery {

inline std::string DetectCAPath() {
    string ca_path;

#if defined(_WIN32) || defined(_WIN64)
    return ca_path;
#elif defined(__linux__) || defined(__unix__)
    if (const char *ca_path_env = std::getenv("SSL_CERT_FILE"); ca_path_env != nullptr) {
        return std::string(ca_path_env);
    }
    const char *ca_paths[] = {
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/ssl/ca-bundle.pem",
        "/etc/ssl/cert.pem",
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
        "/etc/openssl/certs/ca-certificates.crt",
        "/var/lib/ca-certificates/ca-bundle.pem",
        "/usr/local/share/certs/ca-root-nss.crt",
        "/usr/local/etc/openssl/cert.pem" //
    };
    for (const char *path : ca_paths) {
        if (std::ifstream(path).good()) {
            return std::string(path);
        }
    }
#elif defined(__APPLE__)
    if (const char *ca_path_env = std::getenv("SSL_CERT_FILE"); ca_path_env != nullptr) {
        return std::string(ca_path_env);
    }
    const char *mac_ca_paths[] = {
        "/etc/ssl/cert.pem",
        "/usr/local/etc/openssl/cert.pem",
        "/usr/local/etc/openssl@1.1/cert.pem",
        "/opt/homebrew/etc/openssl/cert.pem",
        "/opt/homebrew/etc/openssl@1.1/cert.pem" //
    };
    for (const char *path : mac_ca_paths) {
        if (std::ifstream(path).good()) {
            return std::string(path);
        }
    }
#endif
    return ca_path;
}


struct BigquerySettings {
public:
    static string &DefaultLocation() {
        static string bigquery_default_location = "US";
        return bigquery_default_location;
    }

    static void SetDefaultLocation(ClientContext &context, SetScope scope, Value &parameter) {
        DefaultLocation() = StringValue::Get(parameter);
    }

    static bool &DebugQueryPrint() {
        static bool bigquery_debug_query_print = false;
        return bigquery_debug_query_print;
    }

    static void SetDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
        DebugQueryPrint() = BooleanValue::Get(parameter);
    }

    static bool &ExperimentalFilterPushdown() {
        static bool bigquery_experimental_filter_pushdown = true;
        return bigquery_experimental_filter_pushdown;
    }

    static void SetExperimentalFilterPushdown(ClientContext &context, SetScope scope, Value &parameter) {
        ExperimentalFilterPushdown() = BooleanValue::Get(parameter);
    }

    static string &CurlCaBundlePath() {
        static string curl_ca_bundle_path = "";
        return curl_ca_bundle_path;
    }

    static void SetCurlCaBundlePath(ClientContext &context, SetScope scope, Value &parameter) {
        string path = StringValue::Get(parameter);
        if (path.empty()) {
            throw InvalidInputException("Failed to detect Curl CA bundle path");
        }
        if (!std::ifstream(path).good()) {
            throw InvalidInputException("Curl CA bundle path is not readable");
        }
        CurlCaBundlePath() = path;
    }

    static void TryDetectCurlCaBundlePath() {
        string path = DetectCAPath();
        if (path.empty()) {
            throw InvalidInputException("Failed to detect Curl CA bundle path");
        }
        if (!std::ifstream(path).good()) {
            throw InvalidInputException("Curl CA bundle path is not readable");
        }
        CurlCaBundlePath() = path;
    }

    static bool &ExperimentalFetchCatalogFromInformationSchema() {
        static bool bigquery_experimental_fetch_catalog_from_information_schema = true;
        return bigquery_experimental_fetch_catalog_from_information_schema;
    }

    static void SetExperimentalFetchCatalogFromInformationSchema(ClientContext &context,
                                                                 SetScope scope,
                                                                 Value &parameter) {
        ExperimentalFetchCatalogFromInformationSchema() = BooleanValue::Get(parameter);
    }

    static int &QueryTimeoutMs() {
        static int bigquery_query_timeout_ms = 60000;
        return bigquery_query_timeout_ms;
    }

    static void SetQueryTimeoutMs(ClientContext &context, SetScope scope, Value &parameter) {
        int timeout = IntegerValue::Get(parameter);
        if (timeout < 0) {
            throw InvalidInputException("Query timeout must be non-negative");
        } else if (timeout > 200000) {
            std::cout << "Warning: Query timeout is set to a very high value. The BigQuery documentation states that "
                         "the call is not guaranteed to wait for the specified timeout and returns typically after "
                         "around 200 seconds even if the query is not complete (see "
                         "https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#QueryRequest)"
                      << std::endl;
        }
        QueryTimeoutMs() = timeout;
    }
};


} // namespace bigquery
} // namespace duckdb
