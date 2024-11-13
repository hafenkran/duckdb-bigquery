#pragma once

#include "duckdb.hpp"

#include <cstdlib>
#include <fstream>

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
        "/etc/ssl/cert.pem" //
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
        "/usr/local/etc/openssl/cert.pem" //
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
        if (curl_ca_bundle_path.empty()) {
            curl_ca_bundle_path = DetectCAPath();
        }
        if (curl_ca_bundle_path.empty()) {
            throw BinderException("Curl CA bundle path not found. Try setting the 'bq_curl_ca_bundle_path' option.");
        }
        return curl_ca_bundle_path;
    }

    static void SetCurlCaBundlePath(ClientContext &context, SetScope scope, Value &parameter) {
        string path = StringValue::Get(parameter);
        if (path.empty()) {
            throw InvalidInputException("Curl CA bundle path cannot be empty");
        } else if (!std::ifstream(path).good()) {
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
};


} // namespace bigquery
} // namespace duckdb
