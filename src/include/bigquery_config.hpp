#pragma once

#include "duckdb.hpp"

#include <fstream>
#include <cstdlib>

namespace duckdb {
namespace bigquery {

inline std::string DetectCAPath() {
	string ca_path;

#if defined(_WIN32) || defined(_WIN64)
	return ca_path;
#elif defined(__linux__) || defined(__unix__)
    if (const char* ca_path_env = std::getenv("SSL_CERT_FILE"); ca_path_env != nullptr) {
        return std::string(ca_path_env);
    }
    const char* ca_paths[] = {
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/ssl/ca-bundle.pem",
        "/etc/ssl/cert.pem"
    };
	for (const char* path : ca_paths) {
        if (std::ifstream(path).good()) {
            return std::string(path);
        }
    }
#endif

	return ca_path;
}


struct BigqueryConfig {
public:
	static string& DefaultLocation() {
		static string bigquery_default_location = "US";
		return bigquery_default_location;
	}

	static void SetDefaultLocation(ClientContext &context, SetScope scope, Value &parameter){
		DefaultLocation() = StringValue::Get(parameter);
	}

	static bool& DebugQueryPrint() {
		static bool bigquery_debug_query_print = false;
		return bigquery_debug_query_print;
	}

	static void SetDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
		DebugQueryPrint() = BooleanValue::Get(parameter);
	}

	static bool& ExperimentalFilterPushdown() {
		static bool bigquery_experimental_filter_pushdown = true;
		return bigquery_experimental_filter_pushdown;
	}

	static void SetExperimentalFilterPushdown(ClientContext &context, SetScope scope, Value &parameter) {
		ExperimentalFilterPushdown() = BooleanValue::Get(parameter);
	}

	static string& CurlCaBundlePath() {
		static string curl_ca_bundle_path = "";
		if (curl_ca_bundle_path.empty()) {
			curl_ca_bundle_path = DetectCAPath();
		}
		return curl_ca_bundle_path;
	}

	static void SetCurlCaBundlePath(ClientContext &context, SetScope scope, Value &parameter) {
		string path = StringValue::Get(parameter);
		if (!path.empty() || !std::ifstream(path).good()) {
			throw InvalidInputException("Path to CA bundle is not readable");
		}
		CurlCaBundlePath() = path;
	}
};


} // namespace bigquery
} // namespace duckdb
