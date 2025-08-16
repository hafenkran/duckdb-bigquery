#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"

#include "google/cloud/bigquery/storage/v1/arrow.pb.h"

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

    static bool &ExperimentalEnableBigqueryOptions() {
        static bool bigquery_experimental_enable_bigquery_options = false;
        return bigquery_experimental_enable_bigquery_options;
    }

    static void SetExperimentalEnableBigqueryOptions(ClientContext &context, SetScope scope, Value &parameter) {
        ExperimentalEnableBigqueryOptions() = BooleanValue::Get(parameter);
    }

    static int &QueryTimeoutMs() {
        static int bigquery_query_timeout_ms = 90000;
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

    static bool &BignumericAsVarchar() {
        static bool bigquery_bignumeric_as_varchar = false;
        return bigquery_bignumeric_as_varchar;
    }

    static void SetBignumericAsVarchar(ClientContext &context, SetScope scope, Value &parameter) {
        BignumericAsVarchar() = BooleanValue::Get(parameter);
        
        // Warn user if they enable BIGNUMERIC as VARCHAR with V2 engine as default
        if (BooleanValue::Get(parameter) && DefaultScanEngine() == "v2") {
            printf("WARNING: bq_bignumeric_as_varchar=TRUE is only supported with V1 scan engine. "
                   "Current default scan engine is 'v2'. Consider setting bq_default_scan_engine='v1' "
                   "or use engine='v1' parameter in scan functions.\n");
        }
    }

    static int &MaxReadStreams() {
        static int bigquery_max_read_streams = 0; // 0 means default DuckDB thread count
        return bigquery_max_read_streams;
    }

    static void SetMaxReadStreams(ClientContext &context, SetScope scope, Value &parameter) {
        int max_streams = IntegerValue::Get(parameter);
        if (max_streams < 0) {
            throw InvalidInputException("Max read streams must be non-negative (0 or greater). Use 0 to automatically "
                                        "match the number of DuckDB threads.");
        }
        auto &config = DBConfig::GetConfig(context);
        if (config.options.preserve_insertion_order && max_streams > 1) {
            std::cout
                << "Warning: preserve_insertion_order` is set to true, but `max_read_streams` is set to " << max_streams
                << ". This will cause the query to run in a single stream, ignoring the `max_read_streams` setting. "
                << std::endl;
        }
        MaxReadStreams() = max_streams;
    }

    static int GetMaxReadStreams(ClientContext &context) {
        auto &config = DBConfig::GetConfig(context);
        auto &preserve_insertion_order = config.options.preserve_insertion_order;
        if (preserve_insertion_order) {
            // when preserve_insertion_order is true, we can only use 1 stream
            return 1;
        }
        auto &max_read_streams = MaxReadStreams();
        if (MaxReadStreams() == 0) {
            // if max_read_streams is 0, we use the maximum threads from the DuckDB config
            return config.options.maximum_threads;
        }
        return max_read_streams;
    }

    static string &ArrowCompression() {
        static string bigquery_compression = "ZSTD";
        return bigquery_compression;
    }

    static void SetArrowCompression(ClientContext &context, SetScope scope, Value &parameter) {
        string compression = StringValue::Get(parameter);
        if (compression != "UNSPECIFIED" && compression != "LZ4_FRAME" && compression != "ZSTD") {
            throw InvalidInputException("Compression must be one of: UNSPECIFIED, LZ4_FRAME, ZSTD");
        }
        ArrowCompression() = compression;
    }

    static google::cloud::bigquery::storage::v1::ArrowSerializationOptions::CompressionCodec GetArrowCompressionCodec() {
        const string &compression = ArrowCompression();
        if (compression == "UNSPECIFIED") {
            return google::cloud::bigquery::storage::v1::ArrowSerializationOptions::COMPRESSION_UNSPECIFIED;
        } else if (compression == "LZ4_FRAME") {
            return google::cloud::bigquery::storage::v1::ArrowSerializationOptions::LZ4_FRAME;
        } else if (compression == "ZSTD") {
            return google::cloud::bigquery::storage::v1::ArrowSerializationOptions::ZSTD;
        } else {
            // Default fallback
            return google::cloud::bigquery::storage::v1::ArrowSerializationOptions::LZ4_FRAME;
        }
    }

	static string &DefaultScanEngine() {
		static string bigquery_default_scan_engine = "v2";
		return bigquery_default_scan_engine;
	}

	static void SetDefaultScanEngine(ClientContext &context, SetScope scope, Value &parameter) {
		auto engine = StringUtil::Lower(parameter.GetValue<string>());
		if (engine != "v1" && engine != "v2" && engine != "legacy") {
			throw InvalidInputException("Invalid default scan engine: '%s'. Allowed: 'v1', 'v2', 'legacy'", engine);
		}
		DefaultScanEngine() = engine;
	}
};


} // namespace bigquery
} // namespace duckdb
