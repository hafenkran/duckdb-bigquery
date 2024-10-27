#pragma once

#include "duckdb.hpp"

// #include "google/cloud/bigquery/v2/minimal/internal/table_schema.h"

#include "google/cloud/bigquerycontrol/v2/table_client.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <arrow/api.h>
#include <chrono>
#include <regex>
#include <thread>


namespace duckdb {
namespace bigquery {

struct BigqueryUtils;

struct BigqueryConfig {
public:
	BigqueryConfig() = default;
    explicit BigqueryConfig(const std::string &project_id,
                            const std::string &dataset_id = "",
                            const std::string &billing_project_id = "",
                            const std::string &api_endpoint = "",
                            const std::string &grpc_endpoint = "")
        : project_id(project_id),                 //
          dataset_id(dataset_id),                 //
          billing_project_id(billing_project_id), //
          api_endpoint(api_endpoint),             //
          grpc_endpoint(grpc_endpoint) {
        if (project_id.empty()) {
            throw std::invalid_argument("Project ID is required and cannot be empty.");
        }
    }

    BigqueryConfig(const BigqueryConfig &other) = default;
    BigqueryConfig &operator=(const BigqueryConfig &other) = default;

    ~BigqueryConfig() = default;

    const bool has_project_id() const {
        return !project_id.empty();
    }

    const bool has_dataset_id() const {
        return !dataset_id.empty();
    }

    const bool has_billing_project_id() const {
        return !billing_project_id.empty();
    }

    const bool has_api_endpoint() const {
        return !api_endpoint.empty();
    }

    const bool has_grpc_endpoint() const {
        return !grpc_endpoint.empty();
    }

    const bool is_dev_env() const {
        return api_endpoint.find("localhost") != std::string::npos ||
               api_endpoint.find("127.0.0.1") != std::string::npos;
    }

    const string billing_project() const {
        return has_billing_project_id() ? billing_project_id : project_id;
    }

    static BigqueryConfig FromDSN(const string &connection_string);

public:
    string project_id;
    string dataset_id;
    string billing_project_id;
    string api_endpoint;
    string grpc_endpoint;
};

struct BigqueryDatasetRef {
	BigqueryDatasetRef() = default;
	BigqueryDatasetRef(const string &project_id, const string &dataset_id)
		: project_id(project_id), dataset_id(dataset_id) {
	}

    const bool has_dataset_id() const {
        return dataset_id != "";
    }

public:
    string project_id;
    string dataset_id;
    string location;
};

struct BigqueryTableRef {
    BigqueryTableRef() = default;
    BigqueryTableRef(const string &project_id, const string &dataset_id, const string &table_id)
        : project_id(project_id), dataset_id(dataset_id), table_id(table_id) {
    }

    const bool has_dataset_id() const {
        return dataset_id != "";
    }

    const bool has_table_id() const {
        return table_id != "";
    }

    const string TableString() const;
    const string TableStringSimple() const;

public:
    string project_id;
    string dataset_id;
    string table_id;
};

struct BigqueryTypeData {
    string type_name;
    string column_name;
    int64_t precision;
    int64_t scale;
};

struct BigqueryType {
    idx_t oid = 0;
    vector<BigqueryType> children;
};

struct BigqueryUtils {
public:
    // static ConnectionDetails ParseConnectionString(const string &connection_string);
    static BigqueryTableRef ParseTableString(const string &table_string);

    static std::string FormatParentString(const string &project_id);
    static std::string FormatTableString(const std::string &project_id,
                                         const std::string &dataset_id,
                                         const std::string &table_id = "");
    static std::string FormatTableStringSimple(const std::string &project_id,
                                               const std::string &dataset_id,
                                               const std::string &table_id = "");
    static std::string FormatTableString(const BigqueryTableRef &table_ref);
    static std::string FormatTableStringSimple(const BigqueryTableRef &table_ref);

    static google::protobuf::FieldDescriptorProto::Type LogicalTypeToProtoType(const LogicalType &type);

    static bool IsValueQuotable(const Value &value);
    static string WriteQuotedIdentifier(const string &identifier);
    static string ReplaceQuotes(string &identifier, char to_replace = '\'');

    static LogicalType CastToBigqueryType(const LogicalType &type);
    static LogicalType FieldSchemaToLogicalType(const google::cloud::bigquery::v2::TableFieldSchema &field);
    static LogicalType ArrowTypeToLogicalType(const std::shared_ptr<arrow::DataType> &arrow_type);

    static string LogicalTypeToBigquerySQL(const LogicalType &type);
	static LogicalType BigquerySQLToLogicalType(const string &type);

    static string IntervalToBigqueryIntervalString(const interval_t &interval);

private:
	static vector<string> SplitStructFields(const string &struct_field_str);
	static string StructRemoveWhitespaces(const string &struct_str);
};


template <typename FUNC>
bool RetryOperation(FUNC op, int max_attempts, int initial_delay_mss) {
    int attempts = 0;
    int delay = initial_delay_mss;

    while (attempts < max_attempts) {
        if (op()) {
            return true;
        }
        attempts++;
        if (attempts < max_attempts) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            delay *= 2;
        }
    }
    return false;
}

} // namespace bigquery
} // namespace duckdb
