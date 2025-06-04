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

    explicit BigqueryConfig(const string &project_id) : project_id(project_id) {
        if (project_id.empty()) {
            throw std::invalid_argument("Project ID is required and cannot be empty.");
        }
    }

    BigqueryConfig(const BigqueryConfig &other) = default;
    BigqueryConfig &operator=(const BigqueryConfig &other) = default;
    ~BigqueryConfig() = default;

    BigqueryConfig &SetProjectId(const std::string &project_id) {
        if (project_id.empty()) {
            throw std::invalid_argument("Project ID is required and cannot be empty.");
        }
        this->project_id = project_id;
        return *this;
    }

    BigqueryConfig &SetDatasetId(const std::string &dataset_id) {
        this->dataset_id = dataset_id;
        return *this;
    }

    BigqueryConfig &SetBillingProjectId(const std::string &billing_project_id) {
        this->billing_project_id = billing_project_id;
        return *this;
    }

    BigqueryConfig &SetApiEndpoint(const std::string &api_endpoint) {
        this->api_endpoint = api_endpoint;
        return *this;
    }

    BigqueryConfig &SetGrpcEndpoint(const std::string &grpc_endpoint) {
        this->grpc_endpoint = grpc_endpoint;
        return *this;
    }

    bool HasProjectId() const {
        return !project_id.empty();
    }

    bool HasDatasetId() const {
        return !dataset_id.empty();
    }

    bool HasBillingProjectId() const {
        return !billing_project_id.empty();
    }

    bool HasApiEndpoint() const {
        return !api_endpoint.empty();
    }

    bool HasGrpcEndpoint() const {
        return !grpc_endpoint.empty();
    }

    // Hilfsmethoden
    bool IsDevEnv() const {
        return api_endpoint.find("localhost") != std::string::npos ||
               api_endpoint.find("127.0.0.1") != std::string::npos;
    }

    std::string BillingProject() const {
        return HasBillingProjectId() ? billing_project_id : project_id;
    }

    static BigqueryConfig FromDSN(const std::string &connection_string);

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
    static LogicalType FieldSchemaNumericToLogicalType(const google::cloud::bigquery::v2::TableFieldSchema &field);

    static LogicalType ArrowTypeToLogicalType(const std::shared_ptr<arrow::DataType> &arrow_type);
    static std::shared_ptr<arrow::DataType> LogicalTypeToArrowType(const LogicalType &type);
    static std::shared_ptr<arrow::Field> MakeArrowField(const std::string &name,
                                                        const LogicalType &dtype,
                                                        bool nullable = true);
    static std::shared_ptr<arrow::Schema> BuildArrowSchema(const ColumnList &cols);

    static string LogicalTypeToBigquerySQL(const LogicalType &type);
    static LogicalType BigquerySQLToLogicalType(const string &type);
    static LogicalType BigqueryNumericSQLToLogicalType(const string &type);

    static string IntervalToBigqueryIntervalString(const interval_t &interval);

    static string DecimalToString(const hugeint_t &value, const LogicalType &type);
    static pair<int, int> ParseNumericPrecisionAndScale(const string &type);

private:
    static vector<string> SplitStructFields(const string &struct_field_str);
    static string StructRemoveWhitespaces(const string &struct_str);
};


// Utility function to calculate column rank mapping for proper ordering
// Maps column IDs to their ranked positions for reordering
inline std::vector<column_t> CalculateRanks(const std::vector<column_t> &nums) {
    size_t n = nums.size();
    std::vector<std::pair<column_t, column_t>> value_index_pairs(n);
    for (size_t i = 0; i < n; ++i) {
        value_index_pairs[i] = {nums[i], i};
    }
    std::sort(value_index_pairs.begin(), value_index_pairs.end());
    std::vector<column_t> ranks(n);
    for (column_t i = 0; i < n; ++i) {
        ranks[value_index_pairs[i].second] = i;
    }
    return ranks;
}

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
