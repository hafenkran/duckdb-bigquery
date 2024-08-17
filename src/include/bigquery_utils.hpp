#pragma once

#include "duckdb.hpp"

// #include "google/cloud/bigquery/v2/minimal/internal/table_schema.h"

#include "google/cloud/bigquerycontrol/v2/table_client.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <arrow/api.h>
#include <regex>


namespace duckdb {
namespace bigquery {

struct BigqueryUtils;

struct ConnectionDetails {
    string dsn;
    string execution_project_id;
    string project_id;
    string dataset_id;
    string api_endpoint;
    string grpc_endpoint;

    const bool is_valid() const {
        return project_id != "";
    }

    const bool has_api_endpoint() const {
        return api_endpoint != "";
    }

    const bool has_grpc_endpoint() const {
        return grpc_endpoint != "";
    }
};

struct BigqueryDatasetRef {
    string project_id;
    string dataset_id;
    string location;

    const bool has_dataset_id() const {
        return dataset_id != "";
    }
};

struct BigqueryTableRef {
    string project_id;
    string dataset_id;
    string table_id;

    const bool has_dataset_id() const {
        return dataset_id != "";
    }

    const bool has_table_id() const {
        return table_id != "";
    }
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
    static ConnectionDetails ParseConnectionString(const string &connection_string);
    static BigqueryTableRef ParseTableString(const string &table_string);

    static std::string FormatParentString(const string &project_id);
    static std::string FormatTableString(const std::string &project_id,
                                         const std::string &dataset_id,
                                         const std::string &table_id = "");
    static std::string FormatTableStringSimple(const std::string &project_id,
                                               const std::string &dataset_id,
                                               const std::string &table_id = "");
    static std::string ForamtTableString(const BigqueryTableRef &table_ref);
    static std::string FormatTableStringSimple(const BigqueryTableRef &table_ref);

    static google::protobuf::FieldDescriptorProto::Type LogicalTypeToProtoType(const LogicalType &type);

    static bool IsValueQuotable(const Value &value);
    static string WriteQuotedIdentifier(const string &identifier);
    static string ReplaceQuotes(string &identifier, char to_replace = '\'');

    static LogicalType CastToBigqueryType(const LogicalType &type);
    static LogicalType FieldSchemaToLogicalType(const google::cloud::bigquery::v2::TableFieldSchema &field);
    static LogicalType ArrowTypeToLogicalType(const std::shared_ptr<arrow::DataType> &arrow_type);

    static string LogicalTypeToBigquerySQL(const LogicalType &type);

    static string IntervalToBigqueryIntervalString(const interval_t &interval);
};


} // namespace bigquery
} // namespace duckdb
