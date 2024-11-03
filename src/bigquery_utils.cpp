#include <arrow/api.h>
#include <iostream>
#include <regex>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include "duckdb.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/index_map.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"

#include "bigquery_utils.hpp"


namespace duckdb {
namespace bigquery {

BigqueryConfig BigqueryConfig::FromDSN(const std::string &connection_string) {
    std::string billing_project_id, project_id, dataset_id, api_endpoint, grpc_endpoint;

    std::istringstream stream(connection_string);
    std::string segment;

    // Parse the first segment, assuming it could be a special table string
    std::getline(stream, segment, ' ');
    if (segment.find('=') == std::string::npos) {
        auto table_ref = BigqueryUtils::ParseTableString(segment);
        project_id = table_ref.project_id;
        dataset_id = table_ref.dataset_id;

        // Optional error handling if table ID should not be present
        if (table_ref.has_table_id()) {
            throw std::invalid_argument("Table ID is not supported in the connection string");
        }
    }

    // reset stream to parse key=value pairs
    stream.str(connection_string);
    stream.clear();

    // extract key=value pairs from the rest of the connection string
    while (std::getline(stream, segment, ' ')) {
        size_t equal_pos = segment.find('=');
        if (equal_pos != std::string::npos) {
            std::string key = segment.substr(0, equal_pos);
            std::string value = segment.substr(equal_pos + 1);

            // Assign values based on the key
            if (key == "project") {
                project_id = value;
            } else if (key == "dataset") {
                dataset_id = value;
            } else if (key == "billing_project") {
                billing_project_id = value;
            } else if (key == "api_endpoint") {
                api_endpoint = value;
            } else if (key == "grpc_endpoint") {
                grpc_endpoint = value;
            } else {
                throw std::invalid_argument("Unknown key in connection string: " + key);
            }
        } else {
            throw std::invalid_argument("Invalid segment in connection string: " + segment);
        }
    }

    return BigqueryConfig(project_id)
        .SetDatasetId(dataset_id)
        .SetBillingProjectId(billing_project_id)
        .SetApiEndpoint(api_endpoint)
        .SetGrpcEndpoint(grpc_endpoint);
}

BigqueryTableRef BigqueryUtils::ParseTableString(const string &table_string) {
    BigqueryTableRef result;

    std::vector<std::regex> patterns{
        // For projects/<project_id>/datasets/<dataset_id>/tables/<table_id>
        std::regex(R"(projects/([^/]+)/datasets/([^/]+)/tables/([^/]+))"),
        // For <project_id>:<dataset_id>.<table_id>
        std::regex(R"(([^:]+):([^\.]+)\.([^\.]+))"),
        // For <project_id>.<dataset_id>.<table_id>
        std::regex(R"(([^\.]+)\.([^\.]+)\.([^\.]+))"),

        // For <project_id>:<dataset_id> (without table_id)
        std::regex(R"(([^:]+):([^\.]+))"),
        // And <project_id>.<dataset_id> (without table_id, same as above but with dot)
        std::regex(R"(([^\.]+)\.([^\.]+))"),
        // For projects/<project_id>/datasets/<dataset_id>
        std::regex(R"(projects/([^/]+)/datasets/([^/]+))"),

        // Just <project_id>
        std::regex(R"(^([^:/\.]+)$)"),
        // Just projects/<project_id>
        std::regex(R"(^projects/([^/]+)$)"),
    };

    std::smatch matches;
    for (const auto &pattern : patterns) {
        if (std::regex_match(table_string, matches, pattern)) {
            if (matches.size() >= 2) {
                result.project_id = matches[1].str();
            }
            if (matches.size() >= 3) {
                result.dataset_id = matches[2].str();
            }
            if (matches.size() >= 4) {
                result.table_id = matches[3].str();
            }
            return result;
        }
    }

    throw std::invalid_argument("Invalid table string: " + table_string);
}

std::string BigqueryUtils::FormatParentString(const string &project) {
    return "projects/" + project;
}

std::string BigqueryUtils::FormatTableString(const std::string &project_id,
                                             const std::string &dataset_id,
                                             const std::string &table_id) {
    std::string result = "projects/" + project_id + "/datasets/" + dataset_id;
    if (!table_id.empty()) {
        result += "/tables/" + table_id;
    }
    return result;
}

std::string BigqueryUtils::FormatTableStringSimple(const std::string &project_id,
                                                   const std::string &dataset_id,
                                                   const std::string &table_id) {
    std::string result = project_id + "." + dataset_id;
    if (!table_id.empty()) {
        result += "." + table_id;
    }
    return result;
}

std::string BigqueryUtils::FormatTableString(const BigqueryTableRef &table_ref) {
    return FormatTableString(table_ref.project_id, table_ref.dataset_id, table_ref.table_id);
}

std::string BigqueryUtils::FormatTableStringSimple(const BigqueryTableRef &table_ref) {
    return FormatTableStringSimple(table_ref.project_id, table_ref.dataset_id, table_ref.table_id);
}

LogicalType BigqueryUtils::FieldSchemaToLogicalType(const google::cloud::bigquery::v2::TableFieldSchema &field) {
    const auto &bigquery_type = field.type();
    const auto repeated = field.mode() == "REPEATED";

    LogicalType type;
    if (bigquery_type == "STRING") {
        type = LogicalType::VARCHAR;
    } else if (bigquery_type == "JSON") {
        type = LogicalType::VARCHAR;
    } else if (bigquery_type == "BYTES") {
        type = LogicalType::BLOB;
    } else if (bigquery_type == "INTEGER") {
        type = LogicalType::BIGINT;
    } else if (bigquery_type == "INT64") {
        type = LogicalType::BIGINT;
    } else if (bigquery_type == "FLOAT") {
        type = LogicalType::FLOAT;
    } else if (bigquery_type == "FLOAT64") {
        type = LogicalType::DOUBLE;
    } else if (bigquery_type == "BOOLEAN" || bigquery_type == "BOOL") {
        type = LogicalType::BOOLEAN;
    } else if (bigquery_type == "DATE") {
        type = LogicalType::DATE;
    } else if (bigquery_type == "TIME") {
        type = LogicalType::TIME;
    } else if (bigquery_type == "DATETIME") {
        type = LogicalType::TIMESTAMP;
    } else if (bigquery_type == "TIMESTAMP") {
        type = LogicalType::TIMESTAMP;
    } else if (bigquery_type == "INTERVAL") {
        type = LogicalType::INTERVAL;
    } else if (bigquery_type == "NUMERIC") {
        auto precision = (field.precision() == 0) ? 29 : field.precision();
        auto scale = (field.scale() == 0) ? 9 : field.scale();

        if (precision < 1 || precision > 29) {
            throw std::invalid_argument("Precision must be between 1 and 29 for NUMERIC fields.");
        }
        if (scale < 0 || scale > 9) {
            throw std::invalid_argument("Scale must be between 0 and 9 for NUMERIC fields.");
        }
        if (precision - scale < 1 || precision - scale > 29) {
            throw std::invalid_argument(
                "Difference between precision and scale must be at least 1 and at most 29 for NUMERIC fields.");
        }
        type = LogicalType::DECIMAL(precision, scale);
    } else if (bigquery_type == "BIGNUMERIC") {
        auto precision = (field.precision() == 0) ? 38 : field.precision();
        auto scale = (field.scale() == 0) ? 9 : field.scale();

        if (precision < 1 || precision > 38) {
            throw std::invalid_argument("Precision must be between 1 and 38 for BIGNUMERIC fields.");
        }
        if (scale < 0 || scale > 38) {
            throw std::invalid_argument("Scale must be between 0 and 38 for BIGNUMERIC fields.");
        }
        if (precision - scale < 1 || precision - scale > 38) {
            throw std::invalid_argument(
                "Difference between precision and scale must be at least 1 and at most 38 for BIGNUMERIC fields.");
        }
        type = LogicalType::DECIMAL(precision, scale);
    } else if (bigquery_type == "GEOGRAPHY") {
        type = LogicalType::VARCHAR;
    } else if (bigquery_type == "STRUCT" || bigquery_type == "RECORD") {
        child_list_t<LogicalType> new_types;
        for (auto &child : field.fields()) {
            new_types.push_back(make_pair(child.name(), FieldSchemaToLogicalType(child)));
        }
        type = LogicalType::STRUCT(std::move(new_types));
    } else {
        throw InternalException("Unknown BigQuery Type: " + bigquery_type);
    }
    if (repeated) {
        type = LogicalType::LIST(type);
    }
    return type;
}

std::string MapArrowTypeToBigQuery(const std::shared_ptr<arrow::DataType> &arrow_type) {
    if (arrow_type->id() == arrow::Int64Type::type_id) {
        return "INT64";
    } else if (arrow_type->id() == arrow::FloatType::type_id || arrow_type->id() == arrow::DoubleType::type_id) {
        return "FLOAT64";
    } else if (arrow_type->id() == arrow::BooleanType::type_id) {
        return "BOOL";
    } else if (arrow_type->id() == arrow::StringType::type_id) {
        return "STRING";
    } else if (arrow_type->id() == arrow::BinaryType::type_id) {
        return "BYTES";
    } else if (arrow_type->id() == arrow::Date32Type::type_id || arrow_type->id() == arrow::Date64Type::type_id) {
        return "DATE";
    } else if (arrow_type->id() == arrow::TimestampType::type_id) {
        return "TIMESTAMP";
    } else if (arrow_type->id() == arrow::Time32Type::type_id || arrow_type->id() == arrow::Time64Type::type_id) {
        return "TIME";
    } else if (arrow_type->id() == arrow::StructType::type_id) {
        return "STRUCT";
    } else if (arrow_type->id() == arrow::ListType::type_id) {
        return "ARRAY";
    } else {
        return "UNSUPPORTED";
    }
}

LogicalType BigqueryUtils::ArrowTypeToLogicalType(const std::shared_ptr<arrow::DataType> &arrow_type) {
    switch (arrow_type->id()) {
    case arrow::Type::BOOL:
        return LogicalType::BOOLEAN;
    case arrow::Type::INT32:
        return LogicalType::INTEGER;
    case arrow::Type::INT64:
        return LogicalType::BIGINT;
    case arrow::Type::FLOAT:
        return LogicalType::FLOAT;
    case arrow::Type::DOUBLE:
        return LogicalType::DOUBLE;
    case arrow::Type::STRING:
        return LogicalType::VARCHAR;
    case arrow::Type::BINARY:
        return LogicalType::BLOB;
    case arrow::Type::TIMESTAMP:
        return LogicalType::TIMESTAMP;
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
        return LogicalType::DATE;
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
        return LogicalType::TIME;
    case arrow::Type::INTERVAL_MONTH_DAY_NANO:
        return LogicalType::INTERVAL;
    case arrow::Type::DECIMAL128: {
        auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(arrow_type);
        int32_t precision = decimal_type->precision();
        int32_t scale = decimal_type->scale();
        return LogicalType::DECIMAL(precision, scale);
    }
    case arrow::Type::DECIMAL256: {
        auto decimal_type = std::static_pointer_cast<arrow::Decimal256Type>(arrow_type);
        int32_t precision = decimal_type->precision();
        int32_t scale = decimal_type->scale();

        // DuckDB only supports up to DECIMAL(38,scale). So we need to convert DECIMAL256 to
        // DECIMAL(38,scale) if precision is greater than 38.
        if (precision > 38) {
            precision = 38;
            // Adjust the scale if necessary, ensuring it remains within practical limits.
            // This adjustment strategy for the scale is simplistic.
            scale = std::min(scale, precision - 1);
        }
        return LogicalType::DECIMAL(precision, scale);
    }
    case arrow::Type::LIST: {
        auto list_type = std::static_pointer_cast<arrow::ListType>(arrow_type);
        auto element_type = list_type->value_type();
        return LogicalType::LIST(ArrowTypeToLogicalType(element_type));
    }
    case arrow::Type::STRUCT: {
        auto struct_type = std::static_pointer_cast<arrow::StructType>(arrow_type);
        auto fields = struct_type->fields();

        child_list_t<LogicalType> child_types;
        for (auto &field : fields) {
            child_types.push_back(make_pair(field->name(), ArrowTypeToLogicalType(field->type())));
        }
        return LogicalType::STRUCT(std::move(child_types));
    }
    default:
        throw InternalException("Unsupported Arrow type: " + arrow_type->name());
    }
}


bool BigqueryUtils::IsValueQuotable(const Value &value) {
    switch (value.type().id()) {
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::BLOB:
    case LogicalTypeId::DATE:
    case LogicalTypeId::TIME:
    case LogicalTypeId::TIMESTAMP:
        return true;
    default:
        return false;
    }
}

string BigqueryUtils::ReplaceQuotes(string &identifier, char to_replace) {
    char replace_with = ' ';
    if (!identifier.empty() && identifier.front() == to_replace) {
        identifier.front() = replace_with;
    }
    if (!identifier.empty() && identifier.back() == to_replace) {
        identifier.back() = replace_with;
    }
    return identifier;
}

string BigqueryUtils::WriteQuotedIdentifier(const string &identifier) {
    return KeywordHelper::WriteQuoted(identifier, '`');
}

LogicalType BigqueryUtils::CastToBigqueryType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
        return LogicalType::BOOLEAN;
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
        return LogicalType::INTEGER;
    case LogicalTypeId::BIGINT:
        return LogicalType::BIGINT;
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
        return LogicalType::BIGINT; // BigQuery does not differentiate unsigned types
    case LogicalTypeId::UBIGINT:
        throw NotImplementedException("UBIGINT not supported in BigQuery.");
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
        return LogicalType::DOUBLE;
    case LogicalTypeId::BLOB:
        return LogicalType::BLOB;
    case LogicalTypeId::DATE:
        return LogicalType::DATE;
    case LogicalTypeId::DECIMAL:
        return type;
    case LogicalTypeId::TIME:
        return LogicalType::TIME;
    case LogicalTypeId::TIMESTAMP:
        return LogicalType::TIMESTAMP;
    case LogicalTypeId::TIMESTAMP_SEC:
        return LogicalType::TIMESTAMP;
    case LogicalTypeId::TIMESTAMP_MS:
        return LogicalType::TIMESTAMP;
    case LogicalTypeId::TIMESTAMP_NS:
        // return LogicalType::TIMESTAMP;
        throw NotImplementedException("TIMESTAMP with Nano Seconds not supported in BigQuery.");
    case LogicalTypeId::TIMESTAMP_TZ:
        // return LogicalType::TIMESTAMP;
        throw NotImplementedException("TIMESTAMP WITH TIME ZONE not supported in BigQuery.");
    case LogicalTypeId::INTERVAL:
        return LogicalType::VARCHAR;
    case LogicalTypeId::VARCHAR:
        return LogicalType::VARCHAR;
    case LogicalTypeId::UUID:
        return LogicalType::VARCHAR;
    case LogicalTypeId::LIST:
        return type;
    case LogicalTypeId::ARRAY:
        return type;
    case LogicalTypeId::STRUCT:
        return type;
    case LogicalTypeId::MAP:
        return type;
    default:
        return LogicalType::VARCHAR;
    }
}

google::protobuf::FieldDescriptorProto::Type BigqueryUtils::LogicalTypeToProtoType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BLOB:
        return google::protobuf::FieldDescriptorProto::TYPE_BYTES;
    case LogicalTypeId::BIT:
        return google::protobuf::FieldDescriptorProto::TYPE_BOOL;
    case LogicalTypeId::BOOLEAN:
        return google::protobuf::FieldDescriptorProto::TYPE_BOOL;
    case LogicalTypeId::TINYINT:
        return google::protobuf::FieldDescriptorProto::TYPE_INT32;
    case LogicalTypeId::SMALLINT:
        return google::protobuf::FieldDescriptorProto::TYPE_INT32;
    case LogicalTypeId::INTEGER:
        return google::protobuf::FieldDescriptorProto::TYPE_INT32;
    case LogicalTypeId::BIGINT:
        return google::protobuf::FieldDescriptorProto::TYPE_INT64;
    case LogicalTypeId::HUGEINT:
        throw NotImplementedException("HUGEINT not supported in BigQuery.");
        // return google::protobuf::FieldDescriptorProto::TYPE_STRING;
    case LogicalTypeId::UTINYINT:
        return google::protobuf::FieldDescriptorProto::TYPE_UINT32;
    case LogicalTypeId::USMALLINT:
        return google::protobuf::FieldDescriptorProto::TYPE_UINT32;
    case LogicalTypeId::UINTEGER:
        return google::protobuf::FieldDescriptorProto::TYPE_UINT32;
    case LogicalTypeId::UBIGINT:
        throw NotImplementedException("UBIGINT not supported in BigQuery.");
    case LogicalTypeId::UHUGEINT:
        throw NotImplementedException("UHUGEINT not supported in BigQuery.");
    case LogicalTypeId::FLOAT:
        return google::protobuf::FieldDescriptorProto::TYPE_FLOAT;
    case LogicalTypeId::DOUBLE:
        return google::protobuf::FieldDescriptorProto::TYPE_DOUBLE;
    case LogicalTypeId::DECIMAL:
        return google::protobuf::FieldDescriptorProto::TYPE_INT64;
    case LogicalTypeId::DATE:
        return google::protobuf::FieldDescriptorProto::TYPE_INT32;
    case LogicalTypeId::TIME:
        return google::protobuf::FieldDescriptorProto::TYPE_STRING;
    case LogicalTypeId::TIME_TZ:
        throw NotImplementedException("TIME WITH TIME ZONE not supported in BigQuery.");
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_SEC:
    case LogicalTypeId::TIMESTAMP_MS:
        return google::protobuf::FieldDescriptorProto::TYPE_INT64;
    case LogicalTypeId::TIMESTAMP_NS:
    case LogicalTypeId::TIMESTAMP_TZ:
        throw NotImplementedException("TIMESTAMP WITH TIME ZONE not supported in BigQuery.");
    case LogicalTypeId::INTERVAL:
        return google::protobuf::FieldDescriptorProto::TYPE_STRING;
    case LogicalTypeId::VARCHAR:
        return google::protobuf::FieldDescriptorProto::TYPE_STRING;
    case LogicalTypeId::UUID:
        return google::protobuf::FieldDescriptorProto::TYPE_STRING;
    case LogicalTypeId::STRUCT:
        return google::protobuf::FieldDescriptorProto::TYPE_MESSAGE;
    case LogicalTypeId::MAP:
        return google::protobuf::FieldDescriptorProto::TYPE_MESSAGE;
    case LogicalTypeId::LIST:
        return LogicalTypeToProtoType(ListType::GetChildType(type));
    case LogicalTypeId::ARRAY:
        return LogicalTypeToProtoType(ArrayType::GetChildType(type));
    default:
        throw InternalException("Proto: Unsupported type: " + type.ToString());
    }
}

LogicalType BigqueryUtils::BigquerySQLToLogicalType(const string &type) {
    LogicalType result;

    if (type == "STRING") {
        result = LogicalType::VARCHAR;
    } else if (type == "JSON") {
        result = LogicalType::VARCHAR;
    } else if (type == "BYTES") {
        result = LogicalType::BLOB;
    } else if (type == "INTEGER" || type == "INT64") {
        result = LogicalType::BIGINT;
    } else if (type == "FLOAT" || type == "FLOAT64") {
        result = LogicalType::DOUBLE;
    } else if (type == "BOOLEAN" || type == "BOOL") {
        result = LogicalType::BOOLEAN;
    } else if (type == "DATE") {
        result = LogicalType::DATE;
    } else if (type == "TIME") {
        result = LogicalType::TIME;
    } else if (type == "DATETIME" || type == "TIMESTAMP") {
        result = LogicalType::TIMESTAMP;
    } else if (type == "INTERVAL") {
        result = LogicalType::INTERVAL;
    } else if (type == "NUMERIC") {
        // Fallback to default NUMERIC precision and scale
        result = LogicalType::DECIMAL(29, 9);
    } else if (type == "BIGNUMERIC") {
        // Fallback to default BIGNUMERIC precision and scale
        result = LogicalType::DECIMAL(38, 9);
    } else if (type == "GEOGRAPHY") {
        result = LogicalType::VARCHAR;
    } else if (type.find("ARRAY<") == 0) {
        string array_sub_type = type.substr(6, type.size() - 7);
        LogicalType element_logical_type = BigquerySQLToLogicalType(array_sub_type);
        result = LogicalType::LIST(element_logical_type);
    } else if (type.find("STRUCT<") == 0) {
        string struct_sub_type = type.substr(7, type.size() - 8);

        child_list_t<LogicalType> struct_types;
        vector<string> fields = SplitStructFields(struct_sub_type);
        for (const auto &field : fields) {
            size_t pos_space = field.find(' ');
            if (pos_space == std::string::npos) {
                throw BinderException("Invalid field in STRUCT type: " + field);
            }

            string field_name = field.substr(0, pos_space);
            string field_type = field.substr(pos_space + 1);
            LogicalType field_logical_type = BigquerySQLToLogicalType(field_type);
            struct_types.push_back(make_pair(field_name, field_logical_type));
        }

        result = LogicalType::STRUCT(std::move(struct_types));
    } else {
        throw InternalException("Unknown BigQuery Type: " + type);
    }

    return result;
}

string BigqueryUtils::LogicalTypeToBigquerySQL(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
        return "BOOL";
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
        return "INT64";
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
        return "INT64"; // BigQuery does not differentiate unsigned types
    case LogicalTypeId::UBIGINT:
        return "STRING";
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
        return "FLOAT64";
    case LogicalTypeId::BLOB:
        return "BYTES";
    case LogicalTypeId::DATE:
        return "DATE";
    case LogicalTypeId::DECIMAL:
        return "NUMERIC";
    case LogicalTypeId::HUGEINT:
        return "BIGNUMERIC";
    case LogicalTypeId::TIME:
        return "TIME";
    case LogicalTypeId::TIMESTAMP:
        return "TIMESTAMP";
    case LogicalTypeId::TIMESTAMP_TZ:
        throw NotImplementedException("TIMESTAMP WITH TIME ZONE not supported in BigQuery.");
    case LogicalTypeId::TIMESTAMP_SEC:
        return "TIMESTAMP";
    case LogicalTypeId::TIMESTAMP_MS:
        return "TIMESTAMP";
    case LogicalTypeId::TIMESTAMP_NS:
        throw NotImplementedException("TIMESTAMP_NS not supported in BigQuery.");
    case LogicalTypeId::INTERVAL:
        return "INTERVAL";
    case LogicalTypeId::VARCHAR:
        return "STRING";
    case LogicalTypeId::UUID:
        return "STRING";
    case LogicalTypeId::LIST: {
        auto child_type = ListType::GetChildType(type);
        if (child_type.id() == LogicalTypeId::LIST || child_type.id() == LogicalTypeId::ARRAY) {
            throw BinderException("Nested lists or arrays are not supported in BigQuery.");
        }
        return "ARRAY<" + LogicalTypeToBigquerySQL(child_type) + ">";
    }
    case LogicalTypeId::ARRAY: {
        auto child_type = ArrayType::GetChildType(type);
        if (child_type.id() == LogicalTypeId::LIST || child_type.id() == LogicalTypeId::ARRAY) {
            throw BinderException("Nested lists or arrays are not supported in BigQuery.");
        }
        return "ARRAY<" + LogicalTypeToBigquerySQL(child_type) + ">";
    }
    case LogicalTypeId::STRUCT: {
        string struct_string = "STRUCT<";
        for (size_t i = 0; i < StructType::GetChildCount(type); i++) {
            auto child_name = StructType::GetChildName(type, i);
            auto child_type = StructType::GetChildType(type, i);
            if (i > 0) {
                struct_string += ", ";
            }
            struct_string += child_name + " " + LogicalTypeToBigquerySQL(child_type);
        }
        struct_string += ">";
        return struct_string;
    }
    // case LogicalTypeId::MAP: {
    // }
    default:
        throw NotImplementedException("Type not supported in BigQuery: " + type.ToString());
    }
}

std::string BigqueryUtils::IntervalToBigqueryIntervalString(const interval_t &interval) {
    int years = interval.months / 12;
    int months = interval.months % 12;
    int days = interval.days;
    int hours = interval.micros / 3600000000;
    int remaining_micros = interval.micros % 3600000000;
    int minutes = remaining_micros / 60000000;
    int seconds = (remaining_micros % 60000000) / 1000000;

    return std::to_string(years) + "-" + std::to_string(months) + " " + std::to_string(days) + " " +
           std::to_string(hours) + ":" + std::to_string(minutes) + ":" + std::to_string(seconds) + "." +
           std::to_string(remaining_micros % 1000000);
}

const string BigqueryTableRef::TableString() const {
    return BigqueryUtils::FormatTableString(project_id, dataset_id, table_id);
}

const string BigqueryTableRef::TableStringSimple() const {
    return BigqueryUtils::FormatTableStringSimple(project_id, dataset_id, table_id);
}

vector<string> BigqueryUtils::SplitStructFields(const string &struct_field_str) {
    vector<string> fields;
    size_t start = 0;
    int bracket_depth = 0;

    for (size_t i = 0; i < struct_field_str.size(); i++) {
        if (struct_field_str[i] == '<') {
            bracket_depth++;
        } else if (struct_field_str[i] == '>') {
            bracket_depth--;
        } else if (struct_field_str[i] == ',' && bracket_depth == 0) {
            fields.push_back(struct_field_str.substr(start, i - start));
            start = i + 2;
        }
    }

    fields.push_back(struct_field_str.substr(start));
    return fields;
}

string BigqueryUtils::StructRemoveWhitespaces(const string &struct_str) {
    string result;
    for (size_t i = 0; i < struct_str.size(); i++) {
        if (struct_str[i] != ' ') {
            result += struct_str[i];
        }
    }
    return result;
}

uint64_t Iso8601ToMillis(const std::string &iso8601) {
    auto timestamp = Timestamp::FromString(iso8601);
    auto timestamp_ms = Timestamp::GetEpochMs(timestamp);
    return timestamp_ms;
}

} // namespace bigquery
} // namespace duckdb
