#include <arrow/api.h>
#include <iostream>
#include <regex>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include "duckdb.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/index_map.hpp"
#include "duckdb/common/types.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"

#include "bigquery_settings.hpp"
#include "bigquery_utils.hpp"


namespace duckdb {
namespace bigquery {

constexpr int BQ_NUMERIC_PRECISION_DEFAULT = 38;
constexpr int BQ_NUMERIC_SCALE_DEFAULT = 9;
constexpr int BQ_BIGNUMERIC_PRECISION_DEFAULT = 76;
constexpr int BQ_BIGNUMERIC_SCALE_DEFAULT = 38;
constexpr int DUCKDB_DECIMAL_PRECISION_MAX = 38;
constexpr int DUCKDB_DECIMAL_SCALE_MAX = 38;

class BigqueryTypeException : public BinderException {
public:
    explicit BigqueryTypeException(const string &msg, string type_sql) : BinderException(msg) {
    }

    static BigqueryTypeException UnsupportedPrecision(int precision, const string &type) {
        return BigqueryTypeException("DuckDB only supports precision between 1 and " +
                                         std::to_string(DUCKDB_DECIMAL_PRECISION_MAX) + ". Invalid precision '" +
                                         std::to_string(precision) + "' specified for type '" + type + "'.",
                                     type);
    }

    static BigqueryTypeException UnsupportedScale(int scale, const string &type) {
        return BigqueryTypeException("DuckDB only supports scale between 0 and " +
                                         std::to_string(DUCKDB_DECIMAL_SCALE_MAX) + ". Invalid scale '" +
                                         std::to_string(scale) + "' specified for type '" + type + "'.",
                                     type);
    }

    static BigqueryTypeException BignumericNotSupported() {
        return BigqueryTypeException("DuckDB only supports precision between 1 and " +
                                         std::to_string(DUCKDB_DECIMAL_PRECISION_MAX) +
                                         ". BIGNUMERIC fields have a default precision of " +
                                         std::to_string(BQ_BIGNUMERIC_PRECISION_DEFAULT) + ".",
                                     "BIGNUMERIC");
    }

private:
    string type_sql;
};


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
        type = LogicalType::DOUBLE;
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
        type = BigqueryUtils::FieldSchemaNumericToLogicalType(field);
    } else if (bigquery_type == "BIGNUMERIC") {
        type = BigqueryUtils::FieldSchemaNumericToLogicalType(field);
    } else if (bigquery_type == "GEOGRAPHY") {
        if (BigquerySettings::GeographyAsGeometry()) {
            type = LogicalType::BLOB;
            type.SetAlias("GEOMETRY");
        } else {
            type = LogicalType::VARCHAR;
        }
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

LogicalType BigqueryUtils::FieldSchemaNumericToLogicalType(const google::cloud::bigquery::v2::TableFieldSchema &field) {
    auto precision = field.precision();
    auto scale = field.scale();

    // If no precision and scale are provided, BigQuery assumes default max values
    if (precision == 0 && scale == 0) {
        precision = BQ_NUMERIC_PRECISION_DEFAULT;
        scale = BQ_NUMERIC_SCALE_DEFAULT;
    }

    const auto &bigquery_type = field.type();
    if (bigquery_type == "BIGNUMERIC") {
        if (BigquerySettings::BignumericAsVarchar()) {
            return LogicalType::VARCHAR;
        }
        throw BinderException(
            "DuckDB only supports precision between 1 and " + std::to_string(DUCKDB_DECIMAL_PRECISION_MAX) +
            ". BIGNUMERIC fields have a default precision of " + std::to_string(BQ_BIGNUMERIC_PRECISION_DEFAULT) + ".");
    }
    if (precision < 1 || precision > DUCKDB_DECIMAL_PRECISION_MAX) {
        throw BinderException("DuckDB only supports precision between 1 and " +
                              std::to_string(DUCKDB_DECIMAL_PRECISION_MAX) + " for NUMERIC/BIGNUMERIC fields.");
    }
    if (scale < 0 || scale > DUCKDB_DECIMAL_SCALE_MAX) {
        throw BinderException("DuckDB only supports scale between 0 and " + std::to_string(DUCKDB_DECIMAL_SCALE_MAX) +
                              " for NUMERIC/BIGNUMERIC fields.");
    }
    if (precision - scale < 1 || precision - scale > DUCKDB_DECIMAL_PRECISION_MAX) {
        throw BinderException("Difference between precision and scale must be at least 1 and at most " +
                              std::to_string(DUCKDB_DECIMAL_PRECISION_MAX) + " for NUMERIC/BIGNUMERIC fields.");
    }

    return LogicalType::DECIMAL(precision, scale);
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
        if (BigquerySettings::BignumericAsVarchar()) {
            return LogicalType::VARCHAR;
        }

        auto decimal_type = std::static_pointer_cast<arrow::Decimal256Type>(arrow_type);
        int32_t precision = decimal_type->precision();
        int32_t scale = decimal_type->scale();

        if (precision > DUCKDB_DECIMAL_PRECISION_MAX) {
            throw BinderException("Precision exceeds DuckDB's maximum of " +
                                  std::to_string(DUCKDB_DECIMAL_PRECISION_MAX) +
                                  ". Provided: " + std::to_string(precision) + ".");
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

std::shared_ptr<arrow::DataType> BigqueryUtils::LogicalTypeToArrowType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
        return arrow::boolean();
    case LogicalTypeId::TINYINT:
        return arrow::int8();
    case LogicalTypeId::SMALLINT:
        return arrow::int16();
    case LogicalTypeId::INTEGER:
        return arrow::int32();
    case LogicalTypeId::BIGINT:
        return arrow::int64();

    case LogicalTypeId::UTINYINT:
        return arrow::uint8();
    case LogicalTypeId::USMALLINT:
        return arrow::uint16();
    case LogicalTypeId::UINTEGER:
        return arrow::uint32();
    case LogicalTypeId::UBIGINT:
        return arrow::uint64();

    case LogicalTypeId::FLOAT:
        return arrow::float32(); // FLOAT32 in BQ
    case LogicalTypeId::DOUBLE:
        return arrow::float64();

    case LogicalTypeId::VARCHAR:
        return arrow::utf8(); // STRING/JSON
    case LogicalTypeId::BLOB:
        return arrow::binary(); // BYTES
    case LogicalTypeId::DATE:
        return arrow::date32();
    case LogicalTypeId::TIME:
        return arrow::time64(arrow::TimeUnit::MICRO);
    case LogicalTypeId::TIMESTAMP:
        return arrow::timestamp(arrow::TimeUnit::MICRO);
    case LogicalTypeId::TIMESTAMP_SEC:
        return arrow::timestamp(arrow::TimeUnit::SECOND);
    case LogicalTypeId::TIMESTAMP_MS:
        return arrow::timestamp(arrow::TimeUnit::MILLI);
    case LogicalTypeId::TIMESTAMP_NS:
        throw NotImplementedException("BigQuery does not support nanosecond precision (TIMESTAMP_NS).");
    case LogicalTypeId::INTERVAL:
        return arrow::month_day_nano_interval();
    case LogicalTypeId::DECIMAL: {
        auto prec = DecimalType::GetWidth(type);
        auto scale = DecimalType::GetScale(type);
        if (prec <= 38) {
            return arrow::decimal128(prec, scale);
        }
        // BIGNUMERIC → 256 Bit
        return arrow::decimal256(prec, scale);
    }
    case LogicalTypeId::LIST:
        return arrow::list(LogicalTypeToArrowType(ListType::GetChildType(type)));
    case LogicalTypeId::STRUCT: {
        arrow::FieldVector fields;
        for (idx_t i = 0; i < StructType::GetChildCount(type); ++i) {
            fields.push_back(MakeArrowField(StructType::GetChildName(type, i), StructType::GetChildType(type, i)));
        }
        return arrow::struct_(std::move(fields));
    }
    default:
        throw BinderException("LogicalTypeToArrowType: LogicalType '%s' is unsupported", type.ToString());
    }
}

std::shared_ptr<arrow::Field> BigqueryUtils::MakeArrowField(const std::string &name,
                                                            const LogicalType &dtype,
                                                            bool nullable) {
    return arrow::field(name, LogicalTypeToArrowType(dtype), nullable);
}

std::shared_ptr<arrow::Schema> BigqueryUtils::BuildArrowSchema(const ColumnList &cols) {
    arrow::FieldVector fields;
    fields.reserve(cols.LogicalColumnCount());
    for (auto &col : cols.Logical()) {
        auto mapped_bq_type = BigqueryUtils::CastToBigqueryType(col.GetType());
        auto arrow_type = BigqueryUtils::LogicalTypeToArrowType(mapped_bq_type);
        fields.push_back(arrow::field(col.GetName(), std::move(arrow_type), true));
    }
    return arrow::schema(std::move(fields));
}

void BigqueryUtils::PopulateAndMapArrowTableTypes(ClientContext &context,
                                                  ArrowTableSchema &arrow_table,
                                                  ArrowSchemaWrapper &schema_root,
                                                  vector<string> &names,
                                                  vector<LogicalType> &return_types,
                                                  vector<LogicalType> &mapped_bq_types,
                                                  const ColumnList *source_columns) {
    ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), arrow_table, schema_root.arrow_schema);

    names = arrow_table.GetNames();
    return_types = arrow_table.GetTypes();
    if (return_types.empty()) {
        throw BinderException("BigQuery table has no columns");
    }

    if (source_columns) {
        if (source_columns->LogicalColumnCount() != return_types.size()) {
            throw InternalException("Alias propagation: column count mismatch (%llu vs %llu)",
                                    (unsigned long long)source_columns->LogicalColumnCount(),
                                    (unsigned long long)return_types.size());
        }
        idx_t idx = 0;
        for (auto &col : source_columns->Logical()) {
            const LogicalType &src_type = col.GetType();
            if (src_type.HasAlias() && !src_type.GetAlias().empty()) {
                if (!return_types[idx].HasAlias() || return_types[idx].GetAlias().empty()) {
                    return_types[idx].SetAlias(src_type.GetAlias());
                }
            }
            idx++;
        }
    }

    bool requires_cast = false;
    mapped_bq_types.clear();
    mapped_bq_types.reserve(return_types.size());
    for (idx_t i = 0; i < return_types.size(); i++) {
        auto bq_type = BigqueryUtils::CastToBigqueryTypeWithSpatialConversion(return_types[i], &context);
        if (bq_type != return_types[i]) {
            requires_cast = true;
        }
        mapped_bq_types.push_back(bq_type);
    }
    if (!requires_cast) {
        mapped_bq_types.clear(); // signal no cast path needed
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
        // if (BigqueryUtils::IsGeometryType(type)) {
        //     auto geom_type = LogicalType(LogicalTypeId::VARCHAR);
        //     geom_type.SetAlias("GEOGRAPHY");
        //     return geom_type;
        // }
        return type;
    case LogicalTypeId::DATE:
        return LogicalType::DATE;
    case LogicalTypeId::DECIMAL:
        return type;
    case LogicalTypeId::TIME:
        return LogicalType::TIME;
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_SEC:
    case LogicalTypeId::TIMESTAMP_MS:
    case LogicalTypeId::TIMESTAMP_TZ:
        return LogicalType::TIMESTAMP;
    case LogicalTypeId::TIMESTAMP_NS:
        // return LogicalType::TIMESTAMP;
        throw NotImplementedException("TIMESTAMP with Nano Seconds not supported in BigQuery.");
        // throw NotImplementedException("TIMESTAMP WITH TIME ZONE not supported in BigQuery.");
    case LogicalTypeId::INTERVAL:
        return LogicalType::INTERVAL;
    case LogicalTypeId::VARCHAR: {
        LogicalType result = LogicalType::VARCHAR;
        if (type.HasAlias()) {
            result.SetAlias(type.GetAlias());
        }
        return result;
    }
    case LogicalTypeId::UUID:
        return LogicalType::VARCHAR;
    case LogicalTypeId::LIST:
        return LogicalType::LIST(CastToBigqueryType(ListType::GetChildType(type)));
    case LogicalTypeId::ARRAY:
        return LogicalType::LIST(CastToBigqueryType(ArrayType::GetChildType(type)));
    case LogicalTypeId::STRUCT: {
        child_list_t<LogicalType> child_types;
        for (idx_t i = 0; i < StructType::GetChildCount(type); i++) {
            auto child_name = StructType::GetChildName(type, i);
            auto child_type = StructType::GetChildType(type, i);
            child_types.push_back(make_pair(child_name, CastToBigqueryType(child_type)));
        }
        return LogicalType::STRUCT(std::move(child_types));
    }
    case LogicalTypeId::MAP:
        return LogicalType::STRUCT({{"key", CastToBigqueryType(MapType::KeyType(type))},
                                    {"value", CastToBigqueryType(MapType::ValueType(type))}});
    default:
        return LogicalType::VARCHAR;
    }
}

bool BigqueryUtils::IsGeographyType(const LogicalType &type) {
    return type.id() == LogicalTypeId::VARCHAR && type.HasAlias() && type.GetAlias() == "GEOGRAPHY";
}

bool BigqueryUtils::IsGeometryType(const LogicalType &type) {
    return type.id() == LogicalTypeId::BLOB && type.HasAlias() && type.GetAlias() == "GEOMETRY";
}

LogicalType BigqueryUtils::CastToBigqueryTypeWithSpatialConversion(const LogicalType &type, ClientContext *context) {
    // Check for WKT alias on VARCHAR types - convert to GEOMETRY if spatial extension is available
    if (type.id() == LogicalTypeId::BLOB && type.HasAlias() && type.GetAlias() == "GEOMETRY") {
        if (context && BigquerySettings::IsGeometryConversionEnabled(*context)) {
            // Create GEOMETRY type (BLOB with GEOMETRY alias)
            LogicalType geometry_type = LogicalType(LogicalTypeId::VARCHAR);
            geometry_type.SetAlias("GEOGRAPHY");
            return geometry_type;
        }
    }
    LogicalType result = CastToBigqueryType(type);
    return result;
}

google::protobuf::FieldDescriptorProto::Type BigqueryUtils::LogicalTypeToProtoType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BLOB:
        if (BigqueryUtils::IsGeometryType(type)) {
            return google::protobuf::FieldDescriptorProto::TYPE_STRING;
        }
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
        return google::protobuf::FieldDescriptorProto::TYPE_STRING;
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

    if (type == "STRING" || type.rfind("STRING(", 0) == 0) {
        result = LogicalType::VARCHAR;
    } else if (type == "JSON") {
        result = LogicalType::VARCHAR;
    } else if (type == "BYTES" || type.rfind("BYTES(", 0) == 0) {
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
    } else if (type == "NUMERIC" || type.rfind("NUMERIC(", 0) == 0) {
        result = BigqueryUtils::BigqueryNumericSQLToLogicalType(type);
    } else if (type == "BIGNUMERIC" || type.rfind("BIGNUMERIC(", 0) == 0) {
        result = BigqueryUtils::BigqueryNumericSQLToLogicalType(type);
    } else if (type == "GEOGRAPHY") {
        if (BigquerySettings::GeographyAsGeometry()) {
            result = LogicalType::BLOB;
            result.SetAlias("GEOMETRY");
        } else {
            result = LogicalType::VARCHAR;
        }
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
        throw BinderException("Unknown BigQuery Type: " + type);
    }

    return result;
}

LogicalType BigqueryUtils::BigqueryNumericSQLToLogicalType(const string &type) {
    auto precision_and_scale = BigqueryUtils::ParseNumericPrecisionAndScale(type);
    auto precision = precision_and_scale.first;
    auto scale = precision_and_scale.second;

    if (type == "BIGNUMERIC" || type.rfind("BIGNUMERIC(", 0) == 0) {
        if (BigquerySettings::BignumericAsVarchar()) {
            return LogicalType::VARCHAR;
        }
    }

    if (precision == BQ_BIGNUMERIC_PRECISION_DEFAULT) {
        throw BigqueryTypeException::BignumericNotSupported();
    }
    if (precision < 1 || precision > DUCKDB_DECIMAL_PRECISION_MAX) {
        throw BigqueryTypeException::UnsupportedPrecision(precision, type);
    }
    if (scale < 0 || scale > DUCKDB_DECIMAL_SCALE_MAX) {
        throw BigqueryTypeException::UnsupportedScale(scale, type);
    }

    return LogicalType::DECIMAL(precision_and_scale.first, precision_and_scale.second);
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
    case LogicalTypeId::HUGEINT:
        throw NotImplementedException("HUGEINT not supported in BigQuery.");
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
        return "INT64"; // BigQuery does not differentiate unsigned types
    case LogicalTypeId::UBIGINT:
        throw NotImplementedException("UBIGINT not supported in BigQuery.");
    case LogicalTypeId::UHUGEINT:
        throw NotImplementedException("UHUGEINT not supported in BigQuery.");
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
        return "FLOAT64";
    case LogicalTypeId::BLOB:
        return "BYTES";
    case LogicalTypeId::DATE:
        return "DATE";
    case LogicalTypeId::DECIMAL:
        return "NUMERIC";
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

string BigqueryUtils::DecimalToString(const hugeint_t &value, const LogicalType &type) {
    if (type.id() != LogicalTypeId::DECIMAL) {
        throw BinderException("Type is not a DECIMAL type.");
    }

    string decimal_str = Hugeint::ToString(value);
    auto scale = DecimalType::GetScale(type);
    if (scale > 0) {
        if (decimal_str.length() <= static_cast<size_t>(scale)) {
            // Add leading zeros if necessary
            decimal_str = string(scale - decimal_str.length(), '0') + decimal_str;
        }
        decimal_str.insert(decimal_str.length() - scale, ".");
    }
    return decimal_str;
}

pair<int, int> BigqueryUtils::ParseNumericPrecisionAndScale(const string &type) {
    std::regex base_pattern(R"(NUMERIC|BIGNUMERIC)");
    std::regex precision_only_pattern(R"((NUMERIC|BIGNUMERIC)\((\d+)\))");
    std::regex full_numeric_pattern(R"((NUMERIC|BIGNUMERIC)\((\d+),\s*(\d+)\))");

    std::smatch match;
    if (std::regex_match(type, match, full_numeric_pattern)) {
        int precision = std::stoi(match[2]);
        int scale = std::stoi(match[3]);
        return {precision, scale};
    } else if (std::regex_match(type, match, precision_only_pattern)) {
        int precision = std::stoi(match[2]);
        int scale = (match[1].str() == "NUMERIC") ? BQ_NUMERIC_SCALE_DEFAULT : BQ_BIGNUMERIC_SCALE_DEFAULT;
        return {precision, scale};
    } else if (std::regex_match(type, match, base_pattern)) {
        if (type == "NUMERIC") {
            return {BQ_NUMERIC_PRECISION_DEFAULT, BQ_NUMERIC_SCALE_DEFAULT};
        } else {
            return {BQ_BIGNUMERIC_PRECISION_DEFAULT, BQ_BIGNUMERIC_SCALE_DEFAULT};
        }
    }

    throw std::invalid_argument("Invalid NUMERIC/BIGNUMERIC type format: " + type);
}

uint64_t Iso8601ToMillis(const string &iso8601) {
    auto timestamp = Timestamp::FromString(iso8601, false);
    auto timestamp_ms = Timestamp::GetEpochMs(timestamp);
    return timestamp_ms;
}

} // namespace bigquery
} // namespace duckdb
