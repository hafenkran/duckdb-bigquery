#pragma once

#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace bigquery {

enum class BigqueryRelationType {
    STANDARD_TABLE,
    LOGICAL_VIEW,
    MATERIALIZED_VIEW,
    EXTERNAL_TABLE,
    SNAPSHOT,
    CLONE,
    UNKNOWN
};

inline BigqueryRelationType BigqueryRelationTypeFromInformationSchema(const string &raw_type) {
    auto type = StringUtil::Upper(raw_type);
    if (type == "BASE TABLE") {
        return BigqueryRelationType::STANDARD_TABLE;
    }
    if (type == "VIEW") {
        return BigqueryRelationType::LOGICAL_VIEW;
    }
    if (type == "MATERIALIZED VIEW") {
        return BigqueryRelationType::MATERIALIZED_VIEW;
    }
    if (type == "EXTERNAL") {
        return BigqueryRelationType::EXTERNAL_TABLE;
    }
    if (type == "SNAPSHOT") {
        return BigqueryRelationType::SNAPSHOT;
    }
    if (type == "CLONE") {
        return BigqueryRelationType::CLONE;
    }
    return BigqueryRelationType::UNKNOWN;
}

inline BigqueryRelationType BigqueryRelationTypeFromRestResource(const string &raw_type) {
    auto type = StringUtil::Upper(raw_type);
    if (type.empty() || type == "TABLE") {
        return BigqueryRelationType::STANDARD_TABLE;
    }
    if (type == "VIEW") {
        return BigqueryRelationType::LOGICAL_VIEW;
    }
    if (type == "MATERIALIZED_VIEW") {
        return BigqueryRelationType::MATERIALIZED_VIEW;
    }
    if (type == "EXTERNAL") {
        return BigqueryRelationType::EXTERNAL_TABLE;
    }
    if (type == "SNAPSHOT") {
        return BigqueryRelationType::SNAPSHOT;
    }
    if (type == "CLONE") {
        return BigqueryRelationType::CLONE;
    }
    return BigqueryRelationType::UNKNOWN;
}

inline const char *BigqueryRelationTypeToString(BigqueryRelationType relation_type) {
    switch (relation_type) {
    case BigqueryRelationType::STANDARD_TABLE:
        return "TABLE";
    case BigqueryRelationType::LOGICAL_VIEW:
        return "VIEW";
    case BigqueryRelationType::MATERIALIZED_VIEW:
        return "MATERIALIZED_VIEW";
    case BigqueryRelationType::EXTERNAL_TABLE:
        return "EXTERNAL";
    case BigqueryRelationType::SNAPSHOT:
        return "SNAPSHOT";
    case BigqueryRelationType::CLONE:
        return "CLONE";
    case BigqueryRelationType::UNKNOWN:
    default:
        return "UNKNOWN";
    }
}

inline bool BigqueryRelationSupportsStorageRead(BigqueryRelationType relation_type) {
    switch (relation_type) {
    case BigqueryRelationType::STANDARD_TABLE:
    case BigqueryRelationType::SNAPSHOT:
    case BigqueryRelationType::CLONE:
        return true;
    case BigqueryRelationType::LOGICAL_VIEW:
    case BigqueryRelationType::MATERIALIZED_VIEW:
    case BigqueryRelationType::EXTERNAL_TABLE:
    case BigqueryRelationType::UNKNOWN:
    default:
        return false;
    }
}

inline bool BigqueryRelationSupportsMutation(BigqueryRelationType relation_type, bool is_insertable_into) {
    if (!is_insertable_into) {
        return false;
    }
    switch (relation_type) {
    case BigqueryRelationType::STANDARD_TABLE:
    case BigqueryRelationType::CLONE:
        return true;
    case BigqueryRelationType::SNAPSHOT:
    case BigqueryRelationType::LOGICAL_VIEW:
    case BigqueryRelationType::MATERIALIZED_VIEW:
    case BigqueryRelationType::EXTERNAL_TABLE:
    case BigqueryRelationType::UNKNOWN:
    default:
        return false;
    }
}

} // namespace bigquery
} // namespace duckdb
