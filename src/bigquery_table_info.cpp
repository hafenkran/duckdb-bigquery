#include "bigquery_table_info.hpp"

#include "bigquery_utils.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace bigquery {
namespace {

BigqueryRelationType RelationTypeFromInformationSchema(const string &raw_type) {
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

BigqueryRelationType RelationTypeFromRest(const string &raw_type,
                                          bool has_snapshot_definition,
                                          bool has_clone_definition) {
    if (has_clone_definition) {
        return BigqueryRelationType::CLONE;
    }
    if (has_snapshot_definition) {
        return BigqueryRelationType::SNAPSHOT;
    }

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
    return BigqueryRelationType::UNKNOWN;
}

const char *RelationTypeName(BigqueryRelationType type) {
    switch (type) {
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

} // namespace

BigqueryRelationMetadata::BigqueryRelationMetadata() : type(BigqueryRelationType::UNKNOWN), is_insertable_into(false) {
}

BigqueryRelationMetadata::BigqueryRelationMetadata(BigqueryRelationType type_p,
                                                   string raw_type_p,
                                                   bool is_insertable_into_p)
    : type(type_p), raw_type(std::move(raw_type_p)), is_insertable_into(is_insertable_into_p) {
}

BigqueryRelationMetadata BigqueryRelationMetadata::StandardTable() {
    return BigqueryRelationMetadata(BigqueryRelationType::STANDARD_TABLE, "TABLE", true);
}

BigqueryRelationMetadata BigqueryRelationMetadata::FromInformationSchema(const string &raw_type,
                                                                         bool is_insertable_into) {
    return BigqueryRelationMetadata(RelationTypeFromInformationSchema(raw_type), raw_type, is_insertable_into);
}

BigqueryRelationMetadata BigqueryRelationMetadata::FromRest(const string &raw_type,
                                                            bool has_snapshot_definition,
                                                            bool has_clone_definition) {
    auto type = RelationTypeFromRest(raw_type, has_snapshot_definition, has_clone_definition);
    auto is_insertable_into = type == BigqueryRelationType::STANDARD_TABLE || type == BigqueryRelationType::CLONE;
    return BigqueryRelationMetadata(type, raw_type, is_insertable_into);
}

BigqueryReadMode BigqueryRelationMetadata::ReadMode() const {
    switch (type) {
    case BigqueryRelationType::STANDARD_TABLE:
    case BigqueryRelationType::SNAPSHOT:
    case BigqueryRelationType::CLONE:
        return BigqueryReadMode::STORAGE_READ;
    case BigqueryRelationType::LOGICAL_VIEW:
    case BigqueryRelationType::MATERIALIZED_VIEW:
    case BigqueryRelationType::EXTERNAL_TABLE:
        return BigqueryReadMode::QUERY_JOB;
    case BigqueryRelationType::UNKNOWN:
    default:
        return BigqueryReadMode::UNSUPPORTED;
    }
}

bool BigqueryRelationMetadata::SupportsInsert() const {
    return is_insertable_into && (type == BigqueryRelationType::STANDARD_TABLE || type == BigqueryRelationType::CLONE);
}

bool BigqueryRelationMetadata::SupportsUpdateDelete() const {
    return type == BigqueryRelationType::STANDARD_TABLE || type == BigqueryRelationType::CLONE;
}

string BigqueryRelationMetadata::TypeName() const {
    if (type == BigqueryRelationType::UNKNOWN && !raw_type.empty()) {
        return raw_type;
    }
    return RelationTypeName(type);
}

const char *BigqueryRelationMetadata::ReadModeName() const {
    switch (ReadMode()) {
    case BigqueryReadMode::STORAGE_READ:
        return "Storage Read";
    case BigqueryReadMode::QUERY_JOB:
        return "Query Job";
    case BigqueryReadMode::UNSUPPORTED:
    default:
        return "Unsupported";
    }
}

BigqueryTableInfo::BigqueryTableInfo() : create_info(make_uniq<CreateTableInfo>()) {
    create_info->columns.SetAllowDuplicates(true);
}

BigqueryTableInfo::BigqueryTableInfo(const string &project_id, const string &dataset_id, const string &table_id)
    : create_info(make_uniq<CreateTableInfo>(project_id, dataset_id, table_id)) {
    create_info->columns.SetAllowDuplicates(true);
}

BigqueryTableInfo::BigqueryTableInfo(const BigqueryTableRef &table_ref)
    : BigqueryTableInfo(table_ref.project_id, table_ref.dataset_id, table_ref.table_id) {
}

} // namespace bigquery
} // namespace duckdb
