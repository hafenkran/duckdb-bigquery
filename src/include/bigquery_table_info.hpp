#pragma once

#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
namespace bigquery {

struct BigqueryTableRef;

enum class BigqueryRelationType {
    STANDARD_TABLE,
    LOGICAL_VIEW,
    MATERIALIZED_VIEW,
    EXTERNAL_TABLE,
    SNAPSHOT,
    CLONE,
    UNKNOWN
};

enum class BigqueryReadMode { STORAGE_READ, QUERY_JOB, UNSUPPORTED };

class BigqueryRelationMetadata {
public:
    BigqueryRelationMetadata();

    static BigqueryRelationMetadata StandardTable();
    static BigqueryRelationMetadata FromInformationSchema(const string &raw_type, bool is_insertable_into);
    static BigqueryRelationMetadata FromRest(const string &raw_type,
                                             bool has_snapshot_definition,
                                             bool has_clone_definition);

    BigqueryRelationType Type() const {
        return type;
    }

    BigqueryReadMode ReadMode() const;
    bool SupportsInsert() const;
    bool SupportsUpdateDelete() const;
    string TypeName() const;
    const char *ReadModeName() const;

private:
    BigqueryRelationMetadata(BigqueryRelationType type, string raw_type, bool is_insertable_into);

private:
    BigqueryRelationType type;
    string raw_type;
    bool is_insertable_into;
};

struct BigqueryTableInfo {
    BigqueryTableInfo();
    BigqueryTableInfo(const string &project_id, const string &dataset_id, const string &table_id);
    explicit BigqueryTableInfo(const BigqueryTableRef &table_ref);

    BigqueryTableInfo(const BigqueryTableInfo &) = delete;
    BigqueryTableInfo &operator=(const BigqueryTableInfo &) = delete;
    BigqueryTableInfo(BigqueryTableInfo &&) noexcept = default;
    BigqueryTableInfo &operator=(BigqueryTableInfo &&) noexcept = default;

    unique_ptr<CreateTableInfo> create_info;
    BigqueryRelationMetadata relation;
};

} // namespace bigquery
} // namespace duckdb
