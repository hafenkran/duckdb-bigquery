#pragma once

#include "duckdb.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {
namespace bigquery {

struct BigqueryCreateSchemaInfo : public CreateSchemaInfo {
    BigqueryCreateSchemaInfo();
    BigqueryCreateSchemaInfo(const CreateSchemaInfo &info, unordered_map<string, string> options = {});

    unique_ptr<CreateInfo> Copy() const override;
    string ToString() const override;

public:
    unordered_map<string, string> options;
};

struct BigqueryCreateTableInfo : public CreateTableInfo {
    BigqueryCreateTableInfo();
    BigqueryCreateTableInfo(string catalog, string schema, string name, unordered_map<string, string> options = {});
    BigqueryCreateTableInfo(const CreateTableInfo &info, unordered_map<string, string> options = {});
    BigqueryCreateTableInfo(const BigqueryCreateTableInfo &other);

    unique_ptr<CreateInfo> Copy() const override;
    string ToString() const override;

public:
    unordered_map<string, string> options;
};

struct BigqueryCreateViewInfo : public CreateViewInfo {
    BigqueryCreateViewInfo();
    BigqueryCreateViewInfo(SchemaCatalogEntry &schema, string view_name, unordered_map<string, string> options = {});
    BigqueryCreateViewInfo(const CreateViewInfo &info, unordered_map<string, string> options = {});
    BigqueryCreateViewInfo(const BigqueryCreateViewInfo &other);

    unique_ptr<CreateInfo> Copy() const override;
    string ToString() const override;

public:
    unordered_map<string, string> options;
};

} // namespace bigquery
} // namespace duckdb
