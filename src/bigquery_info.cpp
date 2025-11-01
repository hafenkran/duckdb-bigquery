#include "duckdb.hpp"

#include "bigquery_info.hpp"

namespace duckdb {
namespace bigquery {

BigqueryCreateSchemaInfo::BigqueryCreateSchemaInfo() : CreateSchemaInfo() {
}

BigqueryCreateSchemaInfo::BigqueryCreateSchemaInfo(const CreateSchemaInfo &info, unordered_map<string, string> options)
    : CreateSchemaInfo(), options(std::move(options)) {
    this->info_type = info.info_type;
    this->catalog = info.catalog;
    this->schema = info.schema;
    this->type = info.type;
    this->on_conflict = info.on_conflict;
    this->temporary = info.temporary;
    this->internal = info.internal;
    this->sql = info.sql;
    this->dependencies = info.dependencies;
    this->comment = info.comment;
    this->tags = info.tags;
}

unique_ptr<CreateInfo> BigqueryCreateSchemaInfo::Copy() const {
    auto copy = make_uniq<BigqueryCreateSchemaInfo>(*this);
    copy->options = options;
    return std::move(copy);
}

string BigqueryCreateSchemaInfo::ToString() const {
    string base_str = CreateSchemaInfo::ToString();
    if (!base_str.empty() && base_str.back() == ';') {
        base_str.pop_back();
    }
    base_str += " OPTIONS (";
    bool first = true;
    for (const auto &opt : options) {
        if (!first) {
            base_str += ", ";
        }
        base_str += opt.first + "='" + opt.second + "'";
        first = false;
    }
    base_str += ");";
    return base_str;
}

BigqueryCreateTableInfo::BigqueryCreateTableInfo() : CreateTableInfo() {
}

BigqueryCreateTableInfo::BigqueryCreateTableInfo(string catalog,
                                                 string schema,
                                                 string name,
                                                 unordered_map<string, string> options)
    : CreateTableInfo(std::move(catalog), std::move(schema), std::move(name)), options(std::move(options)) {
}

BigqueryCreateTableInfo::BigqueryCreateTableInfo(const CreateTableInfo &info, unordered_map<string, string> options)
    : CreateTableInfo(info.catalog, info.schema, info.table), options(std::move(options)) {
    this->info_type = info.info_type;
    this->type = info.type;
    this->on_conflict = info.on_conflict;
    this->temporary = info.temporary;
    this->internal = info.internal;
    this->sql = info.sql;
    this->dependencies = info.dependencies;
    this->comment = info.comment;
    this->tags = info.tags;

    this->columns = info.columns.Copy();
    for (auto &constraint : info.constraints) {
        this->constraints.push_back(constraint->Copy());
    }
    if (info.query) {
        this->query = unique_ptr_cast<SQLStatement, SelectStatement>(info.query->Copy());
    }
}

BigqueryCreateTableInfo::BigqueryCreateTableInfo(const BigqueryCreateTableInfo &other)
    : BigqueryCreateTableInfo(static_cast<const CreateTableInfo &>(other), other.options) {
}

unique_ptr<CreateInfo> BigqueryCreateTableInfo::Copy() const {
    auto copy = make_uniq<BigqueryCreateTableInfo>(*this);
    copy->options = options;
    return std::move(copy);
}

string BigqueryCreateTableInfo::ToString() const {
    string base_str = CreateTableInfo::ToString();
    if (!base_str.empty() && base_str.back() == ';') {
        base_str.pop_back();
    }
    base_str += " OPTIONS (";
    bool first = true;
    for (const auto &opt : options) {
        if (!first) {
            base_str += ", ";
        }
        base_str += opt.first + "='" + opt.second + "'";
        first = false;
    }
    base_str += ");";
    return base_str;
}

BigqueryCreateViewInfo::BigqueryCreateViewInfo() : CreateViewInfo() {
}

BigqueryCreateViewInfo::BigqueryCreateViewInfo(SchemaCatalogEntry &schema,
                                               string view_name,
                                               unordered_map<string, string> options)
    : CreateViewInfo(schema, std::move(view_name)), options(std::move(options)) {
}

BigqueryCreateViewInfo::BigqueryCreateViewInfo(const CreateViewInfo &info, unordered_map<string, string> options)
    : CreateViewInfo(info.catalog, info.schema, info.view_name), options(std::move(options)) {
    this->info_type = info.info_type;
    this->type = info.type;
    this->on_conflict = info.on_conflict;
    this->temporary = info.temporary;
    this->internal = info.internal;
    this->sql = info.sql;
    this->dependencies = info.dependencies;
    this->comment = info.comment;
    this->tags = info.tags;

    if (info.query) {
        this->query = unique_ptr_cast<SQLStatement, SelectStatement>(info.query->Copy());
    }
}

BigqueryCreateViewInfo::BigqueryCreateViewInfo(const BigqueryCreateViewInfo &other)
    : BigqueryCreateViewInfo(static_cast<const CreateViewInfo &>(other), other.options) {
}

unique_ptr<CreateInfo> BigqueryCreateViewInfo::Copy() const {
    auto copy = make_uniq<BigqueryCreateViewInfo>(*this);
    copy->options = options;
    return std::move(copy);
}

string BigqueryCreateViewInfo::ToString() const {
    string base_str = CreateViewInfo::ToString();
    if (!base_str.empty() && base_str.back() == ';') {
        base_str.pop_back();
    }
    base_str += " OPTIONS (";
    bool first = true;
    for (const auto &opt : options) {
        if (!first) {
            base_str += ", ";
        }
        base_str += opt.first + "='" + opt.second + "'";
        first = false;
    }
    base_str += ");";
    return base_str;
}


} // namespace bigquery
} // namespace duckdb
