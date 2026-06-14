#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"

#include "bigquery_info.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"

#include <cctype>

namespace duckdb {
namespace bigquery {
namespace {

static string QuoteFilterColumnPath(const vector<string> &column_path) {
    vector<string> quoted_path;
    quoted_path.reserve(column_path.size());
    for (auto &entry : column_path) {
        quoted_path.push_back(BigqueryUtils::WriteQuotedIdentifier(entry));
    }
    return StringUtil::Join(quoted_path, ".");
}

static string TransformFilterPath(const vector<string> &column_path, const TableFilter &filter);

static string CreateFilterExpression(const vector<string> &column_path,
                                     const vector<unique_ptr<TableFilter>> &filters,
                                     const string &op) {
    vector<string> filter_entries;
    for (auto &filter : filters) {
        filter_entries.push_back(TransformFilterPath(column_path, *filter));
    }
    return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

static string TransformFilterPath(const vector<string> &column_path, const TableFilter &filter) {
    switch (filter.filter_type) {
    case TableFilterType::CONSTANT_COMPARISON: {
        auto &constant_filter = filter.Cast<ConstantFilter>();

        string constant_string;
        if (BigqueryUtils::IsValueQuotable(constant_filter.constant)) {
            constant_string = KeywordHelper::WriteQuoted(constant_filter.constant.ToString());
        } else {
            constant_string = constant_filter.constant.ToString();
        }

        auto column_ref = QuoteFilterColumnPath(column_path);
        switch (constant_filter.comparison_type) {
        case ExpressionType::COMPARE_EQUAL:
            return column_ref + " = " + constant_string;
        case ExpressionType::COMPARE_GREATERTHAN:
            return column_ref + " > " + constant_string;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            return column_ref + " >= " + constant_string;
        case ExpressionType::COMPARE_LESSTHAN:
            return column_ref + " < " + constant_string;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
            return column_ref + " <= " + constant_string;
        case ExpressionType::COMPARE_NOTEQUAL:
            return column_ref + " != " + constant_string;
        default:
            throw NotImplementedException("Unsupported comparison type");
        }
    }
    case TableFilterType::IS_NULL:
        return QuoteFilterColumnPath(column_path) + " IS NULL";
    case TableFilterType::IS_NOT_NULL:
        return QuoteFilterColumnPath(column_path) + " IS NOT NULL";
    case TableFilterType::CONJUNCTION_AND:
    case TableFilterType::CONJUNCTION_OR: {
        auto &conjunction_filter = dynamic_cast<const ConjunctionFilter &>(filter);
        string op = filter.filter_type == TableFilterType::CONJUNCTION_AND ? "AND" : "OR";
        return CreateFilterExpression(column_path, conjunction_filter.child_filters, op);
    }
    case TableFilterType::STRUCT_EXTRACT: {
        auto &struct_filter = filter.Cast<StructFilter>();
        auto child_path = column_path;
        child_path.push_back(struct_filter.child_name);
        return TransformFilterPath(child_path, *struct_filter.child_filter);
    }
    case TableFilterType::OPTIONAL_FILTER: {
        auto &optional_filter = filter.Cast<OptionalFilter>();
        return TransformFilterPath(column_path, *optional_filter.child_filter);
    }
    case TableFilterType::IN_FILTER: {
        auto &in_filter = filter.Cast<InFilter>();
        vector<string> in_values;
        for (auto &value : in_filter.values) {
            if (BigqueryUtils::IsValueQuotable(value)) {
                in_values.push_back(KeywordHelper::WriteQuoted(value.ToString()));
            } else {
                in_values.push_back(value.ToString());
            }
        }
        return QuoteFilterColumnPath(column_path) + " IN (" + StringUtil::Join(in_values, ", ") + ")";
    }
    default:
        throw InternalException("Unsupported filter type");
    }
}

static bool TryGetLogicalGetFilterColumnName(const LogicalGet &get, idx_t filter_column_idx, string &column_name) {
    ColumnIndex column_index(filter_column_idx);
    if (!column_index.HasPrimaryIndex() || column_index.HasChildren() || column_index.IsVirtualColumn() ||
        column_index.IsRowIdColumn() || column_index.IsEmptyColumn() || filter_column_idx >= get.names.size()) {
        return false;
    }

    column_name = get.GetColumnName(column_index);
    return true;
}

static string TrimTrailingSemicolons(string query) {
    while (true) {
        while (!query.empty() && std::isspace(static_cast<unsigned char>(query.back()))) {
            query.pop_back();
        }
        if (query.empty() || query.back() != ';') {
            return query;
        }
        query.pop_back();
    }
}

} // namespace

std::string BigquerySQL::ExtractFilters(LogicalOperator &child) {
    switch (child.type) {
    case LogicalOperatorType::LOGICAL_FILTER: {
        auto &filter = child.Cast<LogicalFilter>();
        vector<string> filter_entries;
        for (auto &expression : filter.expressions) {
            filter_entries.push_back(expression->ToString());
        }
        if (!child.children.empty()) {
            auto child_filters = ExtractFilters(*child.children[0]);
            if (!child_filters.empty()) {
                filter_entries.push_back(child_filters);
            }
        }
        return StringUtil::Join(filter_entries, " AND ");
    }
    case LogicalOperatorType::LOGICAL_PROJECTION:
        if (child.children.empty()) {
            return string();
        }
        return ExtractFilters(*child.children[0]);
    case LogicalOperatorType::LOGICAL_GET: {
        auto &get = child.Cast<LogicalGet>();
        if (get.table_filters.filters.empty()) {
            return string();
        }
        string filter_sql;
        if (!TryTransformLogicalGetFilters(get, filter_sql)) {
            throw NotImplementedException("Unsupported BigQuery logical get filter");
        }
        return filter_sql;
    }
    case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
        return "false";
    default:
        throw NotImplementedException("Unsupported logical operator type " + LogicalOperatorToString(child.type));
    }
}

string BigquerySQL::TransformFilters(const TableFilterSet &filters,
                                     const std::function<string(idx_t)> &column_name_resolver) {
    vector<string> filter_entries;
    filter_entries.reserve(filters.filters.size());
    for (auto &filter : filters.filters) {
        filter_entries.push_back(TransformFilter(column_name_resolver(filter.first), *filter.second));
    }
    return StringUtil::Join(filter_entries, " AND ");
}

string BigquerySQL::TransformFilter(const string &column_name, const TableFilter &filter) {
    return TransformFilterPath(vector<string>{column_name}, filter);
}

bool BigquerySQL::TryTransformLogicalGetFilters(const LogicalGet &get, string &filter_sql) {
    if (get.table_filters.filters.empty()) {
        filter_sql.clear();
        return true;
    }

    try {
        filter_sql = TransformFilters(get.table_filters, [&](idx_t filter_idx) -> string {
            string column_name;
            if (!TryGetLogicalGetFilterColumnName(get, filter_idx, column_name)) {
                throw InternalException("Unsupported BigQuery logical get filter column");
            }
            return column_name;
        });
        return true;
    } catch (Exception &) {
        return false;
    } catch (std::exception &) {
        return false;
    }
}

string BigquerySQL::TransformExecutionFilters(const vector<column_t> &column_ids,
                                              const TableFilterSet &filters,
                                              const vector<string> &names) {
    return TransformFilters(filters, [&](idx_t filter_idx) -> string {
        if (filter_idx >= column_ids.size()) {
            throw InternalException("BigQuery filter column index out of range");
        }
        column_t logical_col = column_ids[filter_idx];
        if (logical_col == COLUMN_IDENTIFIER_ROW_ID || logical_col < 0) {
            throw InvalidInputException("ROWID cannot be referenced in a WHERE clause for BigQuery tables");
        }
        if (static_cast<idx_t>(logical_col) >= names.size()) {
            throw InternalException("BigQuery logical filter column index out of range");
        }
        return names[logical_col];
    });
}

string BigquerySQL::CreateSubquerySourceSQL(const string &query, const string &alias) {
    return "(" + TrimTrailingSemicolons(query) + ") AS " + alias;
}

string BigquerySQL::AlterTableInfoToSQL(const string &project_id, const AlterTableInfo &info) {
    if (info.schema.empty()) {
        throw BinderException("Schema not specified for AlterTableInfo");
    }
    const auto &table_string = BigqueryUtils::FormatTableStringSimple(project_id, info.schema, info.name);
    std::stringstream stmt;
    stmt << "ALTER TABLE ";
    stmt << BigqueryUtils::WriteQuotedIdentifier(table_string) << " ";

    switch (info.alter_table_type) {
    case AlterTableType::RENAME_COLUMN: {
        // Syntax
        // ALTER TABLE [IF EXISTS] table_name
        // RENAME COLUMN [IF EXISTS] column_to_column[, ...]
        //
        // 		column_to_column :=
        //     		column_name TO new_column_name
        const auto &rename_info = info.Cast<RenameColumnInfo>();
        stmt << "RENAME COLUMN ";
        stmt << BigqueryUtils::WriteQuotedIdentifier(rename_info.old_name) << " TO ";
        stmt << BigqueryUtils::WriteQuotedIdentifier(rename_info.new_name);
        break;
    }
    case AlterTableType::RENAME_TABLE: {
        // Syntax
        // ALTER TABLE [IF EXISTS] table_name RENAME TO new_table_name
        const auto &rename_info = info.Cast<RenameTableInfo>();
        stmt << "RENAME TO ";
        stmt << BigqueryUtils::WriteQuotedIdentifier(rename_info.new_table_name);
        break;
    }
    case AlterTableType::ADD_COLUMN: {
        // Syntax
        // ALTER TABLE table_name ADD COLUMN [IF NOT EXISTS] column [, ...]
        const auto &add_column_info = dynamic_cast<const AddColumnInfo *>(&info);
        // auto add_column_info = info.Cast<AddColumnInfo>();
        stmt << "ADD COLUMN ";
        if (add_column_info->if_column_not_exists) {
            stmt << "IF NOT EXISTS ";
        }
        stmt << BigqueryColumnToSQL(add_column_info->new_column);
        break;
    }
    case AlterTableType::REMOVE_COLUMN: {
        // Syntax
        // ALTER TABLE table_name DROP COLUMN [IF EXISTS] column_name [, ...]
        const auto &remove_column_info = info.Cast<RemoveColumnInfo>();
        stmt << "DROP COLUMN ";
        if (remove_column_info.if_column_exists) {
            stmt << "IF EXISTS ";
        }
        stmt << BigqueryUtils::WriteQuotedIdentifier(remove_column_info.removed_column);
        break;
    }
    case AlterTableType::ALTER_COLUMN_TYPE: {
        // Syntax
        // ALTER TABLE table_name ALTER COLUMN column_name SET DATA TYPE type
        const auto &alter_column_type_info = dynamic_cast<const ChangeColumnTypeInfo *>(&info);
        stmt << "ALTER COLUMN ";
        stmt << BigqueryUtils::WriteQuotedIdentifier(alter_column_type_info->column_name);
        stmt << " SET DATA TYPE " << BigqueryUtils::LogicalTypeToBigquerySQL(alter_column_type_info->target_type);
        break;
    }
    case AlterTableType::SET_DEFAULT: {
        // Syntax
        // ALTER TABLE table_name ALTER COLUMN column_name SET DEFAULT expression
        const auto &set_default_info = dynamic_cast<const SetDefaultInfo *>(&info);
        stmt << "ALTER COLUMN ";
        stmt << BigqueryUtils::WriteQuotedIdentifier(set_default_info->column_name);
        stmt << " SET DEFAULT " << set_default_info->expression->ToString();
        break;
    }
    case AlterTableType::DROP_NOT_NULL: {
        // Syntax
        // ALTER TABLE table_name ALTER COLUMN column_name DROP NOT NULL
        const auto &drop_not_null_info = info.Cast<DropNotNullInfo>();
        stmt << "ALTER COLUMN ";
        stmt << BigqueryUtils::WriteQuotedIdentifier(drop_not_null_info.column_name);
        stmt << " DROP NOT NULL";
        break;
    }
    default:
        throw NotImplementedException("Unsupported Alter Table type: This type of ALTER TABLE is not supported.");
    }

    return stmt.str();
}

string BigquerySQL::CreateSchemaInfoToSQL(const string &project_id, const CreateSchemaInfo &info) {
    const auto &schema_string = BigqueryUtils::FormatTableStringSimple(project_id, info.schema);
    std::stringstream query;
    query << "CREATE SCHEMA ";
    query << BigqueryUtils::WriteQuotedIdentifier(schema_string);

    if (auto *bq_info = dynamic_cast<const BigqueryCreateSchemaInfo *>(&info)) {
        auto options_str = BigquerySQL::BigqueryOptionsToSQL(bq_info->options);
        if (!options_str.empty()) {
            query << " " << options_str;
        }
    }
    return query.str();
}

string BigquerySQL::CreateTableInfoToSQL(const string &project_id, const CreateTableInfo &info) {
    if (info.schema.empty()) {
        throw BinderException("Schema not specified for CreateTableInfo");
    }
    std::stringstream stmt;
    stmt << "CREATE ";
    if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
        stmt << "OR REPLACE ";
    }
    stmt << "TABLE ";
    if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
        stmt << "IF NOT EXISTS ";
    }
    // Append the table name
    auto table_string = BigqueryUtils::FormatTableStringSimple(project_id, info.schema, info.table);
    stmt << BigqueryUtils::WriteQuotedIdentifier(table_string) << " ";
    stmt << BigqueryColumnsToSQL(info.columns, info.constraints);

    if (auto *bq_info = dynamic_cast<const BigqueryCreateTableInfo *>(&info)) {
        if (!bq_info->partition_by.empty()) {
            stmt << " PARTITION BY ";
            for (idx_t i = 0; i < bq_info->partition_by.size(); i++) {
                if (i > 0) {
                    stmt << ", ";
                }
                stmt << bq_info->partition_by[i];
            }
        }
        if (!bq_info->cluster_by.empty()) {
            stmt << " CLUSTER BY ";
            for (idx_t i = 0; i < bq_info->cluster_by.size(); i++) {
                if (i > 0) {
                    stmt << ", ";
                }
                stmt << bq_info->cluster_by[i];
            }
        }
        auto options_str = BigquerySQL::BigqueryOptionsToSQL(bq_info->options);
        if (!options_str.empty()) {
            stmt << " " << options_str;
        }
    }

    return stmt.str();
}

string BigquerySQL::CreateViewInfoToSQL(const string &project_id, const CreateViewInfo &info) {
    if (info.sql.empty()) {
        throw BinderException("Cannot create view in BigQuery from an empty SQL statement.");
    }
    std::stringstream stmt;
    stmt << "CREATE ";
    if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
        stmt << "OR REPLACE ";
    }
    stmt << "VIEW ";
    if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
        stmt << "IF NOT EXISTS ";
    }
    // Append the view name
    auto table_string = BigqueryUtils::FormatTableStringSimple(info.catalog, info.schema, info.view_name);
    stmt << BigqueryUtils::WriteQuotedIdentifier(table_string) << " ";
    if (!info.aliases.empty()) {
        stmt << "(";
        for (size_t i = 0; i < info.aliases.size(); i++) {
            if (i > 0) {
                stmt << ", ";
            }
            stmt << info.aliases[i];
        }
        stmt << ") ";
    }
    stmt << "AS ";
    stmt << info.query->ToString();
    return stmt.str();
}

string BigquerySQL::DropInfoToSQL(const string &project_id, const DropInfo &info) {
    string query;
    query += "DROP ";
    query += CatalogTypeToString(info.type) + " ";
    if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
        query += "IF EXISTS ";
    }

    switch (info.type) {
    case CatalogType::VIEW_ENTRY:
    case CatalogType::TABLE_ENTRY: {
        auto name = project_id + "." + info.schema + "." + info.name;
        query += BigqueryUtils::WriteQuotedIdentifier(name);
        break;
    }
    case CatalogType::SCHEMA_ENTRY: {
        auto name = project_id + "." + info.name;
        query += BigqueryUtils::WriteQuotedIdentifier(name);
        if (info.cascade) {
            query += " CASCADE";
        } else {
            query += " RESTRICT";
        }
        break;
    }
    default:
        throw InternalException("CatalogType not supported");
    }

    return query;
}

string BigquerySQL::LogicalUpdateToSQL(const string &project_id, LogicalUpdate &lu) {
    if (lu.children.empty() || lu.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
        throw NotImplementedException("BigQuery: This type of UPDATE not supported.");
    }
    auto &proj = lu.children[0]->Cast<LogicalProjection>();
    auto table_string = BigqueryUtils::FormatTableStringSimple(project_id, //
                                                               lu.table.schema.name,
                                                               lu.table.name);

    string sql = "UPDATE ";
    sql += BigqueryUtils::WriteQuotedIdentifier(table_string);
    sql += " SET ";
    for (size_t c = 0; c < lu.columns.size(); ++c) {
        if (c > 0) {
            sql += ", ";
        }

        auto &col = lu.table.GetColumn(lu.table.GetColumns().PhysicalToLogical(lu.columns[c]));
        sql += BigqueryUtils::WriteQuotedIdentifier(col.GetName()) + " = ";
        if (lu.expressions[c]->type == ExpressionType::VALUE_DEFAULT) {
            sql += "DEFAULT";
            continue;
        }
        if (lu.expressions[c]->type != ExpressionType::BOUND_REF) {
            throw NotImplementedException("BigQuery UPDATE - Expected a bound reference expression");
        }
        auto &ref = lu.expressions[c]->Cast<BoundReferenceExpression>();
        if (ref.index >= proj.expressions.size()) {
            throw InternalException("BigQuery UPDATE - Projection reference index out of range");
        }
        sql += proj.expressions[ref.index]->ToString();
    }

    auto filters = proj.children.empty() ? string() : ExtractFilters(*proj.children[0]);
    if (!filters.empty()) {
        sql += " WHERE " + filters;
    } else {
        // Each UPDATE statement must have a WHERE clause
        sql += " WHERE true";
    }
    return sql;
}

string BigquerySQL::LogicalDeleteToSQL(const string &project_id, LogicalDelete &ld) {
    auto table_string = BigqueryUtils::FormatTableStringSimple(project_id, //
                                                               ld.table.schema.name,
                                                               ld.table.name);
    std::stringstream sql;
    sql << "DELETE FROM ";
    sql << BigqueryUtils::WriteQuotedIdentifier(table_string);
    try {
        auto filters = ld.children.empty() ? string() : ExtractFilters(*ld.children[0]);
        if (!filters.empty()) {
            sql << " WHERE " + filters;
        } else {
            // Each DELETE statement must have a WHERE clause
            sql << " WHERE true";
        }
    } catch (const NotImplementedException &e) {
        throw NotImplementedException(std::string(e.what()) +
                                      " in DELETE statement - only simple deletes (e.g. DELETE FROM tbl WHERE x=y) are "
                                      "supported in the BigQuery connector");
    }
    return sql.str();
}

string BigquerySQL::BigqueryColumnToSQL(const ColumnDefinition &column) {
    std::stringstream sql;
    sql << "`" << column.Name() << "` ";
    sql << BigqueryUtils::LogicalTypeToBigquerySQL(column.Type());
    if (column.HasDefaultValue()) {
        sql << " DEFAULT (" << column.DefaultValue().ToString() << ")";
    }
    return sql.str();
}

string BigquerySQL::BigqueryColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints) {

    logical_index_set_t columns_not_null;
    logical_index_set_t columns_unique;
    logical_index_set_t columns_primary_key;
    vector<string> constraints_extra;

    for (auto &constraint : constraints) {
        if (constraint->type == ConstraintType::NOT_NULL) {
            auto &constraint_not_null = constraint->Cast<NotNullConstraint>();
            columns_not_null.insert(constraint_not_null.index);
        } else if (constraint->type == ConstraintType::FOREIGN_KEY) {
            throw BinderException("FOREING KEY constraints are not supported by BigQuery.");
        } else if (constraint->type == ConstraintType::UNIQUE) {
            throw BinderException("UNIQUE constraints are not supported by BigQuery.");
        } else if (constraint->type == ConstraintType::CHECK) {
            throw BinderException("CHECK constraints are not supported by BigQuery.");
        } else {
            constraints_extra.push_back(constraint->ToString());
        }
    }

    std::stringstream str;
    str << "(";
    for (auto &column : columns.Logical()) {
        if (column.Oid() > 0) {
            str << ", ";
        }
        str << "`" << column.Name() << "` ";
        str << BigqueryUtils::LogicalTypeToBigquerySQL(column.Type());

        bool is_not_null = columns_not_null.find(column.Logical()) != columns_not_null.end();
        if (is_not_null) {
            str << " NOT NULL";
        }

        // In BigQuery, primary key and unique constraints are handled differently
        // and might not be specified directly in the CREATE TABLE statement like MySQL
        // You would typically handle them with OPTIONS or in separate management operations
        if (column.HasDefaultValue()) {
            str << " DEFAULT (" << column.DefaultValue().ToString() << ")";
        }
    }

    // Constraints in BigQuery are less common in CREATE TABLE
    // They are often managed through separate mechanisms or not directly applicable
    // This is a placeholder for any additional constraints or handling you might need
    // for (auto &extra_constraint : constraints) {
    // BigQuery might not support adding constraints directly in CREATE TABLE like MySQL
    // We might need to handle some constraints separately or differently
    // TODO
    // }

    str << ")";
    return str.str();
}

string BigquerySQL::ColumnsFromInformationSchemaQuery(const string &project_id, const vector<string> &datasets) {
    std::stringstream query;
    bool is_first = true;
    for (const auto &dataset : datasets) {
        if (dataset.empty()) {
            throw BinderException("Dataset name cannot be empty");
        }
        if (is_first) {
            is_first = false;
        } else {
            query << " UNION ALL ";
        }

        auto dataset_query = ColumnsFromInformationSchemaQuery(project_id, dataset, false);
        query << dataset_query;
    }
    query << "ORDER BY table_name, ordinal_position";
    return query.str();
}

string BigquerySQL::ColumnsFromInformationSchemaQuery(const string &project_id,
                                                      const string &dataset_id,
                                                      const bool include_order_by) {
    const auto table_string =
        BigqueryUtils::FormatTableStringSimple(project_id, dataset_id, "INFORMATION_SCHEMA.COLUMNS");

    std::stringstream query;
    query << "SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default, ordinal_position ";
    query << "FROM `" << table_string << "` ";
    query << "WHERE is_system_defined = 'NO' "; // Adjusted the comparison
    query << "AND ordinal_position IS NOT NULL ";
    if (include_order_by) {
        query << "ORDER BY table_name, ordinal_position";
    }
    return query.str();
}

string BigquerySQL::BigqueryOptionsToSQL(const unordered_map<string, string> &options) {
    std::stringstream query;
    if (options.empty()) {
        return "";
    }
    query << "OPTIONS (";
    bool is_first = true;
    for (const auto &option : options) {
        if (is_first) {
            is_first = false;
        } else {
            query << ", ";
        }
        query << option.first << " = '" << option.second << "'";
    }
    query << ")";
    return query.str();
}

} // namespace bigquery
} // namespace duckdb
