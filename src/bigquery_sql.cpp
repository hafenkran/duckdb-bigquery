#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
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

namespace duckdb {
namespace bigquery {

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
        auto &column_ids = get.GetColumnIds();
        string table_filters_exp;
        for (auto &filter : get.table_filters.filters) {
            if (!table_filters_exp.empty()) {
                table_filters_exp += " AND ";
            }
            auto column_id = column_ids[filter.first];
            auto &column_name = get.names[column_id.GetPrimaryIndex()];
            table_filters_exp += TransformFilter(column_name, *filter.second);
        }
        return table_filters_exp;
    }
    case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
        return "false";
    default:
        throw NotImplementedException("Unsupported logical operator type " + LogicalOperatorToString(child.type));
    }
}

string BigquerySQL::CreateExpression(const string &column_name,
                                     vector<unique_ptr<TableFilter>> &filters,
                                     const string &op) {
    vector<string> filter_entries;
    for (auto &filter : filters) {
        filter_entries.push_back(TransformFilter(column_name, *filter));
    }
    return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

string BigquerySQL::TransformFilter(const string &column_name, TableFilter &filter) {
    switch (filter.filter_type) {
    case TableFilterType::CONSTANT_COMPARISON: {
        auto &constant_filter = dynamic_cast<ConstantFilter &>(filter);

        string constant_string;
        if (BigqueryUtils::IsValueQuotable(constant_filter.constant)) {
            constant_string = KeywordHelper::WriteQuoted(constant_filter.constant.ToString());
        } else {
            constant_string = constant_filter.constant.ToString();
        }

        switch (constant_filter.comparison_type) {
        case ExpressionType::COMPARE_EQUAL:
            return "`" + column_name + "` = " + constant_string;
        case ExpressionType::COMPARE_GREATERTHAN:
            return "`" + column_name + "` > " + constant_string;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            return "`" + column_name + "` >= " + constant_string;
        case ExpressionType::COMPARE_LESSTHAN:
            return "`" + column_name + "` < " + constant_string;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
            return "`" + column_name + "` <= " + constant_string;
        case ExpressionType::COMPARE_NOTEQUAL:
            return "`" + column_name + "` != " + constant_string;
        default:
            throw NotImplementedException("Unsupported comparison type");
        }
    }
    case TableFilterType::IS_NULL:
        return "`" + column_name + "` IS NULL";
    case TableFilterType::IS_NOT_NULL:
        return "`" + column_name + "` IS NOT NULL";
    case TableFilterType::CONJUNCTION_AND:
    case TableFilterType::CONJUNCTION_OR: {
        auto &conjunction_filter = dynamic_cast<ConjunctionAndFilter &>(filter);
        string op = filter.filter_type == TableFilterType::CONJUNCTION_AND ? "AND" : "OR";
        return CreateExpression(column_name, conjunction_filter.child_filters, op);
    }
    case TableFilterType::STRUCT_EXTRACT: {
        auto &struct_filter = dynamic_cast<StructFilter &>(filter);
        auto child_name = KeywordHelper::WriteQuoted(struct_filter.child_name, '`');
        auto new_column_name = "`" + column_name + "`." + child_name;
        return TransformFilter(new_column_name, *struct_filter.child_filter);
    }
    case TableFilterType::OPTIONAL_FILTER: {
        auto &optional_filter = filter.Cast<OptionalFilter>();
        return TransformFilter(column_name, *optional_filter.child_filter);
    }
    case TableFilterType::IN_FILTER: {
        auto &in_filter = dynamic_cast<InFilter &>(filter);
        vector<string> in_values;
        for (auto &value : in_filter.values) {
            if (BigqueryUtils::IsValueQuotable(value)) {
                in_values.push_back(KeywordHelper::WriteQuoted(value.ToString()));
            } else {
                in_values.push_back(value.ToString());
            }
        }
        return "`" + column_name + "` IN (" + StringUtil::Join(in_values, ", ") + ")";
    }
    default:
        throw InternalException("Unsupported filter type");
    }
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

string BigquerySQL::ColumnsFromInformationSchemaQuery(const string &project_id,
                                                      const vector<string> &datasets,
                                                      const string &location) {
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

        auto dataset_query = ColumnsFromInformationSchemaQuery(project_id, dataset, location, false);
        query << dataset_query;
    }
    query << " ORDER BY table_schema, table_name, ordinal_position";
    return query.str();
}

string BigquerySQL::ColumnsFromInformationSchemaQuery(const string &project_id,
                                                      const string &dataset_id,
                                                      const string &location,
                                                      const bool include_order_by) {
    const auto columns_table =
        BigqueryUtils::FormatTableStringSimple(project_id, dataset_id, "INFORMATION_SCHEMA.COLUMNS");

    std::stringstream query;
    query << "SELECT cols.table_schema, cols.table_name, cols.column_name, cols.data_type, ";
    query << "cols.is_nullable, cols.column_default, cols.ordinal_position, ";
    if (location.empty()) {
        query << "CAST(NULL AS INT64) AS total_rows, CAST(NULL AS INT64) AS total_logical_bytes ";
        query << "FROM `" << columns_table << "` AS cols ";
    } else {
        const auto storage_table =
            project_id + ".region-" + StringUtil::Lower(location) + ".INFORMATION_SCHEMA.TABLE_STORAGE";
        query << "storage.total_rows, storage.total_logical_bytes ";
        query << "FROM `" << columns_table << "` AS cols ";
        query << "LEFT JOIN `" << storage_table << "` AS storage ";
        query << "ON cols.table_schema = storage.table_schema ";
        query << "AND cols.table_name = storage.table_name ";
        query << "AND storage.deleted = FALSE ";
    }
    query << "WHERE cols.is_system_defined = 'NO' ";
    query << "AND cols.ordinal_position IS NOT NULL ";
    if (include_order_by) {
        query << "ORDER BY cols.table_schema, cols.table_name, cols.ordinal_position";
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
