#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "bigquery_utils.hpp"

#include <functional>

namespace duckdb {
struct ColumnBinding;
class Expression;
class LogicalGet;

namespace bigquery {

struct BigquerySQL {
public:
    static string ExtractFilters(LogicalOperator &child);
    static string TransformFilters(const TableFilterSet &filters,
                                   const std::function<string(idx_t)> &column_name_resolver);
    static string TransformFilter(const string &column_name, const TableFilter &filter);
    static bool TryTransformBoundFilter(
        Expression &expr,
        const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
        const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
        string &filter_sql);
    static bool TryTransformBoundScalarExpression(
        Expression &expr,
        const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
        string &expression_sql);
    static bool TryTransformBoundFilterScalarExpression(
        Expression &expr,
        const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
        const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
        string &expression_sql);
    static bool TryTransformBoundIntegralFloatingExpression(
        Expression &expr,
        const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
        const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
        string &expression_sql);
    static bool TryTransformLogicalGetFilters(const LogicalGet &get, string &filter_sql);
    static string CreateSubquerySourceSQL(const string &query, const string &alias);

    static string AlterTableInfoToSQL(const string &project_id, const AlterTableInfo &info);
    static string CreateSchemaInfoToSQL(const string &project_id, const CreateSchemaInfo &info);
    static string CreateTableInfoToSQL(const string &project_id, const CreateTableInfo &info);
    static string CreateViewInfoToSQL(const string &project_id, const CreateViewInfo &info);
    static string DropInfoToSQL(const string &project_id, const DropInfo &info);

    static string LogicalUpdateToSQL(const string &project_id, LogicalUpdate &lu);
    static string LogicalDeleteToSQL(const string &project_id, LogicalDelete &ld);

    static string BigqueryColumnToSQL(const ColumnDefinition &column);
    static string BigqueryColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints);
    static string BigqueryOptionsToSQL(const unordered_map<string, string> &options);

    static string ColumnsFromInformationSchemaQuery(const string &project_id, const vector<string> &datasets);
    static string ColumnsFromInformationSchemaQuery(const string &project_id,
                                                    const string &dataset,
                                                    const bool include_order_by = true);
};

} // namespace bigquery
} // namespace duckdb
