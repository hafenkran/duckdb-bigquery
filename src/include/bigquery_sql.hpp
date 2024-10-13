#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "bigquery_utils.hpp"

namespace duckdb {
namespace bigquery {

struct BigquerySQL {
public:
    static string ExtractFilters(PhysicalOperator &child);
    static string TransformFilter(const string &column_name, TableFilter &filter);
    static string CreateExpression(const string &column_name, vector<unique_ptr<TableFilter>> &filters, const string &op);

	static string AlterTableInfoToSQL(const string &project_id, const AlterTableInfo &info);
    static string CreateSchemaInfoToSQL(const string &project_id, const CreateSchemaInfo &info);
    static string CreateTableInfoToSQL(const string &project_id, const CreateTableInfo &info);
    static string CreateViewInfoToSQL(const string &project_id, const CreateViewInfo &info);
    static string DropInfoToSQL(const string &project_id, const DropInfo &info);

    static string LogicalUpdateToSQL(const string &project_id, LogicalUpdate &lu, PhysicalOperator &child);
    static string LogicalDeleteToSQL(const string &project_id, LogicalDelete &ld, PhysicalOperator &child);

	static string BigqueryColumnToSQL(const ColumnDefinition &column);
    static string BigqueryColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints);

	static string ColumnsFromInformationSchema(const string &project_id, const string &dataset);
};

} // namespace bigquery
} // namespace duckdb
