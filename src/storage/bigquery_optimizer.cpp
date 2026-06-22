#include "storage/bigquery_optimizer.hpp"

#include "bigquery_query.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <optional>

namespace duckdb {
namespace bigquery {
namespace {

static constexpr const char *BQ_AGGREGATE_SOURCE_ALIAS = "__duckdb_bq_src";
static constexpr const char *BQ_AGGREGATE_COLUMN_PREFIX = "__duckdb_bq_aggr_";
static constexpr const char *BQ_GROUP_COLUMN_PREFIX = "__duckdb_bq_group_";

struct BigqueryAggregateSource {
    idx_t table_index = DConstants::INVALID_INDEX;
    vector<std::optional<string>> column_names;
    vector<std::optional<string>> column_sql;

    BigqueryConfig config;
    shared_ptr<BigqueryClient> bq_client;
    string source_sql;
    string source_filter;
    vector<Value> query_parameters;
    std::optional<int> timeout_ms;
};

static bool IsRestScalarType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::STRUCT:
    case LogicalTypeId::LIST:
    case LogicalTypeId::MAP:
    case LogicalTypeId::ARRAY:
    case LogicalTypeId::UNION:
        return false;
    default:
        return true;
    }
}

static bool TryGetScanBindingColumnName(const LogicalGet &get, idx_t binding_column_idx, string &column_name) {
    const auto &column_ids = get.GetColumnIds();
    if (binding_column_idx >= column_ids.size()) {
        return false;
    }

    const auto &column_index = column_ids[binding_column_idx];
    if (!column_index.HasPrimaryIndex() || column_index.HasChildren() || column_index.IsVirtualColumn() ||
        column_index.IsRowIdColumn() || column_index.IsEmptyColumn()) {
        return false;
    }

    column_name = get.GetColumnName(column_index);
    return true;
}

static string ConjoinFilters(const string &left, const string &right) {
    if (left.empty()) {
        return right;
    }
    if (right.empty()) {
        return left;
    }
    return "(" + left + ") AND (" + right + ")";
}

static bool TryResolveGet(LogicalGet &get, BigqueryAggregateSource &source) {
    auto *scan_bind = dynamic_cast<BigqueryScanBindData *>(get.bind_data.get());
    auto *rest_bind = dynamic_cast<BigqueryQueryRestBindData *>(get.bind_data.get());
    if (!scan_bind && !rest_bind) {
        return false;
    }

    source.table_index = get.table_index;
    source.column_names.clear();
    source.column_sql.clear();

    if (scan_bind) {
        if (!scan_bind->bq_client) {
            return false;
        }
        source.config = scan_bind->bq_config;
        source.bq_client = scan_bind->bq_client;
        source.query_parameters = scan_bind->query_parameters;
        source.timeout_ms = scan_bind->query_timeout_ms;
        source.source_filter = scan_bind->filter_condition;
        if (scan_bind->RequiresQueryExec()) {
            source.source_sql = BigquerySQL::CreateSubquerySourceSQL(scan_bind->query, BQ_AGGREGATE_SOURCE_ALIAS);
        } else {
            source.source_sql = BigqueryUtils::WriteQuotedIdentifier(scan_bind->TableString());
        }
    } else {
        if (!rest_bind->bq_client) {
            return false;
        }
        source.config = rest_bind->config;
        source.bq_client = rest_bind->bq_client;
        source.query_parameters = rest_bind->query_parameters;
        source.timeout_ms = rest_bind->timeout_ms;
        source.source_filter.clear();
        source.source_sql = BigquerySQL::CreateSubquerySourceSQL(rest_bind->query, BQ_AGGREGATE_SOURCE_ALIAS);
    }

    const auto &column_ids = get.GetColumnIds();
    if (column_ids.empty()) {
        return false;
    }
    source.column_names.resize(column_ids.size());
    source.column_sql.resize(column_ids.size());
    for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
        string column_name;
        if (TryGetScanBindingColumnName(get, col_idx, column_name)) {
            source.column_sql[col_idx] = BigqueryUtils::WriteQuotedIdentifier(column_name);
            source.column_names[col_idx] = std::move(column_name);
        }
    }

    string pushed_filter;
    if (!BigquerySQL::TryTransformLogicalGetFilters(get, pushed_filter)) {
        return false;
    }
    source.source_filter = ConjoinFilters(source.source_filter, pushed_filter);
    return true;
}

static bool TryResolveColumnName(const BigqueryAggregateSource &source, const ColumnBinding &binding, string &name) {
    if (binding.table_index != source.table_index || binding.column_index >= source.column_names.size()) {
        return false;
    }
    const auto &column_name = source.column_names[binding.column_index];
    if (!column_name.has_value()) {
        return false;
    }
    name = column_name.value();
    return true;
}

static bool TryResolveColumnSQL(const BigqueryAggregateSource &source, const ColumnBinding &binding, string &sql) {
    if (binding.table_index != source.table_index || binding.column_index >= source.column_sql.size()) {
        return false;
    }
    const auto &column_sql = source.column_sql[binding.column_index];
    if (!column_sql.has_value()) {
        return false;
    }
    sql = column_sql.value();
    return true;
}

static bool TryResolveSource(LogicalOperator &child, BigqueryAggregateSource &source);

static bool TryResolveFilter(LogicalFilter &filter, BigqueryAggregateSource &source) {
    if (filter.children.size() != 1) {
        return false;
    }
    if (!TryResolveSource(*filter.children[0], source)) {
        return false;
    }

    vector<string> filter_entries;
    filter_entries.reserve(filter.expressions.size());
    for (auto &expr : filter.expressions) {
        string filter_sql;
        if (!BigquerySQL::TryTransformBoundFilter(
                *expr,
                [&](const ColumnBinding &binding, string &column_name) -> bool {
                    return TryResolveColumnName(source, binding, column_name);
                },
                filter_sql)) {
            return false;
        }
        filter_entries.push_back(std::move(filter_sql));
    }
    if (filter_entries.empty()) {
        return false;
    }
    source.source_filter = ConjoinFilters(source.source_filter, StringUtil::Join(filter_entries, " AND "));
    return true;
}

static bool TryResolveProjection(LogicalProjection &projection, BigqueryAggregateSource &source) {
    if (projection.children.size() != 1) {
        return false;
    }
    if (!TryResolveSource(*projection.children[0], source)) {
        return false;
    }

    vector<std::optional<string>> projected_names;
    vector<std::optional<string>> projected_sql;
    projected_names.reserve(projection.expressions.size());
    projected_sql.reserve(projection.expressions.size());
    for (auto &expr : projection.expressions) {
        string column_name;
        if (expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
            !TryResolveColumnName(source, expr->Cast<BoundColumnRefExpression>().binding, column_name)) {
            projected_names.emplace_back();
        } else {
            projected_names.push_back(std::move(column_name));
        }

        string expression_sql;
        if (!BigquerySQL::TryTransformBoundScalarExpression(
                *expr,
                [&](const ColumnBinding &binding, string &sql) -> bool {
                    return TryResolveColumnSQL(source, binding, sql);
                },
                expression_sql)) {
            projected_sql.emplace_back();
        } else {
            projected_sql.push_back(std::move(expression_sql));
        }
    }
    source.table_index = projection.table_index;
    source.column_names = std::move(projected_names);
    source.column_sql = std::move(projected_sql);
    return true;
}

static bool TryResolveSource(LogicalOperator &child, BigqueryAggregateSource &source) {
    switch (child.type) {
    case LogicalOperatorType::LOGICAL_GET:
        return TryResolveGet(child.Cast<LogicalGet>(), source);
    case LogicalOperatorType::LOGICAL_FILTER:
        return TryResolveFilter(child.Cast<LogicalFilter>(), source);
    case LogicalOperatorType::LOGICAL_PROJECTION:
        return TryResolveProjection(child.Cast<LogicalProjection>(), source);
    default:
        return false;
    }
}

static bool TryColumnArgumentSQL(const BigqueryAggregateSource &source, Expression &expr, string &argument_sql) {
    if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
        return false;
    }
    auto &colref = expr.Cast<BoundColumnRefExpression>();
    string column_name;
    if (!TryResolveColumnName(source, colref.binding, column_name)) {
        return false;
    }
    argument_sql = BigqueryUtils::WriteQuotedIdentifier(column_name);
    return true;
}

static bool TryAggregateArgumentSQL(const BigqueryAggregateSource &source, Expression &expr, string &argument_sql) {
    return BigquerySQL::TryTransformBoundScalarExpression(
        expr,
        [&](const ColumnBinding &binding, string &sql) -> bool { return TryResolveColumnSQL(source, binding, sql); },
        argument_sql);
}

static bool IsDistinctCountArgumentType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
    case LogicalTypeId::UHUGEINT:
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::DECIMAL:
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::BLOB:
    case LogicalTypeId::DATE:
    case LogicalTypeId::TIME:
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_TZ:
    case LogicalTypeId::TIMESTAMP_SEC:
    case LogicalTypeId::TIMESTAMP_MS:
    case LogicalTypeId::TIMESTAMP_NS:
    case LogicalTypeId::INTERVAL:
        return true;
    default:
        return false;
    }
}

static bool TryAggregateSQL(const BigqueryAggregateSource &source,
                            const BoundAggregateExpression &aggregate,
                            string &aggregate_sql) {
    if (aggregate.filter || aggregate.order_bys) {
        return false;
    }
    const bool is_distinct = aggregate.aggr_type == AggregateType::DISTINCT;
    if (!is_distinct && aggregate.aggr_type != AggregateType::NON_DISTINCT) {
        return false;
    }
    if (!IsRestScalarType(aggregate.return_type)) {
        return false;
    }

    const auto function_name = StringUtil::Lower(aggregate.function.name);
    if (function_name == "count_star") {
        if (is_distinct || !aggregate.children.empty()) {
            return false;
        }
        aggregate_sql = "COUNT(*)";
        return true;
    }
    if (function_name == "count") {
        if (aggregate.children.empty()) {
            if (is_distinct) {
                return false;
            }
            aggregate_sql = "COUNT(*)";
            return true;
        }
        if (aggregate.children.size() != 1) {
            return false;
        }
        auto &argument = *aggregate.children[0];
        if (is_distinct) {
            if (!IsDistinctCountArgumentType(argument.return_type)) {
                return false;
            }
            string argument_sql;
            if (!TryColumnArgumentSQL(source, argument, argument_sql)) {
                return false;
            }
            aggregate_sql = "COUNT(DISTINCT " + argument_sql + ")";
            return true;
        }
        if (argument.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
            !argument.Cast<BoundConstantExpression>().value.IsNull()) {
            aggregate_sql = "COUNT(*)";
            return true;
        }
        string argument_sql;
        if (!TryAggregateArgumentSQL(source, argument, argument_sql)) {
            return false;
        }
        aggregate_sql = "COUNT(" + argument_sql + ")";
        return true;
    }
    if (is_distinct) {
        return false;
    }

    string bigquery_function;
    if (function_name == "sum") {
        bigquery_function = "SUM";
    } else if (function_name == "avg") {
        bigquery_function = "AVG";
    } else if (function_name == "min") {
        bigquery_function = "MIN";
    } else if (function_name == "max") {
        bigquery_function = "MAX";
    } else if (function_name == "bool_and") {
        bigquery_function = "LOGICAL_AND";
    } else if (function_name == "bool_or") {
        bigquery_function = "LOGICAL_OR";
    } else {
        return false;
    }

    if (aggregate.children.size() != 1) {
        return false;
    }
    string argument_sql;
    if (!TryAggregateArgumentSQL(source, *aggregate.children[0], argument_sql)) {
        return false;
    }
    aggregate_sql = bigquery_function + "(" + argument_sql + ")";
    return true;
}

static bool HasSingleCompleteGroupingSet(const LogicalAggregate &aggregate) {
    if (aggregate.groups.empty()) {
        return aggregate.grouping_sets.empty();
    }
    if (aggregate.grouping_sets.size() != 1 || aggregate.grouping_sets[0].size() != aggregate.groups.size()) {
        return false;
    }
    for (idx_t group_idx = 0; group_idx < aggregate.groups.size(); group_idx++) {
        if (aggregate.grouping_sets[0].find(group_idx) == aggregate.grouping_sets[0].end()) {
            return false;
        }
    }
    return true;
}

static bool TryBuildAggregateQuery(LogicalAggregate &aggregate,
                                   BigqueryAggregateSource &source,
                                   vector<string> &names,
                                   vector<LogicalType> &types,
                                   string &query) {
    if (!HasSingleCompleteGroupingSet(aggregate) || !aggregate.grouping_functions.empty()) {
        return false;
    }
    if (aggregate.children.size() != 1 || aggregate.expressions.empty()) {
        return false;
    }
    if (!TryResolveSource(*aggregate.children[0], source)) {
        return false;
    }

    vector<string> select_entries;
    vector<string> group_entries;
    select_entries.reserve(aggregate.groups.size() + aggregate.expressions.size());
    group_entries.reserve(aggregate.groups.size());
    names.reserve(aggregate.groups.size() + aggregate.expressions.size());
    types.reserve(aggregate.groups.size() + aggregate.expressions.size());
    for (idx_t group_idx = 0; group_idx < aggregate.groups.size(); group_idx++) {
        auto &group = aggregate.groups[group_idx];
        if (!IsRestScalarType(group->return_type)) {
            return false;
        }
        string group_sql;
        if (!TryColumnArgumentSQL(source, *group, group_sql)) {
            return false;
        }
        const auto internal_name = string(BQ_GROUP_COLUMN_PREFIX) + to_string(group_idx);
        select_entries.push_back(group_sql + " AS " + internal_name);
        group_entries.push_back(std::move(group_sql));
        names.push_back(internal_name);
        types.push_back(group->return_type);
    }
    for (idx_t aggregate_idx = 0; aggregate_idx < aggregate.expressions.size(); aggregate_idx++) {
        auto &expr = aggregate.expressions[aggregate_idx];
        if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
            return false;
        }
        auto &aggregate_expr = expr->Cast<BoundAggregateExpression>();
        string aggregate_sql;
        if (!TryAggregateSQL(source, aggregate_expr, aggregate_sql)) {
            return false;
        }
        const auto internal_name = string(BQ_AGGREGATE_COLUMN_PREFIX) + to_string(aggregate_idx);
        select_entries.push_back(aggregate_sql + " AS " + internal_name);
        names.push_back(expr->GetAlias().empty() ? internal_name : expr->GetAlias());
        types.push_back(expr->return_type);
    }

    query = "SELECT " + StringUtil::Join(select_entries, ", ") + " FROM " + source.source_sql;
    if (!source.source_filter.empty()) {
        query += " WHERE " + source.source_filter;
    }
    if (!group_entries.empty()) {
        query += " GROUP BY " + StringUtil::Join(group_entries, ", ");
    }
    return true;
}

static unique_ptr<LogicalOperator> CreateAggregateQueryGet(LogicalAggregate &aggregate,
                                                           BigqueryAggregateSource &source,
                                                           vector<string> names,
                                                           vector<LogicalType> types,
                                                           string query) {
    const bool grouped = !aggregate.groups.empty();
    const auto estimated_row_count =
        grouped && aggregate.has_estimated_cardinality ? aggregate.estimated_cardinality : 1;

    auto bind_data = make_uniq<BigqueryQueryRestBindData>();
    bind_data->config = source.config;
    bind_data->bq_client = source.bq_client;
    bind_data->query = std::move(query);
    bind_data->query_parameters = std::move(source.query_parameters);
    bind_data->timeout_ms = source.timeout_ms;
    bind_data->names = names;
    bind_data->types = types;
    bind_data->estimated_row_count = estimated_row_count;

    auto function = BigqueryQueryFunction();
    const auto table_index = grouped ? aggregate.group_index : aggregate.aggregate_index;
    auto result = make_uniq<LogicalGet>(table_index,
                                        std::move(function),
                                        std::move(bind_data),
                                        std::move(types),
                                        std::move(names));
    for (idx_t col_idx = 0; col_idx < result->returned_types.size(); col_idx++) {
        result->AddColumnId(col_idx);
    }
    result->estimated_cardinality = estimated_row_count;
    result->has_estimated_cardinality = true;
    return std::move(result);
}

static void RewriteAggregate(unique_ptr<LogicalOperator> &node, vector<ReplacementBinding> &binding_replacements) {
    if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        return;
    }

    auto &aggregate = node->Cast<LogicalAggregate>();
    BigqueryAggregateSource source;
    vector<string> names;
    vector<LogicalType> types;
    string query;
    if (!TryBuildAggregateQuery(aggregate, source, names, types, query)) {
        return;
    }

    const auto group_count = aggregate.groups.size();
    if (group_count > 0) {
        for (idx_t aggregate_idx = 0; aggregate_idx < aggregate.expressions.size(); aggregate_idx++) {
            binding_replacements.emplace_back(ColumnBinding(aggregate.aggregate_index, aggregate_idx),
                                              ColumnBinding(aggregate.group_index, group_count + aggregate_idx));
        }
    }
    node = CreateAggregateQueryGet(aggregate, source, std::move(names), std::move(types), std::move(query));
}

static void RewriteBigqueryAggregates(unique_ptr<LogicalOperator> &node,
                                      vector<ReplacementBinding> &binding_replacements) {
    if (!node || !BigquerySettings::EnableAggregatePushdown()) {
        return;
    }

    for (auto &child : node->children) {
        RewriteBigqueryAggregates(child, binding_replacements);
    }
    RewriteAggregate(node, binding_replacements);
}

} // namespace

void BigqueryOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    (void)input;
    vector<ReplacementBinding> binding_replacements;
    RewriteBigqueryAggregates(plan, binding_replacements);
    if (!binding_replacements.empty()) {
        ColumnBindingReplacer replacer;
        replacer.replacement_bindings = std::move(binding_replacements);
        replacer.VisitOperator(*plan);
    }
}

} // namespace bigquery
} // namespace duckdb
