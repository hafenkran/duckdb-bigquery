#include "storage/bigquery_optimizer.hpp"

#include "bigquery_query.hpp"
#include "bigquery_scan.hpp"
#include "bigquery_settings.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
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

struct BigqueryAggregateSource {
    idx_t table_index = DConstants::INVALID_INDEX;
    vector<std::optional<string>> column_names;

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
    for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
        string column_name;
        if (TryGetScanBindingColumnName(get, col_idx, column_name)) {
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
    projected_names.reserve(projection.expressions.size());
    for (auto &expr : projection.expressions) {
        if (expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
            projected_names.emplace_back();
            continue;
        }
        auto &colref = expr->Cast<BoundColumnRefExpression>();
        string column_name;
        if (!TryResolveColumnName(source, colref.binding, column_name)) {
            projected_names.emplace_back();
            continue;
        }
        projected_names.push_back(std::move(column_name));
    }
    source.table_index = projection.table_index;
    source.column_names = std::move(projected_names);
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

static bool TryAggregateSQL(const BigqueryAggregateSource &source,
                            const BoundAggregateExpression &aggregate,
                            string &aggregate_sql) {
    if (aggregate.aggr_type != AggregateType::NON_DISTINCT || aggregate.filter || aggregate.order_bys) {
        return false;
    }
    if (!IsRestScalarType(aggregate.return_type)) {
        return false;
    }

    const auto function_name = StringUtil::Lower(aggregate.function.name);
    if (function_name == "count_star") {
        if (!aggregate.children.empty()) {
            return false;
        }
        aggregate_sql = "COUNT(*)";
        return true;
    }
    if (function_name == "count") {
        if (aggregate.children.empty()) {
            aggregate_sql = "COUNT(*)";
            return true;
        }
        if (aggregate.children.size() != 1) {
            return false;
        }
        auto &argument = *aggregate.children[0];
        if (argument.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
            !argument.Cast<BoundConstantExpression>().value.IsNull()) {
            aggregate_sql = "COUNT(*)";
            return true;
        }
        string argument_sql;
        if (!TryColumnArgumentSQL(source, argument, argument_sql)) {
            return false;
        }
        aggregate_sql = "COUNT(" + argument_sql + ")";
        return true;
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
    if (!TryColumnArgumentSQL(source, *aggregate.children[0], argument_sql)) {
        return false;
    }
    aggregate_sql = bigquery_function + "(" + argument_sql + ")";
    return true;
}

static bool TryBuildAggregateQuery(LogicalAggregate &aggregate,
                                   BigqueryAggregateSource &source,
                                   vector<string> &names,
                                   vector<LogicalType> &types,
                                   string &query) {
    if (!aggregate.groups.empty() || !aggregate.grouping_sets.empty() || !aggregate.grouping_functions.empty()) {
        return false;
    }
    if (aggregate.children.size() != 1 || aggregate.expressions.empty()) {
        return false;
    }
    if (!TryResolveSource(*aggregate.children[0], source)) {
        return false;
    }

    vector<string> select_entries;
    select_entries.reserve(aggregate.expressions.size());
    names.reserve(aggregate.expressions.size());
    types.reserve(aggregate.expressions.size());
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
    return true;
}

static unique_ptr<LogicalOperator> CreateAggregateQueryGet(LogicalAggregate &aggregate,
                                                           BigqueryAggregateSource &source,
                                                           vector<string> names,
                                                           vector<LogicalType> types,
                                                           string query) {
    auto bind_data = make_uniq<BigqueryQueryRestBindData>();
    bind_data->config = source.config;
    bind_data->bq_client = source.bq_client;
    bind_data->query = std::move(query);
    bind_data->query_parameters = std::move(source.query_parameters);
    bind_data->timeout_ms = source.timeout_ms;
    bind_data->names = names;
    bind_data->types = types;
    bind_data->estimated_row_count = 1;

    auto function = BigqueryQueryFunction();
    auto result = make_uniq<LogicalGet>(aggregate.aggregate_index,
                                        std::move(function),
                                        std::move(bind_data),
                                        std::move(types),
                                        std::move(names));
    for (idx_t col_idx = 0; col_idx < result->returned_types.size(); col_idx++) {
        result->AddColumnId(col_idx);
    }
    result->estimated_cardinality = 1;
    result->has_estimated_cardinality = true;
    return std::move(result);
}

static void RewriteAggregate(unique_ptr<LogicalOperator> &node) {
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

    node = CreateAggregateQueryGet(aggregate, source, std::move(names), std::move(types), std::move(query));
}

static void RewriteBigqueryAggregates(unique_ptr<LogicalOperator> &node) {
    if (!node || !BigquerySettings::EnableAggregatePushdown()) {
        return;
    }

    for (auto &child : node->children) {
        RewriteBigqueryAggregates(child);
    }
    RewriteAggregate(node);
}

} // namespace

void BigqueryOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    (void)input;
    RewriteBigqueryAggregates(plan);
}

} // namespace bigquery
} // namespace duckdb
