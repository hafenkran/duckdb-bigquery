#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
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

#include <cmath>
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

static bool IsFilterLiteralType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::SQLNULL:
    case LogicalTypeId::BOOLEAN:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::DECIMAL:
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::DATE:
    case LogicalTypeId::TIME:
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::STRING_LITERAL:
    case LogicalTypeId::INTEGER_LITERAL:
        return true;
    default:
        return false;
    }
}

static bool TryTransformFilterLiteral(const Value &value, string &literal_sql) {
    if (!IsFilterLiteralType(value.type())) {
        return false;
    }
    if (value.IsNull()) {
        literal_sql = "NULL";
        return true;
    }
    if (BigqueryUtils::IsValueQuotable(value) || value.type().id() == LogicalTypeId::STRING_LITERAL) {
        literal_sql = KeywordHelper::WriteQuoted(value.ToString());
    } else {
        literal_sql = value.ToString();
    }
    return true;
}

static string TransformFilterLiteral(const Value &value) {
    string literal_sql;
    if (!TryTransformFilterLiteral(value, literal_sql)) {
        throw NotImplementedException("Unsupported BigQuery filter literal type");
    }
    return literal_sql;
}

static bool TryTransformComparisonOperator(ExpressionType type, string &op) {
    switch (type) {
    case ExpressionType::COMPARE_EQUAL:
        op = "=";
        return true;
    case ExpressionType::COMPARE_NOTEQUAL:
        op = "!=";
        return true;
    case ExpressionType::COMPARE_GREATERTHAN:
        op = ">";
        return true;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        op = ">=";
        return true;
    case ExpressionType::COMPARE_LESSTHAN:
        op = "<";
        return true;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        op = "<=";
        return true;
    default:
        return false;
    }
}

static string TransformComparisonOperator(ExpressionType type) {
    string op;
    if (!TryTransformComparisonOperator(type, op)) {
        throw NotImplementedException("Unsupported comparison type");
    }
    return op;
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
        return QuoteFilterColumnPath(column_path) + " " + TransformComparisonOperator(constant_filter.comparison_type) +
               " " + TransformFilterLiteral(constant_filter.constant);
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
            in_values.push_back(TransformFilterLiteral(value));
        }
        return QuoteFilterColumnPath(column_path) + " IN (" + StringUtil::Join(in_values, ", ") + ")";
    }
    default:
        throw InternalException("Unsupported filter type");
    }
}

static bool TryTransformConstantExpression(Expression &expr, string &literal_sql) {
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
        auto &cast = expr.Cast<BoundCastExpression>();
        if (cast.try_cast || cast.child->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
            !IsFilterLiteralType(cast.return_type)) {
            return false;
        }
        auto &constant = cast.child->Cast<BoundConstantExpression>();
        Value casted;
        if (!constant.value.DefaultTryCastAs(cast.return_type, casted, nullptr)) {
            return false;
        }
        return TryTransformFilterLiteral(casted, literal_sql);
    }
    if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
        return false;
    }
    auto &constant = expr.Cast<BoundConstantExpression>();
    return TryTransformFilterLiteral(constant.value, literal_sql);
}

static bool IsArithmeticScalarType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::DECIMAL:
    case LogicalTypeId::INTEGER_LITERAL:
        return true;
    default:
        return false;
    }
}

static bool IsModuloScalarType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::DECIMAL:
    case LogicalTypeId::INTEGER_LITERAL:
        return true;
    default:
        return false;
    }
}

static bool IsIntegralScalarType(const LogicalType &type) {
    switch (type.id()) {
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
    case LogicalTypeId::INTEGER_LITERAL:
        return true;
    default:
        return false;
    }
}

static bool IsFilterStringCastSourceType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::DECIMAL:
    case LogicalTypeId::BLOB:
    case LogicalTypeId::DATE:
    case LogicalTypeId::TIME:
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_SEC:
    case LogicalTypeId::TIMESTAMP_MS:
        return true;
    case LogicalTypeId::VARCHAR:
        return !BigqueryUtils::IsGeographyType(type);
    default:
        return false;
    }
}

static bool IsFilterNumericCastSourceType(const LogicalType &type) {
    if (type.id() == LogicalTypeId::BOOLEAN || IsArithmeticScalarType(type) || IsIntegralScalarType(type)) {
        return true;
    }
    if (type.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::STRING_LITERAL) {
        return true;
    }
    return type.id() == LogicalTypeId::VARCHAR && !BigqueryUtils::IsGeographyType(type);
}

static bool TryGetFilterNumericCastType(const LogicalType &type, string &cast_type) {
    switch (type.id()) {
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
        cast_type = "INT64";
        return true;
    case LogicalTypeId::UBIGINT:
        cast_type = "NUMERIC";
        return true;
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::UHUGEINT:
        cast_type = "BIGNUMERIC";
        return true;
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
        cast_type = "FLOAT64";
        return true;
    case LogicalTypeId::DECIMAL: {
        const auto width = DecimalType::GetWidth(type);
        const auto scale = DecimalType::GetScale(type);
        cast_type = width - scale <= 29 && scale <= 9 ? "NUMERIC" : "BIGNUMERIC";
        return true;
    }
    default:
        return false;
    }
}

static bool TryUnwrapIntegralFloatingCast(Expression &expr, Expression *&unwrapped) {
    if (expr.GetExpressionClass() != ExpressionClass::BOUND_CAST) {
        return false;
    }
    auto &cast = expr.Cast<BoundCastExpression>();
    if (cast.try_cast ||
        (cast.return_type.id() != LogicalTypeId::FLOAT && cast.return_type.id() != LogicalTypeId::DOUBLE) ||
        !IsIntegralScalarType(cast.child->return_type)) {
        return false;
    }
    unwrapped = cast.child.get();
    return true;
}

static bool TryTransformIntegralFloatingModuloConstant(Expression &expr, string &constant_sql) {
    if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
        return false;
    }
    auto &constant = expr.Cast<BoundConstantExpression>();
    if (constant.value.IsNull()) {
        return false;
    }
    if (IsIntegralScalarType(constant.value.type())) {
        return TryTransformConstantExpression(expr, constant_sql);
    }
    if (constant.value.type().id() != LogicalTypeId::FLOAT && constant.value.type().id() != LogicalTypeId::DOUBLE) {
        return false;
    }

    const auto value = constant.value.GetValue<double>();
    static constexpr double MAX_SAFE_INTEGER = 9007199254740992.0;
    if (!std::isfinite(value) || value != std::trunc(value) || value < -MAX_SAFE_INTEGER || value > MAX_SAFE_INTEGER) {
        return false;
    }
    constant_sql = std::to_string(static_cast<int64_t>(value));
    return true;
}

enum class ScalarExpressionContext : uint8_t { AGGREGATE, FILTER };

static bool TryTransformBoundScalarExpressionInternal(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    ScalarExpressionContext context,
    string &expression_sql) {
    switch (expr.GetExpressionClass()) {
    case ExpressionClass::BOUND_COLUMN_REF: {
        auto &colref = expr.Cast<BoundColumnRefExpression>();
        return column_sql_resolver(colref.binding, expression_sql);
    }
    case ExpressionClass::BOUND_CONSTANT:
        if (context == ScalarExpressionContext::AGGREGATE && !IsArithmeticScalarType(expr.return_type)) {
            return false;
        }
        return TryTransformConstantExpression(expr, expression_sql);
    case ExpressionClass::BOUND_CAST: {
        if (context == ScalarExpressionContext::AGGREGATE) {
            if (!IsArithmeticScalarType(expr.return_type)) {
                return false;
            }
            return TryTransformConstantExpression(expr, expression_sql);
        }
        auto &cast = expr.Cast<BoundCastExpression>();
        if (cast.try_cast) {
            return false;
        }
        string cast_type;
        if (cast.return_type.id() == LogicalTypeId::VARCHAR) {
            if (!IsFilterStringCastSourceType(cast.child->return_type)) {
                return false;
            }
            cast_type = "STRING";
        } else if (!TryGetFilterNumericCastType(cast.return_type, cast_type) ||
                   !IsFilterNumericCastSourceType(cast.child->return_type)) {
            if (TryTransformConstantExpression(expr, expression_sql)) {
                return true;
            }
            return false;
        }
        string child_sql;
        if (!TryTransformBoundScalarExpressionInternal(*cast.child,
                                                       column_sql_resolver,
                                                       integral_floating_sql_resolver,
                                                       ScalarExpressionContext::FILTER,
                                                       child_sql)) {
            return false;
        }
        if (cast.child->return_type.id() == LogicalTypeId::BOOLEAN && cast_type != "INT64") {
            child_sql = "CAST(" + child_sql + " AS INT64)";
        }
        expression_sql = "CAST(" + child_sql + " AS " + cast_type + ")";
        return true;
    }
    case ExpressionClass::BOUND_FUNCTION: {
        auto &function = expr.Cast<BoundFunctionExpression>();
        const auto function_name = StringUtil::Lower(function.function.name);
        const bool lower =
            context == ScalarExpressionContext::FILTER && function_name == "lower" && function.children.size() == 1;
        if (lower) {
            if (function.return_type.id() != LogicalTypeId::VARCHAR ||
                function.children[0]->return_type.id() != LogicalTypeId::VARCHAR) {
                return false;
            }
            string child_sql;
            if (!TryTransformBoundScalarExpressionInternal(*function.children[0],
                                                           column_sql_resolver,
                                                           integral_floating_sql_resolver,
                                                           ScalarExpressionContext::FILTER,
                                                           child_sql)) {
                return false;
            }
            expression_sql = "LOWER(" + child_sql + ")";
            return true;
        }
        const bool unary_minus = function_name == "-" && function.children.size() == 1;
        const bool basic_binary_operator =
            (function_name == "+" || function_name == "-" || function_name == "*") && function.children.size() == 2;
        const bool division = function_name == "/" && function.children.size() == 2;
        const bool modulo = function_name == "%" && function.children.size() == 2;
        bool integral_floating_modulo = false;
        vector<string> child_entries;
        if (modulo && context == ScalarExpressionContext::FILTER &&
            (function.return_type.id() == LogicalTypeId::FLOAT || function.return_type.id() == LogicalTypeId::DOUBLE)) {
            for (auto &child : function.children) {
                Expression *unwrapped = nullptr;
                string child_sql;
                if (TryUnwrapIntegralFloatingCast(*child, unwrapped)) {
                    if (!TryTransformBoundScalarExpressionInternal(*unwrapped,
                                                                   column_sql_resolver,
                                                                   integral_floating_sql_resolver,
                                                                   context,
                                                                   child_sql)) {
                        break;
                    }
                } else if (child->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
                           integral_floating_sql_resolver) {
                    auto &colref = child->Cast<BoundColumnRefExpression>();
                    if (!integral_floating_sql_resolver(colref.binding, child_sql)) {
                        break;
                    }
                } else if (TryTransformIntegralFloatingModuloConstant(*child, child_sql)) {
                    // DuckDB binds e.g. CAST(i AS DOUBLE) % 2 as a DOUBLE modulo with a 2.0 literal.
                    // BigQuery MOD does not accept FLOAT64, so only integer-valued FLOAT/DOUBLE constants are
                    // converted back to integer literals for this integer-origin modulo path.
                } else {
                    break;
                }
                child_entries.push_back(std::move(child_sql));
            }
            integral_floating_modulo = child_entries.size() == function.children.size();
        }
        if ((!unary_minus && !basic_binary_operator && !division && !modulo) ||
            !IsArithmeticScalarType(function.return_type) ||
            (modulo && !IsModuloScalarType(function.return_type) && !integral_floating_modulo)) {
            return false;
        }

        if (!integral_floating_modulo) {
            child_entries.clear();
            child_entries.reserve(function.children.size());
            for (auto &child : function.children) {
                auto child_expr = child.get();
                if (division && child_expr->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
                    auto &cast = child_expr->Cast<BoundCastExpression>();
                    if (!cast.try_cast &&
                        (cast.return_type.id() == LogicalTypeId::FLOAT ||
                         cast.return_type.id() == LogicalTypeId::DOUBLE) &&
                        IsArithmeticScalarType(cast.child->return_type)) {
                        child_expr = cast.child.get();
                    }
                }
                if (!IsArithmeticScalarType(child_expr->return_type) ||
                    (modulo && !IsModuloScalarType(child_expr->return_type))) {
                    return false;
                }
                string child_sql;
                if (!TryTransformBoundScalarExpressionInternal(*child_expr,
                                                               column_sql_resolver,
                                                               integral_floating_sql_resolver,
                                                               context,
                                                               child_sql)) {
                    return false;
                }
                child_entries.push_back(std::move(child_sql));
            }
        }
        if (unary_minus) {
            expression_sql = "(-" + child_entries[0] + ")";
        } else if (division) {
            expression_sql = "IEEE_DIVIDE(" + child_entries[0] + ", " + child_entries[1] + ")";
        } else if (modulo) {
            expression_sql = "(CASE WHEN " + child_entries[1] + " = 0 THEN NULL ELSE MOD(" + child_entries[0] + ", " +
                             child_entries[1] + ") END)";
            if (integral_floating_modulo) {
                expression_sql = "CAST(" + expression_sql + " AS FLOAT64)";
            }
        } else {
            expression_sql = "(" + child_entries[0] + " " + function_name + " " + child_entries[1] + ")";
        }
        return true;
    }
    default:
        return false;
    }
}

static bool TryTransformBoundFilterExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql);

static bool TryTransformComparisonFilter(
    BoundComparisonExpression &comparison,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql) {
    string op;
    if (!TryTransformComparisonOperator(comparison.GetExpressionType(), op)) {
        return false;
    }

    string left_sql;
    string right_sql;
    if (!TryTransformBoundScalarExpressionInternal(*comparison.left,
                                                   column_sql_resolver,
                                                   integral_floating_sql_resolver,
                                                   ScalarExpressionContext::FILTER,
                                                   left_sql) ||
        !TryTransformBoundScalarExpressionInternal(*comparison.right,
                                                   column_sql_resolver,
                                                   integral_floating_sql_resolver,
                                                   ScalarExpressionContext::FILTER,
                                                   right_sql)) {
        return false;
    }
    filter_sql = "(" + left_sql + " " + op + " " + right_sql + ")";
    return true;
}

static bool TryTransformConjunctionFilter(
    BoundConjunctionExpression &conjunction,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql) {
    string op;
    switch (conjunction.GetExpressionType()) {
    case ExpressionType::CONJUNCTION_AND:
        op = "AND";
        break;
    case ExpressionType::CONJUNCTION_OR:
        op = "OR";
        break;
    default:
        return false;
    }

    vector<string> entries;
    entries.reserve(conjunction.children.size());
    for (auto &child : conjunction.children) {
        string child_sql;
        if (!TryTransformBoundFilterExpression(*child,
                                               column_sql_resolver,
                                               integral_floating_sql_resolver,
                                               child_sql)) {
            return false;
        }
        entries.push_back(std::move(child_sql));
    }
    if (entries.empty()) {
        return false;
    }
    filter_sql = "(" + StringUtil::Join(entries, " " + op + " ") + ")";
    return true;
}

static bool TryTransformInFilter(
    BoundOperatorExpression &op,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql) {
    if (op.children.size() < 2) {
        return false;
    }

    string expression_sql;
    if (!TryTransformBoundScalarExpressionInternal(*op.children[0],
                                                   column_sql_resolver,
                                                   integral_floating_sql_resolver,
                                                   ScalarExpressionContext::FILTER,
                                                   expression_sql)) {
        return false;
    }

    vector<string> literals;
    literals.reserve(op.children.size() - 1);
    for (idx_t child_idx = 1; child_idx < op.children.size(); child_idx++) {
        string literal_sql;
        if (!TryTransformConstantExpression(*op.children[child_idx], literal_sql) || literal_sql == "NULL") {
            return false;
        }
        literals.push_back(std::move(literal_sql));
    }
    filter_sql = expression_sql + " IN (" + StringUtil::Join(literals, ", ") + ")";
    return true;
}

static bool TryTransformOperatorFilter(
    BoundOperatorExpression &op,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql) {
    switch (op.GetExpressionType()) {
    case ExpressionType::OPERATOR_IS_NULL:
    case ExpressionType::OPERATOR_IS_NOT_NULL: {
        if (op.children.size() != 1) {
            return false;
        }
        string expression_sql;
        if (!TryTransformBoundScalarExpressionInternal(*op.children[0],
                                                       column_sql_resolver,
                                                       integral_floating_sql_resolver,
                                                       ScalarExpressionContext::FILTER,
                                                       expression_sql)) {
            return false;
        }
        const auto operator_sql =
            op.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ? "IS NULL" : "IS NOT NULL";
        filter_sql = expression_sql + " " + operator_sql;
        return true;
    }
    case ExpressionType::COMPARE_IN:
        return TryTransformInFilter(op, column_sql_resolver, integral_floating_sql_resolver, filter_sql);
    default:
        return false;
    }
}

static bool TryTransformBoundFilterExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql) {
    switch (expr.GetExpressionClass()) {
    case ExpressionClass::BOUND_COMPARISON:
        return TryTransformComparisonFilter(expr.Cast<BoundComparisonExpression>(),
                                            column_sql_resolver,
                                            integral_floating_sql_resolver,
                                            filter_sql);
    case ExpressionClass::BOUND_CONJUNCTION:
        return TryTransformConjunctionFilter(expr.Cast<BoundConjunctionExpression>(),
                                             column_sql_resolver,
                                             integral_floating_sql_resolver,
                                             filter_sql);
    case ExpressionClass::BOUND_OPERATOR:
        return TryTransformOperatorFilter(expr.Cast<BoundOperatorExpression>(),
                                          column_sql_resolver,
                                          integral_floating_sql_resolver,
                                          filter_sql);
    default:
        return false;
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

bool BigquerySQL::TryTransformBoundFilter(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql) {
    return TryTransformBoundFilterExpression(expr, column_sql_resolver, integral_floating_sql_resolver, filter_sql);
}

bool BigquerySQL::TryTransformBoundScalarExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    string &expression_sql) {
    return TryTransformBoundScalarExpressionInternal(expr,
                                                     column_sql_resolver,
                                                     {},
                                                     ScalarExpressionContext::AGGREGATE,
                                                     expression_sql);
}

bool BigquerySQL::TryTransformBoundFilterScalarExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &expression_sql) {
    return TryTransformBoundScalarExpressionInternal(expr,
                                                     column_sql_resolver,
                                                     integral_floating_sql_resolver,
                                                     ScalarExpressionContext::FILTER,
                                                     expression_sql);
}

bool BigquerySQL::TryTransformBoundIntegralFloatingExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &expression_sql) {
    Expression *unwrapped = nullptr;
    if (TryUnwrapIntegralFloatingCast(expr, unwrapped)) {
        return TryTransformBoundScalarExpressionInternal(*unwrapped,
                                                         column_sql_resolver,
                                                         integral_floating_sql_resolver,
                                                         ScalarExpressionContext::FILTER,
                                                         expression_sql);
    }
    if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF || !integral_floating_sql_resolver) {
        return false;
    }
    auto &colref = expr.Cast<BoundColumnRefExpression>();
    return integral_floating_sql_resolver(colref.binding, expression_sql);
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
