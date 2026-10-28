#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
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

#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"

#include "bigquery_info.hpp"
#include "bigquery_sql.hpp"
#include "bigquery_utils.hpp"

#include <cctype>
#include <cmath>

namespace duckdb {
namespace bigquery {

static ExpressionClass ClassifySQLExpression(const Expression &expr) {
    if (BoundCastExpression::IsCast(expr)) {
        return ExpressionClass::LEGACY_BOUND_CAST;
    }
    if (BoundComparisonExpression::IsComparison(expr)) {
        return ExpressionClass::LEGACY_BOUND_COMPARISON;
    }
    return expr.GetExpressionClass();
}

namespace {

static string QuoteFilterColumnPath(const vector<string> &column_path) {
    vector<string> quoted_path;
    quoted_path.reserve(column_path.size());
    for (auto &entry : column_path) {
        quoted_path.push_back(BigqueryUtils::WriteQuotedIdentifier(entry));
    }
    return StringUtil::Join(quoted_path, ".");
}

static string WriteBigQueryStringLiteral(const string &value) {
    string escaped;
    escaped.reserve(value.size() + 2);
    escaped.push_back('"');
    for (const auto c : value) {
        if (c == '\\' || c == '"') {
            escaped.push_back('\\');
        }
        escaped.push_back(c);
    }
    escaped.push_back('"');
    return escaped;
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
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::UHUGEINT:
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
    switch (value.type().id()) {
    case LogicalTypeId::DATE:
        literal_sql = "DATE " + SQLString::ToString(value.ToString());
        return true;
    case LogicalTypeId::TIME:
        literal_sql = "TIME " + SQLString::ToString(value.ToString());
        return true;
    case LogicalTypeId::TIMESTAMP:
        // Both BigQuery DATETIME and TIMESTAMP map to DuckDB TIMESTAMP. A canonical string literal
        // lets BigQuery coerce to the source column's temporal type without assuming either one.
        literal_sql = SQLString::ToString(value.ToString());
        return true;
    default:
        break;
    }
    if (value.type().id() == LogicalTypeId::VARCHAR || value.type().id() == LogicalTypeId::STRING_LITERAL) {
        literal_sql = WriteBigQueryStringLiteral(value.ToString());
    } else if (BigqueryUtils::IsValueQuotable(value)) {
        literal_sql = SQLString::ToString(value.ToString());
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

// A null result imposes no constraint. Dropping an optional child widens OR to TRUE but preserves the other AND children.
static unique_ptr<Expression> RemoveOptionalFilterExpressions(unique_ptr<Expression> expression) {
    if (ExpressionFilter::IsOptionalExpression(*expression)) {
        return nullptr;
    }
    if (expression->GetExpressionClass() != ExpressionClass::BOUND_CONJUNCTION) {
        return expression;
    }

    auto &conjunction = expression->Cast<BoundConjunctionExpression>();
    const bool is_or = conjunction.GetExpressionType() == ExpressionType::CONJUNCTION_OR;
    vector<unique_ptr<Expression>> required_children;
    for (auto &child : conjunction.GetChildrenMutable()) {
        auto required_child = RemoveOptionalFilterExpressions(std::move(child));
        if (!required_child) {
            if (is_or) {
                return nullptr;
            }
            continue;
        }
        required_children.push_back(std::move(required_child));
    }
    if (required_children.empty()) {
        return nullptr;
    }
    if (required_children.size() == 1) {
        return std::move(required_children[0]);
    }
    conjunction.GetChildrenMutable() = std::move(required_children);
    return expression;
}

// An empty result means the filter imposes no constraint (e.g. an untranslatable optional filter).
static string CreateFilterExpression(const vector<string> &column_path,
                                     const vector<unique_ptr<TableFilter>> &filters,
                                     const string &op) {
    vector<string> filter_entries;
    for (auto &filter : filters) {
        auto entry = TransformFilterPath(column_path, *filter);
        if (entry.empty()) {
            if (op == "OR") {
                return string();
            }
            continue;
        }
        filter_entries.push_back(std::move(entry));
    }
    if (filter_entries.empty()) {
        return string();
    }
    return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

static string TransformFilterPath(const vector<string> &column_path, const TableFilter &filter) {
    switch (filter.filter_type) {
    case TableFilterType::EXPRESSION_FILTER: {
        if (ExpressionFilter::IsOptionalFilter(filter)) {
            return string();
        }
        const auto &expression_filter = filter.Cast<ExpressionFilter>();
        LogicalType column_type = LogicalType::ANY;
        ExpressionIterator::VisitExpression<BoundReferenceExpression>(
            *expression_filter.expr,
            [&](const BoundReferenceExpression &expr) { column_type = expr.GetReturnType(); });
        BoundColumnRefExpression column(column_type, ColumnBinding(TableIndex(0), ProjectionIndex(0)));
        auto expression = RemoveOptionalFilterExpressions(filter.ToExpression(column));
        if (!expression) {
            return string();
        }
        string sql;
        if (!BigquerySQL::TryTransformBoundFilter(
                *expression,
                [&](const ColumnBinding &, string &result) {
                    result = QuoteFilterColumnPath(column_path);
                    return true;
                },
                {},
                sql)) {
            throw NotImplementedException("Unsupported BigQuery filter expression: %s", expression->ToString());
        }
        return sql;
    }
    case TableFilterType::LEGACY_CONSTANT_COMPARISON: {
        auto &constant_filter = filter.Cast<LegacyConstantFilter>();
        return QuoteFilterColumnPath(column_path) + " " + TransformComparisonOperator(constant_filter.comparison_type) +
               " " + TransformFilterLiteral(constant_filter.constant);
    }
    case TableFilterType::LEGACY_IS_NULL:
        return QuoteFilterColumnPath(column_path) + " IS NULL";
    case TableFilterType::LEGACY_IS_NOT_NULL:
        return QuoteFilterColumnPath(column_path) + " IS NOT NULL";
    case TableFilterType::LEGACY_CONJUNCTION_AND:
    case TableFilterType::LEGACY_CONJUNCTION_OR: {
        auto &conjunction_filter = dynamic_cast<const LegacyConjunctionFilter &>(filter);
        string op = filter.filter_type == TableFilterType::LEGACY_CONJUNCTION_AND ? "AND" : "OR";
        return CreateFilterExpression(column_path, conjunction_filter.child_filters, op);
    }
    case TableFilterType::LEGACY_STRUCT_EXTRACT: {
        auto &struct_filter = filter.Cast<LegacyStructFilter>();
        auto child_path = column_path;
        child_path.push_back(struct_filter.child_name.GetIdentifierName());
        return TransformFilterPath(child_path, *struct_filter.child_filter);
    }
    case TableFilterType::LEGACY_OPTIONAL_FILTER: {
        // Optional filters (Top-N dynamic filters, join bloom/min-max filters, ...) are not required for
        // correctness, so drop them when they cannot be expressed in BigQuery SQL.
        auto &optional_filter = filter.Cast<LegacyOptionalFilter>();
        if (!optional_filter.child_filter) {
            return string();
        }
        try {
            return TransformFilterPath(column_path, *optional_filter.child_filter);
        } catch (Exception &) {
            return string();
        }
    }
    case TableFilterType::LEGACY_IN_FILTER: {
        auto &in_filter = filter.Cast<LegacyInFilter>();
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
    if (ClassifySQLExpression(expr) == ExpressionClass::LEGACY_BOUND_CAST) {
        auto &cast = expr.Cast<BoundFunctionExpression>();
        if (BoundCastExpression::IsTryCast(cast) ||
            ClassifySQLExpression(*BoundCastExpression::ChildMutable(cast)) != ExpressionClass::BOUND_CONSTANT ||
            !IsFilterLiteralType(cast.GetReturnType())) {
            return false;
        }
        auto &constant = BoundCastExpression::ChildMutable(cast)->Cast<BoundConstantExpression>();
        auto casted = constant.GetValue().DefaultTryCastAs(cast.GetReturnType());
        if (!casted) {
            return false;
        }
        return TryTransformFilterLiteral(*casted, literal_sql);
    }
    if (ClassifySQLExpression(expr) != ExpressionClass::BOUND_CONSTANT) {
        return false;
    }
    auto &constant = expr.Cast<BoundConstantExpression>();
    return TryTransformFilterLiteral(constant.GetValue(), literal_sql);
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

static bool IsFilterTemporalCastSourceType(const LogicalType &type) {
    if (type.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::STRING_LITERAL) {
        return true;
    }
    return type.id() == LogicalTypeId::VARCHAR && !BigqueryUtils::IsGeographyType(type);
}

static bool TryGetFilterTemporalCastType(const LogicalType &type, string &cast_type) {
    switch (type.id()) {
    case LogicalTypeId::DATE:
        cast_type = "DATE";
        return true;
    case LogicalTypeId::TIME:
        cast_type = "TIME";
        return true;
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_SEC:
    case LogicalTypeId::TIMESTAMP_MS:
        cast_type = "TIMESTAMP";
        return true;
    default:
        return false;
    }
}

static bool TryUnwrapIntegralFloatingCast(Expression &expr, Expression *&unwrapped) {
    if (ClassifySQLExpression(expr) != ExpressionClass::LEGACY_BOUND_CAST) {
        return false;
    }
    auto &cast = expr.Cast<BoundFunctionExpression>();
    if (BoundCastExpression::IsTryCast(cast) ||
        (cast.GetReturnType().id() != LogicalTypeId::FLOAT && cast.GetReturnType().id() != LogicalTypeId::DOUBLE) ||
        !IsIntegralScalarType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
        return false;
    }
    unwrapped = BoundCastExpression::ChildMutable(cast).get();
    return true;
}

static bool TryTransformIntegralFloatingModuloConstant(Expression &expr, string &constant_sql) {
    if (ClassifySQLExpression(expr) != ExpressionClass::BOUND_CONSTANT) {
        return false;
    }
    auto &constant = expr.Cast<BoundConstantExpression>();
    if (constant.GetValue().IsNull()) {
        return false;
    }
    if (IsIntegralScalarType(constant.GetValue().type())) {
        return TryTransformConstantExpression(expr, constant_sql);
    }
    if (constant.GetValue().type().id() != LogicalTypeId::FLOAT &&
        constant.GetValue().type().id() != LogicalTypeId::DOUBLE) {
        return false;
    }

    const auto value = constant.GetValue().GetValue<double>();
    static constexpr double MAX_SAFE_INTEGER = 9007199254740992.0;
    if (!std::isfinite(value) || value != std::trunc(value) || value < -MAX_SAFE_INTEGER || value > MAX_SAFE_INTEGER) {
        return false;
    }
    constant_sql = std::to_string(static_cast<int64_t>(value));
    return true;
}

static bool IsDirectGroupColumnSQL(const string &sql) {
    // Direct source columns are stored as quoted identifiers. Derived fragments include functions/operators.
    return sql.size() >= 2 && sql.front() == '`' && sql.back() == '`';
}

// TODO(group-by-abs): Keep GROUP_BY as a separate scalar-expression context that stays narrower than FILTER.
enum class ScalarExpressionContext : uint8_t { AGGREGATE, FILTER, GROUP_BY, AGGREGATE_TRY_TEMPORAL_CAST_CHILD };

static bool TryTransformBoundScalarExpressionInternal(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    ScalarExpressionContext context,
    string &expression_sql);

static bool TryTransformFilterListStringExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    ScalarExpressionContext context,
    string &expression_sql);

static bool TryTransformFilterStructStringExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    ScalarExpressionContext context,
    string &expression_sql);

static string FloatingModuloSQL(const string &left_sql, const string &right_sql);

static bool TryTransformGroupFloatingModuloOperand(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    string &operand_sql);

static bool IsFilterListStringElementType(const LogicalType &type) {
    if (type.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::STRING_LITERAL) {
        return true;
    }
    return IsFilterStringCastSourceType(type);
}

static bool TryTransformFilterListConstructorChildren(Expression &expr,
                                                      const vector<unique_ptr<Expression>> *&children) {
    if (ClassifySQLExpression(expr) == ExpressionClass::BOUND_OPERATOR) {
        auto &op = expr.Cast<BoundOperatorExpression>();
        if (op.GetExpressionType() != ExpressionType::ARRAY_CONSTRUCTOR) {
            return false;
        }
        children = &op.GetChildren();
        return true;
    }
    if (ClassifySQLExpression(expr) != ExpressionClass::BOUND_FUNCTION) {
        return false;
    }

    auto &function = expr.Cast<BoundFunctionExpression>();
    const auto function_name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
    if (function_name != "list_value" && function_name != "list_pack") {
        return false;
    }
    children = &function.GetChildren();
    return true;
}

static bool TryTransformGroupTemporalCastChild(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    string &expression_sql) {
    if (ClassifySQLExpression(expr) == ExpressionClass::BOUND_COLUMN_REF) {
        if (!IsFilterTemporalCastSourceType(expr.GetReturnType())) {
            return false;
        }
        auto &colref = expr.Cast<BoundColumnRefExpression>();
        return column_sql_resolver(colref.Binding(), expression_sql);
    }
    if (ClassifySQLExpression(expr) != ExpressionClass::BOUND_FUNCTION) {
        return false;
    }

    auto &function = expr.Cast<BoundFunctionExpression>();
    const auto function_name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
    if (function_name != "lower" || function.GetChildren().size() != 1 ||
        function.GetReturnType().id() != LogicalTypeId::VARCHAR ||
        function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
        return false;
    }

    string child_sql;
    if (!TryTransformGroupTemporalCastChild(*function.GetChildren()[0], column_sql_resolver, child_sql)) {
        return false;
    }
    expression_sql = "LOWER(" + child_sql + ")";
    return true;
}

static bool TryTransformGroupArithmeticExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    string &expression_sql) {
    switch (ClassifySQLExpression(expr)) {
    case ExpressionClass::BOUND_COLUMN_REF: {
        if (!IsArithmeticScalarType(expr.GetReturnType())) {
            return false;
        }
        auto &colref = expr.Cast<BoundColumnRefExpression>();
        return column_sql_resolver(colref.Binding(), expression_sql);
    }
    case ExpressionClass::BOUND_CONSTANT:
        if (!IsArithmeticScalarType(expr.GetReturnType()) || !TryTransformConstantExpression(expr, expression_sql) ||
            expression_sql == "NULL") {
            return false;
        }
        return true;
    case ExpressionClass::BOUND_FUNCTION: {
        auto &function = expr.Cast<BoundFunctionExpression>();
        const auto function_name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
        if (function_name == "abs" && function.GetChildren().size() == 1) {
            if (!IsArithmeticScalarType(function.GetReturnType()) ||
                !IsArithmeticScalarType(function.GetChildren()[0]->GetReturnType()) ||
                !TryTransformGroupArithmeticExpression(*function.GetChildren()[0],
                                                       column_sql_resolver,
                                                       expression_sql)) {
                return false;
            }
            expression_sql = "ABS(" + expression_sql + ")";
            return true;
        }

        const bool unary_minus = function_name == "-" && function.GetChildren().size() == 1;
        const bool basic_binary_operator = (function_name == "+" || function_name == "-" || function_name == "*") &&
                                           function.GetChildren().size() == 2;
        const bool division = function_name == "/" && function.GetChildren().size() == 2;
        const bool modulo = function_name == "%" && function.GetChildren().size() == 2;
        const bool floating_modulo = modulo && (function.GetReturnType().id() == LogicalTypeId::FLOAT ||
                                                function.GetReturnType().id() == LogicalTypeId::DOUBLE);
        if ((!unary_minus && !basic_binary_operator && !division && !modulo) ||
            !IsArithmeticScalarType(function.GetReturnType()) ||
            (modulo && !IsModuloScalarType(function.GetReturnType()) && !floating_modulo)) {
            return false;
        }

        vector<string> child_entries;
        child_entries.reserve(function.GetChildren().size());
        if (floating_modulo) {
            for (auto &child : function.GetChildren()) {
                string child_sql;
                if (!TryTransformGroupFloatingModuloOperand(*child, column_sql_resolver, child_sql)) {
                    return false;
                }
                child_entries.push_back(std::move(child_sql));
            }
        } else {
            for (auto &child : function.GetChildren()) {
                auto child_expr = child.get();
                if (division && ClassifySQLExpression(*child_expr) == ExpressionClass::LEGACY_BOUND_CAST) {
                    auto &cast = child_expr->Cast<BoundFunctionExpression>();
                    if (!BoundCastExpression::IsTryCast(cast) &&
                        (cast.GetReturnType().id() == LogicalTypeId::FLOAT ||
                         cast.GetReturnType().id() == LogicalTypeId::DOUBLE) &&
                        IsArithmeticScalarType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                        child_expr = BoundCastExpression::ChildMutable(cast).get();
                    }
                }
                if (!IsArithmeticScalarType(child_expr->GetReturnType()) ||
                    (modulo && !IsModuloScalarType(child_expr->GetReturnType()))) {
                    return false;
                }
                string child_sql;
                if (!TryTransformGroupArithmeticExpression(*child_expr, column_sql_resolver, child_sql)) {
                    return false;
                }
                child_entries.push_back(std::move(child_sql));
            }
        }
        if (unary_minus) {
            expression_sql = "(-" + child_entries[0] + ")";
        } else if (division) {
            expression_sql = "IEEE_DIVIDE(" + child_entries[0] + ", " + child_entries[1] + ")";
        } else if (floating_modulo) {
            expression_sql = FloatingModuloSQL(child_entries[0], child_entries[1]);
        } else if (modulo) {
            expression_sql = "(CASE WHEN " + child_entries[1] + " = 0 THEN NULL ELSE MOD(" + child_entries[0] + ", " +
                             child_entries[1] + ") END)";
        } else {
            expression_sql = "(" + child_entries[0] + " " + function_name + " " + child_entries[1] + ")";
        }
        return true;
    }
    default:
        return false;
    }
}

static bool TryTransformGroupFunctionExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    string &expression_sql) {
    if (ClassifySQLExpression(expr) != ExpressionClass::BOUND_FUNCTION) {
        return false;
    }

    auto &function = expr.Cast<BoundFunctionExpression>();
    const auto function_name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
    if (function_name == "lower" && function.GetChildren().size() == 1) {
        if (function.GetReturnType().id() != LogicalTypeId::VARCHAR ||
            function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
            return false;
        }

        string child_sql;
        if (!TryTransformBoundScalarExpressionInternal(*function.GetChildren()[0],
                                                       column_sql_resolver,
                                                       {},
                                                       ScalarExpressionContext::GROUP_BY,
                                                       child_sql)) {
            return false;
        }
        expression_sql = "LOWER(" + child_sql + ")";
        return true;
    }

    return TryTransformGroupArithmeticExpression(expr, column_sql_resolver, expression_sql);
}

static bool TryTransformFloatingModuloConstant(Expression &expr, string &constant_sql) {
    if (ClassifySQLExpression(expr) != ExpressionClass::BOUND_CONSTANT) {
        return false;
    }
    auto &constant = expr.Cast<BoundConstantExpression>();
    if (constant.GetValue().IsNull()) {
        return false;
    }
    if (!IsArithmeticScalarType(constant.GetValue().type()) && !IsIntegralScalarType(constant.GetValue().type())) {
        return false;
    }

    auto double_value = constant.GetValue().DefaultTryCastAs(LogicalType::DOUBLE);
    if (!double_value || double_value->IsNull() || !std::isfinite(double_value->GetValue<double>())) {
        return false;
    }
    return TryTransformConstantExpression(expr, constant_sql);
}

static bool TryTransformFloatingModuloOperand(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    ScalarExpressionContext context,
    string &operand_sql) {
    if (TryTransformFloatingModuloConstant(expr, operand_sql)) {
        return true;
    }
    if (expr.GetReturnType().id() != LogicalTypeId::FLOAT && expr.GetReturnType().id() != LogicalTypeId::DOUBLE) {
        return false;
    }
    return TryTransformBoundScalarExpressionInternal(expr,
                                                     column_sql_resolver,
                                                     integral_floating_sql_resolver,
                                                     context,
                                                     operand_sql);
}

static string FloatingModuloSQL(const string &left_sql, const string &right_sql) {
    return "(CASE WHEN " + right_sql + " = 0 THEN IEEE_DIVIDE(0, 0) ELSE (" + left_sql + " - " + right_sql +
           " * TRUNC(IEEE_DIVIDE(" + left_sql + ", " + right_sql + "))) END)";
}

static bool TryTransformGroupFloatingModuloOperand(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    string &operand_sql) {
    if (TryTransformFloatingModuloConstant(expr, operand_sql)) {
        return true;
    }
    if (ClassifySQLExpression(expr) != ExpressionClass::BOUND_COLUMN_REF ||
        (expr.GetReturnType().id() != LogicalTypeId::FLOAT && expr.GetReturnType().id() != LogicalTypeId::DOUBLE)) {
        return false;
    }

    auto &colref = expr.Cast<BoundColumnRefExpression>();
    return column_sql_resolver(colref.Binding(), operand_sql);
}

static bool TryTransformIntegralFloatingModuloOperand(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &operand_sql) {
    Expression *unwrapped = nullptr;
    if (TryUnwrapIntegralFloatingCast(expr, unwrapped)) {
        return TryTransformBoundScalarExpressionInternal(*unwrapped,
                                                         column_sql_resolver,
                                                         integral_floating_sql_resolver,
                                                         ScalarExpressionContext::FILTER,
                                                         operand_sql);
    }
    if (ClassifySQLExpression(expr) != ExpressionClass::BOUND_COLUMN_REF || !integral_floating_sql_resolver) {
        return false;
    }
    auto &colref = expr.Cast<BoundColumnRefExpression>();
    return integral_floating_sql_resolver(colref.Binding(), operand_sql);
}

static bool TryTransformFilterValueStringExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    ScalarExpressionContext context,
    string &expression_sql) {
    if (expr.GetReturnType().id() == LogicalTypeId::LIST || expr.GetReturnType().id() == LogicalTypeId::ARRAY) {
        string child_sql;
        if (!TryTransformFilterListStringExpression(expr,
                                                    column_sql_resolver,
                                                    integral_floating_sql_resolver,
                                                    context,
                                                    child_sql)) {
            return false;
        }
        expression_sql = "IF(" + child_sql + " IS NULL, 'NULL', " + child_sql + ")";
        return true;
    }

    if (expr.GetReturnType().id() == LogicalTypeId::STRUCT) {
        string child_sql;
        if (!TryTransformFilterStructStringExpression(expr,
                                                      column_sql_resolver,
                                                      integral_floating_sql_resolver,
                                                      context,
                                                      child_sql)) {
            return false;
        }
        expression_sql = "IF(" + child_sql + " IS NULL, 'NULL', " + child_sql + ")";
        return true;
    }

    if (!IsFilterListStringElementType(expr.GetReturnType())) {
        return false;
    }

    string child_sql;
    if (!TryTransformBoundScalarExpressionInternal(expr,
                                                   column_sql_resolver,
                                                   integral_floating_sql_resolver,
                                                   context,
                                                   child_sql)) {
        return false;
    }
    expression_sql = "IF(" + child_sql + " IS NULL, 'NULL', CAST(" + child_sql + " AS STRING))";
    return true;
}

static bool TryTransformFilterStructStringExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    ScalarExpressionContext context,
    string &expression_sql) {
    if (expr.GetReturnType().id() != LogicalTypeId::STRUCT ||
        ClassifySQLExpression(expr) != ExpressionClass::BOUND_FUNCTION) {
        return false;
    }

    auto &function = expr.Cast<BoundFunctionExpression>();
    const auto function_name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
    if (function_name != "struct_pack" ||
        function.GetChildren().size() != StructType::GetChildCount(expr.GetReturnType())) {
        return false;
    }

    vector<string> entries;
    entries.reserve(function.GetChildren().size() * 3 + 2);
    entries.push_back("'{'");
    for (idx_t child_idx = 0; child_idx < function.GetChildren().size(); child_idx++) {
        string child_sql;
        if (!TryTransformFilterValueStringExpression(*function.GetChildren()[child_idx],
                                                     column_sql_resolver,
                                                     integral_floating_sql_resolver,
                                                     context,
                                                     child_sql)) {
            return false;
        }

        if (child_idx > 0) {
            entries.push_back("', '");
        }
        entries.push_back(
            WriteBigQueryStringLiteral("'" + StructType::GetChildName(expr.GetReturnType(), child_idx) + "': "));
        entries.push_back(std::move(child_sql));
    }
    entries.push_back("'}'");
    expression_sql = "CONCAT(" + StringUtil::Join(entries, ", ") + ")";
    return true;
}

static bool TryTransformFilterListStringExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    ScalarExpressionContext context,
    string &expression_sql) {
    if (expr.GetReturnType().id() != LogicalTypeId::LIST && expr.GetReturnType().id() != LogicalTypeId::ARRAY) {
        return false;
    }
    if (ClassifySQLExpression(expr) == ExpressionClass::BOUND_COLUMN_REF) {
        auto &colref = expr.Cast<BoundColumnRefExpression>();
        string column_sql;
        if (!column_sql_resolver(colref.Binding(), column_sql) || IsDirectGroupColumnSQL(column_sql)) {
            return false;
        }
        expression_sql = std::move(column_sql);
        return true;
    }

    const vector<unique_ptr<Expression>> *children = nullptr;
    if (!TryTransformFilterListConstructorChildren(expr, children)) {
        return false;
    }

    vector<string> entries;
    entries.reserve(children->size() * 2 + 2);
    entries.push_back("'['");
    for (idx_t child_idx = 0; child_idx < children->size(); child_idx++) {
        string child_sql;
        if (!TryTransformFilterValueStringExpression(*(*children)[child_idx],
                                                     column_sql_resolver,
                                                     integral_floating_sql_resolver,
                                                     context,
                                                     child_sql)) {
            return false;
        }
        if (child_idx > 0) {
            entries.push_back("', '");
        }
        entries.push_back(std::move(child_sql));
    }
    entries.push_back("']'");
    expression_sql = "CONCAT(" + StringUtil::Join(entries, ", ") + ")";
    return true;
}

static bool TryTransformBoundScalarExpressionInternal(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    ScalarExpressionContext context,
    string &expression_sql) {
    switch (ClassifySQLExpression(expr)) {
    case ExpressionClass::BOUND_COLUMN_REF: {
        auto &colref = expr.Cast<BoundColumnRefExpression>();
        return column_sql_resolver(colref.Binding(), expression_sql);
    }
    case ExpressionClass::BOUND_CONSTANT:
        // TODO(group-by-abs): Keep GROUP BY constants local for now; this step only enables ABS(column).
        if (context == ScalarExpressionContext::GROUP_BY) {
            return false;
        }
        if (context == ScalarExpressionContext::AGGREGATE && !IsArithmeticScalarType(expr.GetReturnType())) {
            return false;
        }
        return TryTransformConstantExpression(expr, expression_sql);
    case ExpressionClass::LEGACY_BOUND_CAST: {
        auto &cast = expr.Cast<BoundFunctionExpression>();
        if (context == ScalarExpressionContext::GROUP_BY) {
            string cast_type;
            if (cast.GetReturnType().id() == LogicalTypeId::VARCHAR) {
                if (TryTransformFilterListStringExpression(*BoundCastExpression::ChildMutable(cast),
                                                           column_sql_resolver,
                                                           integral_floating_sql_resolver,
                                                           ScalarExpressionContext::GROUP_BY,
                                                           expression_sql)) {
                    return true;
                }
                if (TryTransformFilterStructStringExpression(*BoundCastExpression::ChildMutable(cast),
                                                             column_sql_resolver,
                                                             integral_floating_sql_resolver,
                                                             ScalarExpressionContext::GROUP_BY,
                                                             expression_sql)) {
                    return true;
                }
                if (!IsFilterStringCastSourceType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                    return false;
                }

                string child_sql;
                if (!TryTransformBoundScalarExpressionInternal(*BoundCastExpression::ChildMutable(cast),
                                                               column_sql_resolver,
                                                               integral_floating_sql_resolver,
                                                               ScalarExpressionContext::GROUP_BY,
                                                               child_sql)) {
                    return false;
                }
                const auto cast_function = BoundCastExpression::IsTryCast(cast) ? "SAFE_CAST" : "CAST";
                expression_sql = string(cast_function) + "(" + child_sql + " AS STRING)";
                return true;
            }
            if (TryGetFilterNumericCastType(cast.GetReturnType(), cast_type)) {
                if (!IsFilterNumericCastSourceType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                    return false;
                }

                string child_sql;
                if (!TryTransformBoundScalarExpressionInternal(*BoundCastExpression::ChildMutable(cast),
                                                               column_sql_resolver,
                                                               integral_floating_sql_resolver,
                                                               ScalarExpressionContext::GROUP_BY,
                                                               child_sql)) {
                    return false;
                }
                if (BoundCastExpression::ChildMutable(cast)->GetReturnType().id() == LogicalTypeId::BOOLEAN &&
                    cast_type != "INT64") {
                    child_sql = "CAST(" + child_sql + " AS INT64)";
                }

                const auto cast_function = BoundCastExpression::IsTryCast(cast) ? "SAFE_CAST" : "CAST";
                expression_sql = string(cast_function) + "(" + child_sql + " AS " + cast_type + ")";
                return true;
            }
            {
                string child_sql;
                if (TryGetFilterTemporalCastType(cast.GetReturnType(), cast_type) &&
                    TryTransformGroupTemporalCastChild(*BoundCastExpression::ChildMutable(cast),
                                                       column_sql_resolver,
                                                       child_sql)) {
                    const auto cast_function = BoundCastExpression::IsTryCast(cast) ? "SAFE_CAST" : "CAST";
                    expression_sql = string(cast_function) + "(" + child_sql + " AS " + cast_type + ")";
                    return true;
                }
            }

            if (!BoundCastExpression::IsTryCast(cast) || cast.GetReturnType().id() != LogicalTypeId::DATE ||
                ClassifySQLExpression(*BoundCastExpression::ChildMutable(cast)) != ExpressionClass::BOUND_COLUMN_REF ||
                !IsFilterTemporalCastSourceType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                return false;
            }

            string child_sql;
            if (!TryTransformBoundScalarExpressionInternal(*BoundCastExpression::ChildMutable(cast),
                                                           column_sql_resolver,
                                                           integral_floating_sql_resolver,
                                                           ScalarExpressionContext::GROUP_BY,
                                                           child_sql) ||
                !IsDirectGroupColumnSQL(child_sql)) {
                return false;
            }

            // GROUP BY only supports TRY_CAST(string_column AS DATE) for now.
            expression_sql = "SAFE_CAST(" + child_sql + " AS DATE)";
            return true;
        }
        if (context == ScalarExpressionContext::AGGREGATE) {
            string cast_type;
            if (cast.GetReturnType().id() == LogicalTypeId::VARCHAR) {
                if (TryTransformFilterListStringExpression(*BoundCastExpression::ChildMutable(cast),
                                                           column_sql_resolver,
                                                           integral_floating_sql_resolver,
                                                           ScalarExpressionContext::AGGREGATE,
                                                           expression_sql)) {
                    return true;
                }
                if (TryTransformFilterStructStringExpression(*BoundCastExpression::ChildMutable(cast),
                                                             column_sql_resolver,
                                                             integral_floating_sql_resolver,
                                                             ScalarExpressionContext::AGGREGATE,
                                                             expression_sql)) {
                    return true;
                }
                if (!IsFilterStringCastSourceType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                    return false;
                }

                string child_sql;
                if (!TryTransformBoundScalarExpressionInternal(*BoundCastExpression::ChildMutable(cast),
                                                               column_sql_resolver,
                                                               integral_floating_sql_resolver,
                                                               ScalarExpressionContext::AGGREGATE,
                                                               child_sql)) {
                    return false;
                }
                expression_sql = "CAST(" + child_sql + " AS STRING)";
                return true;
            }
            if (TryGetFilterNumericCastType(cast.GetReturnType(), cast_type)) {
                if (!IsFilterNumericCastSourceType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                    return false;
                }

                string child_sql;
                if (!TryTransformBoundScalarExpressionInternal(*BoundCastExpression::ChildMutable(cast),
                                                               column_sql_resolver,
                                                               integral_floating_sql_resolver,
                                                               ScalarExpressionContext::AGGREGATE,
                                                               child_sql)) {
                    return false;
                }
                if (BoundCastExpression::ChildMutable(cast)->GetReturnType().id() == LogicalTypeId::BOOLEAN &&
                    cast_type != "INT64") {
                    child_sql = "CAST(" + child_sql + " AS INT64)";
                }

                const auto cast_function = BoundCastExpression::IsTryCast(cast) ? "SAFE_CAST" : "CAST";
                expression_sql = string(cast_function) + "(" + child_sql + " AS " + cast_type + ")";
                return true;
            }
            if (TryGetFilterTemporalCastType(cast.GetReturnType(), cast_type)) {
                // Aggregate temporal casts stay narrow: string-like inputs only.
                if (!IsFilterTemporalCastSourceType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                    return false;
                }

                string child_sql;
                if (ClassifySQLExpression(*BoundCastExpression::ChildMutable(cast)) ==
                    ExpressionClass::BOUND_CONSTANT) {
                    if (!TryTransformConstantExpression(*BoundCastExpression::ChildMutable(cast), child_sql)) {
                        return false;
                    }
                } else {
                    // Keep the child in aggregate context so filter-only functions stay local.
                    if (!TryTransformBoundScalarExpressionInternal(
                            *BoundCastExpression::ChildMutable(cast),
                            column_sql_resolver,
                            integral_floating_sql_resolver,
                            BoundCastExpression::IsTryCast(cast)
                                ? ScalarExpressionContext::AGGREGATE_TRY_TEMPORAL_CAST_CHILD
                                : ScalarExpressionContext::AGGREGATE,
                            child_sql)) {
                        return false;
                    }
                }

                const auto cast_function = BoundCastExpression::IsTryCast(cast) ? "SAFE_CAST" : "CAST";
                expression_sql = string(cast_function) + "(" + child_sql + " AS " + cast_type + ")";
                return true;
            }
            if (!IsArithmeticScalarType(expr.GetReturnType())) {
                return false;
            }
            return TryTransformConstantExpression(expr, expression_sql);
        }
        string cast_type;
        if (cast.GetReturnType().id() == LogicalTypeId::VARCHAR) {
            if (context == ScalarExpressionContext::FILTER) {
                if (TryTransformFilterListStringExpression(*BoundCastExpression::ChildMutable(cast),
                                                           column_sql_resolver,
                                                           integral_floating_sql_resolver,
                                                           ScalarExpressionContext::FILTER,
                                                           expression_sql)) {
                    return true;
                }
                if (TryTransformFilterStructStringExpression(*BoundCastExpression::ChildMutable(cast),
                                                             column_sql_resolver,
                                                             integral_floating_sql_resolver,
                                                             ScalarExpressionContext::FILTER,
                                                             expression_sql)) {
                    return true;
                }
            }
            if (!IsFilterStringCastSourceType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                return false;
            }
            cast_type = "STRING";
        } else if (TryGetFilterNumericCastType(cast.GetReturnType(), cast_type)) {
            if (!IsFilterNumericCastSourceType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                return false;
            }
        } else if (TryGetFilterTemporalCastType(cast.GetReturnType(), cast_type)) {
            if (!IsFilterTemporalCastSourceType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                return false;
            }
        } else {
            if (TryTransformConstantExpression(expr, expression_sql)) {
                return true;
            }
            return false;
        }
        string child_sql;
        if (!TryTransformBoundScalarExpressionInternal(*BoundCastExpression::ChildMutable(cast),
                                                       column_sql_resolver,
                                                       integral_floating_sql_resolver,
                                                       ScalarExpressionContext::FILTER,
                                                       child_sql)) {
            return false;
        }
        if (BoundCastExpression::ChildMutable(cast)->GetReturnType().id() == LogicalTypeId::BOOLEAN &&
            cast_type != "INT64") {
            child_sql = "CAST(" + child_sql + " AS INT64)";
        }
        const auto cast_function = BoundCastExpression::IsTryCast(cast) ? "SAFE_CAST" : "CAST";
        expression_sql = string(cast_function) + "(" + child_sql + " AS " + cast_type + ")";
        return true;
    }
    case ExpressionClass::BOUND_OPERATOR:
        if (context != ScalarExpressionContext::FILTER && context != ScalarExpressionContext::GROUP_BY) {
            return false;
        }
        return TryTransformFilterListStringExpression(expr,
                                                      column_sql_resolver,
                                                      integral_floating_sql_resolver,
                                                      context,
                                                      expression_sql);
    case ExpressionClass::BOUND_FUNCTION: {
        auto &function = expr.Cast<BoundFunctionExpression>();
        const auto function_name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());

        if (context == ScalarExpressionContext::FILTER &&
            (function_name == "struct_extract" || function_name == "struct_extract_at")) {
            vector<StructExtractPathEntry> path;
            const auto &base = PeelStructExtractPath(function, path);
            if (path.empty() || base.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
                return false;
            }
            string field_sql;
            if (!column_sql_resolver(base.Cast<BoundColumnRefExpression>().Binding(), field_sql)) {
                return false;
            }
            for (const auto &entry : path) {
                if (entry.child_name.GetIdentifierName().empty()) {
                    return false;
                }
                field_sql += "." + BigqueryUtils::WriteQuotedIdentifier(entry.child_name.GetIdentifierName());
            }
            expression_sql = std::move(field_sql);
            return true;
        }

        if (context == ScalarExpressionContext::GROUP_BY) {
            return TryTransformGroupFunctionExpression(expr, column_sql_resolver, expression_sql);
        }

        // GROUP BY function support is handled above; the remaining rules are for aggregate/filter contexts.
        if (context == ScalarExpressionContext::FILTER &&
            (expr.GetReturnType().id() == LogicalTypeId::LIST || expr.GetReturnType().id() == LogicalTypeId::ARRAY)) {
            return TryTransformFilterListStringExpression(expr,
                                                          column_sql_resolver,
                                                          integral_floating_sql_resolver,
                                                          context,
                                                          expression_sql);
        }

        //
        const bool sin =
            (context == ScalarExpressionContext::AGGREGATE || context == ScalarExpressionContext::FILTER) &&
            function_name == "sin" && function.GetChildren().size() == 1;
        if (sin) {
            if (!IsArithmeticScalarType(function.GetReturnType()) ||
                !IsArithmeticScalarType(function.GetChildren()[0]->GetReturnType())) {
                return false;
            }

            string child_sql;
            if (!TryTransformBoundScalarExpressionInternal(*function.GetChildren()[0],
                                                           column_sql_resolver,
                                                           integral_floating_sql_resolver,
                                                           context,
                                                           child_sql)) {
                return false;
            }

            expression_sql = "SIN(" + child_sql + ")";
            return true;
        }

        const bool lower =
            (context == ScalarExpressionContext::AGGREGATE || context == ScalarExpressionContext::FILTER ||
             context == ScalarExpressionContext::AGGREGATE_TRY_TEMPORAL_CAST_CHILD) &&
            function_name == "lower" && function.GetChildren().size() == 1;
        if (lower) {
            if (function.GetReturnType().id() != LogicalTypeId::VARCHAR ||
                function.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
                return false;
            }

            // Transform child recursively to handle nested expressions, e.g., LOWER(UPPER(x)) or LOWER(CAST(x AS VARCHAR))
            string child_sql;
            const auto lower_child_context = context == ScalarExpressionContext::FILTER
                                                 ? ScalarExpressionContext::FILTER
                                                 : ScalarExpressionContext::AGGREGATE;
            if (!TryTransformBoundScalarExpressionInternal(*function.GetChildren()[0],
                                                           column_sql_resolver,
                                                           integral_floating_sql_resolver,
                                                           lower_child_context,
                                                           child_sql)) {
                return false;
            }

            expression_sql = "LOWER(" + child_sql + ")";
            return true;
        }

        const bool abs = (context == ScalarExpressionContext::AGGREGATE || context == ScalarExpressionContext::FILTER ||
                          context == ScalarExpressionContext::GROUP_BY) && //
                         function_name == "abs" &&                         //
                         function.GetChildren().size() == 1;
        if (abs) {
            // Filters may use recursive ABS expressions; GROUP BY currently only accepts ABS(column).
            // Restrict GROUP BY ABS(...) to column references only to keep this grouping profile intentionally narrow.
            if (context == ScalarExpressionContext::GROUP_BY &&
                function.GetChildren()[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
                return false;
            }

            // Restrict ABS to arithmetic scalar types only, since BQ ABS does not support e.g. STRING or DATE types.
            if (!IsArithmeticScalarType(function.GetReturnType()) ||
                !IsArithmeticScalarType(function.GetChildren()[0]->GetReturnType())) {
                return false;
            }

            // TODO(group-by-abs): Recurse in the current context so GROUP BY ABS(...) stays narrower than filters.
            // Transform child recursively to handle nested expressions, e.g., ABS(-x) or ABS(CAST(x AS INT64))
            string child_sql;
            if (!TryTransformBoundScalarExpressionInternal(*function.GetChildren()[0],
                                                           column_sql_resolver,
                                                           integral_floating_sql_resolver,
                                                           context,
                                                           child_sql)) {
                return false;
            }

            expression_sql = "ABS(" + child_sql + ")";
            return true;
        }

        const bool group_column_plus_constant =
            context == ScalarExpressionContext::GROUP_BY && function_name == "+" && function.GetChildren().size() == 2;
        if (group_column_plus_constant) {
            if (!IsArithmeticScalarType(function.GetReturnType()) ||
                !IsArithmeticScalarType(function.GetChildren()[0]->GetReturnType()) ||
                !IsArithmeticScalarType(function.GetChildren()[1]->GetReturnType()) ||
                function.GetChildren()[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
                return false;
            }

            string left_sql;
            string right_sql;
            if (!TryTransformBoundScalarExpressionInternal(*function.GetChildren()[0],
                                                           column_sql_resolver,
                                                           integral_floating_sql_resolver,
                                                           ScalarExpressionContext::GROUP_BY,
                                                           left_sql) ||
                !IsDirectGroupColumnSQL(left_sql) ||
                !TryTransformConstantExpression(*function.GetChildren()[1], right_sql) || right_sql == "NULL") {
                return false;
            }

            // Keep GROUP BY arithmetic exact: column + non-null numeric literal only.
            expression_sql = "(" + left_sql + " + " + right_sql + ")";
            return true;
        }

        // TODO(group-by-abs): Do not enable arithmetic, casts, LOWER, or other functions for GROUP BY in this change.
        if (context == ScalarExpressionContext::GROUP_BY) {
            return false;
        }

        const bool unary_minus = function_name == "-" && function.GetChildren().size() == 1;
        const bool basic_binary_operator = (function_name == "+" || function_name == "-" || function_name == "*") &&
                                           function.GetChildren().size() == 2;
        const bool division = function_name == "/" && function.GetChildren().size() == 2;
        const bool modulo = function_name == "%" && function.GetChildren().size() == 2;

        bool integral_floating_modulo = false;
        bool floating_modulo = false;
        vector<string> child_entries;
        if (modulo && (context == ScalarExpressionContext::FILTER || context == ScalarExpressionContext::AGGREGATE) &&
            (function.GetReturnType().id() == LogicalTypeId::FLOAT ||
             function.GetReturnType().id() == LogicalTypeId::DOUBLE)) {
            string left_sql;
            string right_sql;
            if (TryTransformFloatingModuloOperand(*function.GetChildren()[0],
                                                  column_sql_resolver,
                                                  integral_floating_sql_resolver,
                                                  context,
                                                  left_sql) &&
                TryTransformFloatingModuloOperand(*function.GetChildren()[1],
                                                  column_sql_resolver,
                                                  integral_floating_sql_resolver,
                                                  context,
                                                  right_sql)) {
                child_entries.push_back(std::move(left_sql));
                child_entries.push_back(std::move(right_sql));
                integral_floating_modulo = true;
                floating_modulo = true;
            } else if (context == ScalarExpressionContext::FILTER) {
                child_entries.clear();
                for (auto &child : function.GetChildren()) {
                    string child_sql;
                    if (!TryTransformIntegralFloatingModuloOperand(*child,
                                                                   column_sql_resolver,
                                                                   integral_floating_sql_resolver,
                                                                   child_sql)) {
                        if (TryTransformIntegralFloatingModuloConstant(*child, child_sql)) {
                            // DuckDB binds e.g. CAST(i AS DOUBLE) % 2 as a DOUBLE modulo with a 2.0 literal.
                            // BigQuery MOD does not accept FLOAT64, so only integer-valued FLOAT/DOUBLE constants are
                            // converted back to integer literals for this integer-origin modulo path.
                        } else {
                            break;
                        }
                    }
                    child_entries.push_back(std::move(child_sql));
                }
                integral_floating_modulo = child_entries.size() == function.GetChildren().size();
            }
        }
        if ((!unary_minus && !basic_binary_operator && !division && !modulo) ||
            !IsArithmeticScalarType(function.GetReturnType()) ||
            (modulo && !IsModuloScalarType(function.GetReturnType()) && !integral_floating_modulo)) {
            return false;
        }

        if (!integral_floating_modulo) {
            child_entries.clear();
            child_entries.reserve(function.GetChildren().size());
            for (auto &child : function.GetChildren()) {
                auto child_expr = child.get();
                if (division && ClassifySQLExpression(*child_expr) == ExpressionClass::LEGACY_BOUND_CAST) {
                    auto &cast = child_expr->Cast<BoundFunctionExpression>();
                    if (!BoundCastExpression::IsTryCast(cast) &&
                        (cast.GetReturnType().id() == LogicalTypeId::FLOAT ||
                         cast.GetReturnType().id() == LogicalTypeId::DOUBLE) &&
                        IsArithmeticScalarType(BoundCastExpression::ChildMutable(cast)->GetReturnType())) {
                        child_expr = BoundCastExpression::ChildMutable(cast).get();
                    }
                }
                if (!IsArithmeticScalarType(child_expr->GetReturnType()) ||
                    (modulo && !IsModuloScalarType(child_expr->GetReturnType()))) {
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
            if (floating_modulo) {
                expression_sql = FloatingModuloSQL(child_entries[0], child_entries[1]);
            } else {
                expression_sql = "(CASE WHEN " + child_entries[1] + " = 0 THEN NULL ELSE MOD(" + child_entries[0] +
                                 ", " + child_entries[1] + ") END)";
            }
            if (integral_floating_modulo && !floating_modulo) {
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
    BoundFunctionExpression &comparison,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql) {
    string op;
    if (!TryTransformComparisonOperator(comparison.GetExpressionType(), op)) {
        return false;
    }

    string left_sql;
    string right_sql;
    if (!TryTransformBoundScalarExpressionInternal(*BoundComparisonExpression::LeftMutable(comparison),
                                                   column_sql_resolver,
                                                   integral_floating_sql_resolver,
                                                   ScalarExpressionContext::FILTER,
                                                   left_sql) ||
        !TryTransformBoundScalarExpressionInternal(*BoundComparisonExpression::RightMutable(comparison),
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
    entries.reserve(conjunction.GetChildren().size());
    for (auto &child : conjunction.GetChildren()) {
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

template <class OPERATOR>
static bool TryTransformInFilter(
    OPERATOR &op,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql) {
    if (op.GetChildren().size() < 2) {
        return false;
    }

    string expression_sql;
    if (!TryTransformBoundScalarExpressionInternal(*op.GetChildren()[0],
                                                   column_sql_resolver,
                                                   integral_floating_sql_resolver,
                                                   ScalarExpressionContext::FILTER,
                                                   expression_sql)) {
        return false;
    }

    vector<string> literals;
    literals.reserve(op.GetChildren().size() - 1);
    for (idx_t child_idx = 1; child_idx < op.GetChildren().size(); child_idx++) {
        string literal_sql;
        if (!TryTransformConstantExpression(*op.GetChildren()[child_idx], literal_sql) || literal_sql == "NULL") {
            return false;
        }
        literals.push_back(std::move(literal_sql));
    }
    filter_sql = expression_sql + " IN (" + StringUtil::Join(literals, ", ") + ")";
    return true;
}

template <class OPERATOR>
static bool TryTransformOperatorFilter(
    OPERATOR &op,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    const std::function<bool(const ColumnBinding &, string &)> &integral_floating_sql_resolver,
    string &filter_sql) {
    switch (op.GetExpressionType()) {
    case ExpressionType::OPERATOR_IS_NULL:
    case ExpressionType::OPERATOR_IS_NOT_NULL: {
        if (op.GetChildren().size() != 1) {
            return false;
        }
        string expression_sql;
        if (!TryTransformBoundScalarExpressionInternal(*op.GetChildren()[0],
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
    switch (ClassifySQLExpression(expr)) {
    case ExpressionClass::LEGACY_BOUND_COMPARISON:
        return TryTransformComparisonFilter(expr.Cast<BoundFunctionExpression>(),
                                            column_sql_resolver,
                                            integral_floating_sql_resolver,
                                            filter_sql);
    case ExpressionClass::BOUND_CONJUNCTION:
        return TryTransformConjunctionFilter(expr.Cast<BoundConjunctionExpression>(),
                                             column_sql_resolver,
                                             integral_floating_sql_resolver,
                                             filter_sql);
    case ExpressionClass::BOUND_FUNCTION:
        return TryTransformOperatorFilter(expr.Cast<BoundFunctionExpression>(),
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
    const auto &column_ids = get.GetColumnIds();
    if (filter_column_idx >= column_ids.size()) {
        return false;
    }

    const auto &column_index = column_ids[filter_column_idx];
    // FULL_READ child indexes describe requested fields; the filter expression still uses the root STRUCT.
    if (!column_index.HasPrimaryIndex() || column_index.IsPushdownExtract() || column_index.IsVirtualColumn() ||
        column_index.IsRowIdColumn() || column_index.IsEmptyColumn() ||
        column_index.GetPrimaryIndex() >= get.names.size()) {
        return false;
    }

    column_name = get.GetColumnName(column_index).GetIdentifierName();
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
        if (!get.table_filters.HasFilters() && !get.table_filters.HasMultiColumnFilters()) {
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
    if (filters.HasMultiColumnFilters()) {
        throw NotImplementedException("BigQuery multi-column scan filters require local evaluation");
    }
    vector<string> filter_entries;
    filter_entries.reserve(filters.FilterCount());
    for (auto &filter : filters) {
        auto entry = TransformFilter(column_name_resolver(filter.GetIndex().GetIndex()), filter.Filter());
        if (!entry.empty()) {
            filter_entries.push_back(std::move(entry));
        }
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

bool BigquerySQL::TryTransformBoundGroupScalarExpression(
    Expression &expr,
    const std::function<bool(const ColumnBinding &, string &)> &column_sql_resolver,
    string &expression_sql) {
    // Used for GROUP BY keys and projection aliases that later become GROUP BY keys.
    // TODO(group-by-abs): GROUP BY uses its own narrow capability profile, not FILTER.
    return TryTransformBoundScalarExpressionInternal(expr,
                                                     column_sql_resolver,
                                                     {},
                                                     ScalarExpressionContext::GROUP_BY,
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
    if (ClassifySQLExpression(expr) != ExpressionClass::BOUND_COLUMN_REF || !integral_floating_sql_resolver) {
        return false;
    }
    auto &colref = expr.Cast<BoundColumnRefExpression>();
    return integral_floating_sql_resolver(colref.Binding(), expression_sql);
}

bool BigquerySQL::TryTransformLogicalGetFilters(const LogicalGet &get, string &filter_sql) {
    if (!get.table_filters.HasFilters() && !get.table_filters.HasMultiColumnFilters()) {
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
    if (info.GetQualifiedName().Schema().GetIdentifierName().empty()) {
        throw BinderException("Schema not specified for AlterTableInfo");
    }
    const auto &table_string =
        BigqueryUtils::FormatTableStringSimple(project_id,
                                               info.GetQualifiedName().Schema().GetIdentifierName(),
                                               info.GetQualifiedName().Name().GetIdentifierName());
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
        stmt << BigqueryUtils::WriteQuotedIdentifier(rename_info.old_name.GetIdentifierName()) << " TO ";
        stmt << BigqueryUtils::WriteQuotedIdentifier(rename_info.new_name.GetIdentifierName());
        break;
    }
    case AlterTableType::RENAME_TABLE: {
        // Syntax
        // ALTER TABLE [IF EXISTS] table_name RENAME TO new_table_name
        const auto &rename_info = info.Cast<RenameTableInfo>();
        stmt << "RENAME TO ";
        stmt << BigqueryUtils::WriteQuotedIdentifier(rename_info.new_table_name.GetIdentifierName());
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
        stmt << BigqueryUtils::WriteQuotedIdentifier(remove_column_info.removed_column.GetIdentifierName());
        break;
    }
    case AlterTableType::ALTER_COLUMN_TYPE: {
        // Syntax
        // ALTER TABLE table_name ALTER COLUMN column_name SET DATA TYPE type
        const auto &alter_column_type_info = dynamic_cast<const ChangeColumnTypeInfo *>(&info);
        stmt << "ALTER COLUMN ";
        stmt << QuoteFilterColumnPath(BigqueryUtils::ToStrings(alter_column_type_info->column_path));
        stmt << " SET DATA TYPE " << BigqueryUtils::LogicalTypeToBigquerySQL(alter_column_type_info->target_type);
        break;
    }
    case AlterTableType::SET_DEFAULT: {
        // Syntax
        // ALTER TABLE table_name ALTER COLUMN column_name SET DEFAULT expression
        const auto &set_default_info = dynamic_cast<const SetDefaultInfo *>(&info);
        stmt << "ALTER COLUMN ";
        stmt << QuoteFilterColumnPath(BigqueryUtils::ToStrings(set_default_info->column_path));
        stmt << " SET DEFAULT " << set_default_info->expression->ToString();
        break;
    }
    case AlterTableType::DROP_NOT_NULL: {
        // Syntax
        // ALTER TABLE table_name ALTER COLUMN column_name DROP NOT NULL
        const auto &drop_not_null_info = info.Cast<DropNotNullInfo>();
        stmt << "ALTER COLUMN ";
        stmt << QuoteFilterColumnPath(BigqueryUtils::ToStrings(drop_not_null_info.column_path));
        stmt << " DROP NOT NULL";
        break;
    }
    default:
        throw NotImplementedException("Unsupported Alter Table type: This type of ALTER TABLE is not supported.");
    }

    return stmt.str();
}

string BigquerySQL::CreateSchemaInfoToSQL(const string &project_id, const CreateSchemaInfo &info) {
    const auto &schema_string =
        BigqueryUtils::FormatTableStringSimple(project_id, info.GetQualifiedName().Schema().GetIdentifierName());
    std::stringstream query;
    query << "CREATE SCHEMA ";
    if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
        query << "IF NOT EXISTS ";
    }
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
    if (info.GetQualifiedName().Schema().GetIdentifierName().empty()) {
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
    auto table_string = BigqueryUtils::FormatTableStringSimple(project_id,
                                                               info.GetQualifiedName().Schema().GetIdentifierName(),
                                                               info.GetQualifiedName().Name().GetIdentifierName());
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
    auto table_string = BigqueryUtils::FormatTableStringSimple(info.GetQualifiedName().Catalog().GetIdentifierName(),
                                                               info.GetQualifiedName().Schema().GetIdentifierName(),
                                                               info.GetQualifiedName().Name().GetIdentifierName());
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
        auto name = project_id + "." + info.GetQualifiedName().Schema().GetIdentifierName() + "." +
                    info.GetQualifiedName().Name().GetIdentifierName();
        query += BigqueryUtils::WriteQuotedIdentifier(name);
        break;
    }
    case CatalogType::SCHEMA_ENTRY: {
        auto name = project_id + "." + info.GetQualifiedName().Name().GetIdentifierName();
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
                                                               lu.table.schema.name.GetIdentifierName(),
                                                               lu.table.name.GetIdentifierName());

    string sql = "UPDATE ";
    sql += BigqueryUtils::WriteQuotedIdentifier(table_string);
    sql += " SET ";
    for (size_t c = 0; c < lu.columns.size(); ++c) {
        if (c > 0) {
            sql += ", ";
        }

        auto &col = lu.table.GetColumn(lu.table.GetColumns().PhysicalToLogical(lu.columns[c]));
        sql += BigqueryUtils::WriteQuotedIdentifier(col.GetName().GetIdentifierName()) + " = ";
        if (lu.expressions[c]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
            sql += "DEFAULT";
            continue;
        }
        if (lu.expressions[c]->GetExpressionType() != ExpressionType::BOUND_REF) {
            throw NotImplementedException("BigQuery UPDATE - Expected a bound reference expression");
        }
        auto &ref = lu.expressions[c]->Cast<BoundReferenceExpression>();
        if (ref.Index() >= proj.expressions.size()) {
            throw InternalException("BigQuery UPDATE - Projection reference index out of range");
        }
        sql += proj.expressions[ref.Index()]->ToString();
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
                                                               ld.table.schema.name.GetIdentifierName(),
                                                               ld.table.name.GetIdentifierName());
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

        // In BigQuery, primary key and unique constraints are handled differently
        // and might not be specified directly in the CREATE TABLE statement like MySQL
        // You would typically handle them with OPTIONS or in separate management operations
        if (column.HasDefaultValue()) {
            str << " DEFAULT (" << column.DefaultValue().ToString() << ")";
        }

        bool is_not_null = columns_not_null.find(column.Logical()) != columns_not_null.end();
        if (is_not_null) {
            str << " NOT NULL";
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
