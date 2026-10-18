#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "bigquery_sql.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

using namespace duckdb;
using namespace duckdb::bigquery;

namespace {

static unique_ptr<Expression> Comparison(ExpressionType type, int32_t value) {
    return BoundComparisonExpression::Create(type,
                                             make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL),
                                             make_uniq<BoundConstantExpression>(Value::INTEGER(value)));
}

static unique_ptr<Expression> Conjunction(ExpressionType type,
                                          unique_ptr<Expression> left,
                                          unique_ptr<Expression> right) {
    return make_uniq<BoundConjunctionExpression>(type, std::move(left), std::move(right));
}

static unique_ptr<Expression> Optional(bool selectivity_optional) {
    auto child = Comparison(ExpressionType::COMPARE_GREATERTHAN, 9000);
    if (selectivity_optional) {
        return CreateSelectivityOptionalFilterExpression(std::move(child), LogicalType::INTEGER, 0.5f, 100);
    }
    return CreateOptionalFilterExpression(std::move(child), LogicalType::INTEGER);
}

static void RequireEquivalentFilter(Connection &connection, unique_ptr<Expression> expression, const string &expected) {
    ExpressionFilter filter(std::move(expression));
    auto sql = BigquerySQL::TransformFilter("i", filter);
    // These numeric predicates use the same SQL semantics; adapt only identifier quotes for DuckDB.
    sql = sql.empty() ? "true" : StringUtil::Replace(sql, "`", "\"");
    const string query = "SELECT i FROM (VALUES (NULL), (-1), (0), (4999), (5000), (9001), (9999)) AS source(i) WHERE ";
    const string order = " ORDER BY i NULLS LAST";
    INFO(sql);
    auto actual_result = connection.Query(query + sql + order);
    REQUIRE(!actual_result->HasError());
    actual_result->Complete();
    REQUIRE(!actual_result->HasError());
    auto expected_result = connection.Query(query + expected + order);
    REQUIRE(!expected_result->HasError());
    expected_result->Complete();
    REQUIRE(!expected_result->HasError());
    CHECK(actual_result->Equals(*expected_result));
}

} // namespace

TEST_CASE("Optional scan filters preserve required predicates under AND and OR", "[bigquery][filter]") {
    DuckDB database(nullptr);
    Connection connection(database);

    for (const bool selectivity_optional : {false, true}) {
        CAPTURE(selectivity_optional);
        RequireEquivalentFilter(connection, Optional(selectivity_optional), "true");
        for (const bool optional_first : {false, true}) {
            CAPTURE(optional_first);
            auto required = Comparison(ExpressionType::COMPARE_LESSTHAN, 5000);
            auto optional = Optional(selectivity_optional);
            auto left = optional_first ? std::move(optional) : std::move(required);
            auto right = optional_first ? std::move(required) : std::move(optional);
            RequireEquivalentFilter(connection,
                                    Conjunction(ExpressionType::CONJUNCTION_AND, left->Copy(), right->Copy()),
                                    "i < 5000");
            RequireEquivalentFilter(connection,
                                    Conjunction(ExpressionType::CONJUNCTION_OR, std::move(left), std::move(right)),
                                    "true");
        }

        RequireEquivalentFilter(connection,
                                Conjunction(ExpressionType::CONJUNCTION_AND,
                                            Optional(selectivity_optional),
                                            Optional(selectivity_optional)),
                                "true");
        RequireEquivalentFilter(connection,
                                Conjunction(ExpressionType::CONJUNCTION_OR,
                                            Conjunction(ExpressionType::CONJUNCTION_AND,
                                                        Comparison(ExpressionType::COMPARE_LESSTHAN, 5000),
                                                        Optional(selectivity_optional)),
                                            Comparison(ExpressionType::COMPARE_GREATERTHAN, 9000)),
                                "i < 5000 OR i > 9000");
        RequireEquivalentFilter(connection,
                                Conjunction(ExpressionType::CONJUNCTION_AND,
                                            Comparison(ExpressionType::COMPARE_LESSTHAN, 5000),
                                            Conjunction(ExpressionType::CONJUNCTION_OR,
                                                        Comparison(ExpressionType::COMPARE_GREATERTHAN, 9000),
                                                        Optional(selectivity_optional))),
                                "i < 5000");
        RequireEquivalentFilter(connection,
                                Conjunction(ExpressionType::CONJUNCTION_OR,
                                            ExpressionFilter::CreateNullCheckExpression(
                                                make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL),
                                                ExpressionType::OPERATOR_IS_NULL),
                                            Optional(selectivity_optional)),
                                "true");
    }
}

TEST_CASE("Required unsupported scan filters still fail beside optional filters", "[bigquery][filter]") {
    auto unsupported = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
    unsupported->GetChildrenMutable().push_back(Comparison(ExpressionType::COMPARE_LESSTHAN, 5000));
    ExpressionFilter filter(Conjunction(ExpressionType::CONJUNCTION_AND, std::move(unsupported), Optional(false)));
    REQUIRE_THROWS_AS(BigquerySQL::TransformFilter("i", filter), NotImplementedException);
}
