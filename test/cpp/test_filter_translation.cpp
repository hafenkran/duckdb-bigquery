#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "bigquery_sql.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

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

static unique_ptr<Expression> StructField(Connection &connection,
                                          unique_ptr<Expression> input,
                                          idx_t index,
                                          bool by_index) {
    auto key =
        by_index
            ? Value::BIGINT(index + 1)
            : Value(StringUtil::Upper(StructType::GetChildName(input->GetReturnType(), index).GetIdentifierName()));
    vector<unique_ptr<Expression>> arguments;
    arguments.push_back(std::move(input));
    arguments.push_back(make_uniq<BoundConstantExpression>(std::move(key)));
    FunctionBinder binder(*connection.context);
    if (by_index) {
        return binder.BindScalarFunction(make_shared_ptr<ScalarFunction>(StructExtractAtFun::GetFunction()),
                                         std::move(arguments));
    }
    auto functions = StructExtractFun::GetFunctions();
    return binder.BindScalarFunction(functions.GetFunctionByOffset(0), std::move(arguments));
}

static unique_ptr<LogicalGet> FilterGet(const LogicalType &customer_type, vector<ColumnIndex> column_ids) {
    TableFunction function(Identifier("filter_translation_test"), {}, nullptr);
    auto get =
        make_uniq<LogicalGet>(TableIndex(0),
                              BoundTableFunction(function),
                              nullptr,
                              vector<LogicalType>{LogicalType::INTEGER, LogicalType::INTEGER, customer_type},
                              vector<Identifier>{Identifier("id"), Identifier("marker"), Identifier("customer_info")});
    get->SetColumnIds(std::move(column_ids));
    return get;
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

TEST_CASE("STRUCT scan filters use schema field names for named and positional extracts", "[bigquery][filter]") {
    DuckDB database(nullptr);
    Connection connection(database);
    const auto address_type = LogicalType::STRUCT(
        {{Identifier("city"), LogicalType::VARCHAR}, {Identifier("postal.code"), LogicalType::VARCHAR}});
    const auto customer_type =
        LogicalType::STRUCT({{Identifier("name"), LogicalType::VARCHAR}, {Identifier("address"), address_type}});
    auto column = make_uniq<BoundReferenceExpression>(customer_type, 0ULL);

    for (const bool by_index : {false, true}) {
        CAPTURE(by_index);
        auto name = StructField(connection, column->Copy(), 0, by_index);
        ExpressionFilter name_filter(
            BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL,
                                              std::move(name),
                                              make_uniq<BoundConstantExpression>(Value("Alice Smith"))));
        CHECK(BigquerySQL::TransformFilter("customer_info", name_filter) ==
              "(`customer_info`.`name` = \"Alice Smith\")");

        auto city = StructField(connection, StructField(connection, column->Copy(), 1, by_index), 0, by_index);
        ExpressionFilter city_filter(
            BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL,
                                              std::move(city),
                                              make_uniq<BoundConstantExpression>(Value("Los Angeles"))));
        CHECK(BigquerySQL::TransformFilter("customer_info", city_filter) ==
              "(`customer_info`.`address`.`city` = \"Los Angeles\")");

        auto postal_code = StructField(connection, StructField(connection, column->Copy(), 1, by_index), 1, by_index);
        ExpressionFilter null_filter(
            ExpressionFilter::CreateNullCheckExpression(std::move(postal_code), ExpressionType::OPERATOR_IS_NULL));
        CHECK(BigquerySQL::TransformFilter("customer.info", null_filter) ==
              "`customer.info`.`address`.`postal.code` IS NULL");
    }
}

TEST_CASE("DML STRUCT filters resolve reordered and pruned scan projections", "[bigquery][filter]") {
    DuckDB database(nullptr);
    Connection connection(database);
    const auto customer_type = LogicalType::STRUCT(
        {{Identifier("name"), LogicalType::VARCHAR},
         {Identifier("address"), LogicalType::STRUCT({{Identifier("city"), LogicalType::VARCHAR}})}});
    const vector<pair<vector<ColumnIndex>, idx_t>> projections = {
        {{ColumnIndex(0), ColumnIndex(1), ColumnIndex(2)}, 2},
        {{ColumnIndex(1), ColumnIndex(2)}, 1},
        {{ColumnIndex(2)}, 0},
        {{ColumnIndex(2), ColumnIndex(0)}, 0},
        {{ColumnIndex(1), ColumnIndex(2, {ColumnIndex(0), ColumnIndex(1, {ColumnIndex(0)})})}, 1}};

    for (const auto &projection : projections) {
        for (const bool nested : {false, true}) {
            CAPTURE(projection.second, nested);
            auto get = FilterGet(customer_type, projection.first);
            auto column = make_uniq<BoundReferenceExpression>(customer_type, 0ULL);
            auto field = nested
                             ? StructField(connection, StructField(connection, std::move(column), 1, false), 0, false)
                             : StructField(connection, std::move(column), 0, false);
            auto predicate = BoundComparisonExpression::Create(
                ExpressionType::COMPARE_EQUAL,
                std::move(field),
                make_uniq<BoundConstantExpression>(Value(nested ? "New York" : "Bob Johnson")));
            get->table_filters.PushFilter(ProjectionIndex(projection.second),
                                          make_uniq<ExpressionFilter>(std::move(predicate)));
            const string expected = nested ? "(`customer_info`.`address`.`city` = \"New York\")"
                                           : "(`customer_info`.`name` = \"Bob Johnson\")";
            string filter_sql;
            REQUIRE(BigquerySQL::TryTransformLogicalGetFilters(*get, filter_sql));
            CHECK(filter_sql == expected);
            CHECK(BigquerySQL::ExtractFilters(*get) == expected);
        }
    }
}

TEST_CASE("DML preserves every required filter after projection remapping", "[bigquery][filter]") {
    auto get = FilterGet(LogicalType::INTEGER, {ColumnIndex(1), ColumnIndex(0)});
    get->table_filters.PushFilter(
        ProjectionIndex(0),
        ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(10)));
    get->table_filters.PushFilter(
        ProjectionIndex(1),
        ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(0)));
    string filter_sql;
    REQUIRE(BigquerySQL::TryTransformLogicalGetFilters(*get, filter_sql));
    CHECK(filter_sql == "(`marker` < 10) AND (`id` > 0)");
}

TEST_CASE("DML rejects unsupported scan projections instead of losing filters", "[bigquery][filter]") {
    const auto customer_type = LogicalType::STRUCT({{Identifier("name"), LogicalType::VARCHAR}});
    ColumnIndex extracted_field(2, {ColumnIndex(0)});
    extracted_field.SetPushdownExtractType(customer_type);
    const vector<pair<vector<ColumnIndex>, idx_t>> projections = {{{}, 0},
                                                                  {{ColumnIndex(2)}, 1},
                                                                  {{ColumnIndex(3)}, 0},
                                                                  {{ColumnIndex(COLUMN_IDENTIFIER_ROW_ID)}, 0},
                                                                  {{ColumnIndex(COLUMN_IDENTIFIER_EMPTY)}, 0},
                                                                  {{ColumnIndex(COLUMN_IDENTIFIER_ROW_NUMBER)}, 0},
                                                                  {{ColumnIndex(string("customer_info"))}, 0},
                                                                  {{extracted_field}, 0}};
    for (idx_t index = 0; index < projections.size(); index++) {
        CAPTURE(index);
        const auto &projection = projections[index];
        auto get = FilterGet(customer_type, projection.first);
        get->table_filters.PushFilter(
            ProjectionIndex(projection.second),
            ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(10)));
        string filter_sql;
        CHECK_FALSE(BigquerySQL::TryTransformLogicalGetFilters(*get, filter_sql));
        CHECK_THROWS_AS(BigquerySQL::ExtractFilters(*get), NotImplementedException);
    }
}

TEST_CASE("DML rejects filters that only exist in the multi-column filter set", "[bigquery][filter]") {
    auto get = FilterGet(LogicalType::INTEGER, {ColumnIndex(0), ColumnIndex(1)});
    auto predicate = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL,
                                                       make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL),
                                                       make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 1ULL));
    get->table_filters.PushMultiColumnFilter(
        make_uniq<ExpressionFilter>(std::move(predicate),
                                    vector<ProjectionIndex>{ProjectionIndex(0), ProjectionIndex(1)}));
    string filter_sql;
    CHECK_FALSE(BigquerySQL::TryTransformLogicalGetFilters(*get, filter_sql));
    CHECK_THROWS_AS(BigquerySQL::ExtractFilters(*get), NotImplementedException);
}
