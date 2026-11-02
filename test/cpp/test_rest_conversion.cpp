#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "bigquery_utils.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "google/protobuf/util/json_util.h"

using namespace duckdb;
using namespace duckdb::bigquery;

namespace {

static google::protobuf::Value ParseRestJSON(const string &json) {
    google::protobuf::Value result;
    const auto status = google::protobuf::util::JsonStringToMessage(json, &result);
    INFO(json);
    INFO(status.ToString());
    REQUIRE(status.ok());
    return result;
}

static Value RequireRestValue(const string &json, const Value &expected) {
    auto actual = BigqueryUtils::RestValueToValue(ParseRestJSON(json), expected.type());
    REQUIRE(actual.type() == expected.type());
    REQUIRE(Value::NotDistinctFrom(actual, expected));
    return actual;
}

} // namespace

TEST_CASE("REST arrays preserve typed NULL elements and their positions", "[bigquery][rest]") {
    auto actual = RequireRestValue(
        R"([{"v":"1"},{"v":null},{"v":"3"}])",
        Value::LIST(LogicalType::BIGINT, {Value::BIGINT(1), Value(LogicalType::BIGINT), Value::BIGINT(3)}));
    const auto &children = ListValue::GetChildren(actual);
    REQUIRE(children.size() == 3);
    CHECK(children[1].IsNull());
    CHECK(children[1].type() == LogicalType::BIGINT);
}

TEST_CASE("REST empty arrays, NULL arrays and arrays of NULLs stay distinct", "[bigquery][rest]") {
    RequireRestValue("[]", Value::LIST(LogicalType::BIGINT, {}));
    RequireRestValue("null", Value(LogicalType::LIST(LogicalType::BIGINT)));
    RequireRestValue(R"([{"v":null},{"v":null}])",
                     Value::LIST(LogicalType::BIGINT, {Value(LogicalType::BIGINT), Value(LogicalType::BIGINT)}));
}

TEST_CASE("REST string arrays distinguish NULL from the string NULL", "[bigquery][rest]") {
    RequireRestValue(R"([{"v":"NULL"},{"v":null},{"v":""}])",
                     Value::LIST(LogicalType::VARCHAR, {Value("NULL"), Value(LogicalType::VARCHAR), Value("")}));
}

TEST_CASE("REST STRUCT fields preserve NULLs inside arrays", "[bigquery][rest]") {
    const auto type = LogicalType::STRUCT(
        {{Identifier("id"), LogicalType::BIGINT}, {Identifier("items"), LogicalType::LIST(LogicalType::BIGINT)}});
    RequireRestValue(R"({"f":[{"v":null},{"v":[{"v":"1"},{"v":null},{"v":"3"}]}]})",
                     Value::STRUCT(type,
                                   {Value(LogicalType::BIGINT),
                                    Value::LIST(LogicalType::BIGINT,
                                                {Value::BIGINT(1), Value(LogicalType::BIGINT), Value::BIGINT(3)})}));
}

TEST_CASE("REST arrays of STRUCT preserve NULL records and fields", "[bigquery][rest]") {
    const auto type =
        LogicalType::STRUCT({{Identifier("id"), LogicalType::BIGINT}, {Identifier("label"), LogicalType::VARCHAR}});
    auto actual =
        RequireRestValue(R"([{"v":{"f":[{"v":null},{"v":"a"}]}},{"v":null},{"v":{"f":[{"v":"3"},{"v":null}]}}])",
                         Value::LIST(type,
                                     {Value::STRUCT(type, {Value(LogicalType::BIGINT), Value("a")}),
                                      Value(type),
                                      Value::STRUCT(type, {Value::BIGINT(3), Value(LogicalType::VARCHAR)})}));
    const auto &children = ListValue::GetChildren(actual);
    REQUIRE(children.size() == 3);
    CHECK(children[1].IsNull());
    CHECK(children[1].type() == type);
}

TEST_CASE("REST row batches preserve NULL array elements across rows", "[bigquery][rest]") {
    google::protobuf::RepeatedPtrField<google::protobuf::Struct> rows;
    *rows.Add() = ParseRestJSON(R"({"f":[{"v":[{"v":"99"}]},{"v":"skip"}]})").struct_value();
    *rows.Add() = ParseRestJSON(R"({"f":[{"v":[{"v":"1"},{"v":null},{"v":"3"}]},{"v":null}]})").struct_value();
    *rows.Add() = ParseRestJSON(R"({"f":[{"v":[]},{"v":"empty"}]})").struct_value();
    *rows.Add() = ParseRestJSON(R"({"f":[{"v":null},{"v":"null array"}]})").struct_value();

    const vector<LogicalType> types = {LogicalType::LIST(LogicalType::BIGINT), LogicalType::VARCHAR};
    DataChunk output;
    output.Initialize(Allocator::DefaultAllocator(), types);
    BigqueryUtils::FillChunkFromRestRows(rows, 1, 3, types, output);

    REQUIRE(output.size() == 3);
    CHECK(output.GetValue(0, 0).type() == types[0]);
    CHECK(Value::NotDistinctFrom(
        output.GetValue(0, 0),
        Value::LIST(LogicalType::BIGINT, {Value::BIGINT(1), Value(LogicalType::BIGINT), Value::BIGINT(3)})));
    CHECK(output.GetValue(1, 0).IsNull());
    CHECK(output.GetValue(1, 0).type() == LogicalType::VARCHAR);
    CHECK(Value::NotDistinctFrom(output.GetValue(0, 1), Value::LIST(LogicalType::BIGINT, {})));
    CHECK(Value::NotDistinctFrom(output.GetValue(1, 1), Value("empty")));
    CHECK(output.GetValue(0, 2).IsNull());
    CHECK(output.GetValue(0, 2).type() == types[0]);
    CHECK(Value::NotDistinctFrom(output.GetValue(1, 2), Value("null array")));
}

TEST_CASE("REST timestamps preserve signed integer microseconds", "[bigquery][rest][timestamp]") {
    const pair<const char *, int64_t> cases[] = {{"-62135596800000000", -62135596800000000LL},
                                                 {"-62135596799999999", -62135596799999999LL},
                                                 {"-1000001", -1000001},
                                                 {"-1000000", -1000000},
                                                 {"-1", -1},
                                                 {"-0", 0},
                                                 {"0", 0},
                                                 {"1", 1},
                                                 {"100000", 100000},
                                                 {"120000", 120000},
                                                 {"123000", 123000},
                                                 {"123400", 123400},
                                                 {"123450", 123450},
                                                 {"123456", 123456},
                                                 {"1123456", 1123456},
                                                 {"253402300799999998", 253402300799999998LL},
                                                 {"253402300799999999", 253402300799999999LL}};
    for (const auto &test : cases) {
        INFO(test.first);
        RequireRestValue("\"" + string(test.first) + "\"", Value::TIMESTAMP(timestamp_t(test.second)));
    }
}

TEST_CASE("REST timestamp microseconds reject malformed and out-of-range values", "[bigquery][rest][timestamp]") {
    const char *cases[] = {"",
                           "-",
                           "+",
                           "1.5",
                           "0.000001",
                           "1e6",
                           "NaN",
                           "inf",
                           "1x",
                           "1-2",
                           " 1",
                           "1 ",
                           "9223372036854775808",
                           "-9223372036854775809",
                           "-62135596800000001",
                           "253402300800000000"};
    for (const auto &test : cases) {
        INFO(test);
        auto value = ParseRestJSON("\"" + string(test) + "\"");
        REQUIRE_THROWS_AS(BigqueryUtils::RestValueToValue(value, LogicalType::TIMESTAMP), ConversionException);
    }
    REQUIRE_THROWS_AS(BigqueryUtils::RestValueToValue(ParseRestJSON("1"), LogicalType::TIMESTAMP), ConversionException);
}

TEST_CASE("REST ISO datetimes and timestamp NULLs retain their types and precision", "[bigquery][rest][timestamp]") {
    RequireRestValue(R"("0001-01-01T00:00:00.000001")", Value::TIMESTAMP(timestamp_t(-62135596799999999LL)));
    RequireRestValue(R"("2024-01-15T12:34:56.123456")", Value::TIMESTAMP(2024, 1, 15, 12, 34, 56, 123456));
    RequireRestValue(R"("2024-01-15 12:34:56.123456")", Value::TIMESTAMP(2024, 1, 15, 12, 34, 56, 123456));
    RequireRestValue(R"("2024-01-15")", Value::TIMESTAMP(2024, 1, 15, 0, 0, 0, 0));
    RequireRestValue(R"("9999-12-31T23:59:59.999999")", Value::TIMESTAMP(timestamp_t(253402300799999999LL)));
    RequireRestValue("null", Value(LogicalType::TIMESTAMP));
    RequireRestValue(R"("-1")", Value::TIMESTAMPTZ(timestamp_tz_t(-1)));
    RequireRestValue("null", Value(LogicalType::TIMESTAMP_TZ));
}
