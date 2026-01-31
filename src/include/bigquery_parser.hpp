#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryState : public ClientContextState {
public:
    explicit BigqueryState(unique_ptr<ParserExtensionParseData> parse_data) : parse_data(std::move(parse_data)) {
    }

    void QueryEnd() override {
        parse_data.reset();
    }

    unique_ptr<ParserExtensionParseData> parse_data;
};

struct BigqueryParseData : ParserExtensionParseData {
    unique_ptr<SQLStatement> statement;
    unordered_map<string, string> options;
    vector<string> partition_by;
    vector<string> cluster_by;

    BigqueryParseData(unique_ptr<SQLStatement> statement,
                      unordered_map<string, string> options,
                      vector<string> partition_by,
                      vector<string> cluster_by)
        : statement(std::move(statement)), options(std::move(options)), partition_by(std::move(partition_by)),
          cluster_by(std::move(cluster_by)) {
    }

    string ToString() const override {
        return "BigQueryParseData";
    }

    unique_ptr<ParserExtensionParseData> Copy() const override {
        return make_uniq_base<ParserExtensionParseData, BigqueryParseData>(statement->Copy(),
                                                                           options,
                                                                           partition_by,
                                                                           cluster_by);
    }
};

ParserExtensionParseResult BigqueryParse(ParserExtensionInfo *info, const std::string &query);

ParserExtensionPlanResult BigqueryPlan(ParserExtensionInfo *info,
                                       ClientContext &context,
                                       unique_ptr<ParserExtensionParseData> parse_data);

struct BigqueryParserExtension : public ParserExtension {
    BigqueryParserExtension() : ParserExtension() {
        parse_function = BigqueryParse;
        plan_function = BigqueryPlan;
    }
};

BoundStatement BigqueryBind(ClientContext &context,
                            Binder &binder,
                            OperatorExtensionInfo *info,
                            SQLStatement &statement);

struct BigqueryOperatorExtension : public OperatorExtension {
    BigqueryOperatorExtension() : OperatorExtension() {
        Bind = BigqueryBind;
    }

    std::string GetName() override {
        return "bigquery";
    }

    unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override {
        throw InternalException("bigquery operator should not be serialized");
    }
};

} // namespace bigquery
} // namespace duckdb


// CREATE OR REPLACE TABLE bq3.some_dataset.some_awesome_table2(i INTEGER)
// 	OPTIONS (
// 		hey='hallo',
// 		wurst='würstchen'
// 	);
