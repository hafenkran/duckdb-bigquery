#include "duckdb/common/types.hpp"

#include "bigquery_attach.hpp"
#include "bigquery_client.hpp"
#include "bigquery_utils.hpp"

#include <iostream>

namespace duckdb {
namespace bigquery {

struct AttachFunctionData : public TableFunctionData {
    AttachFunctionData() = default;

    bool finished = false;
    bool overwrite = false;

    string project_id;
    string dataset_id;
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context,
                                           TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types,
                                           vector<string> &names) {
    auto table_string = input.inputs[0].GetValue<string>();
    auto dataset_ref = BigqueryUtils::ParseTableString(table_string);
    if (dataset_ref.table_id.empty()) {
        throw ParserException("Invalid attach statement: %s", table_string);
    }

    auto result = make_uniq<AttachFunctionData>();
    result->project_id = std::move(dataset_ref.project_id);
    result->dataset_id = std::move(dataset_ref.dataset_id);

    for (auto &kv : input.named_parameters) {
        if (kv.first == "overwrite") {
            result->overwrite = kv.second.GetValue<bool>();
        }
    }

    return_types.emplace_back(duckdb::LogicalTypeId::BOOLEAN);
    names.emplace_back("Success");
    return std::move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<AttachFunctionData>();
    if (data.finished) {
        return;
    }

    auto client = BigqueryClient(data.project_id);
    auto dconn = Connection(context.db->GetDatabase(context));
    {
        auto tables_ref = client.GetTables(data.dataset_id);
        for (auto &table_ref : tables_ref) {
            auto table = BigqueryUtils::FormatTableStringSimple(table_ref);
            dconn.TableFunction("bigquery_scan", {Value(table)})->CreateView(table, data.overwrite, false);
        }
    }

    data.finished = true;
}

BigqueryAttachFunction::BigqueryAttachFunction()
    : TableFunction("bigquery_attach", {LogicalType::VARCHAR}, AttachFunction, AttachBind) {
    named_parameters["overwrite"] = LogicalTypeId::BOOLEAN;
}

} // namespace bigquery
} // namespace duckdb
