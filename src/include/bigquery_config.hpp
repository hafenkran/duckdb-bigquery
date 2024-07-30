#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryConfig {
public:
	static string& DefaultLocation() {
		static string bigquery_default_location = "US";
		return bigquery_default_location;
	}

	static void SetDefaultLocation(ClientContext &context, SetScope scope, Value &parameter){
		DefaultLocation() = StringValue::Get(parameter);
	}

	static bool& DebugQueryPrint() {
		static bool bigquery_debug_query_print = false;
		return bigquery_debug_query_print;
	}

	static void SetDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
		DebugQueryPrint() = BooleanValue::Get(parameter);
	}

	static bool& ExperimentalFilterPushdown() {
		static bool bigquery_experimental_filter_pushdown = true;
		return bigquery_experimental_filter_pushdown;
	}

	static void SetExperimentalFilterPushdown(ClientContext &context, SetScope scope, Value &parameter) {
		ExperimentalFilterPushdown() = BooleanValue::Get(parameter);
	}
};


} // namespace bigquery
} // namespace duckdb
