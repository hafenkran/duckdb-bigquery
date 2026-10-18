#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryOptimizer {
public:
    static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
    static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace bigquery
} // namespace duckdb
