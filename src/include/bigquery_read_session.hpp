#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

struct BigqueryReadSessionStreamLimits {
    idx_t max_stream_count;
    idx_t preferred_min_stream_count;
};

} // namespace bigquery
} // namespace duckdb
