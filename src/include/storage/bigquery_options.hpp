#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/access_mode.hpp"

namespace duckdb {
namespace bigquery {


struct BigqueryOptions {
    // access mode
    AccessMode access_mode = AccessMode::READ_WRITE;
};


} // namespace bigquery
} // namespace duckdb
