#include "bigquery_geometry_cast.hpp"

#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {
namespace bigquery {

static BoundCastInfo BindWKTGeomCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
    return DefaultCasts::GetDefaultCastFunction(input, source, target);
}

void RegisterWKTGeometryCast(DatabaseInstance &db) {
    auto &casts = DBConfig::GetConfig(db).GetCastFunctions();
    LogicalType source = LogicalType(LogicalTypeId::VARCHAR);
    source.SetAlias("GEOGRAPHY");
    // BigQuery uses WGS84 (OGC:CRS84) as the default CRS for GEOGRAPHY
    LogicalType target = LogicalType::GEOMETRY("OGC:CRS84");
    casts.RegisterCastFunction(source, target, BindWKTGeomCast);
}

} // namespace bigquery
} // namespace duckdb
