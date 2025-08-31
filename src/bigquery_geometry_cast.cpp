#include "bigquery_geometry_cast.hpp"
#include "bigquery_settings.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <iostream>

namespace duckdb {
namespace bigquery {

struct WKTGeomCastData : public BoundCastData {
	WKTGeomCastData() : has_function(false),
						resolved_function({LogicalType::VARCHAR}, LogicalType::BLOB, nullptr) {
		target_type = LogicalType::BLOB;
		target_type.SetAlias("GEOMETRY");
		source_type = LogicalType::VARCHAR;
		source_type.SetAlias("WKT");
		function_return_type = target_type;
	}

	unique_ptr<BoundCastData> Copy() const override {
		auto result = make_uniq<WKTGeomCastData>();
		result->has_function = has_function;
		result->resolved_function = resolved_function;
		result->bind_data = bind_data ? bind_data->Copy() : nullptr;
		result->function_return_type = function_return_type;
		result->target_type = target_type;
		result->source_type = source_type;
		return result;
	}

	bool has_function;
	ScalarFunction resolved_function;
	unique_ptr<FunctionData> bind_data;
	LogicalType function_return_type;
	LogicalType target_type;
	LogicalType source_type;
};

struct WKTGeomCastLocalState : public FunctionLocalState {
    explicit WKTGeomCastLocalState(ClientContext &context_p) : context(context_p) {
    }

    ClientContext &context;
    DataChunk input_chunk;
    DataChunk output_chunk;
    unique_ptr<ExpressionExecutor> executor;
    unique_ptr<BoundFunctionExpression> expression;
};

static unique_ptr<FunctionLocalState> InitWKTGeomCastLocalState(CastLocalStateParameters &params) {
    if (!params.context) {
        return nullptr; // no context -> fall back to NOP behavior
    }
    auto &cast_data = params.cast_data->Cast<WKTGeomCastData>();
    if (!cast_data.has_function) {
        return nullptr; // nothing to do
    }

    auto state = make_uniq<WKTGeomCastLocalState>(*params.context);
    // Initialize input chunk with original source_type (preserve alias GEOGRAPHY)
    state->input_chunk.Initialize(Allocator::Get(*params.context), {cast_data.source_type});
    // Output chunk uses the function return type; we will cast to target if they differ
    state->output_chunk.Initialize(Allocator::Get(*params.context), {cast_data.function_return_type});

	vector<unique_ptr<Expression>> args;
    args.push_back(make_uniq<BoundReferenceExpression>(cast_data.source_type, 0));
    state->expression = make_uniq<BoundFunctionExpression>(cast_data.function_return_type,
                                                           cast_data.resolved_function,
                                                           std::move(args),
                                                           cast_data.bind_data ? cast_data.bind_data->Copy() : nullptr);
    state->executor = make_uniq<ExpressionExecutor>(*params.context);
    state->executor->AddExpression(*state->expression);
    return std::move(state);
}

static bool ExecuteWKTGeomCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    auto &cdata = parameters.cast_data->Cast<WKTGeomCastData>();
    if (!cdata.has_function) {
        result.Reference(source);
        return true;
    }
    if (!parameters.local_state) {
        VectorOperations::Copy(source, result, count, 0, 0);
        return true;
    }
    auto &lstate = parameters.local_state->Cast<WKTGeomCastLocalState>();
    lstate.input_chunk.data[0].Reference(source);
    lstate.input_chunk.SetCardinality(count);
    lstate.executor->Execute(lstate.input_chunk, lstate.output_chunk);
    auto &func_result = lstate.output_chunk.data[0];
    if (func_result.GetType() == result.GetType()) {
        result.Reference(func_result);
    } else if (cdata.function_return_type == cdata.target_type) {
        result.Reference(func_result);
    } else {
        auto &client_ctx = lstate.context; // get ClientContext from local state
        VectorOperations::Cast(client_ctx, func_result, result, count);
    }
    return true;
}

static BoundCastInfo BindWKTGeomCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
    auto state = make_uniq<WKTGeomCastData>();
    state->target_type = target;
    state->source_type = source;
    auto context = input.context;

    if (!context) {
        return BoundCastInfo(DefaultCasts::NopCast, std::move(state));
    }
    if (!BigquerySettings::GeographyAsGeometry()) {
        return BoundCastInfo(DefaultCasts::NopCast, std::move(state));
    }
    if (!BigquerySettings::IsSpatialExtensionLoaded(*context)) {
        return BoundCastInfo(DefaultCasts::NopCast, std::move(state));
    }
    try {
        auto &catalog = Catalog::GetSystemCatalog(*context);
        auto &func_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(*context, DEFAULT_SCHEMA, "ST_GeomFromText");
        vector<LogicalType> args{LogicalType(LogicalTypeId::VARCHAR)};

        auto func = func_entry.functions.GetFunctionByArguments(*context, args);
        state->resolved_function = func;
        state->function_return_type = func.return_type; // store real return type

        if (func.bind) {
            vector<unique_ptr<Expression>> dummy_args;
            dummy_args.push_back(make_uniq<BoundReferenceExpression>(LogicalType(LogicalTypeId::VARCHAR), 0));
            state->bind_data = func.bind(*context, state->resolved_function, dummy_args);
        }

        state->has_function = true;
        return BoundCastInfo(ExecuteWKTGeomCast, std::move(state), InitWKTGeomCastLocalState);
    } catch (Exception &ex) {
        return BoundCastInfo(DefaultCasts::NopCast, std::move(state));
    } catch (...) {
        return BoundCastInfo(DefaultCasts::NopCast, std::move(state));
    }
}

void RegisterWKTGeometryCast(DatabaseInstance &db) {
    auto &casts = DBConfig::GetConfig(db).GetCastFunctions();
    LogicalType source = LogicalType(LogicalTypeId::VARCHAR);
    source.SetAlias("GEOGRAPHY");
    LogicalType target = LogicalType(LogicalTypeId::BLOB);
    target.SetAlias("GEOMETRY");
    casts.RegisterCastFunction(source, target, BindWKTGeomCast);
}

} // namespace bigquery
} // namespace duckdb
