#include "bigquery_geography_winding.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <cstring>
#include <vector>

namespace duckdb {
namespace bigquery {

struct Vertex {
    double values[4] = {0, 0, 0, 0};
};

using Ring = vector<Vertex>;

struct Polygon {
    uint32_t meta;
    vector<Ring> rings;
};

class GeometryBlobReader {
public:
    GeometryBlobReader(const char *data, uint32_t size) : begin(data), pos(data), end(data + size) {
    }

    template <class T>
    T Read() {
        if (pos + sizeof(T) > end) {
            throw InvalidInputException("Unexpected end of geometry data at position %zu", pos - begin);
        }
        T value;
        memcpy(&value, pos, sizeof(T));
        value = BSwapIfBE(value);
        pos += sizeof(T);
        return value;
    }

private:
    const char *begin;
    const char *pos;
    const char *end;
};

class GeometryBlobWriter {
public:
    GeometryBlobWriter(char *data, uint32_t size) : begin(data), pos(data), end(data + size) {
    }

    template <class T>
    void Write(const T &value) {
        if (pos + sizeof(T) > end) {
            throw InternalException("Writing beyond end of geometry buffer at position %zu", pos - begin);
        }
        auto stored = BSwapIfBE(value);
        memcpy(pos, &stored, sizeof(T));
        pos += sizeof(T);
    }

    void Finalize() const {
        D_ASSERT(pos == end);
    }

private:
    char *begin;
    char *pos;
    char *end;
};

static idx_t VertexDimensions(VertexType type) {
    switch (type) {
    case VertexType::XY:
        return 2;
    case VertexType::XYZ:
    case VertexType::XYM:
        return 3;
    case VertexType::XYZM:
        return 4;
    default:
        throw InvalidInputException("Unsupported vertex type %d", static_cast<int>(type));
    }
}

static double SignedArea(const Ring &ring) {
    double area = 0.0;
    const auto n = ring.size();
    for (idx_t i = 0; i < n; i++) {
        const auto j = (i + 1) % n;
        area += (ring[j].values[0] - ring[i].values[0]) * (ring[j].values[1] + ring[i].values[1]);
    }
    return area;
}

static bool ReverseRingIfNeeded(Ring &ring, bool should_be_ccw) {
    if (ring.size() < 4) {
        return false;
    }

    const auto area = SignedArea(ring);
    const bool is_ccw = area < 0;
    if (is_ccw == should_be_ccw) {
        return false;
    }

    std::reverse(ring.begin() + 1, ring.end() - 1);
    return true;
}

static bool FixPolygonWinding(Polygon &polygon) {
    if (polygon.rings.empty()) {
        return false;
    }

    bool changed = ReverseRingIfNeeded(polygon.rings[0], true);
    for (idx_t ring_idx = 1; ring_idx < polygon.rings.size(); ring_idx++) {
        changed |= ReverseRingIfNeeded(polygon.rings[ring_idx], false);
    }
    return changed;
}

static Ring ReadRing(GeometryBlobReader &reader, idx_t dimensions) {
    Ring ring;
    const auto vertex_count = reader.Read<uint32_t>();
    ring.reserve(vertex_count);

    for (uint32_t vertex_idx = 0; vertex_idx < vertex_count; vertex_idx++) {
        Vertex vertex;
        for (idx_t dim_idx = 0; dim_idx < dimensions; dim_idx++) {
            vertex.values[dim_idx] = reader.Read<double>();
        }
        ring.push_back(vertex);
    }

    return ring;
}

static void WriteRing(GeometryBlobWriter &writer, const Ring &ring, idx_t dimensions) {
    writer.Write<uint32_t>(UnsafeNumericCast<uint32_t>(ring.size()));
    for (const auto &vertex : ring) {
        for (idx_t dim_idx = 0; dim_idx < dimensions; dim_idx++) {
            writer.Write<double>(vertex.values[dim_idx]);
        }
    }
}

static Polygon ReadPolygonBody(GeometryBlobReader &reader, idx_t dimensions, uint32_t meta) {
    Polygon polygon;
    polygon.meta = meta;

    const auto ring_count = reader.Read<uint32_t>();
    polygon.rings.reserve(ring_count);
    for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
        polygon.rings.push_back(ReadRing(reader, dimensions));
    }
    return polygon;
}

static void WritePolygonBody(GeometryBlobWriter &writer, const Polygon &polygon, idx_t dimensions) {
    writer.Write<uint32_t>(UnsafeNumericCast<uint32_t>(polygon.rings.size()));
    for (const auto &ring : polygon.rings) {
        WriteRing(writer, ring, dimensions);
    }
}

static bool NormalizePolygonGeometry(const string_t &input_geom,
                                     string_t &result_geom,
                                     Vector &result_vector,
                                     idx_t dimensions) {
    GeometryBlobReader reader(input_geom.GetData(), UnsafeNumericCast<uint32_t>(input_geom.GetSize()));

    const auto byte_order = reader.Read<uint8_t>();
    if (byte_order != 1) {
        throw InvalidInputException("Unsupported byte order %d in geometry", byte_order);
    }

    const auto meta = reader.Read<uint32_t>();
    auto polygon = ReadPolygonBody(reader, dimensions, meta);
    if (!FixPolygonWinding(polygon)) {
        result_geom = input_geom;
        return false;
    }

    auto blob = StringVector::EmptyString(result_vector, input_geom.GetSize());
    GeometryBlobWriter writer(blob.GetDataWriteable(), UnsafeNumericCast<uint32_t>(blob.GetSize()));
    writer.Write<uint8_t>(byte_order);
    writer.Write<uint32_t>(polygon.meta);
    WritePolygonBody(writer, polygon, dimensions);
    writer.Finalize();
    blob.Finalize();

    result_geom = blob;
    return true;
}

static bool NormalizeMultiPolygonGeometry(const string_t &input_geom,
                                          string_t &result_geom,
                                          Vector &result_vector,
                                          idx_t dimensions) {
    GeometryBlobReader reader(input_geom.GetData(), UnsafeNumericCast<uint32_t>(input_geom.GetSize()));

    const auto byte_order = reader.Read<uint8_t>();
    if (byte_order != 1) {
        throw InvalidInputException("Unsupported byte order %d in geometry", byte_order);
    }

    const auto meta = reader.Read<uint32_t>();
    const auto polygon_count = reader.Read<uint32_t>();

    vector<Polygon> polygons;
    polygons.reserve(polygon_count);

    bool changed = false;
    for (uint32_t polygon_idx = 0; polygon_idx < polygon_count; polygon_idx++) {
        const auto polygon_byte_order = reader.Read<uint8_t>();
        if (polygon_byte_order != 1) {
            throw InvalidInputException("Unsupported polygon byte order %d in multipolygon", polygon_byte_order);
        }

        const auto polygon_meta = reader.Read<uint32_t>();
        auto polygon = ReadPolygonBody(reader, dimensions, polygon_meta);
        changed |= FixPolygonWinding(polygon);
        polygons.push_back(std::move(polygon));
    }

    if (!changed) {
        result_geom = input_geom;
        return false;
    }

    auto blob = StringVector::EmptyString(result_vector, input_geom.GetSize());
    GeometryBlobWriter writer(blob.GetDataWriteable(), UnsafeNumericCast<uint32_t>(blob.GetSize()));
    writer.Write<uint8_t>(byte_order);
    writer.Write<uint32_t>(meta);
    writer.Write<uint32_t>(polygon_count);

    for (const auto &polygon : polygons) {
        writer.Write<uint8_t>(1);
        writer.Write<uint32_t>(polygon.meta);
        WritePolygonBody(writer, polygon, dimensions);
    }

    writer.Finalize();
    blob.Finalize();

    result_geom = blob;
    return true;
}

bool ForcePolygonCCW(const string_t &input_geom, string_t &result_geom, Vector &result_vector) {
    const auto type_info = Geometry::GetType(input_geom);
    const auto dimensions = VertexDimensions(type_info.second);

    switch (type_info.first) {
    case GeometryType::POLYGON:
        return NormalizePolygonGeometry(input_geom, result_geom, result_vector, dimensions);
    case GeometryType::MULTIPOLYGON:
        return NormalizeMultiPolygonGeometry(input_geom, result_geom, result_vector, dimensions);
    default:
        result_geom = input_geom;
        return false;
    }
}

void BqForcePolygonCCWFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    const auto count = args.size();

    UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](const string_t &input_geom) {
        string_t output_geom;
        ForcePolygonCCW(input_geom, output_geom, result);
        return output_geom;
    });

    StringVector::AddHeapReference(input, result);
}

} // namespace bigquery
} // namespace duckdb
