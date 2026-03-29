#include "bigquery_geography_winding.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <functional>
#include <unordered_map>
#include <utility>
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

struct VertexKey {
    uint64_t x_bits;
    uint64_t y_bits;

    bool operator==(const VertexKey &other) const {
        return x_bits == other.x_bits && y_bits == other.y_bits;
    }
};

struct VertexKeyHash {
    size_t operator()(const VertexKey &key) const {
        auto seed = std::hash<uint64_t> {}(key.x_bits);
        seed ^= std::hash<uint64_t> {}(key.y_bits) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        return seed;
    }
};

struct CanonicalEdgeKey {
    VertexKey first;
    VertexKey second;

    bool operator==(const CanonicalEdgeKey &other) const {
        return first == other.first && second == other.second;
    }
};

struct CanonicalEdgeKeyHash {
    size_t operator()(const CanonicalEdgeKey &key) const {
        auto seed = VertexKeyHash {}(key.first);
        seed ^= VertexKeyHash {}(key.second) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        return seed;
    }
};

struct DirectedEdge {
    Vertex from;
    Vertex to;
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

static uint64_t DoubleBits(double value) {
    uint64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    return bits;
}

static VertexKey GetVertexKey(const Vertex &vertex) {
    return {DoubleBits(vertex.values[0]), DoubleBits(vertex.values[1])};
}

static bool VertexKeyLess(const VertexKey &lhs, const VertexKey &rhs) {
    if (lhs.x_bits != rhs.x_bits) {
        return lhs.x_bits < rhs.x_bits;
    }
    return lhs.y_bits < rhs.y_bits;
}

static bool SameVertex(const Vertex &lhs, const Vertex &rhs) {
    return GetVertexKey(lhs) == GetVertexKey(rhs);
}

static CanonicalEdgeKey GetCanonicalEdgeKey(const Vertex &lhs, const Vertex &rhs) {
    auto lhs_key = GetVertexKey(lhs);
    auto rhs_key = GetVertexKey(rhs);
    if (VertexKeyLess(rhs_key, lhs_key)) {
        std::swap(lhs_key, rhs_key);
    }
    return {lhs_key, rhs_key};
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

static void ReverseRing(Ring &ring) {
    if (ring.size() < 4) {
        return;
    }
    std::reverse(ring.begin() + 1, ring.end() - 1);
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

    ReverseRing(ring);
    return true;
}

static double EdgeLengthSquared(const DirectedEdge &edge) {
    auto dx = edge.to.values[0] - edge.from.values[0];
    auto dy = edge.to.values[1] - edge.from.values[1];
    return dx * dx + dy * dy;
}

static bool LiesInInteriorOfSegment(const Vertex &point, const DirectedEdge &edge) {
    if (SameVertex(point, edge.from) || SameVertex(point, edge.to)) {
        return false;
    }

    auto dx = edge.to.values[0] - edge.from.values[0];
    auto dy = edge.to.values[1] - edge.from.values[1];
    auto px = point.values[0] - edge.from.values[0];
    auto py = point.values[1] - edge.from.values[1];
    auto edge_length_squared = EdgeLengthSquared(edge);
    if (edge_length_squared == 0) {
        return false;
    }

    auto cross = dx * py - dy * px;
    auto tolerance = 1e-12 * std::max(1.0, edge_length_squared);
    if (std::fabs(cross) > tolerance) {
        return false;
    }

    auto dot = px * dx + py * dy;
    return dot > tolerance && dot < edge_length_squared - tolerance;
}

static void EnsureClosedRing(Ring &ring) {
    if (ring.empty()) {
        return;
    }
    if (!SameVertex(ring.front(), ring.back())) {
        ring.push_back(ring.front());
    }
}

static void NormalizeRingStart(Ring &ring) {
    if (ring.size() < 4) {
        return;
    }

    idx_t best_idx = 0;
    for (idx_t i = 1; i + 1 < ring.size(); i++) {
        if (VertexKeyLess(GetVertexKey(ring[i]), GetVertexKey(ring[best_idx]))) {
            best_idx = i;
        }
    }
    if (best_idx == 0) {
        return;
    }

    Ring rotated;
    rotated.reserve(ring.size());
    for (idx_t i = 0; i + 1 < ring.size(); i++) {
        auto src_idx = (best_idx + i) % (ring.size() - 1);
        rotated.push_back(ring[src_idx]);
    }
    rotated.push_back(rotated.front());
    ring = std::move(rotated);
}

static void ForceHoleClockwise(Ring &ring) {
    EnsureClosedRing(ring);
    if (SignedArea(ring) < 0) {
        ReverseRing(ring);
    }
}

static void ExtractRingEdges(const Ring &ring, vector<DirectedEdge> &edges) {
    if (ring.size() < 2) {
        return;
    }
    for (idx_t i = 0; i + 1 < ring.size(); i++) {
        if (SameVertex(ring[i], ring[i + 1])) {
            continue;
        }
        edges.push_back({ring[i], ring[i + 1]});
    }
}

static void SplitEdgesAtIntermediateHoleVertices(vector<DirectedEdge> &edges, const vector<Vertex> &hole_vertices) {
    if (edges.empty() || hole_vertices.empty()) {
        return;
    }

    vector<DirectedEdge> split_edges;
    split_edges.reserve(edges.size());
    for (const auto &edge : edges) {
        vector<std::pair<double, Vertex>> split_points;
        split_points.reserve(hole_vertices.size());
        for (const auto &candidate : hole_vertices) {
            if (!LiesInInteriorOfSegment(candidate, edge)) {
                continue;
            }

            auto dx = edge.to.values[0] - edge.from.values[0];
            auto dy = edge.to.values[1] - edge.from.values[1];
            auto px = candidate.values[0] - edge.from.values[0];
            auto py = candidate.values[1] - edge.from.values[1];
            auto edge_length_squared = EdgeLengthSquared(edge);
            auto distance_along_edge = (px * dx + py * dy) / edge_length_squared;
            split_points.emplace_back(distance_along_edge, candidate);
        }

        if (split_points.empty()) {
            split_edges.push_back(edge);
            continue;
        }

        std::sort(split_points.begin(), split_points.end(), [](const auto &lhs, const auto &rhs) {
            if (lhs.first != rhs.first) {
                return lhs.first < rhs.first;
            }
            return VertexKeyLess(GetVertexKey(lhs.second), GetVertexKey(rhs.second));
        });

        Ring ordered_points;
        ordered_points.reserve(split_points.size() + 2);
        ordered_points.push_back(edge.from);
        for (const auto &split_point : split_points) {
            if (SameVertex(ordered_points.back(), split_point.second)) {
                continue;
            }
            ordered_points.push_back(split_point.second);
        }
        ordered_points.push_back(edge.to);

        for (idx_t i = 0; i + 1 < ordered_points.size(); i++) {
            if (SameVertex(ordered_points[i], ordered_points[i + 1])) {
                continue;
            }
            split_edges.push_back({ordered_points[i], ordered_points[i + 1]});
        }
    }

    edges = std::move(split_edges);
}

static bool IsOppositeEdge(const DirectedEdge &lhs, const DirectedEdge &rhs) {
    return SameVertex(lhs.from, rhs.to) && SameVertex(lhs.to, rhs.from);
}

static bool TryMergeTouchingHoleRings(vector<Ring> &rings) {
    if (rings.size() <= 2) {
        return false;
    }

    vector<DirectedEdge> edges;
    std::unordered_map<VertexKey, Vertex, VertexKeyHash> unique_hole_vertices;
    for (idx_t ring_idx = 1; ring_idx < rings.size(); ring_idx++) {
        auto hole = rings[ring_idx];
        EnsureClosedRing(hole);
        ForceHoleClockwise(hole);
        for (idx_t vertex_idx = 0; vertex_idx + 1 < hole.size(); vertex_idx++) {
            unique_hole_vertices.emplace(GetVertexKey(hole[vertex_idx]), hole[vertex_idx]);
        }
        ExtractRingEdges(hole, edges);
    }
    if (edges.empty()) {
        return false;
    }

    vector<Vertex> hole_vertices;
    hole_vertices.reserve(unique_hole_vertices.size());
    for (const auto &entry : unique_hole_vertices) {
        hole_vertices.push_back(entry.second);
    }
    SplitEdgesAtIntermediateHoleVertices(edges, hole_vertices);

    std::unordered_map<CanonicalEdgeKey, vector<idx_t>, CanonicalEdgeKeyHash> unmatched_by_key;
    vector<bool> cancelled(edges.size(), false);
    bool merged = false;

    for (idx_t edge_idx = 0; edge_idx < edges.size(); edge_idx++) {
        auto key = GetCanonicalEdgeKey(edges[edge_idx].from, edges[edge_idx].to);
        auto &bucket = unmatched_by_key[key];

        bool found_opposite = false;
        for (idx_t bucket_pos = 0; bucket_pos < bucket.size(); bucket_pos++) {
            auto candidate_idx = bucket[bucket_pos];
            if (cancelled[candidate_idx]) {
                continue;
            }
            if (IsOppositeEdge(edges[candidate_idx], edges[edge_idx])) {
                cancelled[candidate_idx] = true;
                cancelled[edge_idx] = true;
                bucket.erase(bucket.begin() + bucket_pos);
                merged = true;
                found_opposite = true;
                break;
            }
        }
        if (!found_opposite) {
            bucket.push_back(edge_idx);
        }
    }

    if (!merged) {
        return false;
    }

    vector<DirectedEdge> remaining_edges;
    remaining_edges.reserve(edges.size());
    for (idx_t i = 0; i < edges.size(); i++) {
        if (!cancelled[i]) {
            remaining_edges.push_back(edges[i]);
        }
    }

    std::unordered_map<VertexKey, vector<idx_t>, VertexKeyHash> outgoing_edges;
    std::unordered_map<VertexKey, idx_t, VertexKeyHash> incoming_degree;
    std::unordered_map<VertexKey, idx_t, VertexKeyHash> outgoing_degree;
    for (idx_t i = 0; i < remaining_edges.size(); i++) {
        auto from_key = GetVertexKey(remaining_edges[i].from);
        auto to_key = GetVertexKey(remaining_edges[i].to);
        outgoing_edges[from_key].push_back(i);
        outgoing_degree[from_key]++;
        incoming_degree[to_key]++;
    }

    for (const auto &entry : outgoing_degree) {
        auto incoming = incoming_degree.find(entry.first);
        if (incoming == incoming_degree.end() || incoming->second != entry.second || entry.second != 1) {
            throw InvalidInputException(
                "Cannot normalize polygon holes for BigQuery GEOGRAPHY: merged hole graph is not a simple ring");
        }
    }
    for (const auto &entry : incoming_degree) {
        auto outgoing = outgoing_degree.find(entry.first);
        if (outgoing == outgoing_degree.end() || outgoing->second != entry.second || entry.second != 1) {
            throw InvalidInputException(
                "Cannot normalize polygon holes for BigQuery GEOGRAPHY: merged hole graph is not a simple ring");
        }
    }

    vector<bool> visited(remaining_edges.size(), false);
    vector<Ring> rebuilt_holes;
    rebuilt_holes.reserve(rings.size() - 1);
    for (idx_t start_idx = 0; start_idx < remaining_edges.size(); start_idx++) {
        if (visited[start_idx]) {
            continue;
        }

        Ring ring;
        ring.reserve(remaining_edges.size() + 1);
        ring.push_back(remaining_edges[start_idx].from);

        auto start_vertex = GetVertexKey(remaining_edges[start_idx].from);
        idx_t current_idx = start_idx;
        while (true) {
            if (visited[current_idx]) {
                throw InvalidInputException(
                    "Cannot normalize polygon holes for BigQuery GEOGRAPHY: encountered reused edge while stitching");
            }

            visited[current_idx] = true;
            ring.push_back(remaining_edges[current_idx].to);

            auto current_vertex = GetVertexKey(remaining_edges[current_idx].to);
            if (current_vertex == start_vertex) {
                break;
            }

            auto next_it = outgoing_edges.find(current_vertex);
            if (next_it == outgoing_edges.end() || next_it->second.size() != 1) {
                throw InvalidInputException(
                    "Cannot normalize polygon holes for BigQuery GEOGRAPHY: merged hole graph has ambiguous branch");
            }
            current_idx = next_it->second[0];
        }

        EnsureClosedRing(ring);
        if (ring.size() < 4) {
            throw InvalidInputException(
                "Cannot normalize polygon holes for BigQuery GEOGRAPHY: merged hole ring is degenerate");
        }

        ForceHoleClockwise(ring);
        NormalizeRingStart(ring);
        rebuilt_holes.push_back(std::move(ring));
    }

    rings.erase(rings.begin() + 1, rings.end());
    for (auto &hole : rebuilt_holes) {
        rings.push_back(std::move(hole));
    }
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

    if (TryMergeTouchingHoleRings(polygon.rings)) {
        changed = true;
        changed |= ReverseRingIfNeeded(polygon.rings[0], true);
        for (idx_t ring_idx = 1; ring_idx < polygon.rings.size(); ring_idx++) {
            changed |= ReverseRingIfNeeded(polygon.rings[ring_idx], false);
        }
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

static idx_t SerializedRingSize(const Ring &ring, idx_t dimensions) {
    return sizeof(uint32_t) + ring.size() * dimensions * sizeof(double);
}

static idx_t SerializedPolygonBodySize(const Polygon &polygon, idx_t dimensions) {
    idx_t size = sizeof(uint32_t);
    for (const auto &ring : polygon.rings) {
        size += SerializedRingSize(ring, dimensions);
    }
    return size;
}

static idx_t SerializedPolygonGeometrySize(const Polygon &polygon, idx_t dimensions) {
    return sizeof(uint8_t) + sizeof(uint32_t) + SerializedPolygonBodySize(polygon, dimensions);
}

static idx_t SerializedMultiPolygonGeometrySize(const vector<Polygon> &polygons, idx_t dimensions) {
    idx_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
    for (const auto &polygon : polygons) {
        size += sizeof(uint8_t) + sizeof(uint32_t) + SerializedPolygonBodySize(polygon, dimensions);
    }
    return size;
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

    auto blob = StringVector::EmptyString(result_vector, SerializedPolygonGeometrySize(polygon, dimensions));
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

    auto blob = StringVector::EmptyString(result_vector, SerializedMultiPolygonGeometrySize(polygons, dimensions));
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

static string_t NormalizeBigQueryGeographyWKTValue(const string_t &input_wkt, Vector &result_vector) {
    if (input_wkt.GetSize() == 0) {
        return StringVector::AddString(result_vector, "", 0);
    }

    Vector geometry_vector(LogicalType::GEOMETRY());
    string_t geometry_value;
    Geometry::FromString(input_wkt, geometry_value, geometry_vector, true);

    string_t normalized_geometry;
    ForcePolygonCCW(geometry_value, normalized_geometry, geometry_vector);
    return Geometry::ToString(result_vector, normalized_geometry);
}

string NormalizeBigQueryGeographyWKT(const string &wkt) {
    if (wkt.empty()) {
        return wkt;
    }

    Vector result_vector(LogicalType::VARCHAR);
    auto input_wkt = string_t(wkt.c_str(), UnsafeNumericCast<uint32_t>(wkt.size()));
    return NormalizeBigQueryGeographyWKTValue(input_wkt, result_vector).GetString();
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

void BqNormalizeGeographyWKTFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    const auto count = args.size();

    UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](const string_t &input_wkt) {
        return NormalizeBigQueryGeographyWKTValue(input_wkt, result);
    });
}

} // namespace bigquery
} // namespace duckdb
