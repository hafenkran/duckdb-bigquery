#include "bigquery_geography_winding.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <cmath>
#include <sstream>
#include <vector>

namespace duckdb {
namespace bigquery {

// ─── Coordinate & Ring Helpers ──────────────────────────────────────────────

struct Coord {
    double x;
    double y;
    string original; // preserve exact text representation
};

/// Compute signed area of a ring using the shoelace formula.
/// Positive = CW, Negative = CCW (in a Y-up coordinate system).
static double SignedArea(const vector<Coord> &ring) {
    double area = 0.0;
    size_t n = ring.size();
    for (size_t i = 0; i < n; i++) {
        size_t j = (i + 1) % n;
        area += (ring[j].x - ring[i].x) * (ring[j].y + ring[i].y);
    }
    return area;
}

/// Reverse the coordinate order of a ring, keeping the closing point == opening point.
static void ReverseRing(vector<Coord> &ring) {
    if (ring.size() < 4) {
        return; // need at least 3 unique points + closing point
    }
    // Keep first and last point (they're the same), reverse interior points
    std::reverse(ring.begin() + 1, ring.end() - 1);
}

// ─── WKT Parsing Helpers ────────────────────────────────────────────────────

/// Skip whitespace in a string starting at pos.
static void SkipWS(const string &s, size_t &pos) {
    while (pos < s.size() && std::isspace(static_cast<unsigned char>(s[pos]))) {
        pos++;
    }
}

/// Parse a single coordinate pair "x y" from the WKT string.
/// Preserves the original text representation to avoid floating point precision loss.
static Coord ParseCoord(const string &s, size_t &pos) {
    SkipWS(s, pos);
    Coord c;
    size_t start = pos;

    // Parse X
    while (pos < s.size() && s[pos] != ' ' && s[pos] != ',' && s[pos] != ')') {
        pos++;
    }
    string x_str = s.substr(start, pos - start);
    c.x = std::stod(x_str);

    // Skip space between X and Y
    SkipWS(s, pos);

    start = pos;
    // Parse Y
    while (pos < s.size() && s[pos] != ' ' && s[pos] != ',' && s[pos] != ')') {
        pos++;
    }
    string y_str = s.substr(start, pos - start);
    c.y = std::stod(y_str);

    // Skip optional Z/M values
    while (pos < s.size() && s[pos] != ',' && s[pos] != ')') {
        pos++;
    }

    // Preserve original text representation to avoid floating point precision loss
    c.original = x_str + " " + y_str;

    return c;
}

/// Parse a ring "(x1 y1, x2 y2, ...)" from the WKT string.
static vector<Coord> ParseRing(const string &s, size_t &pos) {
    vector<Coord> ring;
    SkipWS(s, pos);
    if (pos < s.size() && s[pos] == '(') {
        pos++; // skip '('
    }

    while (pos < s.size()) {
        SkipWS(s, pos);
        if (s[pos] == ')') {
            pos++; // skip ')'
            break;
        }
        ring.push_back(ParseCoord(s, pos));
        SkipWS(s, pos);
        if (pos < s.size() && s[pos] == ',') {
            pos++; // skip ','
        }
    }

    return ring;
}

/// Reconstruct a ring as WKT text from coordinates.
static string RingToWKT(const vector<Coord> &ring) {
    string result = "(";
    for (size_t i = 0; i < ring.size(); i++) {
        if (i > 0) {
            result += ", ";
        }
        result += ring[i].original;
    }
    result += ")";
    return result;
}

/// Fix a single polygon's ring winding: exterior CCW, holes CW.
/// rings[0] = exterior, rings[1..n] = holes.
static void FixPolygonWinding(vector<vector<Coord>> &rings) {
    if (rings.empty()) {
        return;
    }

    // Exterior ring: must be CCW (negative signed area)
    double ext_area = SignedArea(rings[0]);
    if (ext_area > 0) {
        // CW → reverse to CCW
        ReverseRing(rings[0]);
    }

    // Holes: must be CW (positive signed area)
    for (size_t i = 1; i < rings.size(); i++) {
        double hole_area = SignedArea(rings[i]);
        if (hole_area < 0) {
            // CCW → reverse to CW
            ReverseRing(rings[i]);
        }
    }
}

// ─── Top-level WKT Processing ───────────────────────────────────────────────

/// Process a POLYGON WKT: fix ring winding and return corrected WKT.
static string ProcessPolygon(const string &s, size_t &pos) {
    SkipWS(s, pos);
    if (pos < s.size() && s[pos] == '(') {
        pos++; // skip outer '('
    }

    vector<vector<Coord>> rings;
    while (pos < s.size()) {
        SkipWS(s, pos);
        if (s[pos] == ')') {
            pos++; // skip outer ')'
            break;
        }
        rings.push_back(ParseRing(s, pos));
        SkipWS(s, pos);
        if (pos < s.size() && s[pos] == ',') {
            pos++; // skip ',' between rings
        }
    }

    FixPolygonWinding(rings);

    string result = "(";
    for (size_t i = 0; i < rings.size(); i++) {
        if (i > 0) {
            result += ", ";
        }
        result += RingToWKT(rings[i]);
    }
    result += ")";
    return result;
}

string ForcePolygonCCW(const string &wkt) {
    if (wkt.empty()) {
        return wkt;
    }

    // Find the geometry type prefix
    size_t pos = 0;
    SkipWS(wkt, pos);

    // Check for MULTIPOLYGON
    if (wkt.compare(pos, 12, "MULTIPOLYGON") == 0) {
        pos += 12;
        SkipWS(wkt, pos);

        if (pos >= wkt.size() || wkt[pos] != '(') {
            return wkt; // malformed, return as-is
        }
        pos++; // skip outer '('

        string result = "MULTIPOLYGON (";
        bool first = true;

        while (pos < wkt.size()) {
            SkipWS(wkt, pos);
            if (wkt[pos] == ')') {
                pos++; // skip outer ')'
                break;
            }
            if (!first) {
                result += ", ";
            }
            first = false;

            result += ProcessPolygon(wkt, pos);

            SkipWS(wkt, pos);
            if (pos < wkt.size() && wkt[pos] == ',') {
                pos++; // skip ',' between polygons
            }
        }

        result += ")";
        return result;
    }

    // Check for POLYGON
    if (wkt.compare(pos, 7, "POLYGON") == 0) {
        pos += 7;
        SkipWS(wkt, pos);

        string result = "POLYGON ";
        result += ProcessPolygon(wkt, pos);
        return result;
    }

    // Not a polygon type — pass through unchanged
    return wkt;
}

// ─── DuckDB Scalar Function ─────────────────────────────────────────────────

void BqForcePolygonCCWFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto count = args.size();

    UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input_wkt) {
        auto wkt_str = input_wkt.GetString();
        auto fixed = ForcePolygonCCW(wkt_str);
        return StringVector::AddString(result, fixed);
    });
}

} // namespace bigquery
} // namespace duckdb
