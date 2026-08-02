# Working with Geometries

Since DuckDB 1.5, [`GEOMETRY` is a built-in DuckDB
type](https://duckdb.org/docs/current/sql/data_types/geometry). The BigQuery
extension can therefore expose BigQuery `GEOGRAPHY` columns as native DuckDB
geometry and write geometry values back without loading another extension or
enabling a BigQuery-specific setting.

The type itself is built in, but most functions for spatial analysis and CRS
transformation still come from DuckDB's
[`spatial` extension](https://duckdb.org/docs/stable/core_extensions/spatial/overview).
Load `spatial` when those functions are needed; it is not required merely to
read, store, or write a `GEOMETRY` value.

!!! note "DuckDB versions before 1.5"

    Older DuckDB versions obtained `GEOMETRY` from `spatial`, and older builds
    of this extension used `bq_geography_as_geometry` to opt into the mapping.
    That setting is no longer used; the current mapping is automatic.

## Geometry and Geography

DuckDB and BigQuery use different names because they model different
semantics:

| DuckDB `GEOMETRY` | BigQuery `GEOGRAPHY` |
| --- | --- |
| A general geometry in a Cartesian coordinate space | A point set on the surface of the Earth |
| Can carry an optional coordinate reference system | Uses WGS84 longitude and latitude |
| The execution engine treats coordinates as Cartesian | Edges are spherical geodesics between vertices |
| Can represent data in geographic or projected CRSs | Does not represent arbitrary projected CRSs |

BigQuery documents these semantics under
[Working with geospatial data](https://cloud.google.com/bigquery/docs/geospatial-data#coordinate_systems_and_edges).
They matter even when both systems display the same WKT. A line between two
distant vertices is straight in Cartesian coordinates but follows a geodesic
in BigQuery. Areas, lengths, distances, polygons crossing the antimeridian, and
shapes near the poles can therefore produce different results.

## Type Mapping

The extension uses this bidirectional mapping:

| Direction | Source | Destination |
| --- | --- | --- |
| Read | BigQuery `GEOGRAPHY` | DuckDB `GEOMETRY('OGC:CRS84')` |
| Create or write | DuckDB `GEOMETRY` | BigQuery `GEOGRAPHY` |

`OGC:CRS84` identifies WGS84 coordinates with `X` as longitude and `Y` as
latitude. This matches BigQuery's coordinate order and makes the expected CRS
visible in the DuckDB type.

WKT is used at the API boundary. On reads, the extension receives the
BigQuery geography representation as text, parses it into DuckDB's native
geometry format, and assigns `OGC:CRS84`. On writes, it converts the DuckDB
geometry to WKT and sends that text through the Storage Write API as the
BigQuery `GEOGRAPHY` field value.

The common Simple Features shapes can cross this boundary: `POINT`,
`LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, and
`GEOMETRYCOLLECTION`.

## Read Geography

Both attached table scans and `bigquery_scan` return `GEOGRAPHY` columns as
CRS-aware `GEOMETRY` values. The default `bigquery_query` path and its optional
REST result path use the same logical type mapping.

=== "Project ID"

    ```sql
    -- Read a BigQuery GEOGRAPHY column without creating a catalog.
    SELECT
          place_id,
          ST_AsWKT(geom) AS wkt,
          typeof(geom) AS duckdb_type,
          ST_CRS(geom) AS crs
      FROM bigquery_scan('my-gcp-project.my_dataset.places')
      LIMIT 2;
    ┌──────────┬──────────────────────────────┬───────────────────────────┬───────────┐
    │ place_id │ wkt                          │ duckdb_type               │ crs       │
    │ int64    │ varchar                      │ varchar                   │ varchar   │
    ├──────────┼──────────────────────────────┼───────────────────────────┼───────────┤
    │        1 │ POINT (-0.1276 51.5072)      │ GEOMETRY('OGC:CRS84')     │ OGC:CRS84 │
    │        2 │ LINESTRING (-0.2 51.5, 0 52) │ GEOMETRY('OGC:CRS84')     │ OGC:CRS84 │
    └──────────┴──────────────────────────────┴───────────────────────────┴───────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the dataset containing the GEOGRAPHY column.
    ATTACH 'project=my-gcp-project dataset=my_dataset'
      AS bq (TYPE bigquery, READ_ONLY);

    -- Read the GEOGRAPHY column through the attached catalog.
    SELECT
          place_id,
          ST_AsWKT(geom) AS wkt,
          typeof(geom) AS duckdb_type,
          ST_CRS(geom) AS crs
      FROM bq.my_dataset.places
      LIMIT 2;
    ┌──────────┬──────────────────────────────┬───────────────────────────┬───────────┐
    │ place_id │ wkt                          │ duckdb_type               │ crs       │
    │ int64    │ varchar                      │ varchar                   │ varchar   │
    ├──────────┼──────────────────────────────┼───────────────────────────┼───────────┤
    │        1 │ POINT (-0.1276 51.5072)      │ GEOMETRY('OGC:CRS84')     │ OGC:CRS84 │
    │        2 │ LINESTRING (-0.2 51.5, 0 52) │ GEOMETRY('OGC:CRS84')     │ OGC:CRS84 │
    └──────────┴──────────────────────────────┴───────────────────────────┴───────────┘
    ```

`ST_AsWKT` and `ST_CRS` are built-in DuckDB geometry functions. This inspection
therefore does not require `spatial`.

## Write Geometry

A DuckDB `GEOMETRY` column becomes a BigQuery `GEOGRAPHY` column when an
attached table is created. Use `OGC:CRS84` values so the coordinates already
have the semantics BigQuery expects:

```sql
-- Create a BigQuery table with a GEOGRAPHY destination column.
CREATE TABLE bq.my_dataset.places (
      place_id BIGINT,
      geom GEOMETRY
  );

-- Write longitude first and latitude second.
INSERT INTO bq.my_dataset.places
VALUES (
      1,
      'POINT(-0.1276 51.5072)'::GEOMETRY('OGC:CRS84')
  );
```

The attached `INSERT` completes without returning a result row. Query the
destination table separately when the written geometry needs to be verified.

The extension does not transform coordinates when writing. A value in a
projected CRS such as `EPSG:3857` must be transformed to `OGC:CRS84` first;
simply changing or omitting its CRS would leave the numeric coordinates
unchanged and BigQuery would interpret them as longitude and latitude.

Use `spatial` for an actual transformation:

```sql
-- Install and load spatial functions for CRS transformation.
INSTALL spatial;
LOAD spatial;

-- Create projected source data with coordinates in EPSG:3857.
CREATE TEMP TABLE local_projected_places AS
SELECT
      place_id,
      ST_Transform(geom, 'EPSG:3857') AS geom
  FROM (VALUES
      (2, 'POINT(2.3522 48.8566)'::GEOMETRY('OGC:CRS84')),
      (3, 'POINT(13.4050 52.5200)'::GEOMETRY('OGC:CRS84'))
  ) AS places(place_id, geom);

-- Transform projected coordinates before writing them to BigQuery.
INSERT INTO bq.my_dataset.places
SELECT
      place_id,
      ST_Transform(geom, 'OGC:CRS84')
  FROM local_projected_places;

-- Verify the rows after the INSERT completes without a result row.
SELECT
      place_id,
      round(ST_X(geom), 4) AS longitude,
      round(ST_Y(geom), 4) AS latitude
  FROM bq.my_dataset.places
  WHERE place_id IN (2, 3)
  ORDER BY place_id;
┌──────────┬───────────┬──────────┐
│ place_id │ longitude │ latitude │
│ int64    │ double    │ double   │
├──────────┼───────────┼──────────┤
│        2 │    2.3522 │  48.8566 │
│        3 │    13.405 │    52.52 │
└──────────┴───────────┴──────────┘
```

## Polygon Normalization

Polygon orientation is more significant on a sphere than on a plane because a
closed boundary can describe either side of the Earth. Before every Storage
Write operation, the extension automatically normalizes `POLYGON` and
`MULTIPOLYGON` values for the BigQuery write path:

- exterior rings are oriented counterclockwise;
- interior rings are oriented clockwise; and
- touching hole rings that share boundary segments are merged when the result
  is unambiguous.

Other geometry shapes pass through the normalization step unchanged. Preview
the normalized geometry locally with `bigquery_normalize_geography`:

```sql
-- Preview the polygon that the Storage Write path will serialize.
SELECT bigquery_normalize_geography(
      'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'
          ::GEOMETRY('OGC:CRS84')
  ) AS normalized;
┌──────────────────────────────────────────┐
│                normalized                │
│                 geometry                 │
├──────────────────────────────────────────┤
│ POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))  │
└──────────────────────────────────────────┘
```

The function runs entirely in DuckDB and does not submit a BigQuery request.
It is a targeted write normalization step, not a general geometry validator or
repair tool. BigQuery can still reject self-intersections, invalid coordinates,
ambiguous topology, and other invalid geography input. See the
[`bigquery_normalize_geography` reference](../function-reference/bigquery-normalize-geography.md)
for its contract.

## Choose Where Spatial Operations Run

Spatial expressions in ordinary DuckDB SQL execute locally after the geometry
has been read. Load `spatial` for functions such as intersections, distances,
areas, and transformations:

```sql
-- Install and load spatial functions for local predicates.
INSTALL spatial;
LOAD spatial;

-- Evaluate the spatial predicate locally in DuckDB.
SELECT place_id
  FROM bq.my_dataset.places
  WHERE ST_Intersects(
      geom,
      'POLYGON((-1 50, 1 50, 1 52, -1 52, -1 50))'
          ::GEOMETRY('OGC:CRS84')
  );
┌──────────┐
│ place_id │
│ int64    │
├──────────┤
│        1 │
└──────────┘
```

Use `bigquery_query` when BigQuery should evaluate its own geography functions
with BigQuery's spherical semantics:

=== "Project ID"

    ```sql
    -- Let BigQuery calculate a spherical distance in meters.
    SELECT distance_m
      FROM bigquery_query(
          'my-gcp-project',
          'SELECT ST_DISTANCE(
               ST_GEOGPOINT(-0.1276, 51.5072),
               ST_GEOGPOINT(2.3522, 48.8566)
           ) AS distance_m'
      );
    ┌────────────────────┐
    │ distance_m         │
    │ double             │
    ├────────────────────┤
    │ 343556.06034104165 │
    └────────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the project whose query configuration should be reused.
    ATTACH 'project=my-gcp-project'
      AS bq (TYPE bigquery, READ_ONLY);

    -- Run the same geography calculation through the attached catalog.
    SELECT distance_m
      FROM bigquery_query(
          'bq',
          'SELECT ST_DISTANCE(
               ST_GEOGPOINT(-0.1276, 51.5072),
               ST_GEOGPOINT(2.3522, 48.8566)
           ) AS distance_m'
      );
    ┌────────────────────┐
    │ distance_m         │
    │ double             │
    ├────────────────────┤
    │ 343556.06034104165 │
    └────────────────────┘
    ```

The returned scalar is a regular DuckDB value. If the GoogleSQL result itself
contains `GEOGRAPHY`, the extension maps it back to
`GEOMETRY('OGC:CRS84')`.

## Semantic Boundaries

Keep these boundaries in mind when moving geometries between both systems:

- WKT does not carry CRS metadata. The extension assigns `OGC:CRS84` when
  reading BigQuery and assumes compatible longitude/latitude coordinates when
  writing.
- The extension does not tessellate long planar edges before BigQuery
  interprets them as geodesics.
- Local DuckDB geometry calculations use Cartesian execution semantics unless
  a function explicitly provides different behavior. BigQuery geography
  functions use their documented spherical or spheroidal semantics.
- Automatic polygon normalization does not replace validation in BigQuery.
  Large polygons, antimeridian crossings, polar data, and complex collections
  should be checked after a round trip.

For the other type mappings, see [Data Type Mapping](../concepts/data-types.md). Continue
with [Writing & Modifying Data](writing.md) for insert and CTAS
workflows, or [Additional Settings](configuration.md) for Storage Read and
Storage Write configuration.
