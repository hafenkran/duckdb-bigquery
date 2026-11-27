# Shared assertions for shorthand, named and overridden dataset connections.
# The caller attaches the mock dataset as shorthand.

query T
SELECT path FROM duckdb_databases() WHERE database_name = 'shorthand';
----
{BQ_TEST_PROJECT}

query T
SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'shorthand';
----
{BQ_TEST_DATASET}

statement ok
USE shorthand;

query TT
SELECT current_database(), current_schema();
----
shorthand	{BQ_TEST_DATASET}

query TTTTTT
DESCRIBE shorthand.{BQ_TEST_DATASET}.target;
----
answer	BIGINT	YES	NULL	NULL	NULL
label	VARCHAR	YES	NULL	NULL	NULL

statement ok
USE memory;

statement ok
DETACH shorthand;
