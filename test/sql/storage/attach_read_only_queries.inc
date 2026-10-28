# Shared SQL assertions for live and local-mock READ_ONLY attachments.
# The caller provides bq_ro, bq_rw, BQ_TEST_PROJECT and BQ_TEST_DATASET.

foreach rest false true

query I
SELECT answer FROM bigquery_query('bq_ro', 'SELECT 42 AS answer', use_rest_api := {rest});
----
42

query I
SELECT answer FROM bigquery_query('bq_ro', 'SELECT ? AS answer', 42, use_rest_api := {rest});
----
42

statement error
SELECT * FROM bigquery_query('bq_ro', 'DELETE FROM `{BQ_TEST_PROJECT}.{BQ_TEST_DATASET}.read_only_guard` WHERE true', use_rest_api := {rest});
----
read-only

statement error
SELECT * FROM bigquery_query('bq_ro', 'INSERT INTO `{BQ_TEST_PROJECT}.{BQ_TEST_DATASET}.read_only_guard` VALUES (2)', use_rest_api := {rest});
----
read-only

statement error
SELECT * FROM bigquery_query('bq_ro', 'UPDATE `{BQ_TEST_PROJECT}.{BQ_TEST_DATASET}.read_only_guard` SET i=2 WHERE true', use_rest_api := {rest});
----
read-only

statement error
SELECT * FROM bigquery_query('bq_ro', 'CREATE TABLE `{BQ_TEST_PROJECT}.{BQ_TEST_DATASET}.read_only_new_table` AS SELECT 42 AS answer', use_rest_api := {rest});
----
read-only

statement error
SELECT * FROM bigquery_query('bq_ro', 'DROP TABLE `{BQ_TEST_PROJECT}.{BQ_TEST_DATASET}.read_only_guard`', use_rest_api := {rest});
----
read-only

statement error
SELECT * FROM bigquery_query('bq_ro', 'DELETE FROM `{BQ_TEST_PROJECT}.{BQ_TEST_DATASET}.read_only_guard` WHERE true; SELECT 42 AS answer', use_rest_api := {rest});
----
read-only

statement error
SELECT * FROM bigquery_query('bq_ro', 'DROP TABLE `{BQ_TEST_PROJECT}.{BQ_TEST_DATASET}.read_only_guard`; SELECT 42 AS answer', use_rest_api := {rest});
----
read-only

query I
SELECT total_bytes_processed >= 0 FROM bigquery_query('bq_ro', 'DELETE FROM `{BQ_TEST_PROJECT}.{BQ_TEST_DATASET}.read_only_guard` WHERE true', dry_run := true, use_rest_api := {rest});
----
true

query I
SELECT answer FROM bigquery_query('bq_rw', 'DELETE FROM `{BQ_TEST_PROJECT}.{BQ_TEST_DATASET}.read_only_guard` WHERE false; SELECT 42 AS answer', use_rest_api := {rest});
----
42

endloop
