# /// script
# requires-python = ">=3.10"
# dependencies = ["google-cloud-bigquery-storage>=2,<3", "pyarrow>=20,<21"]
# ///

"""Verify cloud access guards through SQL and recorded loopback HTTP/gRPC requests.

Use run_mock_tests.py --suite access with an existing matching build. All credentials
are synthetic; metadata, OAuth and Storage Read endpoints run on loopback.
"""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mock_bigquery import DATASET, PROJECT, LocalBigQuery


class SQLLogicTests(unittest.TestCase):
    def test_sqllogic_from_relocated_checkout(self):
        with (
            tempfile.TemporaryDirectory(prefix="bigquery relocated checkout ") as directory,
            LocalBigQuery() as service,
        ):
            root = Path(directory)
            test_file = root / "test/sql/misc/relocated.test"
            test_file.parent.mkdir(parents=True)
            test_file.write_text("query I\nSELECT 42\n----\n42\n", encoding="utf-8")
            with patch("mock_bigquery.ROOT", root):
                service.run_sqllogic("misc/relocated.test", {})

    def run_test_file(self, service, test_file):
        return service.run_sqllogic(
            test_file,
            {
                "BQ_TEST_PROJECT": PROJECT,
                "BQ_TEST_DATASET": DATASET,
                "BQ_GUARD_MOCK_API_ENDPOINT": service.endpoint,
                "BQ_GUARD_MOCK_GRPC_ENDPOINT": f"127.0.0.1:{service.grpc_port}",
            },
        )

    def test_external_access_sqllogic(self):
        with LocalBigQuery(reject_auth=True) as service:
            self.run_test_file(service, "misc/bigquery_external_access.test")
            warmup = [
                i
                for i, (_, path, payload) in enumerate(service.requests)
                if path.endswith("/queries")
                and payload.get("query") == "SELECT 42 AS answer -- external_access_setup"
                and not payload.get("dryRun")
            ]
            self.assertEqual(len(warmup), 1, service.requests)
            self.assertEqual(service.requests[warmup[0] + 1 :], [])
            self.assertEqual(service.grpc_requests, [])

    def test_read_only_sqllogic(self):
        def statement_type(query):
            query = query.strip().upper()
            if ";" in query:
                return "SCRIPT"
            if query.startswith("CREATE TABLE "):
                return "CREATE_TABLE_AS_SELECT"
            if query.startswith("DROP TABLE "):
                return "DROP_TABLE"
            return query.split()[0]

        with LocalBigQuery(statement_type=statement_type) as service:
            self.run_test_file(service, "storage/attach_read_only_query_mock.test")
            executions = service.execution_requests()
            self.assertEqual(len(executions), 6, executions)
            for _, _, payload in executions:
                query = payload["query"].strip()
                self.assertTrue(
                    query.startswith("SELECT ")
                    or query == f"DELETE FROM `{PROJECT}.{DATASET}.read_only_guard` WHERE false; SELECT 42 AS answer",
                    query,
                )
            validations = [payload for _, path, payload in service.requests if path.endswith("/jobs") and payload]
            self.assertEqual(len(validations), 18, validations)
            self.assertTrue(all(payload["configuration"].get("dryRun") for payload in validations))
            self.assertEqual([name for name, _ in service.grpc_requests].count("CreateReadSession"), 3)
            self.assertEqual([name for name, _ in service.grpc_requests].count("ReadRows"), 3)


class ExternalAccessTests(unittest.TestCase):
    def assert_denied(self, result, service):
        self.assertEqual(service.requests, [], result.stderr)
        self.assertEqual(service.grpc_requests, [], result.stderr)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Permission Error", result.stderr)

    def test_direct_query_sends_no_requests(self):
        for rest in (True, False):
            with self.subTest(rest=rest), LocalBigQuery() as service:
                result = service.run_sql(
                    "SET enable_external_access=false;\n" + service.query_sql(rest=rest, attached=False)
                )
                self.assert_denied(result, service)

    def test_existing_attachment_sends_no_requests(self):
        operations = {
            "query_rest": lambda s: s.query_sql(),
            "query_storage": lambda s: s.query_sql(rest=False),
            "dry_run": lambda s: s.query_sql(extra=", dry_run := true"),
            "execute": lambda s: "SELECT * FROM bigquery_execute('guarded', 'SELECT 42 AS answer');",
            "execute_to_table": lambda s: (
                "SELECT * FROM bigquery_execute('guarded', 'SELECT 42 AS answer', "
                f"destination_table := '{DATASET}.target');"
            ),
            "jobs": lambda s: "SELECT * FROM bigquery_jobs('guarded');",
            "job_by_id": lambda s: "SELECT * FROM bigquery_jobs('guarded', jobId := 'mock_query');",
            "load": lambda s: (
                f"SELECT * FROM bigquery_load('guarded', '{DATASET}.target', "
                "source_uris := ['gs://synthetic-bucket/input.parquet']);"
            ),
            "extract": lambda s: (
                f"SELECT * FROM bigquery_extract('guarded', source_table := '{DATASET}.target', "
                "destination_uris := ['gs://synthetic-bucket/output.parquet'], format := 'PARQUET');"
            ),
            "catalog_scan": lambda s: f"SELECT * FROM guarded.{DATASET}.target;",
            "attach": lambda s: s.attach_sql().replace("AS guarded", "AS another"),
        }
        for name, operation in operations.items():
            with self.subTest(operation=name), LocalBigQuery() as service:
                result = service.run_sql(
                    service.attach_sql() + "SET enable_external_access=false;\n" + operation(service)
                )
                self.assert_denied(result, service)

    def test_direct_functions_send_no_requests(self):
        operations = {
            "scan": lambda s: (
                f"SELECT * FROM bigquery_scan('{PROJECT}.{DATASET}.target', "
                f"api_endpoint := '{s.endpoint}', grpc_endpoint := '127.0.0.1:{s.grpc_port}');"
            ),
            "execute": lambda s: (
                f"SELECT * FROM bigquery_execute('{PROJECT}', 'SELECT 42 AS answer', api_endpoint := '{s.endpoint}');"
            ),
            "jobs": lambda s: f"SELECT * FROM bigquery_jobs('{PROJECT}', api_endpoint := '{s.endpoint}');",
            "extract": lambda s: (
                f"SELECT * FROM bigquery_extract('{PROJECT}', source_table := '{DATASET}.target', "
                "destination_uris := ['gs://synthetic-bucket/output.parquet'], format := 'PARQUET', "
                f"api_endpoint := '{s.endpoint}');"
            ),
        }
        for name, operation in operations.items():
            with self.subTest(operation=name), LocalBigQuery() as service:
                result = service.run_sql("SET enable_external_access=false;\n" + operation(service))
                self.assert_denied(result, service)

    def test_functions_without_endpoint_options_do_not_authenticate(self):
        operations = (
            f"SELECT * FROM bigquery_attach('{PROJECT}.{DATASET}');",
            f"SELECT * FROM bigquery_load('{PROJECT}', '{DATASET}.target', "
            "source_uris := ['gs://synthetic-bucket/input.parquet']);",
        )
        for operation in operations:
            with self.subTest(operation=operation), LocalBigQuery(reject_auth=True) as service:
                result = service.run_sql("SET enable_external_access=false;\n" + operation, oauth=True)
                self.assert_denied(result, service)

    def test_no_oauth_request_when_disabled(self):
        with LocalBigQuery() as service:
            result = service.run_sql(
                "SET enable_external_access=false;\n" + service.query_sql(attached=False), oauth=True
            )
            self.assert_denied(result, service)

    def test_prepared_query_rechecks_external_access(self):
        for rest in (True, False):
            with self.subTest(rest=rest), LocalBigQuery() as service:
                result = service.run_sql(
                    service.attach_sql()
                    + "PREPARE guarded_query AS "
                    + service.query_sql(rest=rest)
                    + "SET enable_external_access=false;\nEXECUTE guarded_query;"
                )
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("Permission Error", result.stderr)
                self.assertEqual(len(service.requests), 1, service.requests)
                self.assertTrue(service.requests[0][2].get("dryRun"), service.requests)
                self.assertEqual(service.execution_requests(), [])
                self.assertEqual(service.grpc_requests, [])

    def test_cached_authentication_rechecks_external_access(self):
        with LocalBigQuery() as service:
            warmup = "SELECT 42 AS answer -- warmup"
            result = service.run_sql(
                service.attach_sql()
                + "BEGIN;\n"
                + service.query_sql(warmup)
                + "SET enable_external_access=false;\n"
                + service.query_sql()
            )
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("Permission Error", result.stderr)
            self.assertEqual(len(service.requests), 2, service.requests)
            self.assertTrue(all(request[2].get("query") == warmup for request in service.requests))

    def test_prepared_scan_rechecks_external_access(self):
        with LocalBigQuery() as service:
            result = service.run_sql(
                f"PREPARE guarded_scan AS SELECT * FROM bigquery_scan('{PROJECT}.{DATASET}.target', "
                f"api_endpoint := '{service.endpoint}', grpc_endpoint := '127.0.0.1:{service.grpc_port}');\n"
                "SET enable_external_access=false;\nEXECUTE guarded_scan;"
            )
            self.assertEqual(len(service.requests), 1, service.requests)
            self.assertEqual(service.requests[0][0], "GET")
            self.assertEqual(service.grpc_requests, [])
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("Permission Error", result.stderr)

    def test_enabled_access_still_authenticates_and_reads(self):
        with LocalBigQuery() as service:
            result = service.run_sql(service.query_sql(attached=False), oauth=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('"answer":42', result.stdout)
            self.assertTrue(any(path == "/token" for _, path, _ in service.requests))
            self.assertEqual(len(service.execution_requests()), 1)

    def test_local_utilities_remain_usable(self):
        with LocalBigQuery() as service:
            result = service.run_sql(
                service.attach_sql() + "SET enable_external_access=false;\n"
                "SELECT * FROM bigquery_clear_cache();\n"
                "SELECT bigquery_normalize_geography('POINT(1 2)'::GEOMETRY('OGC:CRS84')) IS NOT NULL AS normalized;"
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('"normalized":true', result.stdout)
            self.assertEqual(service.requests, [])


class ReadOnlyTests(unittest.TestCase):
    def assert_read_only_denied(self, result, service):
        self.assertEqual(service.execution_requests(), [], result.stderr)
        self.assertEqual(service.grpc_requests, [], result.stderr)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("read-only", result.stderr)

    def test_script_never_executes_through_read_only(self):
        scripts = (
            f"DELETE FROM `{PROJECT}.{DATASET}.target` WHERE true; SELECT 42 AS answer",
            f"DROP TABLE `{PROJECT}.{DATASET}.target`; SELECT 42 AS answer",
            "EXECUTE IMMEDIATE 'DELETE FROM target WHERE true'; SELECT 42 AS answer",
        )
        for query in scripts:
            for rest in (True, False):
                with self.subTest(query=query, rest=rest), LocalBigQuery(statement_type="SCRIPT") as service:
                    result = service.run_sql(service.attach_sql(read_only=True) + service.query_sql(query, rest))
                    self.assert_read_only_denied(result, service)

    def test_dml_and_ddl_never_execute_through_read_only(self):
        statements = {
            "DELETE": f"DELETE FROM `{PROJECT}.{DATASET}.target` WHERE true",
            "INSERT": f"INSERT INTO `{PROJECT}.{DATASET}.target` VALUES (42)",
            "UPDATE": f"UPDATE `{PROJECT}.{DATASET}.target` SET answer=42 WHERE true",
            "MERGE": f"MERGE `{PROJECT}.{DATASET}.target` t USING (SELECT 42 AS answer) s "
            "ON false WHEN NOT MATCHED THEN INSERT (answer) VALUES (s.answer)",
            "CREATE_TABLE_AS_SELECT": f"CREATE TABLE `{PROJECT}.{DATASET}.new_table` AS SELECT 42 AS answer",
            "DROP_TABLE": f"DROP TABLE `{PROJECT}.{DATASET}.target`",
            "CALL": f"CALL `{PROJECT}.{DATASET}.mutating_procedure`()",
            "EXPORT_DATA": "EXPORT DATA OPTIONS(uri='gs://synthetic-bucket/out-*.csv', format='CSV') AS SELECT 42",
        }
        for statement_type, query in statements.items():
            for rest in (True, False):
                with self.subTest(statement=statement_type, rest=rest), LocalBigQuery(statement_type) as service:
                    result = service.run_sql(service.attach_sql(read_only=True) + service.query_sql(query, rest))
                    self.assert_read_only_denied(result, service)

    def test_unknown_statement_type_fails_closed(self):
        for statement_type in (None, "FUTURE_STATEMENT"):
            for rest in (True, False):
                with self.subTest(statement=statement_type, rest=rest), LocalBigQuery(statement_type) as service:
                    result = service.run_sql(service.attach_sql(read_only=True) + service.query_sql(rest=rest))
                    self.assert_read_only_denied(result, service)

    def test_validation_error_never_executes_query(self):
        with LocalBigQuery(validation_error=True) as service:
            result = service.run_sql(service.attach_sql(read_only=True) + service.query_sql())
            self.assertEqual(service.execution_requests(), [])
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("synthetic validation error", result.stderr)

    def test_read_only_dry_run_does_not_execute_mutations(self):
        with LocalBigQuery(statement_type="SCRIPT") as service:
            result = service.run_sql(
                service.attach_sql(read_only=True)
                + service.query_sql("DELETE FROM target WHERE true; SELECT 42 AS answer", extra=", dry_run := true")
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(service.execution_requests(), [])
            self.assertEqual(len(service.requests), 1)
            self.assertTrue(service.requests[0][2].get("dryRun"))

    def test_other_mutating_functions_remain_blocked(self):
        operations = (
            "SELECT * FROM bigquery_execute('guarded', 'DELETE FROM target WHERE true');",
            f"SELECT * FROM bigquery_load('guarded', '{DATASET}.target', "
            "source_uris := ['gs://synthetic-bucket/input.parquet']);",
            f"SELECT * FROM bigquery_extract('guarded', source_table := '{DATASET}.target', "
            "destination_uris := ['gs://synthetic-bucket/output.parquet'], format := 'PARQUET');",
        )
        for operation in operations:
            with self.subTest(operation=operation), LocalBigQuery() as service:
                result = service.run_sql(service.attach_sql(read_only=True) + operation)
                self.assert_read_only_denied(result, service)

    def test_read_only_job_listing_remains_allowed(self):
        with LocalBigQuery() as service:
            result = service.run_sql(service.attach_sql(read_only=True) + "SELECT * FROM bigquery_jobs('guarded');")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(service.requests), 1)
            self.assertEqual(service.requests[0][0], "GET")
            self.assertTrue(service.requests[0][1].endswith("/jobs"))

    def test_writable_attachment_keeps_scripts(self):
        for rest in (True, False):
            with self.subTest(rest=rest), LocalBigQuery(statement_type="SCRIPT") as service:
                result = service.run_sql(
                    service.attach_sql() + service.query_sql("DELETE FROM target WHERE true; SELECT 42 AS answer", rest)
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('"answer":42', result.stdout)
                self.assertEqual(len(service.execution_requests()), 1)

    def test_read_only_validation_preserves_parameters_and_billing(self):
        with LocalBigQuery() as service:
            attachment = service.attach_sql(read_only=True).replace(
                "dataset=", "billing_project=guard-billing dataset="
            )
            sql = service.query_sql("SELECT ? AS answer").replace("AS answer',", "AS answer', 42,")
            result = service.run_sql(attachment + sql)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('"answer":42', result.stdout)
            validation = [payload for _, path, payload in service.requests if path.endswith("/jobs")]
            self.assertEqual(len(validation), 1)
            configuration = validation[0]["configuration"]
            self.assertTrue(configuration["dryRun"])
            self.assertEqual(configuration["query"]["query"], "SELECT ? AS answer")
            self.assertEqual(configuration["query"]["parameterMode"], "POSITIONAL")
            self.assertEqual(configuration["query"]["queryParameters"][0]["parameterValue"]["value"], "42")
            self.assertEqual(validation[0]["jobReference"]["projectId"], "guard-billing")
            self.assertTrue(any("/projects/guard-billing/jobs" in path for _, path, _ in service.requests))

    def test_read_only_select_still_succeeds(self):
        for rest in (True, False):
            with self.subTest(rest=rest), LocalBigQuery() as service:
                result = service.run_sql(service.attach_sql(read_only=True) + service.query_sql(rest=rest))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('"answer":42', result.stdout)
                self.assertEqual(len(service.execution_requests()), 1)
                if not rest:
                    self.assertEqual([name for name, _ in service.grpc_requests], ["CreateReadSession", "ReadRows"])
