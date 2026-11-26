"""Verify repeated execution of a prepared BigQuery dry run."""

import json
import unittest

from mock_bigquery import PROJECT, LocalBigQuery


class QueryDryRunTests(unittest.TestCase):
    def test_prepared_dry_run_executes_twice(self):
        with LocalBigQuery() as service:
            query = service.query_sql(attached=False, rest=False, extra=", dry_run := true")
            result = service.run_sql(f"PREPARE p AS {query}\nEXECUTE p;\nEXECUTE p;")

        self.assertEqual(result.returncode, 0, result.stderr)
        with self.subTest("statistics"):
            result_sets = [json.loads(line) for line in result.stdout.splitlines()]
            statistics_row = {"total_bytes_processed": 0, "cache_hit": False, "location": "US"}
            self.assertEqual(result_sets[-2:], [[statistics_row], [statistics_row]])

        with self.subTest("requests"):
            requests = [(method, path, (payload or {}).get("dryRun")) for method, path, payload in service.requests]
            self.assertEqual(requests, [("POST", f"/bigquery/v2/projects/{PROJECT}/queries", True)] * 2)
