"""Check when bigquery_load stages its source and what it uploads."""

from io import BytesIO
import json
from pathlib import Path
import unittest

import pyarrow.parquet as parquet

from mock_bigquery import DATASET, LocalBigQuery, sql_string


class LoadMaterializationTests(unittest.TestCase):
    def run_load(self, sql, *, reject_upload=False):
        with LocalBigQuery(validation_error=reject_upload) as service:
            staging = Path(service.temp) / "staging"
            staging.mkdir()
            setup = f"{service.secret_sql()}\n{service.attach_sql()}\n"
            cleanup_check = f"\nSELECT count(*) AS staged_files FROM glob({sql_string(staging / '*.parquet')});\n"
            result = service.run_shell(setup + sql + cleanup_check, extra_env={"TMPDIR": str(staging)})
            self.assertEqual(list(staging.iterdir()), [])
            return result, service.uploads

    def test_prepared_source_table_loads_current_rows(self):
        result, uploads = self.run_load(
            f"""
            CREATE TEMP TABLE source_rows (id INTEGER);
            INSERT INTO source_rows VALUES (1);
            PREPARE load_rows AS SELECT * FROM bigquery_load(
                'guarded', '{DATASET}.target', source_table := 'source_rows'
            );
            BEGIN;
            INSERT INTO source_rows VALUES (2);
            EXECUTE load_rows;
            INSERT INTO source_rows VALUES (3);
            EXECUTE load_rows;
            COMMIT;
        """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = [sorted(parquet.read_table(BytesIO(upload))["id"].to_pylist()) for upload in uploads]
        self.assertEqual(rows, [[1, 2], [1, 2, 3]])
        self.assertEqual(json.loads(result.stdout.splitlines()[-1]), [{"staged_files": 0}])

    def test_materialization_with_multiple_threads_and_empty_input(self):
        load_source = f"SELECT * FROM bigquery_load('guarded', '{DATASET}.target', source_table := 'source_rows');"
        load_empty = f"SELECT * FROM bigquery_load('guarded', '{DATASET}.target', source_table := 'empty_source');"
        # Repetition covers the race between pipeline completion and worker task destruction.
        result, uploads = self.run_load(
            f"""
            SET threads=4;
            CREATE TEMP TABLE source_rows AS SELECT i AS id FROM range(300000) t(i);
            {load_source * 4}
            SET threads=1;
            CREATE TEMP TABLE empty_source AS SELECT * FROM source_rows WHERE false;
            EXPLAIN ANALYZE {load_empty}
        """
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        row_counts = [parquet.ParquetFile(BytesIO(upload)).metadata.num_rows for upload in uploads]
        self.assertEqual(row_counts, [300000] * 4 + [0])
        self.assertIn("BIGQUERY_LOAD", result.stdout)

    def test_failed_upload_removes_staged_file(self):
        result, uploads = self.run_load(
            f"""
            CREATE TEMP TABLE source_rows AS SELECT 1 AS id;
            SELECT * FROM bigquery_load('guarded', '{DATASET}.target', source_table := 'source_rows');
        """,
            reject_upload=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("synthetic validation error", result.stderr)
        self.assertEqual(len(uploads), 1)

    def test_failed_copy_removes_staged_file(self):
        result, uploads = self.run_load(
            f"""
            SET threads=4;
            CREATE TEMP VIEW source_rows AS
            SELECT CASE WHEN i >= 250000 THEN error('invalid source row') ELSE i END AS id
            FROM range(300000) t(i);
            SELECT * FROM bigquery_load('guarded', '{DATASET}.target', source_table := 'source_rows');
        """
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid source row", result.stderr)
        self.assertEqual(uploads, [])
