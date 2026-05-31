#!/usr/bin/env python3

import csv
import subprocess
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]

DUCKDB = ROOT / "build/release/duckdb"
STREAM_NUMS = [1, 4, 16]
PREFETCH_QUEUE_SIZES = [0, 1, 2, 4]
RUNS_PER_CASE = 1
PRINT_DUCKDB_OUTPUT = False
RESULTS_CSV = ROOT / "benchmark/results/file_download_streams_prefetch.csv"
RESULTS_PNG = ROOT / "benchmark/results/file_download_streams_prefetch.png"

SQL_SETUP_TEMPLATE = """
SET preserve_insertion_order = false;
SET bq_max_read_streams = {streams};
SET bq_read_prefetch_queue_size = {prefetch_queue_size};

ATTACH 'project=bigquery-public-data dataset=pypi billing_project=hafenkran' AS bigquery_public_data (TYPE bigquery, READ_ONLY);
"""

QUERY_SQL = """
SELECT *
FROM bigquery_public_data.pypi.file_downloads
WHERE project = 'pydantic'
  AND timestamp >= DATE '2024-01-01'
  AND timestamp < DATE '2024-01-10'
LIMIT 10000000
"""


def make_sql(streams, prefetch_queue_size):
    return f"""
{SQL_SETUP_TEMPLATE.format(streams=streams, prefetch_queue_size=prefetch_queue_size)}
CREATE TEMP TABLE benchmark_result AS
{QUERY_SQL};
SELECT count(*) FROM benchmark_result;
"""


def run_duckdb(sql):
    start = time.perf_counter()
    result = subprocess.run(
        [str(DUCKDB), "-csv", "-noheader"],
        input=sql,
        text=True,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        check=True,
    )
    elapsed = time.perf_counter() - start

    if PRINT_DUCKDB_OUTPUT:
        print(result.stdout, end="")

    rows_read = int(result.stdout.strip().splitlines()[-1])
    return elapsed, rows_read


def main():
    results = []

    print("streams,prefetch_queue_size,run,seconds,rows_read", flush=True)
    for streams in STREAM_NUMS:
        for prefetch_queue_size in PREFETCH_QUEUE_SIZES:
            sql = make_sql(streams, prefetch_queue_size)
            for run in range(1, RUNS_PER_CASE + 1):
                elapsed, rows_read = run_duckdb(sql)
                results.append(
                    {
                        "streams": streams,
                        "prefetch_queue_size": prefetch_queue_size,
                        "run": run,
                        "seconds": elapsed,
                        "rows_read": rows_read,
                    }
                )
                print(f"{streams},{prefetch_queue_size},{run},{elapsed:.3f},{rows_read}", flush=True)

    write_csv(results)
    write_plot(results)


def write_csv(results):
    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["streams", "prefetch_queue_size", "run", "seconds", "rows_read"])
        writer.writeheader()
        writer.writerows(results)


def write_plot(results):
    by_case = {}
    for row in results:
        key = (row["prefetch_queue_size"], row["streams"])
        by_case.setdefault(key, []).append(row["seconds"])

    streams = sorted({row["streams"] for row in results})

    RESULTS_PNG.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(8, 4.5))
    for prefetch_queue_size in sorted({row["prefetch_queue_size"] for row in results}):
        avg_seconds = []
        for stream in streams:
            values = by_case[(prefetch_queue_size, stream)]
            avg_seconds.append(sum(values) / len(values))
        plt.plot(streams, avg_seconds, marker="o", label=f"prefetch={prefetch_queue_size}")
    plt.xlabel("bq_max_read_streams")
    plt.ylabel("seconds")
    plt.title("BigQuery file_downloads prefetch benchmark")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(RESULTS_PNG)


if __name__ == "__main__":
    main()
