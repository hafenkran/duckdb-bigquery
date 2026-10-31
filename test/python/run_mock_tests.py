# /// script
# requires-python = ">=3.10"
# dependencies = ["google-cloud-bigquery-storage>=2,<3", "pyarrow>=20,<21"]
# ///

"""Run local BigQuery regression tests against an existing matching build."""

import argparse
from pathlib import Path
import unittest

from mock_bigquery import BINARIES
import test_access_guards
import test_ddl_idempotence
import test_storage_write_commit


SUITES = {
    "sql": (test_access_guards.SQLLogicTests, test_ddl_idempotence.DDLIdempotenceTests),
    "access": (
        test_access_guards.SQLLogicTests,
        test_access_guards.ExternalAccessTests,
        test_access_guards.ReadOnlyTests,
    ),
    "ddl": (test_ddl_idempotence.DDLIdempotenceTests,),
    "write": (test_storage_write_commit.StorageWriteCommitTests,),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=BINARIES.build_dir)
    parser.add_argument("--suite", choices=("all", *SUITES, "ctas"), default="all")
    parser.add_argument("-k", metavar="PATTERN", help="Run test names containing this string, or matching a wildcard")
    parser.add_argument("--list", action="store_true", help="List selected tests without starting servers or DuckDB")
    options = parser.parse_args()
    BINARIES.build_dir = options.build_dir.resolve()
    loader = unittest.TestLoader()
    if options.k:
        loader.testNamePatterns = [options.k if "*" in options.k else f"*{options.k}*"]
    if options.suite == "ctas":
        classes = SUITES["ddl"]
    elif options.suite == "all":
        classes = (*SUITES["access"], *SUITES["ddl"], *SUITES["write"])
    else:
        classes = SUITES[options.suite]
    tests = [test for cls in classes for test in loader.loadTestsFromTestCase(cls)]
    if options.suite == "ctas":
        tests = [test for test in tests if test._testMethodName == "test_ctas_writes_refreshed_column_types"]
    if not tests:
        parser.error("No tests matched the selection")
    if options.list:
        for test in tests:
            print(test.id())
        return 0

    required = set()
    for test in tests:
        if isinstance(test, (test_access_guards.SQLLogicTests, test_ddl_idempotence.DDLIdempotenceTests)):
            required.add(BINARIES.unittest)
        else:
            required.update((BINARIES.shell, BINARIES.extension))
    for binary in sorted(required):
        if not binary.is_file():
            parser.error(f"Existing matching build required: {binary}. Build it separately before running tests.")
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(tests))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
