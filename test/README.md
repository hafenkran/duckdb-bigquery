# Testing this extension

Run the commands below from the repository root. The `sql/` directory contains
[SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html), and `python/`
provides local BigQuery mocks and additional checks around those SQL tests.

## Build environment

For a local vcpkg installation, set its toolchain path before building. Choose
an installed C/C++ compiler pair; this example uses GCC 14 and Ninja:

```sh
export VCPKG_ROOT="/path/to/vcpkg"
export VCPKG_TOOLCHAIN_PATH="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
export CC=gcc-14
export CXX=g++-14
export GEN=ninja
```

Replace `/path/to/vcpkg` with your installation directory. The Makefile reads
`VCPKG_TOOLCHAIN_PATH`; setting `VCPKG_ROOT` alone does not select the toolchain.
`CC`, `CXX`, and `GEN` are optional selections for your local tools. CMake caches
the compiler choice when a build directory is first configured.

## Build and run SQLLogicTests

Build the extension separately, then run the tests against that matching build:

```sh
make debug
make test_debug
```

For a release build:

```sh
make release
make test
```

The `debug`, `release`, `test_debug`, and `test_release` targets come from
DuckDB's extension makefile. The test targets use existing binaries and do not
build the extension themselves. The build must match the checked-out sources;
older binaries can still reproduce bugs that are already fixed on the branch.

Live BigQuery tests need credentials and the `BQ_TEST_PROJECT` and
`BQ_TEST_DATASET` environment variables. They can create or modify cloud
resources. After a matching debug build, select one file with:

```sh
make test_debug T='test/sql/storage/attach_create_table_as_types.test'
```

Mock-specific `.test` files are discovered during a normal SQLLogicTest run but
skipped when their required endpoint variables are absent. The standalone
[CTAS type test](sql/storage/attach_create_table_as_types.test) uses live
BigQuery; its [_mock.test counterpart](sql/storage/attach_create_table_as_types_mock.test)
uses the local servers described below.

## Local BigQuery mock tests

The Python tests start REST and TLS gRPC servers on `127.0.0.1` and use synthetic
credentials. DuckDB and the C++ extension run normally against these servers.
The tests check SQL results, recorded requests, and decoded Storage Write rows.
No Google account or BigQuery resources are needed.

Python 3.10+, `uv`, and `openssl` must be installed. The launcher declares its
Python dependencies; `uv` installs them in an isolated environment.

After a matching debug build, run all local suites with:

```sh
make test-mock
```

`test-mock` always uses existing binaries. It does not configure or build the
extension. Choose a smaller suite, another build, or individual test names with:

```sh
make test-mock TEST_SUITE=access
make test-mock TEST_SUITE=write TEST_BUILD_DIR=build/release
make test-mock TEST_ARGS='-k prepared'
make test-mock TEST_ARGS=--list
```

| Suite | Tests | Required binaries |
| --- | --- | --- |
| `all` (default) | All suites below | Test runner, shell, extension |
| `sql` | Access guards, read-only queries, DDL idempotence and CTAS SQLLogicTests | Test runner |
| `access` | Access SQLLogicTests and additional shell checks for authentication, prepared statements and read-only validation | Test runner, shell, extension |
| `ddl` | Schema/table idempotence and CTAS SQLLogicTests | Test runner |
| `ctas` | CTAS values, nested types, NULLs and multiple chunks | Test runner |
| `write` | Storage Write commit confirmations and error responses | Shell, extension |

The launcher fails when required binaries are missing or no tests match the
selection. `--list` only lists tests and needs no binaries or running servers.

The `.test` files hold SQL statements and expected results. Python starts the
servers, sets `BQ_GUARD_MOCK_*`, `BQ_DDL_MOCK_*`, `BQ_DDL_INFO_SCHEMA`, and the
synthetic project/dataset variables, then runs the existing SQLLogicTest runner.
Additional Python assertions check which requests and values reached the mocks.

## Test code and CI

[python/mock_bigquery.py](python/mock_bigquery.py) owns server startup, temporary
certificates, cleanup, binary paths, subprocess execution and the common query
fixture. The three `python/test_*.py` modules contain the assertions and their
specific responses. [python/run_mock_tests.py](python/run_mock_tests.py) is the
shared entry point.

Linux amd64 CI runs `test-mock` against the existing test bundle, including pull
requests without GCP credentials. The bundle contains the shell as well as the
test runner and extension, so the write-error tests run there too. No second
extension build is performed by the test job.
