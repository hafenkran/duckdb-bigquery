require bigquery

require-env BQ_DDL_MOCK_API_ENDPOINT

require-env BQ_DDL_MOCK_GRPC_ENDPOINT

require-env BQ_TEST_PROJECT

require-env BQ_TEST_DATASET

require-env BQ_DDL_INFO_SCHEMA

statement ok
SET bq_experimental_use_info_schema={BQ_DDL_INFO_SCHEMA};

statement ok
CREATE TEMPORARY SECRET ddl_secret (TYPE bigquery, SCOPE 'bq://{BQ_TEST_PROJECT}', ACCESS_TOKEN 'synthetic-test-token');

statement ok
ATTACH 'project={BQ_TEST_PROJECT} api_endpoint={BQ_DDL_MOCK_API_ENDPOINT} grpc_endpoint={BQ_DDL_MOCK_GRPC_ENDPOINT}' AS ddl (TYPE bigquery, SECRET ddl_secret);
