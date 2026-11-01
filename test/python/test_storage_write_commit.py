# /// script
# requires-python = ">=3.10"
# dependencies = ["google-cloud-bigquery-storage>=2,<3"]
# ///

"""Exercise SQL INSERT commits against loopback-only BigQuery REST and gRPC mocks.

Use run_mock_tests.py --suite write with an existing matching build. This test does
not build the extension, use real credentials, or contact BigQuery.
"""

import subprocess
import unittest
from urllib.parse import urlparse

import grpc
from google.cloud.bigquery_storage_v1 import types

from mock_bigquery import JSONHandler, LocalService, unary_rpc

PROJECT = "duckdb-commit-test"
DATASET = "commit_test"
TABLE = f"projects/{PROJECT}/datasets/{DATASET}/tables/target"
STREAM = f"{TABLE}/streams/pending"
SUCCESS_MARKER = "insert_commit_confirmed"


class LocalStorageWrite(LocalService):
    def __init__(self, response=None, transport_error=None):
        self.response = response if response is not None else types.BatchCommitWriteStreamsResponse()
        self.transport_error = transport_error
        self.appended_rows = 0
        self.calls = []

    def http_handler(self):
        class MetadataHandler(JSONHandler):
            def do_GET(self):
                path = urlparse(self.path).path
                table_ref = {"projectId": PROJECT, "datasetId": DATASET, "tableId": "target"}
                dataset_ref = {"projectId": PROJECT, "datasetId": DATASET}
                if path.endswith("/tables/target"):
                    payload = {
                        "tableReference": table_ref,
                        "schema": {"fields": [{"name": "i", "type": "INTEGER"}]},
                    }
                elif path.endswith("/tables"):
                    payload = {"tables": [{"tableReference": table_ref}]}
                elif path.endswith(f"/datasets/{DATASET}"):
                    payload = {"datasetReference": dataset_ref, "location": "US"}
                elif path.endswith("/datasets"):
                    payload = {"datasets": [{"datasetReference": dataset_ref, "location": "US"}]}
                else:
                    self.send_error(404)
                    return
                self.respond(payload)

        return MetadataHandler

    def rpc_handlers(self):
        handlers = {
            "CreateWriteStream": unary_rpc(types.CreateWriteStreamRequest, types.WriteStream, self.create_stream),
            "FinalizeWriteStream": unary_rpc(
                types.FinalizeWriteStreamRequest, types.FinalizeWriteStreamResponse, self.finalize_stream
            ),
            "BatchCommitWriteStreams": unary_rpc(
                types.BatchCommitWriteStreamsRequest,
                types.BatchCommitWriteStreamsResponse,
                self.commit_stream,
            ),
            "AppendRows": grpc.stream_stream_rpc_method_handler(
                self.append_rows,
                request_deserializer=types.AppendRowsRequest.deserialize,
                response_serializer=types.AppendRowsResponse.serialize,
            ),
        }
        return [grpc.method_handlers_generic_handler("google.cloud.bigquery.storage.v1.BigQueryWrite", handlers)]

    def create_stream(self, request, context):
        self.calls.append(("create", request))
        return types.WriteStream(name=STREAM, type_=types.WriteStream.Type.PENDING)

    def append_rows(self, requests, context):
        for request in requests:
            self.calls.append(("append", request))
            self.appended_rows += len(request.proto_rows.rows.serialized_rows)
            yield types.AppendRowsResponse(append_result={"offset": request.offset})

    def finalize_stream(self, request, context):
        self.calls.append(("finalize", request))
        return types.FinalizeWriteStreamResponse(row_count=self.appended_rows)

    def commit_stream(self, request, context):
        self.calls.append(("commit", request))
        if self.transport_error is not None:
            context.abort(self.transport_error, "synthetic commit transport failure")
        return self.response

    def insert(self):
        dsn = (
            f"project={PROJECT} dataset={DATASET} "
            f"api_endpoint=http://127.0.0.1:{self.rest.server_port} "
            f"grpc_endpoint=127.0.0.1:{self.grpc_port}"
        )
        sql = f"""
            SET bq_experimental_use_info_schema=false;
            CREATE TEMPORARY SECRET commit_test_secret (
                TYPE bigquery, SCOPE 'bq://{PROJECT}', ACCESS_TOKEN 'synthetic-test-token'
            );
            ATTACH '{dsn}'
                AS bq (TYPE bigquery, SECRET commit_test_secret);
            INSERT INTO bq.{DATASET}.target VALUES (11), (22);
            SELECT '{SUCCESS_MARKER}' AS outcome;
        """
        try:
            return self.run_shell(sql, changes=True)
        except subprocess.TimeoutExpired as error:
            stages = [name for name, _ in self.calls]
            raise AssertionError(f"INSERT timed out after RPC stages {stages}: {error.stderr!r}") from None


class StorageWriteCommitTests(unittest.TestCase):
    def run_insert(self, response=None, transport_error=None):
        with LocalStorageWrite(response, transport_error) as service:
            result = service.insert()
        self.assertEqual(service.appended_rows, 2, result.stderr)
        commits = [request for name, request in service.calls if name == "commit"]
        self.assertEqual(len(commits), 1, result.stderr)
        self.assertEqual(commits[0].parent, TABLE)
        self.assertEqual(list(commits[0].write_streams), [STREAM])
        self.assertTrue(any(name == "finalize" for name, _ in service.calls), result.stderr)
        return result

    def assert_insert_failed(self, result, *details):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn(SUCCESS_MARKER, result.stdout)
        self.assertNotRegex(result.stdout, r"(?m)^changes:\s+2\b")
        for detail in details:
            self.assertIn(detail, result.stderr)

    def test_confirmed_commit_succeeds(self):
        response = types.BatchCommitWriteStreamsResponse(commit_time={"seconds": 1})
        result = self.run_insert(response)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(SUCCESS_MARKER, result.stdout)
        self.assertRegex(result.stdout, r"(?m)^changes:\s+2\s+total_changes:\s+2$")

    def test_stream_error_fails_insert(self):
        response = types.BatchCommitWriteStreamsResponse(
            stream_errors=[{"code": "INVALID_STREAM_STATE", "entity": STREAM, "error_message": "stream rejected"}]
        )
        self.assert_insert_failed(self.run_insert(response), "INVALID_STREAM_STATE", STREAM, "stream rejected")

    def test_stream_errors_take_precedence_over_commit_time(self):
        response = types.BatchCommitWriteStreamsResponse(
            commit_time={"seconds": 1},
            stream_errors=[{"code": "STREAM_NOT_FOUND", "entity": STREAM, "error_message": "stream missing"}],
        )
        self.assert_insert_failed(self.run_insert(response), "STREAM_NOT_FOUND", "stream missing")

    def test_all_stream_errors_are_reported(self):
        response = types.BatchCommitWriteStreamsResponse(
            stream_errors=[
                {"code": "INVALID_STREAM_STATE", "entity": STREAM, "error_message": "first error"},
                {"code": "KMS_PERMISSION_DENIED", "entity": TABLE, "error_message": "second error"},
            ]
        )
        self.assert_insert_failed(self.run_insert(response), "first error", "second error", "KMS_PERMISSION_DENIED")

    def test_error_code_is_reported_without_message(self):
        response = types.BatchCommitWriteStreamsResponse(stream_errors=[{"code": "INVALID_STREAM_STATE"}])
        self.assert_insert_failed(self.run_insert(response), "INVALID_STREAM_STATE", STREAM)

    def test_missing_commit_confirmation_fails_insert(self):
        self.assert_insert_failed(self.run_insert(), "commit", STREAM)

    def test_commit_transport_error_fails_insert(self):
        self.assert_insert_failed(
            self.run_insert(transport_error=grpc.StatusCode.INVALID_ARGUMENT), "synthetic commit transport failure"
        )

    def test_commit_permission_error_remains_a_permission_error(self):
        self.assert_insert_failed(
            self.run_insert(transport_error=grpc.StatusCode.PERMISSION_DENIED),
            "Permission Error",
            "synthetic commit transport failure",
        )
