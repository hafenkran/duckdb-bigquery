"""Shared loopback servers and DuckDB process helpers for BigQuery tests."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
from urllib.parse import urlparse

import grpc
from google.cloud.bigquery_storage_v1 import types
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "duckdb-access-test"
DATASET = "guard_test"
STREAM = f"projects/{PROJECT}/locations/us/sessions/test/streams/read"
SCHEMA = {"fields": [{"name": "answer", "type": "INTEGER", "mode": "NULLABLE"}]}


@dataclass
class TestBinaries:
    build_dir: Path = ROOT / "build/debug"

    @property
    def shell(self):
        return self.build_dir / ("duckdb.exe" if os.name == "nt" else "duckdb")

    @property
    def extension(self):
        return self.build_dir / "extension/bigquery/bigquery.duckdb_extension"

    @property
    def unittest(self):
        return self.build_dir / "test" / ("unittest.exe" if os.name == "nt" else "unittest")


BINARIES = TestBinaries()


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def unary_rpc(request_type, response_type, callback):
    return grpc.unary_unary_rpc_method_handler(
        callback,
        request_deserializer=request_type.deserialize,
        response_serializer=response_type.serialize,
    )


class JSONHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, payload, status=200):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class LocalService:
    def __enter__(self):
        self.resources = ExitStack()
        try:
            self.temp = self.resources.enter_context(tempfile.TemporaryDirectory(prefix="duckdb-bigquery-mock-"))
            self.certificate = Path(self.temp) / "server.crt"
            private_key = Path(self.temp) / "server.key"
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-keyout",
                    str(private_key),
                    "-out",
                    str(self.certificate),
                    "-days",
                    "1",
                    "-subj",
                    "/CN=localhost",
                    "-addext",
                    "subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:bigquerystorage.googleapis.com",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                check=True,
                timeout=10,
            )
            executor = ThreadPoolExecutor(max_workers=2)
            self.resources.callback(executor.shutdown, wait=True)
            self.grpc = grpc.server(executor)
            self.grpc.add_generic_rpc_handlers(self.rpc_handlers())
            credentials = grpc.ssl_server_credentials(((private_key.read_bytes(), self.certificate.read_bytes()),))
            self.grpc_port = self.grpc.add_secure_port("127.0.0.1:0", credentials)
            if not self.grpc_port:
                raise RuntimeError("Could not bind loopback gRPC server")
            self.grpc.start()
            self.resources.callback(lambda: self.grpc.stop(0).wait())

            self.rest = ThreadingHTTPServer(("127.0.0.1", 0), self.http_handler())
            self.resources.callback(self.rest.server_close)
            thread = threading.Thread(target=self.rest.serve_forever, daemon=True)
            thread.start()
            self.resources.callback(thread.join)
            self.resources.callback(self.rest.shutdown)
            return self
        except BaseException:
            self.resources.close()
            raise

    def __exit__(self, *args):
        self.resources.close()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.rest.server_port}"

    def environment(self, extra=None):
        env = dict(os.environ)
        env.update(
            {
                "GOOGLE_APPLICATION_CREDENTIALS": str(Path(self.temp) / "no-real-credentials.json"),
                "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH": str(self.certificate),
            }
        )
        env.update(extra or {})
        return env

    def run_shell(self, sql, *, changes=False):
        return subprocess.run(
            [str(BINARIES.shell), "-init", os.devnull, "-unsigned", "-batch", "-json"],
            input=".bail on\n"
            + (".changes on\n" if changes else "")
            + f"LOAD {sql_string(BINARIES.extension)};\n"
            + sql,
            cwd=ROOT,
            env=self.environment(),
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )

    def run_sqllogic(self, filename, extra):
        result = subprocess.run(
            [str(BINARIES.unittest), "--test-dir", str(ROOT), (ROOT / "test/sql" / filename).as_posix()],
            cwd=ROOT,
            env=self.environment(extra),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        output = result.stdout + result.stderr
        if result.returncode != 0 or "All tests passed" not in output:
            raise AssertionError(f"SQLLogicTest {filename} failed (exit {result.returncode}):\n{output}")
        return result


class LocalBigQuery(LocalService):
    def __init__(self, statement_type="SELECT", reject_auth=False, validation_error=False):
        self.statement_type = statement_type
        self.reject_auth = reject_auth
        self.validation_error = validation_error
        self.requests = []
        self.grpc_requests = []

    def http_handler(self):
        owner = self

        class Handler(JSONHandler):
            def do_POST(self):
                path = urlparse(self.path).path
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if path == "/token":
                    owner.requests.append(("POST", path, None))
                    if owner.reject_auth:
                        self.respond({"error": "invalid_grant", "error_description": "synthetic rejection"}, 400)
                    else:
                        self.respond(
                            {"access_token": "synthetic-test-token", "expires_in": 3600, "token_type": "Bearer"}
                        )
                    return
                payload = json.loads(body)
                owner.requests.append(("POST", path, payload))
                if path.endswith("/queries"):
                    response = {
                        "jobComplete": True,
                        "jobReference": owner.job_reference,
                        "schema": SCHEMA,
                        "totalRows": "1",
                        "totalBytesProcessed": "0",
                        "cacheHit": False,
                    }
                    if not payload.get("dryRun"):
                        response["rows"] = [{"f": [{"v": "42"}]}]
                    self.respond(response)
                elif path.endswith("/jobs"):
                    job = owner.job(payload.get("configuration", {}).get("query", {}).get("query"))
                    job["configuration"] = payload.get("configuration", {})
                    if owner.validation_error:
                        job["status"]["errorResult"] = {
                            "reason": "invalidQuery",
                            "message": "synthetic validation error",
                        }
                    self.respond(job)
                else:
                    self.respond({"error": {"code": 404, "message": "Unexpected mock request"}}, 404)

            def do_GET(self):
                path = urlparse(self.path).path
                owner.requests.append(("GET", path, None))
                table_ref = {"projectId": PROJECT, "datasetId": DATASET, "tableId": "target"}
                dataset_ref = {"projectId": PROJECT, "datasetId": DATASET}
                if "/jobs/" in path:
                    payload = owner.job()
                elif path.endswith("/jobs"):
                    payload = {"jobs": []}
                elif path.endswith("/tables/target"):
                    payload = {"tableReference": table_ref, "schema": SCHEMA}
                elif path.endswith("/tables"):
                    payload = {"tables": [{"tableReference": table_ref}]}
                elif path.endswith(f"/datasets/{DATASET}"):
                    payload = {"datasetReference": dataset_ref, "location": "US"}
                elif path.endswith("/datasets"):
                    payload = {"datasets": [{"datasetReference": dataset_ref, "location": "US"}]}
                else:
                    self.respond({"error": {"code": 404, "message": "Unexpected mock request"}}, 404)
                    return
                self.respond(payload)

        return Handler

    def rpc_handlers(self):
        handlers = {
            "CreateReadSession": grpc.unary_unary_rpc_method_handler(
                self.create_session,
                request_deserializer=types.CreateReadSessionRequest.deserialize,
                response_serializer=types.ReadSession.serialize,
            ),
            "ReadRows": grpc.unary_stream_rpc_method_handler(
                self.read_rows,
                request_deserializer=types.ReadRowsRequest.deserialize,
                response_serializer=types.ReadRowsResponse.serialize,
            ),
        }
        return [grpc.method_handlers_generic_handler("google.cloud.bigquery.storage.v1.BigQueryRead", handlers)]

    @property
    def job_reference(self):
        return {"projectId": PROJECT, "jobId": "mock_query", "location": "US"}

    def job(self, query=None):
        query_stats = {"schema": SCHEMA, "totalBytesProcessed": "0"}
        statement_type = self.statement_type
        if callable(statement_type):
            statement_type = statement_type(query) if query is not None else "SELECT"
        if statement_type is not None:
            query_stats["statementType"] = statement_type
        return {
            "jobReference": self.job_reference,
            "status": {"state": "DONE"},
            "statistics": {"query": query_stats},
            "configuration": {
                "query": {"destinationTable": {"projectId": PROJECT, "datasetId": DATASET, "tableId": "target"}}
            },
        }

    def create_session(self, request, context):
        self.grpc_requests.append(("CreateReadSession", request))
        schema = pa.schema([pa.field("answer", pa.int64())])
        return types.ReadSession(
            name=f"projects/{PROJECT}/locations/us/sessions/test",
            data_format=types.DataFormat.ARROW,
            arrow_schema={"serialized_schema": schema.serialize().to_pybytes()},
            streams=[{"name": STREAM}],
            estimated_row_count=1,
        )

    def read_rows(self, request, context):
        self.grpc_requests.append(("ReadRows", request))
        batch = pa.record_batch([pa.array([42], pa.int64())], names=["answer"])
        stream = pa.BufferOutputStream()
        with pa.ipc.new_stream(stream, batch.schema) as writer:
            writer.write_batch(batch)
        yield types.ReadRowsResponse(
            arrow_record_batch={"serialized_record_batch": stream.getvalue().to_pybytes(), "row_count": 1},
            row_count=1,
        )

    def secret_sql(self, oauth=False):
        credentials = "ACCESS_TOKEN 'synthetic-test-token'"
        if oauth:
            credentials = (
                "REFRESH_TOKEN 'synthetic-refresh', CLIENT_ID 'synthetic-client', "
                f"CLIENT_SECRET 'synthetic-secret', TOKEN_URI '{self.endpoint}/token'"
            )
        return f"CREATE TEMPORARY SECRET guard_secret (TYPE bigquery, SCOPE 'bq://{PROJECT}', {credentials});"

    def attach_sql(self, read_only=False):
        dsn = (
            f"project={PROJECT} dataset={DATASET} api_endpoint={self.endpoint} "
            f"grpc_endpoint=127.0.0.1:{self.grpc_port}"
        )
        mode = ", READ_ONLY" if read_only else ""
        return f"ATTACH '{dsn}' AS guarded (TYPE bigquery, SECRET guard_secret{mode});"

    def query_sql(self, query="SELECT 42 AS answer", rest=True, attached=True, extra=""):
        target = "guarded" if attached else PROJECT
        options = f", use_rest_api := {str(rest).lower()}"
        if not attached:
            options += f", api_endpoint := '{self.endpoint}', grpc_endpoint := '127.0.0.1:{self.grpc_port}'"
        return f"SELECT * FROM bigquery_query('{target}', {sql_string(query)}{options}{extra});"

    def run_sql(self, sql, oauth=False):
        setup = "SET bq_experimental_use_info_schema=false;\n" + self.secret_sql(oauth) + "\n"
        return self.run_shell(setup + sql)

    def execution_requests(self):
        return [
            request
            for request in self.requests
            if request[0] == "POST"
            and (
                request[1].endswith("/queries")
                and not request[2].get("dryRun")
                or request[1].endswith("/jobs")
                and not request[2].get("configuration", {}).get("dryRun")
            )
        ]
