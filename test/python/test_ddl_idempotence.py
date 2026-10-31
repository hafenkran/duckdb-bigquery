# /// script
# requires-python = ">=3.10"
# dependencies = ["google-cloud-bigquery-storage>=2,<3", "pyarrow>=20,<21"]
# ///

"""Run DDL SQLLogicTests against stateful loopback metadata and Storage Read mocks."""

import json
import re
import unittest
from urllib.parse import urlparse

import grpc
from google.cloud.bigquery_storage_v1 import types
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
import pyarrow as pa

from mock_bigquery import DATASET, PROJECT, STREAM, JSONHandler, LocalBigQuery, unary_rpc


class LocalDDLBigQuery(LocalBigQuery):
    def __init__(self):
        super().__init__()
        self.datasets = {DATASET: "US", "ddl_existing_schema": "EU"}
        self.tables = {
            (DATASET, "target"): {
                "fields": [{"name": "answer", "type": "INTEGER"}, {"name": "label", "type": "STRING"}],
                "rows": [(42, "duck")],
            }
        }
        self.ddl = []
        self.read_table = None
        self.read_fields = []
        self.write_streams = {}
        self.committed_rows = {}

    def http_handler(self):
        owner = self

        class Handler(JSONHandler):
            def do_POST(self):
                path = urlparse(self.path).path
                payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                owner.requests.append(("POST", path, payload))
                if not path.endswith("/queries") or payload.get("dryRun"):
                    self.respond({"error": {"code": 400, "message": "Unexpected mock request"}}, 400)
                    return
                query = payload["query"]
                if "INFORMATION_SCHEMA.COLUMNS" in query:
                    datasets = re.findall(r"`" + re.escape(PROJECT) + r"\.([^.]+)\.INFORMATION_SCHEMA.COLUMNS`", query)
                    rows = []
                    for (dataset, name), table in sorted(owner.tables.items()):
                        if dataset not in datasets:
                            continue
                        for ordinal, field in enumerate(table["fields"], 1):
                            values = [
                                dataset,
                                name,
                                field["name"],
                                {"INTEGER": "INT64"}.get(field["type"], field["type"]),
                                "YES",
                                "NULL",
                                str(ordinal),
                            ]
                            rows.append({"f": [{"v": value} for value in values]})
                    self.respond(
                        {
                            "jobComplete": True,
                            "jobReference": owner.job_reference,
                            "rows": rows,
                            "totalRows": str(len(rows)),
                        }
                    )
                    return
                error = owner.execute_ddl(payload["query"])
                if error:
                    self.respond({"error": {"code": 409, "message": error, "status": "ALREADY_EXISTS"}}, 409)
                    return
                self.respond({"jobComplete": True, "jobReference": owner.job_reference, "totalRows": "0"})

            def do_GET(self):
                path = urlparse(self.path).path
                owner.requests.append(("GET", path, None))
                parts = path.split("/")
                if path.endswith("/datasets"):
                    self.respond({"datasets": [owner.dataset(name) for name in owner.datasets]})
                    return
                dataset = parts[parts.index("datasets") + 1]
                if dataset not in owner.datasets:
                    self.respond({"error": {"code": 404, "message": "Dataset not found"}}, 404)
                elif path.endswith("/tables"):
                    self.respond({"tables": [owner.table(ds, name) for ds, name in owner.tables if ds == dataset]})
                elif "/tables/" in path:
                    name = parts[parts.index("tables") + 1]
                    if (dataset, name) not in owner.tables:
                        self.respond({"error": {"code": 404, "message": "Table not found"}}, 404)
                    else:
                        self.respond(owner.table(dataset, name))
                else:
                    self.respond(owner.dataset(dataset))

        return Handler

    def dataset(self, name):
        return {"datasetReference": {"projectId": PROJECT, "datasetId": name}, "location": self.datasets[name]}

    def table(self, dataset, name):
        return {
            "tableReference": {"projectId": PROJECT, "datasetId": dataset, "tableId": name},
            "schema": {"fields": self.tables[(dataset, name)]["fields"]},
        }

    def execute_ddl(self, query):
        match = re.fullmatch(r"CREATE (TABLE|SCHEMA) (IF NOT EXISTS )?`([^`]+)`(?:\s+\((.*)\))?\s*", query, re.DOTALL)
        if match is None:
            raise AssertionError(f"Unexpected mock DDL: {query}")
        kind, ignore, identifier, columns = match.groups()
        parts = identifier.split(".")
        if kind == "SCHEMA":
            exists = parts[1] in self.datasets
            if not exists:
                self.datasets[parts[1]] = "US"
        else:
            key = (parts[1], parts[2])
            exists = key in self.tables
            if not exists:
                self.tables[key] = {"fields": self.parse_fields(columns), "rows": []}
        self.ddl.append({"query": query, "kind": kind, "ignore": bool(ignore), "existed": exists, "name": identifier})
        if exists and not ignore:
            return f"Already Exists: {kind.title()} {identifier}"
        return None

    @classmethod
    def parse_fields(cls, columns):
        fields = []
        start = depth = 0
        for index, char in enumerate(columns + ","):
            if char in "<([":
                depth += 1
            elif char in ">)]":
                depth -= 1
            elif char == "," and depth == 0:
                name, sql_type = columns[start:index].strip().split(None, 1)
                field = {"name": name.strip("`")}
                if sql_type.startswith("ARRAY<"):
                    field["mode"] = "REPEATED"
                    sql_type = sql_type[6:-1]
                if sql_type.startswith("STRUCT<"):
                    field.update(type="RECORD", fields=cls.parse_fields(sql_type[7:-1]))
                else:
                    field["type"] = {"INT64": "INTEGER", "FLOAT64": "FLOAT"}.get(sql_type, sql_type)
                fields.append(field)
                start = index + 1
        return fields

    def rpc_handlers(self):
        handlers = {
            "CreateWriteStream": unary_rpc(types.CreateWriteStreamRequest, types.WriteStream, self.create_write_stream),
            "FinalizeWriteStream": unary_rpc(
                types.FinalizeWriteStreamRequest, types.FinalizeWriteStreamResponse, self.finalize_write_stream
            ),
            "BatchCommitWriteStreams": unary_rpc(
                types.BatchCommitWriteStreamsRequest, types.BatchCommitWriteStreamsResponse, self.commit_write_streams
            ),
            "AppendRows": grpc.stream_stream_rpc_method_handler(
                self.append_rows,
                request_deserializer=types.AppendRowsRequest.deserialize,
                response_serializer=types.AppendRowsResponse.serialize,
            ),
        }
        return super().rpc_handlers() + [
            grpc.method_handlers_generic_handler("google.cloud.bigquery.storage.v1.BigQueryWrite", handlers)
        ]

    def create_write_stream(self, request, context):
        self.grpc_requests.append(("CreateWriteStream", request))
        parts = request.parent.split("/")
        name = f"{request.parent}/streams/pending"
        self.write_streams[name] = {"table": (parts[3], parts[5]), "rows": [], "message": None}
        return types.WriteStream(name=name, type_=types.WriteStream.Type.PENDING)

    def append_rows(self, requests, context):
        for request in requests:
            self.grpc_requests.append(("AppendRows", request))
            stream = self.write_streams[request.write_stream]
            if request.proto_rows.writer_schema.proto_descriptor.name:
                file_descriptor = descriptor_pb2.FileDescriptorProto(name="mock_write.proto", syntax="proto2")
                file_descriptor.message_type.add().CopyFrom(request.proto_rows.writer_schema.proto_descriptor)
                pool = descriptor_pool.DescriptorPool()
                descriptor = pool.Add(file_descriptor).message_types_by_name[file_descriptor.message_type[0].name]
                stream["message"] = message_factory.GetMessageClass(descriptor)
            if request.offset != len(stream["rows"]):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Unexpected append offset")
            for serialized in request.proto_rows.rows.serialized_rows:
                stream["rows"].append(stream["message"].FromString(serialized))
            yield types.AppendRowsResponse(append_result={"offset": request.offset})

    def finalize_write_stream(self, request, context):
        self.grpc_requests.append(("FinalizeWriteStream", request))
        return types.FinalizeWriteStreamResponse(row_count=len(self.write_streams[request.name]["rows"]))

    def commit_write_streams(self, request, context):
        self.grpc_requests.append(("BatchCommitWriteStreams", request))
        for name in request.write_streams:
            stream = self.write_streams[name]
            table = self.tables[stream["table"]]
            self.committed_rows[stream["table"]] = stream["rows"]
            table["rows"].extend(
                tuple(self.field_value(row, field) for field in table["fields"]) for row in stream["rows"]
            )
        return types.BatchCommitWriteStreamsResponse(commit_time={"seconds": 1})

    @classmethod
    def field_value(cls, row, field):
        value = getattr(row, field["name"])
        if field.get("mode") == "REPEATED":
            if field["type"] == "RECORD":
                return [{child["name"]: cls.field_value(item, child) for child in field["fields"]} for item in value]
            return list(value)
        if not row.HasField(field["name"]):
            return None
        if field["type"] == "RECORD":
            return {child["name"]: cls.field_value(value, child) for child in field["fields"]}
        return value

    def arrow_schema(self):
        def arrow_type(field):
            if field["type"] == "RECORD":
                result = pa.struct([pa.field(child["name"], arrow_type(child)) for child in field["fields"]])
            else:
                result = {"INTEGER": pa.int64(), "INT64": pa.int64(), "FLOAT": pa.float64(), "STRING": pa.string()}[
                    field["type"]
                ]
            return pa.list_(result) if field.get("mode") == "REPEATED" else result

        return pa.schema([pa.field(field["name"], arrow_type(field)) for field in self.read_fields])

    def create_session(self, request, context):
        self.grpc_requests.append(("CreateReadSession", request))
        parts = request.read_session.table.split("/")
        self.read_table = (parts[3], parts[5])
        selected = list(request.read_session.read_options.selected_fields)
        fields = self.tables[self.read_table]["fields"]
        self.read_fields = [field for field in fields if not selected or field["name"] in selected]
        return types.ReadSession(
            name=f"projects/{PROJECT}/locations/us/sessions/test",
            data_format=types.DataFormat.ARROW,
            arrow_schema={"serialized_schema": self.arrow_schema().serialize().to_pybytes()},
            streams=[{"name": STREAM}],
            estimated_row_count=len(self.tables[self.read_table]["rows"]),
        )

    def read_rows(self, request, context):
        self.grpc_requests.append(("ReadRows", request))
        table = self.tables[self.read_table]
        schema = self.arrow_schema()
        arrays = []
        for field in schema:
            index = next(i for i, col in enumerate(table["fields"]) if col["name"] == field.name)
            arrays.append(pa.array([row[index] for row in table["rows"]], field.type))
        batch = pa.record_batch(arrays, schema=schema)
        stream = pa.BufferOutputStream()
        with pa.ipc.new_stream(stream, schema) as writer:
            writer.write_batch(batch)
        yield types.ReadRowsResponse(
            arrow_record_batch={"serialized_record_batch": stream.getvalue().to_pybytes(), "row_count": batch.num_rows},
            row_count=batch.num_rows,
        )


class DDLIdempotenceTests(unittest.TestCase):
    def run_file(self, service, filename, info_schema):
        return service.run_sqllogic(
            "storage/" + filename,
            {
                "BQ_TEST_PROJECT": PROJECT,
                "BQ_TEST_DATASET": DATASET,
                "BQ_DDL_INFO_SCHEMA": str(info_schema).lower(),
                "BQ_DDL_MOCK_API_ENDPOINT": service.endpoint,
                "BQ_DDL_MOCK_GRPC_ENDPOINT": f"127.0.0.1:{service.grpc_port}",
            },
        )

    def test_existing_tables_keep_actual_schema(self):
        for info_schema in (False, True):
            with self.subTest(info_schema=info_schema), LocalDDLBigQuery() as service:
                self.run_file(service, "attach_create_table_if_not_exists.test", info_schema)
                self.assertEqual(
                    service.tables[(DATASET, "target")]["fields"],
                    [{"name": "answer", "type": "INTEGER"}, {"name": "label", "type": "STRING"}],
                )
                self.assertEqual(service.tables[(DATASET, "target")]["rows"], [(42, "duck")])
                no_ops = [event for event in service.ddl if event["existed"] and event["ignore"]]
                self.assertEqual(len(no_ops), 9, service.ddl)
                self.assertEqual([name for name, _ in service.grpc_requests].count("ReadRows"), 6)
                for cache in ("warm", "cold", "reload"):
                    self.assertEqual(
                        service.tables[(DATASET, f"new_{cache}")]["fields"][0], {"name": "i", "type": "INTEGER"}
                    )

    def test_schema_conflict_clause_reaches_bigquery(self):
        for info_schema in (False, True):
            with self.subTest(info_schema=info_schema), LocalDDLBigQuery() as service:
                self.run_file(service, "attach_create_schema_if_not_exists.test", info_schema)
                self.assertEqual(service.datasets["ddl_existing_schema"], "EU")
                self.assertIn("ddl_created_schema", service.datasets)
                self.assertIn("ddl_new_schema", service.datasets)
                self.assertEqual(sum(event["ignore"] for event in service.ddl), 6, service.ddl)
                self.assertEqual(sum(event["existed"] and not event["ignore"] for event in service.ddl), 2)
                self.assertTrue(all(event["kind"] == "SCHEMA" for event in service.ddl))

    def test_ctas_writes_refreshed_column_types(self):
        with LocalDDLBigQuery() as service:
            self.run_file(service, "attach_create_table_as_types_mock.test", False)
            rows = service.tables[(DATASET, "ctas_nested")]["rows"]
            self.assertEqual(
                rows,
                [(i, i + 0.5, [i, i + 1], {"n": i, "f": i + 0.5}, [{"n": i}], "duck", None) for i in range(2)],
            )
            self.assertEqual(service.tables[(DATASET, "ctas_chunks")]["rows"], [(i,) for i in range(4097)])
            self.assertEqual(len(service.committed_rows), 2)
