"""Exercise exact REST timestamps through inline, polled and paginated responses."""

import json
import unittest
from urllib.parse import parse_qs, urlparse

from mock_bigquery import JSONHandler, LocalService

PROJECT = "duckdb-rest-timestamps"
MICROS = [
    -62135596800000000,
    -62135596799999999,
    -1000001,
    -1000000,
    -1,
    0,
    1,
    100000,
    120000,
    123000,
    123400,
    123450,
    123456,
    1123456,
    253402300799999998,
    253402300799999999,
]
SCHEMA = {"fields": [{"name": "micros", "type": "INTEGER"}, {"name": "ts", "type": "TIMESTAMP"}]}
NESTED_SCHEMA = {
    "fields": [
        {
            "name": "record",
            "type": "RECORD",
            "fields": [
                {"name": "before_epoch", "type": "TIMESTAMP"},
                {"name": "last", "type": "TIMESTAMP"},
                {"name": "civil", "type": "DATETIME"},
            ],
        },
        {"name": "instants", "type": "TIMESTAMP", "mode": "REPEATED"},
        {"name": "missing", "type": "TIMESTAMP"},
    ]
}


class LocalRESTTimestamps(LocalService):
    def __init__(self):
        self.requests = []
        self.jobs = {}

    def rpc_handlers(self):
        return []

    def http_handler(self):
        owner = self

        class Handler(JSONHandler):
            def reject(self, message):
                self.respond({"error": {"code": 400, "message": message}}, 400)

            def do_POST(self):
                path = urlparse(self.path).path
                payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                owner.requests.append(("POST", path, payload))
                if path != f"/bigquery/v2/projects/{PROJECT}/queries":
                    self.reject("Unexpected timestamp mock request")
                    return
                if payload.get("formatOptions", {}).get("useInt64Timestamp") is not True:
                    self.reject("Missing formatOptions.useInt64Timestamp=true on jobs.query")
                    return
                modes = [
                    mode
                    for mode in ("inline", "polled", "paged", "nested")
                    if f"-- rest_timestamp_{mode}" in payload.get("query", "")
                ]
                if len(modes) != 1:
                    self.reject("Expected exactly one REST timestamp scenario")
                    return
                mode = modes[0]
                schema = NESTED_SCHEMA if mode == "nested" else SCHEMA
                response = {"jobComplete": True, "schema": schema, "totalBytesProcessed": "0"}
                if payload.get("dryRun"):
                    self.respond(response)
                    return

                rows = owner.rows(mode)
                if mode in ("inline", "nested"):
                    response.update(rows=rows, totalRows=str(len(rows)))
                else:
                    job_id = f"{mode}_{len(owner.jobs)}"
                    job_ref = {"projectId": PROJECT, "jobId": job_id, "location": "US"}
                    owner.jobs[job_id] = {"mode": mode, "jobReference": job_ref, "polls": 0}
                    response["jobReference"] = job_ref
                    if mode == "polled":
                        response = {"jobComplete": False, "jobReference": job_ref}
                    else:
                        response.update(rows=rows[:6], totalRows=str(len(rows)), pageToken="next-6")
                self.respond(response)

            def do_GET(self):
                url = urlparse(self.path)
                params = parse_qs(url.query)
                owner.requests.append(("GET", url.path, params))
                job_id = url.path.rsplit("/", 1)[-1]
                job = owner.jobs.get(job_id)
                if job is None:
                    self.reject("Unknown timestamp job")
                    return
                if url.path == f"/bigquery/v2/projects/{PROJECT}/jobs/{job_id}":
                    self.respond({"jobReference": job["jobReference"], "status": {"state": "DONE"}})
                    return
                if url.path != f"/bigquery/v2/projects/{PROJECT}/queries/{job_id}":
                    self.reject("Unexpected timestamp mock request")
                    return
                if params.get("formatOptions.useInt64Timestamp") != ["true"]:
                    self.reject("Missing formatOptions.useInt64Timestamp=true on jobs.getQueryResults")
                    return

                response = {
                    "jobComplete": True,
                    "jobReference": job["jobReference"],
                    "schema": SCHEMA,
                    "totalRows": str(len(MICROS)),
                }
                if job["mode"] == "polled":
                    job["polls"] += 1
                    if job["polls"] == 1:
                        self.respond({"jobComplete": False, "jobReference": job["jobReference"]})
                        return
                    response["rows"] = owner.rows("polled")
                else:
                    token = params.get("page_token", params.get("pageToken", [""]))[0]
                    if token == "next-6":
                        response.update(rows=owner.rows("paged")[6:11], pageToken="next-11")
                    elif token == "next-11":
                        response["rows"] = owner.rows("paged")[11:]
                    else:
                        self.reject("Unexpected timestamp page token")
                        return
                self.respond(response)

        return Handler

    @staticmethod
    def rows(mode):
        if mode != "nested":
            return [{"f": [{"v": str(micros)}, {"v": str(micros)}]} for micros in MICROS]
        return [
            {
                "f": [
                    {"v": {"f": [{"v": "-1"}, {"v": "253402300799999999"}, {"v": "2024-01-15T12:34:56.123456"}]}},
                    {
                        "v": [
                            {"v": str(micros)}
                            for micros in (-1000001, -1, 0, 1, 253402300799999999, -62135596800000000)
                        ]
                    },
                    {"v": None},
                ]
            }
        ]


class RestTimestampTests(unittest.TestCase):
    def test_rest_timestamp_sqllogic(self):
        with LocalRESTTimestamps() as service:
            service.run_sqllogic(
                "functions/function_bigquery_query_rest_timestamps.test",
                {"BQ_REST_TIMESTAMP_MOCK_API_ENDPOINT": service.endpoint},
            )

        posts = [payload for method, _, payload in service.requests if method == "POST"]
        self.assertTrue(posts, service.requests)
        modes = {
            mode
            for mode in ("inline", "polled", "paged", "nested")
            if any(f"-- rest_timestamp_{mode}" in payload["query"] for payload in posts if not payload.get("dryRun"))
        }
        self.assertEqual(modes, {"inline", "polled", "paged", "nested"})
        for payload in posts:
            self.assertIs(payload["formatOptions"]["useInt64Timestamp"], True)

        self.assertEqual({job["mode"] for job in service.jobs.values()}, {"polled", "paged"})
        for job_id, job in service.jobs.items():
            with self.subTest(job=job_id):
                gets = [
                    params
                    for method, path, params in service.requests
                    if method == "GET" and path.endswith(f"/queries/{job_id}")
                ]
                self.assertEqual(len(gets), 2, service.requests)
                for params in gets:
                    self.assertEqual(params.get("formatOptions.useInt64Timestamp"), ["true"])
                if job["mode"] == "polled":
                    self.assertEqual(job["polls"], 2)
                else:
                    tokens = [params.get("page_token", params.get("pageToken")) for params in gets]
                    self.assertEqual(tokens, [["next-6"], ["next-11"]])
