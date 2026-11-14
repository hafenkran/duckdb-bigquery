"""Check SDK patch application without configuring or building dependencies."""

from pathlib import Path
import subprocess
import tempfile
import unittest
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[2]
PATCH = ROOT / "external/patches/google-cloud-cpp-2.47.1-bigquery-timestamp-format.patch"
SDK_FILE = "google/cloud/bigquerycontrol/v2/internal/job_rest_stub.cc"


class SDKPatchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="bigquery sdk patch ")
        self.addCleanup(self.directory.cleanup)
        self.source = Path(self.directory.name)
        self.stub = self.source / SDK_FILE
        self.stub.parent.mkdir(parents=True)
        with ZipFile(ROOT / "external/vendor/google-cloud-cpp-2.47.1.zip") as archive:
            filename = next(name for name in archive.namelist() if name.endswith("/" + SDK_FILE))
            self.stub.write_bytes(archive.read(filename))

    def apply_patch(self):
        return subprocess.run(
            [
                "cmake",
                f"-DSOURCE_DIR={self.source}",
                f"-DPATCH_FILE={PATCH}",
                "-P",
                str(ROOT / "external/apply_patch.cmake"),
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def test_timestamp_patch_can_be_reapplied(self):
        result = self.apply_patch()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        applied = self.stub.read_bytes()
        self.assertIn(b'"formatOptions.useInt64Timestamp"', applied)
        result = self.apply_patch()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.stub.read_bytes(), applied)
        self.assertIn("Patch already applied", result.stdout)

    def test_timestamp_patch_rejects_conflicting_source_without_modifying_it(self):
        source = self.stub.read_bytes().replace(
            b'query_params.push_back({"location", request.location()});',
            b'query_params.push_back({"location", "conflicting-location"});',
        )
        self.stub.write_bytes(source)
        result = self.apply_patch()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Patch neither applies nor is already applied", result.stderr)
        self.assertEqual(self.stub.read_bytes(), source)

    def test_timestamp_patch_rejects_modified_applied_hunk(self):
        result = self.apply_patch()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        source = self.stub.read_bytes().replace(b"request.format_options().use_int64_timestamp()", b"false")
        self.stub.write_bytes(source)
        result = self.apply_patch()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Patch neither applies nor is already applied", result.stderr)
        self.assertEqual(self.stub.read_bytes(), source)
