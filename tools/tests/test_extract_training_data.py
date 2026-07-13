"""Unit tests for `tools/extract_training_data.py`'s pure logic: arg
parsing/validation, the S3-endpoint-string parser, SQL-literal escaping,
and manifest assembly.

Deliberately does NOT test `connect_catalog()` / `resolve_source()` /
`get_snapshot_id()` / `extract_table()` -- those touch a live DuckDB
connection against a real (or port-forwarded) Lakekeeper REST catalog and
are exercised by the live kind run instead (brief Step 3; see
docs/runbooks/ml-extraction.md and `extract_training_data.py`'s module
docstring for that evidence). This split matches the brief's instruction:
"the script's pure logic ... gets unit tests runnable WITHOUT a cluster
... the end-to-end proof is the live kind run."

Run with: python -m unittest discover -s tools/tests
(stdlib unittest only -- no pytest dependency needed to test a script whose
own runtime dependency surface is deliberately just duckdb.)
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import extract_training_data as etd  # noqa: E402


class TestParseTables(unittest.TestCase):
    def test_splits_and_strips(self):
        self.assertEqual(etd.parse_tables("job_info, trace,mon_jdls_parsed"), ["job_info", "trace", "mon_jdls_parsed"])

    def test_default_tables_constant_matches_brief(self):
        self.assertEqual(etd.DEFAULT_TABLES, ("job_info", "trace", "mon_jdls_parsed"))

    def test_empty_and_whitespace_entries_dropped(self):
        self.assertEqual(etd.parse_tables("job_info,, trace , "), ["job_info", "trace"])

    def test_single_table(self):
        self.assertEqual(etd.parse_tables("job_info"), ["job_info"])


class TestValidateArgs(unittest.TestCase):
    def _args(self, **overrides):
        parser = etd.build_arg_parser()
        defaults = dict(
            tables=",".join(etd.DEFAULT_TABLES),
            output_dir="/tmp/out",
            catalog_uri="http://127.0.0.1:18181/catalog",
            token="dummy",
            warehouse="default",
            s3_endpoint=None,
            s3_access_key=None,
            s3_secret_key=None,
            s3_region="us-east-1",
            s3_url_style="path",
        )
        defaults.update(overrides)
        return parser.parse_args(
            [
                "--tables", defaults["tables"],
                "--output-dir", defaults["output_dir"],
                "--catalog-uri", defaults["catalog_uri"],
                "--token", defaults["token"],
                "--warehouse", defaults["warehouse"],
                "--s3-region", defaults["s3_region"],
                "--s3-url-style", defaults["s3_url_style"],
                *(["--s3-endpoint", defaults["s3_endpoint"]] if defaults["s3_endpoint"] else []),
                *(["--s3-access-key", defaults["s3_access_key"]] if defaults["s3_access_key"] else []),
                *(["--s3-secret-key", defaults["s3_secret_key"]] if defaults["s3_secret_key"] else []),
            ]
        )

    def test_default_args_valid(self):
        self.assertEqual(etd.validate_args(self._args()), [])

    def test_vended_credentials_path_valid_with_no_s3_flags(self):
        # No --s3-endpoint given: vended credentials, the cyfronet-cutover default.
        self.assertEqual(etd.validate_args(self._args()), [])

    def test_s3_endpoint_without_keys_is_invalid(self):
        errors = etd.validate_args(self._args(s3_endpoint="127.0.0.1:19000"))
        self.assertTrue(any("--s3-access-key and --s3-secret-key are required" in e for e in errors))

    def test_s3_endpoint_with_both_keys_is_valid(self):
        errors = etd.validate_args(
            self._args(s3_endpoint="127.0.0.1:19000", s3_access_key="ak", s3_secret_key="sk")
        )
        self.assertEqual(errors, [])

    def test_s3_keys_without_endpoint_is_invalid(self):
        errors = etd.validate_args(self._args(s3_access_key="ak", s3_secret_key="sk"))
        self.assertTrue(any("have no effect without --s3-endpoint" in e for e in errors))

    def test_invalid_table_identifier_rejected(self):
        errors = etd.validate_args(self._args(tables="job_info; DROP TABLE x"))
        self.assertTrue(any("not a valid SQL identifier" in e for e in errors))

    def test_empty_tables_rejected(self):
        errors = etd.validate_args(self._args(tables=" , ,"))
        self.assertTrue(any("must name at least one table" in e for e in errors))


class TestParseArgs(unittest.TestCase):
    def test_missing_required_arg_exits(self):
        with self.assertRaises(SystemExit):
            etd.parse_args(["--output-dir", "/tmp/out"])  # missing --catalog-uri/--token

    def test_full_argv_parses_and_passes_validation(self):
        args = etd.parse_args(
            [
                "--output-dir", "/tmp/out",
                "--catalog-uri", "http://127.0.0.1:18181/catalog",
                "--token", "dummy",
            ]
        )
        self.assertEqual(args.tables, ",".join(etd.DEFAULT_TABLES))
        self.assertEqual(args.warehouse, "default")
        self.assertIsNone(args.s3_endpoint)

    def test_s3_override_without_keys_exits(self):
        with self.assertRaises(SystemExit):
            etd.parse_args(
                [
                    "--output-dir", "/tmp/out",
                    "--catalog-uri", "http://127.0.0.1:18181/catalog",
                    "--token", "dummy",
                    "--s3-endpoint", "127.0.0.1:19000",
                ]
            )


class TestSplitS3Endpoint(unittest.TestCase):
    def test_https_scheme_strips_and_ssl_true(self):
        self.assertEqual(etd.split_s3_endpoint("https://minio.example.org:9000"), ("minio.example.org:9000", True))

    def test_http_scheme_strips_and_ssl_false(self):
        self.assertEqual(etd.split_s3_endpoint("http://127.0.0.1:19000"), ("127.0.0.1:19000", False))

    def test_bare_host_port_defaults_ssl_true(self):
        self.assertEqual(etd.split_s3_endpoint("127.0.0.1:19000"), ("127.0.0.1:19000", True))


class TestEscapeSqlLiteral(unittest.TestCase):
    def test_doubles_single_quotes(self):
        self.assertEqual(etd.escape_sql_literal("o'brien"), "o''brien")

    def test_no_quotes_unchanged(self):
        self.assertEqual(etd.escape_sql_literal("http://127.0.0.1:18181/catalog"), "http://127.0.0.1:18181/catalog")


class TestBuildManifest(unittest.TestCase):
    def test_manifest_shape_and_iso_timestamp(self):
        results = [
            etd.TableResult(
                table="job_info",
                source="lake.alice.job_info",
                source_schema="alice",
                row_count=1000,
                snapshot_id=5293861648653929208,
                output_file="job_info.parquet",
                fallback_reason="lake.contract.job_info not queryable: CatalogException(...)",
            ),
            etd.TableResult(
                table="trace",
                source="lake.contract.trace",
                source_schema="contract",
                row_count=1000,
                snapshot_id=8118311486491511736,
                output_file="trace.parquet",
                fallback_reason=None,
            ),
        ]
        extracted_at = datetime(2026, 7, 13, 1, 2, 3, tzinfo=timezone.utc)
        manifest = etd.build_manifest(
            results,
            extracted_at=extracted_at,
            catalog_uri="http://127.0.0.1:18181/catalog",
            warehouse="default",
        )

        self.assertEqual(manifest["extracted_at"], "2026-07-13T01:02:03Z")
        self.assertEqual(manifest["catalog_uri"], "http://127.0.0.1:18181/catalog")
        self.assertEqual(manifest["warehouse"], "default")
        self.assertEqual(set(manifest["tables"]), {"job_info", "trace"})

        job_info_entry = manifest["tables"]["job_info"]
        self.assertEqual(job_info_entry["row_count"], 1000)
        self.assertEqual(job_info_entry["snapshot_id"], 5293861648653929208)
        self.assertEqual(job_info_entry["source_schema"], "alice")
        self.assertIn("fallback_reason", job_info_entry)

        trace_entry = manifest["tables"]["trace"]
        self.assertEqual(trace_entry["source_schema"], "contract")
        self.assertNotIn("fallback_reason", trace_entry)

    def test_manifest_is_json_serializable(self):
        import json

        results = [
            etd.TableResult(
                table="job_info",
                source="lake.alice.job_info",
                source_schema="alice",
                row_count=1000,
                snapshot_id=1,
                output_file="job_info.parquet",
            )
        ]
        manifest = etd.build_manifest(
            results,
            extracted_at=datetime.now(timezone.utc),
            catalog_uri="http://127.0.0.1:18181/catalog",
            warehouse="default",
        )
        json.dumps(manifest)  # must not raise


if __name__ == "__main__":
    unittest.main()
