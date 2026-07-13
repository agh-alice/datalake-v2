"""Unit tests for `tools/extract_training_data.py`'s pure logic: arg
parsing/validation, the S3-endpoint-string parser, SQL-literal escaping,
manifest assembly, and (P3T4 review Important 2) `resolve_source()`'s
fallback-cause disambiguation branching.

Deliberately does NOT test `connect_catalog()` / `get_snapshot_id()` /
`extract_table()` end-to-end, or the REST helpers' actual HTTP transport --
those touch a live DuckDB connection and/or a real (or port-forwarded)
Lakekeeper REST catalog and are exercised by the live kind run instead
(brief Step 3; see docs/runbooks/ml-extraction.md and
`extract_training_data.py`'s module docstring for that evidence). This
split matches the brief's instruction: "the script's pure logic ... gets
unit tests runnable WITHOUT a cluster ... the end-to-end proof is the live
kind run."

`resolve_source()` itself IS unit-tested (P3T4 review Important 2): it only
takes a DuckDB connection object and, on fallback, makes REST calls through
the module-level `_rest_get_json()` function -- both are faked below
(`FakeConnection`, `_fake_rest_get_json()`), so the branching is exercised
without a live cluster.

Run with: python -m unittest discover -s tools/tests
(stdlib unittest only -- no pytest dependency needed to test a script whose
own runtime dependency surface is deliberately just duckdb; pytest also
runs this file fine via its unittest.TestCase discovery, if pytest happens
to be installed.)
"""

from __future__ import annotations

import sys
import unittest
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

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


class FakeConnection:
    """Mimics `duckdb.DuckDBPyConnection` just enough for `resolve_source()`:
    it only ever calls `con.execute(sql)` (no `.fetchone()`/`.fetchall()`),
    once per candidate ref (`lake.contract.<table>`, then possibly
    `lake.alice.<table>`), to probe queryability. `raise_for` names the
    refs (substring-matched against the executed SQL) that should raise,
    simulating a DuckDB `CatalogException` -- everything else succeeds."""

    def __init__(self, *, raise_for: frozenset[str] = frozenset()):
        self.raise_for = raise_for
        self.executed: list[str] = []

    def execute(self, sql: str, *args, **kwargs):
        self.executed.append(sql)
        for ref in self.raise_for:
            if ref in sql:
                raise RuntimeError(f"simulated CatalogException: {ref} does not exist! (sql: {sql})")
        return self


def _fake_rest_get_json(*, config_response=None, config_error=None, views_response=None, views_error=None):
    """Fake REST responder standing in for `extract_training_data._rest_get_json`
    (the module's one network-transport function -- see that function's
    docstring). Dispatches on URL shape: `/v1/config` -> the warehouse-prefix
    discovery call, `.../namespaces/<ns>/views` -> the view-listing call.
    Raises `AssertionError` on any other URL so a test can't silently pass
    by hitting an un-mocked code path."""

    def fake(url: str, token: str, *, timeout: float = etd.REST_TIMEOUT_SECONDS):
        if "/v1/config" in url:
            if config_error is not None:
                raise config_error
            return config_response
        if "/namespaces/" in url and url.endswith("/views"):
            if views_error is not None:
                raise views_error
            return views_response
        raise AssertionError(f"unexpected REST URL in test: {url!r}")

    return fake


class TestResolveSource(unittest.TestCase):
    CATALOG_URI = "http://127.0.0.1:18181/catalog"
    WAREHOUSE = "default"
    TOKEN = "dummy"
    # Verified live 2026-07-13 against this repo's kind cluster: Lakekeeper's
    # /v1/config?warehouse=default returns `overrides.prefix` as a warehouse
    # UUID, NOT the warehouse name -- see rest_catalog_prefix()'s docstring.
    CONFIG_RESPONSE = {
        "overrides": {"prefix": "2b6351e0-7e14-11f1-806a-db43a36e9447"},
        "defaults": {"prefix": "should-not-be-used-when-overrides-present"},
    }

    def _resolve(self, con, table="job_info"):
        return etd.resolve_source(
            con,
            table,
            catalog_uri=self.CATALOG_URI,
            warehouse=self.WAREHOUSE,
            token=self.TOKEN,
        )

    def test_contract_works_no_fallback(self):
        con = FakeConnection()
        schema, ref, reason = self._resolve(con)
        self.assertEqual(schema, "contract")
        self.assertEqual(ref, "lake.contract.job_info")
        self.assertIsNone(reason)
        # No REST call needed when the contract read succeeds outright.
        self.assertEqual(con.executed, ["SELECT 1 FROM lake.contract.job_info LIMIT 0"])

    def test_contract_fails_view_exists_gives_upstream_reason(self):
        con = FakeConnection(raise_for=frozenset({"lake.contract.job_info"}))
        views_response = {"identifiers": [{"namespace": ["contract"], "name": "job_info"}]}
        fake_rest = _fake_rest_get_json(config_response=self.CONFIG_RESPONSE, views_response=views_response)
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            schema, ref, reason = self._resolve(con)
        self.assertEqual(schema, "alice")
        self.assertEqual(ref, "lake.alice.job_info")
        self.assertIsNotNone(reason)
        self.assertIn("duckdb-iceberg cannot read REST-catalog views (view EXISTS server-side)", reason)

    def test_contract_fails_view_absent_from_list_gives_apply_views_reason(self):
        con = FakeConnection(raise_for=frozenset({"lake.contract.job_info"}))
        views_response = {"identifiers": [{"namespace": ["contract"], "name": "some_other_table"}]}
        fake_rest = _fake_rest_get_json(config_response=self.CONFIG_RESPONSE, views_response=views_response)
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            schema, ref, reason = self._resolve(con)
        self.assertEqual(schema, "alice")
        self.assertIn("contract view NOT FOUND server-side", reason)
        self.assertIn("apply-views", reason)

    def test_contract_fails_views_endpoint_404_gives_apply_views_reason(self):
        con = FakeConnection(raise_for=frozenset({"lake.contract.job_info"}))
        http_404 = urllib.error.HTTPError(url="x", code=404, msg="Not Found", hdrs=None, fp=None)
        fake_rest = _fake_rest_get_json(config_response=self.CONFIG_RESPONSE, views_error=http_404)
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            schema, ref, reason = self._resolve(con)
        self.assertEqual(schema, "alice")
        self.assertIn("contract view NOT FOUND server-side", reason)
        self.assertIn("apply-views", reason)

    def test_contract_fails_rest_check_itself_fails_gives_unknown_reason(self):
        con = FakeConnection(raise_for=frozenset({"lake.contract.job_info"}))
        fake_rest = _fake_rest_get_json(config_error=RuntimeError("network unreachable"))
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            schema, ref, reason = self._resolve(con)
        self.assertEqual(schema, "alice")
        self.assertIn("contract read failed; could not determine cause (REST check failed:", reason)
        self.assertIn("network unreachable", reason)

    def test_contract_fails_views_endpoint_non_404_error_gives_unknown_reason(self):
        con = FakeConnection(raise_for=frozenset({"lake.contract.job_info"}))
        http_500 = urllib.error.HTTPError(url="x", code=500, msg="Internal Server Error", hdrs=None, fp=None)
        fake_rest = _fake_rest_get_json(config_response=self.CONFIG_RESPONSE, views_error=http_500)
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            schema, ref, reason = self._resolve(con)
        self.assertEqual(schema, "alice")
        self.assertIn("contract read failed; could not determine cause (REST check failed:", reason)

    def test_alice_also_fails_raises_hard_error_without_rest_call(self):
        con = FakeConnection(raise_for=frozenset({"lake.contract.job_info", "lake.alice.job_info"}))
        # Deliberately NOT mocking _rest_get_json: alice's own readability is
        # checked before any REST disambiguation is attempted, so a real
        # network call must never happen on this path (see resolve_source()'s
        # ordering rationale in its docstring). If the code changed to call
        # the real urllib transport here, this test would hang/fail instead
        # of asserting cleanly -- that failure mode is the point.
        with self.assertRaises(RuntimeError) as ctx:
            self._resolve(con)
        message = str(ctx.exception)
        self.assertIn("lake.contract.job_info", message)
        self.assertIn("lake.alice.job_info", message)
        self.assertIn("unreadable", message)


class TestRestCatalogPrefix(unittest.TestCase):
    def test_prefers_overrides_prefix(self):
        fake_rest = _fake_rest_get_json(
            config_response={"overrides": {"prefix": "uuid-1"}, "defaults": {"prefix": "uuid-2"}}
        )
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            prefix = etd.rest_catalog_prefix("http://127.0.0.1:18181/catalog", "default", "dummy")
        self.assertEqual(prefix, "uuid-1")

    def test_falls_back_to_defaults_prefix_when_overrides_missing(self):
        fake_rest = _fake_rest_get_json(config_response={"overrides": {}, "defaults": {"prefix": "uuid-2"}})
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            prefix = etd.rest_catalog_prefix("http://127.0.0.1:18181/catalog", "default", "dummy")
        self.assertEqual(prefix, "uuid-2")

    def test_raises_when_no_prefix_anywhere(self):
        fake_rest = _fake_rest_get_json(config_response={"overrides": {}, "defaults": {}})
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            with self.assertRaises(RuntimeError):
                etd.rest_catalog_prefix("http://127.0.0.1:18181/catalog", "default", "dummy")


class TestCheckContractViewExists(unittest.TestCase):
    CONFIG_RESPONSE = {"overrides": {"prefix": "uuid-1"}, "defaults": {}}

    def test_true_when_table_listed(self):
        views_response = {"identifiers": [{"namespace": ["contract"], "name": "job_info"}]}
        fake_rest = _fake_rest_get_json(config_response=self.CONFIG_RESPONSE, views_response=views_response)
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            self.assertTrue(
                etd.check_contract_view_exists(
                    "http://127.0.0.1:18181/catalog", "default", "dummy", "contract", "job_info"
                )
            )

    def test_false_when_identifiers_empty(self):
        fake_rest = _fake_rest_get_json(config_response=self.CONFIG_RESPONSE, views_response={"identifiers": []})
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            self.assertFalse(
                etd.check_contract_view_exists(
                    "http://127.0.0.1:18181/catalog", "default", "dummy", "contract", "job_info"
                )
            )

    def test_false_on_404(self):
        http_404 = urllib.error.HTTPError(url="x", code=404, msg="Not Found", hdrs=None, fp=None)
        fake_rest = _fake_rest_get_json(config_response=self.CONFIG_RESPONSE, views_error=http_404)
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            self.assertFalse(
                etd.check_contract_view_exists(
                    "http://127.0.0.1:18181/catalog", "default", "dummy", "contract", "job_info"
                )
            )

    def test_reraises_non_404_http_error(self):
        http_500 = urllib.error.HTTPError(url="x", code=500, msg="Internal Server Error", hdrs=None, fp=None)
        fake_rest = _fake_rest_get_json(config_response=self.CONFIG_RESPONSE, views_error=http_500)
        with mock.patch.object(etd, "_rest_get_json", fake_rest):
            with self.assertRaises(urllib.error.HTTPError):
                etd.check_contract_view_exists(
                    "http://127.0.0.1:18181/catalog", "default", "dummy", "contract", "job_info"
                )


if __name__ == "__main__":
    unittest.main()
