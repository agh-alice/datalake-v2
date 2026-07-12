"""Unit tests for pipeline.py's env-var driven incremental `initial_value`
overrides (Plan 2 Task 6; mechanism documented in docs/runbooks/backfill.md).

Exercises only the pure resolution helpers (`_resolve_naive_initial_value`,
`_resolve_int_initial_value`) -- NOT `build_job_info_source`/
`build_trace_source`/`build_mon_jdls_resource` themselves, which call dlt's
`sql_database`/`sql_table` and need a live PG connection for reflection
(no fake-able seam for that here).

The env hook threading through those three end to end (env var -> pod ->
`_resolve_naive_initial_value`/`_resolve_int_initial_value` -> dlt
incremental cursor -> actual row filtering) is NOT exercised by
`hack/run-ingest-once.sh` (that script never sets an `INGEST_INITIAL_*`
var, only the default/unset path) -- it was instead verified live,
one-off, in Plan 2 Task 6: `hack/reset-pipeline.sh`, then a manual
Workflow with `INGEST_INITIAL_JOB_INFO=2030-01-01` against the freshly
reset kind fixture, then an Iceberg-scan probe. Result: `job_info
count=0` (every fixture row's `last_update` is well before 2030, so the
override correctly excluded all of them) while `trace count=1000` and
`mon_jdls_parsed count=1000` loaded normally on their (also freshly reset)
defaults -- confirming the override reaches dlt's cursor and actually
changes what gets fetched, not just that the value parses. Not repeatable
as an automated test here (needs the live cluster); see
docs/runbooks/backfill.md for the reusable manual procedure.
"""

from __future__ import annotations

import pendulum
import pytest

from alice_ingest.pipeline import (
    _resolve_int_initial_value,
    _resolve_naive_initial_value,
)


class TestResolveNaiveInitialValue:
    """job_info's `INGEST_INITIAL_JOB_INFO` cursor override -- naive
    (production's `last_update` is `timestamp WITHOUT time zone`, see
    pipeline.py's module docstring on why aware-vs-naive is load-bearing,
    not cosmetic)."""

    def test_default_when_env_var_absent(self):
        default = pendulum.naive(2026, 1, 1)
        assert _resolve_naive_initial_value({}, "INGEST_INITIAL_JOB_INFO", default) == default

    def test_default_when_env_var_empty_string(self):
        default = pendulum.naive(2026, 1, 1)
        env = {"INGEST_INITIAL_JOB_INFO": ""}
        assert _resolve_naive_initial_value(env, "INGEST_INITIAL_JOB_INFO", default) == default

    def test_parses_date_only(self):
        env = {"INGEST_INITIAL_JOB_INFO": "2026-03-01"}
        result = _resolve_naive_initial_value(env, "INGEST_INITIAL_JOB_INFO", pendulum.naive(2026, 1, 1))
        assert result == pendulum.naive(2026, 3, 1)

    def test_parses_date_and_time(self):
        env = {"INGEST_INITIAL_JOB_INFO": "2026-03-01T12:30:45"}
        result = _resolve_naive_initial_value(env, "INGEST_INITIAL_JOB_INFO", pendulum.naive(2026, 1, 1))
        assert result == pendulum.naive(2026, 3, 1, 12, 30, 45)

    def test_result_has_no_tzinfo(self):
        env = {"INGEST_INITIAL_JOB_INFO": "2026-03-01"}
        result = _resolve_naive_initial_value(env, "INGEST_INITIAL_JOB_INFO", pendulum.naive(2026, 1, 1))
        assert result.tzinfo is None

    def test_rejects_offset_aware_input(self):
        # An aware literal pushed down against job_info's naive `last_update`
        # column forces a session-timezone cast in SQL, silently shifting the
        # cursor -- the exact bug class pipeline.py's module docstring warns
        # about (mirror image of the one fixed in a610069). Refuse loudly
        # instead of silently dropping the offset.
        env = {"INGEST_INITIAL_JOB_INFO": "2026-03-01T00:00:00+02:00"}
        with pytest.raises(SystemExit, match="INGEST_INITIAL_JOB_INFO"):
            _resolve_naive_initial_value(env, "INGEST_INITIAL_JOB_INFO", pendulum.naive(2026, 1, 1))

    def test_rejects_malformed_input(self):
        env = {"INGEST_INITIAL_JOB_INFO": "not-a-date"}
        with pytest.raises(SystemExit, match="INGEST_INITIAL_JOB_INFO"):
            _resolve_naive_initial_value(env, "INGEST_INITIAL_JOB_INFO", pendulum.naive(2026, 1, 1))


class TestResolveIntInitialValue:
    """trace's `INGEST_INITIAL_TRACE` (epoch-ms `laststatuschangetimestamp`)
    and mon_jdls's `INGEST_INITIAL_MON_JDLS` (`job_id`) cursor overrides --
    both plain integers, no unit conversion (pipeline.py never converts
    units; it hands the literal straight to dlt's incremental cursor)."""

    def test_default_when_env_var_absent(self):
        assert _resolve_int_initial_value({}, "INGEST_INITIAL_TRACE", 0) == 0

    def test_default_when_env_var_empty_string(self):
        env = {"INGEST_INITIAL_TRACE": ""}
        assert _resolve_int_initial_value(env, "INGEST_INITIAL_TRACE", 0) == 0

    def test_parses_integer(self):
        env = {"INGEST_INITIAL_TRACE": "1700000000000"}
        assert _resolve_int_initial_value(env, "INGEST_INITIAL_TRACE", 0) == 1700000000000

    def test_default_preserved_when_nonzero(self):
        assert _resolve_int_initial_value({}, "INGEST_INITIAL_MON_JDLS", 501) == 501

    def test_rejects_non_integer(self):
        env = {"INGEST_INITIAL_MON_JDLS": "not-an-int"}
        with pytest.raises(SystemExit, match="INGEST_INITIAL_MON_JDLS"):
            _resolve_int_initial_value(env, "INGEST_INITIAL_MON_JDLS", 0)

    def test_rejects_float_string(self):
        # int("500.0") raises ValueError -- must not silently truncate a
        # fractional job_id/epoch, which would be nonsensical for either
        # cursor type.
        env = {"INGEST_INITIAL_TRACE": "500.0"}
        with pytest.raises(SystemExit, match="INGEST_INITIAL_TRACE"):
            _resolve_int_initial_value(env, "INGEST_INITIAL_TRACE", 0)


# ---------------------------------------------------------------------------
# N3 (final-review, controller decision): mon_jdls's JDL list fields (e.g.
# `Packages`) must land as VALUE columns on `mon_jdls_parsed`, not spin off
# child tables (`mon_jdls_parsed__jdl__packages`) -- the ML consumer's data
# contract expects Packages as a column, not a separate table.
# ---------------------------------------------------------------------------


class FakeSqlTableResource:
    """Stands in for the object `dlt.sources.sql_database.sql_table(...)`
    returns, recording exactly what `build_mon_jdls_resource()` does to it
    -- without needing a live PostgreSQL connection for schema reflection.
    `sql_table()` reflects EAGERLY at construction time (confirmed against
    the pinned dlt 1.28.2: calling it with an unreachable PG URL raises
    `sqlalchemy.exc.OperationalError` immediately, before any resource
    object exists) -- this file's module docstring's "no fake-able seam"
    note is about exercising the actual incremental-cursor/row-filtering
    behavior end to end, which is true; monkeypatching the module-level
    `sql_table` NAME `pipeline.py` imports is a real, separate seam for
    testing what `build_mon_jdls_resource` does to whatever `sql_table`
    returns, used here."""

    def __init__(self):
        self.mapped: list = []
        self.hints: dict | None = None
        self._max_table_nesting: int | None = None

    def add_map(self, fn):
        self.mapped.append(fn)
        return self

    def apply_hints(self, **kwargs):
        self.hints = kwargs
        return self

    @property
    def max_table_nesting(self):
        return self._max_table_nesting

    @max_table_nesting.setter
    def max_table_nesting(self, value):
        self._max_table_nesting = value


class TestBuildMonJdlsResourceSetsMaxTableNesting:
    """Verified empirically against the pinned dlt 1.28.2 in this project's
    venv (see TestMaxTableNestingSuppressesChildTables below for the
    underlying mechanism): `resource.max_table_nesting = 1` is a settable
    PROPERTY on `DltResource` (`dlt/extract/resource.py`), NOT an
    `apply_hints()` kwarg -- `apply_hints()` has no `max_table_nesting`
    parameter at all in 1.28.2 (confirmed via `inspect.signature
    (DltResource.apply_hints)`)."""

    def test_sets_max_table_nesting_to_one(self, monkeypatch):
        from alice_ingest import pipeline as pipeline_module

        fake_resource = FakeSqlTableResource()
        monkeypatch.setattr(pipeline_module, "sql_table", lambda **kwargs: fake_resource)

        pipeline_module.build_mon_jdls_resource("postgresql://fake/db", env={})

        assert fake_resource.max_table_nesting == 1

    def test_still_applies_the_pre_existing_hints(self, monkeypatch):
        from alice_ingest import pipeline as pipeline_module

        fake_resource = FakeSqlTableResource()
        monkeypatch.setattr(pipeline_module, "sql_table", lambda **kwargs: fake_resource)

        pipeline_module.build_mon_jdls_resource("postgresql://fake/db", env={})

        assert fake_resource.hints["table_name"] == "mon_jdls_parsed"
        assert fake_resource.hints["table_format"] == "iceberg"
        assert fake_resource.mapped  # parse_jdl was add_map'd


class TestMaxTableNestingSuppressesChildTables:
    """The underlying dlt mechanism N3's fix relies on, proven directly
    against dlt's own extract+normalize pipeline steps (no destination I/O
    -- the `dummy` destination, `pipeline.extract()` + `pipeline.normalize()`
    only) using a resource shaped exactly like `parse_jdl`'s real output
    (`ingest/tests/fixtures/jdl_samples.json`'s `Packages` field: a JSON
    list of strings). Confirms both halves of N3's requirement: list fields
    stop spinning off child tables, AND scalar dict fields (`jdl__ttl` etc.)
    still flatten normally -- `max_table_nesting=0` would suppress BOTH
    (verified interactively; not what N3 wants), `max_table_nesting=1` is
    the value that keeps exactly the dict-flattening while demoting list
    fields to JSON-typed value columns."""

    _SAMPLE_JDL = {
        "TTL": "3600",
        "Packages": ["AliPhysics::vAN-20260101", "ROOT::v6-30"],
        "LPMPassName": "pass1",
    }

    def _normalize(self, tmp_path, pipeline_name: str, max_table_nesting: int | None):
        import dlt

        sample_jdl = self._SAMPLE_JDL

        @dlt.resource(name="mon_jdls_parsed", write_disposition="append")
        def mon_jdls():
            yield {"job_id": 1, "jdl_parse_ok": True, "jdl": sample_jdl}

        resource = mon_jdls()
        if max_table_nesting is not None:
            resource.max_table_nesting = max_table_nesting

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            pipelines_dir=str(tmp_path),
            destination="dummy",
            dataset_name="alice",
        )
        pipeline.extract(resource)
        pipeline.normalize()
        return pipeline.default_schema.tables

    def test_default_nesting_spins_off_a_packages_child_table(self, tmp_path):
        # Baseline / regression documentation: this is the bug N3 fixes.
        tables = self._normalize(tmp_path, "p2_final_n3_default", None)

        assert "mon_jdls_parsed__jdl__packages" in tables
        assert "jdl__packages" not in tables["mon_jdls_parsed"]["columns"]

    def test_max_table_nesting_one_keeps_packages_as_a_value_column(self, tmp_path):
        tables = self._normalize(tmp_path, "p2_final_n3_nesting1", 1)

        assert "mon_jdls_parsed__jdl__packages" not in tables
        assert tables["mon_jdls_parsed"]["columns"]["jdl__packages"]["data_type"] == "json"

    def test_max_table_nesting_one_still_flattens_scalar_dict_fields(self, tmp_path):
        tables = self._normalize(tmp_path, "p2_final_n3_flatten", 1)

        cols = tables["mon_jdls_parsed"]["columns"]
        assert "jdl__ttl" in cols
        assert "jdl__lpm_pass_name" in cols
