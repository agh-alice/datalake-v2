"""Unit tests for pipeline.py's env-var driven incremental `initial_value`
overrides (Plan 2 Task 6; mechanism documented in docs/runbooks/backfill.md).

Exercises only the pure resolution helpers (`_resolve_naive_initial_value`,
`_resolve_int_initial_value`) -- NOT `build_job_info_source`/
`build_trace_source`/`build_mon_jdls_resource` themselves, which call dlt's
`sql_database`/`sql_table` and need a live PG connection for reflection
(no fake-able seam for that here). Those three, and the env hook actually
threading through them end to end, are proven live by
`hack/run-ingest-once.sh` against the kind fixture (see docs/runbooks/
backfill.md), not by this unit suite.
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
