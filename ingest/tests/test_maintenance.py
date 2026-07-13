"""TDD for alice_ingest.maintenance (Plan 2 Task 5; Plan 3 Task 3 extends this
module with Trino physical maintenance + the data-layer freshness gate).

Written BEFORE alice_ingest/maintenance.py exists -- first run must be RED
(collection error: `ModuleNotFoundError: No module named 'alice_ingest.maintenance'`).

Scope: unit-test the pure orchestration logic (which tables get maintained,
what gets called on each table, how before/after snapshot counts turn into a
summary) against fakes -- no live Lakekeeper/Iceberg needed. The pyiceberg
maintenance API SHAPE itself (`table.maintenance.expire_snapshots()
.older_than(dt).commit()`) was verified live against the pinned dependency
(dlt[pyiceberg]==1.28.2 -> pyiceberg==0.11.1) in a throwaway venv, 2026-07-12
-- see maintenance.py's module docstring for the verification trail. These
fakes mirror that verified interface shape (a `.maintenance` property
returning an object with `.expire_snapshots()` -> a builder with
`.older_than(dt)` (chainable) and `.commit()`), not pyiceberg's internals.

Plan 3 Task 3 section (below TestNoBlockedAlternativeNeeded): Trino physical
maintenance (`ALTER TABLE ... EXECUTE optimize/expire_snapshots/
remove_orphan_files`) and the nightly data-layer freshness gate
(`max(committed_at)` from each core table's `"<table>$snapshots"` metadata
table). Exact procedure syntax, the 7d retention floor, and the vended-
credentials S3 LIST permission for `remove_orphan_files` were all verified
LIVE against the pinned Trino 476 on this kind cluster, 2026-07-12/13 -- see
maintenance.py's module docstring for the verification trail (docs page
https://trino.io/docs/476/connector/iceberg.html cross-checked against a real
probe pod). These tests exercise the pure orchestration logic against a
scripted fake Trino client (`ScriptedTrinoClient`, exact-SQL-keyed, mirrors
the FakeTrinoClient/FakeShowColumnsClient pattern already established in
test_views.py) -- no live Trino needed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from alice_ingest.maintenance import (
    CORE_FRESHNESS_TABLES,
    DEFAULT_FRESHNESS_MAX_AGE_HOURS,
    DEFAULT_MAINTENANCE_OLDER_THAN_DAYS,
    DEFAULT_TRINO_MAINTENANCE_RETENTION_THRESHOLD,
    FreshnessResult,
    KubernetesApiError,
    NIGHTLY_CRON_WORKFLOW_NAME,
    OverlapGuardResult,
    TableMaintenanceResult,
    TrinoMaintenanceResult,
    WorkflowsClient,
    check_freshness,
    check_nightly_overlap_guard,
    expire_table_snapshots,
    format_freshness_summary,
    format_table_summary,
    format_trino_maintenance_summary,
    is_cron_workflow_running,
    list_alice_tables,
    list_trino_alice_tables,
    run,
    run_freshness_check,
    run_trino,
    run_trino_maintenance,
    run_trino_table_maintenance,
    table_freshness,
)
from alice_ingest.views import TrinoQueryError


class FakeExpireSnapshotsBuilder:
    """Mirrors pyiceberg 0.11.1's real `ExpireSnapshots` builder shape
    (verified live): `.older_than(dt)` returns self (chainable), `.commit()`
    applies the pending expiry. This fake actually performs the expiry
    against its owning FakeTable's snapshot list (filtering by
    `timestamp_ms < older_than`), so tests exercise real filtering
    semantics, not just "was commit() called"."""

    def __init__(self, table: "FakeTable"):
        self._table = table
        self.older_than_calls: list[datetime] = []
        self._cutoff: datetime | None = None

    def older_than(self, dt: datetime) -> "FakeExpireSnapshotsBuilder":
        self.older_than_calls.append(dt)
        self._cutoff = dt
        return self

    def commit(self) -> None:
        assert self._cutoff is not None, "commit() called without older_than()"
        self._table._snapshots = [
            s for s in self._table._snapshots if s >= self._cutoff
        ]
        self._table.committed = True


class FakeMaintenance:
    def __init__(self, table: "FakeTable"):
        self._table = table

    def expire_snapshots(self) -> FakeExpireSnapshotsBuilder:
        builder = FakeExpireSnapshotsBuilder(self._table)
        self._table.builders.append(builder)
        return builder


class FakeTable:
    """`snapshots` here is a list of datetimes standing in for pyiceberg
    Snapshot objects' timestamps -- expire_table_snapshots only needs
    `len(table.snapshots())` before/after, never the Snapshot objects'
    other fields, so this simplification is faithful to what the module
    under test actually reads."""

    def __init__(self, identifier: str, snapshots: list[datetime]):
        self.identifier = identifier
        self._snapshots = list(snapshots)
        self.committed = False
        self.refresh_calls = 0
        self.builders: list[FakeExpireSnapshotsBuilder] = []

    @property
    def maintenance(self) -> FakeMaintenance:
        return FakeMaintenance(self)

    def snapshots(self) -> list[datetime]:
        return self._snapshots

    def refresh(self) -> None:
        self.refresh_calls += 1


class FakeCatalog:
    def __init__(self, tables: dict[str, FakeTable]):
        self._tables = tables
        self.list_tables_calls: list[str] = []
        self.load_table_calls: list[str] = []

    def list_tables(self, namespace: str) -> list[tuple[str, str]]:
        self.list_tables_calls.append(namespace)
        return [tuple(identifier.split(".")) for identifier in self._tables]

    def load_table(self, identifier: str) -> FakeTable:
        self.load_table_calls.append(identifier)
        return self._tables[identifier]


NOW = datetime(2026, 7, 12, tzinfo=timezone.utc)
OLD = NOW - timedelta(days=10)
RECENT = NOW - timedelta(days=1)


class TestListAliceTables:
    def test_returns_dotted_identifiers_for_every_table_in_the_namespace(self):
        catalog = FakeCatalog(
            {
                "alice.job_info": FakeTable("alice.job_info", []),
                "alice.trace": FakeTable("alice.trace", []),
            }
        )

        result = list_alice_tables(catalog, "alice")

        assert catalog.list_tables_calls == ["alice"]
        assert sorted(result) == ["alice.job_info", "alice.trace"]

    def test_empty_namespace_returns_empty_list(self):
        catalog = FakeCatalog({})

        assert list_alice_tables(catalog, "alice") == []


class TestExpireTableSnapshots:
    def test_calls_older_than_with_the_given_cutoff_and_commits(self):
        table = FakeTable("alice.job_info", [OLD, RECENT])
        catalog = FakeCatalog({"alice.job_info": table})

        expire_table_snapshots(catalog, "alice.job_info", NOW - timedelta(days=7))

        assert table.builders[0].older_than_calls == [NOW - timedelta(days=7)]
        assert table.committed is True

    def test_refreshes_the_table_after_commit_before_recounting(self):
        table = FakeTable("alice.job_info", [OLD])
        catalog = FakeCatalog({"alice.job_info": table})

        expire_table_snapshots(catalog, "alice.job_info", NOW - timedelta(days=7))

        assert table.refresh_calls == 1

    def test_result_reports_before_after_and_expired_counts(self):
        table = FakeTable("alice.job_info", [OLD, OLD, RECENT])
        catalog = FakeCatalog({"alice.job_info": table})

        result = expire_table_snapshots(catalog, "alice.job_info", NOW - timedelta(days=7))

        assert result == TableMaintenanceResult(
            table="alice.job_info", snapshots_before=3, snapshots_after=1
        )
        assert result.expired == 2

    def test_no_snapshots_older_than_cutoff_expires_nothing(self):
        table = FakeTable("alice.trace", [RECENT, RECENT])
        catalog = FakeCatalog({"alice.trace": table})

        result = expire_table_snapshots(catalog, "alice.trace", NOW - timedelta(days=7))

        assert result.expired == 0
        assert result.snapshots_before == result.snapshots_after == 2


class TestFormatTableSummary:
    def test_matches_the_documented_per_table_log_format(self):
        result = TableMaintenanceResult(
            table="alice.job_info", snapshots_before=5, snapshots_after=2
        )

        assert format_table_summary(result) == (
            "MAINTENANCE table=alice.job_info snapshots_before=5 "
            "snapshots_after=2 expired=3"
        )


class TestRun:
    def test_iterates_every_table_and_prints_a_summary_line_each(
        self, monkeypatch, capsys
    ):
        import alice_ingest.maintenance as maintenance_module

        catalog = FakeCatalog(
            {
                "alice.job_info": FakeTable("alice.job_info", [OLD, RECENT]),
                "alice.trace": FakeTable("alice.trace", [OLD]),
            }
        )
        monkeypatch.setattr(
            maintenance_module, "load_iceberg_catalog", lambda env: catalog
        )

        exit_code = run({})

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "MAINTENANCE table=alice.job_info" in out
        assert "MAINTENANCE table=alice.trace" in out
        assert "MAINTENANCE tables=2 total_expired=2" in out

    def test_no_tables_in_namespace_is_reported_and_not_an_error(
        self, monkeypatch, capsys
    ):
        import alice_ingest.maintenance as maintenance_module

        catalog = FakeCatalog({})
        monkeypatch.setattr(
            maintenance_module, "load_iceberg_catalog", lambda env: catalog
        )

        exit_code = run({})

        assert exit_code == 0
        assert "MAINTENANCE SKIPPED: no tables found" in capsys.readouterr().out

    def test_default_older_than_is_seven_days(self):
        assert DEFAULT_MAINTENANCE_OLDER_THAN_DAYS == "7"

    def test_honors_maintenance_older_than_days_env_override(self, monkeypatch):
        import alice_ingest.maintenance as maintenance_module

        table = FakeTable("alice.job_info", [OLD, RECENT])
        catalog = FakeCatalog({"alice.job_info": table})
        monkeypatch.setattr(
            maintenance_module, "load_iceberg_catalog", lambda env: catalog
        )
        # Freeze "now" so the cutoff math is deterministic.
        monkeypatch.setattr(
            maintenance_module,
            "_now",
            lambda: NOW,
        )

        run({"MAINTENANCE_OLDER_THAN_DAYS": "1"})

        # older_than(NOW - 1 day) -- OLD (10d ago) is older, RECENT (1d ago,
        # exactly at the boundary) is NOT strictly older than NOW - 1 day
        # given the fake's `>=` keep semantics.
        assert table.builders[0].older_than_calls == [NOW - timedelta(days=1)]


class TestNoBlockedAlternativeNeeded:
    """Confirms this module does NOT fall back to the brief's documented
    BLOCKED path (`table.manage_snapshots()` / a `MAINTENANCE SKIPPED: no
    expiry API` message) -- the real expiry API exists on pyiceberg 0.11.1
    (verified live, module docstring), so `run()`'s only "SKIPPED" case is
    an empty namespace, never a missing API."""

    def test_run_never_mentions_no_expiry_api_when_tables_exist(
        self, monkeypatch, capsys
    ):
        import alice_ingest.maintenance as maintenance_module

        catalog = FakeCatalog({"alice.job_info": FakeTable("alice.job_info", [OLD])})
        monkeypatch.setattr(
            maintenance_module, "load_iceberg_catalog", lambda env: catalog
        )

        run({})

        assert "no expiry API" not in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Plan 3 Task 3: Trino physical maintenance (`ALTER TABLE ... EXECUTE
# optimize/expire_snapshots/remove_orphan_files`) -- the load-bearing
# physical GC pyiceberg 0.11.1 cannot do (module docstring, "Scope note").
# Exact procedure syntax + the 7d retention floor verified LIVE against the
# pinned Trino 476 (docs page https://trino.io/docs/476/connector/
# iceberg.html cross-checked against a real probe pod, 2026-07-12/13):
#   ALTER TABLE <t> EXECUTE optimize
#   ALTER TABLE <t> EXECUTE expire_snapshots(retention_threshold => '7d')
#   ALTER TABLE <t> EXECUTE remove_orphan_files(retention_threshold => '7d')
# `iceberg.expire-snapshots.min-retention` / `iceberg.remove-orphan-files.
# min-retention` both default to 7d on this Trino -- retention_threshold =>
# '7d' is therefore exactly AT the floor (proven live: '7d' succeeds, '0s'
# fails with INVALID_PROCEDURE_ARGUMENT), so DEFAULT_TRINO_MAINTENANCE_
# RETENTION_THRESHOLD stays '7d' with no session-property override needed in
# production. `remove_orphan_files` succeeding at all (against a real
# alice.job_info table, and again against a dedicated scratch table with a
# session-property-overridden 0s floor) is itself the proof that Lakekeeper's
# vended S3 credentials permit LIST, not just PUT/GET (unproven before this
# task, per the brief) -- physical file counts genuinely dropped after
# remove_orphan_files and were UNCHANGED after expire_snapshots alone
# (metadata-only), live-verified against MinIO directly via s3fs with the
# harness's root credentials.
# ---------------------------------------------------------------------------


class ScriptedTrinoClient:
    """Records every executed statement; returns per-statement canned rows
    keyed by exact SQL text, FIFO per key (so the same SQL issued twice --
    e.g. a `$files` count before and after maintenance -- can return two
    different canned values in call order). A canned value that is an
    Exception instance is raised instead of returned, for the missing-table
    freshness case. An unscripted statement raises AssertionError rather
    than silently returning [] -- a forgotten script entry must fail loudly,
    not look like an empty result set."""

    def __init__(self, responses: dict[str, list]):
        self._responses = {sql: list(values) for sql, values in responses.items()}
        self.statements: list[str] = []

    def run(self, sql: str, poll_interval: float = 1.0):
        self.statements.append(sql)
        queue = self._responses.get(sql)
        if not queue:
            raise AssertionError(f"unscripted statement: {sql!r}")
        value = queue.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class TestListTrinoAliceTables:
    def test_returns_sorted_table_names_from_show_tables(self):
        client = ScriptedTrinoClient(
            {
                "SHOW TABLES FROM lake.alice": [
                    [["mon_jdls_parsed"], ["job_info"], ["trace"]]
                ],
            }
        )

        result = list_trino_alice_tables(client)

        assert result == ["job_info", "mon_jdls_parsed", "trace"]


class TestRunTrinoTableMaintenance:
    def test_runs_optimize_then_expire_then_remove_orphan_in_order(self):
        client = ScriptedTrinoClient(
            {
                'SELECT count(*) FROM lake.alice."job_info$files"': [[[5]], [[1]]],
                "ALTER TABLE lake.alice.job_info EXECUTE optimize": [[]],
                "ALTER TABLE lake.alice.job_info EXECUTE expire_snapshots(retention_threshold => '7d')": [[]],
                "ALTER TABLE lake.alice.job_info EXECUTE remove_orphan_files(retention_threshold => '7d')": [[]],
            }
        )

        result = run_trino_table_maintenance(client, "job_info")

        assert client.statements == [
            'SELECT count(*) FROM lake.alice."job_info$files"',
            "ALTER TABLE lake.alice.job_info EXECUTE optimize",
            "ALTER TABLE lake.alice.job_info EXECUTE expire_snapshots(retention_threshold => '7d')",
            "ALTER TABLE lake.alice.job_info EXECUTE remove_orphan_files(retention_threshold => '7d')",
            'SELECT count(*) FROM lake.alice."job_info$files"',
        ]
        assert result == TrinoMaintenanceResult(table="job_info", files_before=5, files_after=1)
        assert result.files_removed == 4

    def test_honors_a_non_default_retention_threshold(self):
        client = ScriptedTrinoClient(
            {
                'SELECT count(*) FROM lake.alice."trace$files"': [[[2]], [[2]]],
                "ALTER TABLE lake.alice.trace EXECUTE optimize": [[]],
                "ALTER TABLE lake.alice.trace EXECUTE expire_snapshots(retention_threshold => '14d')": [[]],
                "ALTER TABLE lake.alice.trace EXECUTE remove_orphan_files(retention_threshold => '14d')": [[]],
            }
        )

        result = run_trino_table_maintenance(client, "trace", retention_threshold="14d")

        assert result.files_removed == 0


class TestFormatTrinoMaintenanceSummary:
    def test_matches_the_documented_per_table_log_format(self):
        result = TrinoMaintenanceResult(table="job_info", files_before=5, files_after=1)

        assert format_trino_maintenance_summary(result) == (
            "TRINO MAINTENANCE table=job_info files_before=5 files_after=1 files_removed=4"
        )


class TestRunTrinoMaintenance:
    def test_iterates_every_table_in_sorted_order_and_prints_a_summary_each(self, capsys):
        client = ScriptedTrinoClient(
            {
                'SELECT count(*) FROM lake.alice."job_info$files"': [[[3]], [[1]]],
                "ALTER TABLE lake.alice.job_info EXECUTE optimize": [[]],
                "ALTER TABLE lake.alice.job_info EXECUTE expire_snapshots(retention_threshold => '7d')": [[]],
                "ALTER TABLE lake.alice.job_info EXECUTE remove_orphan_files(retention_threshold => '7d')": [[]],
                'SELECT count(*) FROM lake.alice."trace$files"': [[[2]], [[2]]],
                "ALTER TABLE lake.alice.trace EXECUTE optimize": [[]],
                "ALTER TABLE lake.alice.trace EXECUTE expire_snapshots(retention_threshold => '7d')": [[]],
                "ALTER TABLE lake.alice.trace EXECUTE remove_orphan_files(retention_threshold => '7d')": [[]],
            }
        )

        results = run_trino_maintenance(client, ["trace", "job_info"])

        assert [r.table for r in results] == ["job_info", "trace"]
        out = capsys.readouterr().out
        assert "TRINO MAINTENANCE table=job_info files_before=3 files_after=1 files_removed=2" in out
        assert "TRINO MAINTENANCE table=trace files_before=2 files_after=2 files_removed=0" in out


class TestRunTrino:
    def test_requires_trino_uri_env_var(self):
        with pytest.raises(SystemExit, match="TRINO_URI"):
            run_trino({})

    def test_no_tables_found_is_reported_and_not_an_error(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        client = ScriptedTrinoClient({"SHOW TABLES FROM lake.alice": [[]]})
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_trino({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0
        assert "TRINO MAINTENANCE SKIPPED: no tables found" in capsys.readouterr().out

    def test_happy_path_maintains_every_discovered_table(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        client = ScriptedTrinoClient(
            {
                "SHOW TABLES FROM lake.alice": [[["job_info"]]],
                'SELECT count(*) FROM lake.alice."job_info$files"': [[[4]], [[2]]],
                "ALTER TABLE lake.alice.job_info EXECUTE optimize": [[]],
                "ALTER TABLE lake.alice.job_info EXECUTE expire_snapshots(retention_threshold => '7d')": [[]],
                "ALTER TABLE lake.alice.job_info EXECUTE remove_orphan_files(retention_threshold => '7d')": [[]],
            }
        )
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_trino({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "TRINO MAINTENANCE table=job_info files_before=4 files_after=2 files_removed=2" in out
        assert "TRINO MAINTENANCE tables=1 total_files_removed=2" in out

    def test_honors_retention_threshold_env_override(self, monkeypatch):
        import alice_ingest.maintenance as maintenance_module

        client = ScriptedTrinoClient(
            {
                "SHOW TABLES FROM lake.alice": [[["job_info"]]],
                'SELECT count(*) FROM lake.alice."job_info$files"': [[[1]], [[1]]],
                "ALTER TABLE lake.alice.job_info EXECUTE optimize": [[]],
                "ALTER TABLE lake.alice.job_info EXECUTE expire_snapshots(retention_threshold => '3d')": [[]],
                "ALTER TABLE lake.alice.job_info EXECUTE remove_orphan_files(retention_threshold => '3d')": [[]],
            }
        )
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_trino(
            {"TRINO_URI": "http://trino.trino.svc:8080", "TRINO_MAINTENANCE_RETENTION_THRESHOLD": "3d"}
        )

        assert exit_code == 0


# ---------------------------------------------------------------------------
# Plan 3 Task 3: data-layer freshness gate (design D11 completion). Nightly
# workflow's final `check-freshness` step: `max(committed_at)` from each core
# table's `"<table>$snapshots"` metadata table (column name + table naming
# verified live against Trino 476, 2026-07-12: `SELECT max(committed_at)
# FROM lake.alice."job_info$snapshots"` returned e.g.
# `[['2026-07-12 19:40:03.327 UTC']]`), stale if older than 26h (24h nightly
# cadence + 2h buffer, matching the existing IcebergIngestStale alert's
# window in datalake-alerts.yaml). Injected clock (`now` parameter) --
# stale/fresh/missing-table cases below never touch a real clock.
# ---------------------------------------------------------------------------


NOW = datetime(2026, 7, 13, 3, 0, tzinfo=timezone.utc)


class TestParseTrinoTimestamp:
    def test_parses_the_live_verified_wire_format(self):
        from alice_ingest.maintenance import _parse_trino_timestamp

        result = _parse_trino_timestamp("2026-07-12 19:40:03.327 UTC")

        assert result == datetime(2026, 7, 12, 19, 40, 3, 327000, tzinfo=timezone.utc)

    def test_rejects_a_non_utc_wire_value(self):
        from alice_ingest.maintenance import _parse_trino_timestamp

        with pytest.raises(ValueError, match="UTC"):
            _parse_trino_timestamp("2026-07-12 19:40:03.327 CEST")


class TestTableFreshness:
    def test_fresh_table_reports_its_max_committed_at(self):
        client = ScriptedTrinoClient(
            {
                'SELECT max(committed_at) FROM lake.alice."job_info$snapshots" WHERE operation <> \'replace\'': [
                    [["2026-07-13 02:00:00.000 UTC"]]
                ],
            }
        )

        result = table_freshness(client, "job_info")

        assert result == FreshnessResult(
            table="job_info",
            max_committed_at=datetime(2026, 7, 13, 2, 0, 0, tzinfo=timezone.utc),
            missing=False,
        )

    def test_missing_table_is_reported_as_missing_not_raised(self):
        client = ScriptedTrinoClient(
            {
                'SELECT max(committed_at) FROM lake.alice."job_info$snapshots" WHERE operation <> \'replace\'': [
                    TrinoQueryError(
                        "{'message': \"line 1:32: Table 'lake.alice.job_info' does not exist\", "
                        "'errorName': 'TABLE_NOT_FOUND'}"
                    )
                ],
            }
        )

        result = table_freshness(client, "job_info")

        assert result == FreshnessResult(table="job_info", max_committed_at=None, missing=True)

    def test_table_with_zero_snapshots_reports_null_not_missing(self):
        client = ScriptedTrinoClient(
            {
                'SELECT max(committed_at) FROM lake.alice."job_info$snapshots" WHERE operation <> \'replace\'': [[[None]]],
            }
        )

        result = table_freshness(client, "job_info")

        assert result == FreshnessResult(table="job_info", max_committed_at=None, missing=False)

    def test_a_different_trino_query_error_is_not_swallowed(self):
        client = ScriptedTrinoClient(
            {
                'SELECT max(committed_at) FROM lake.alice."job_info$snapshots" WHERE operation <> \'replace\'': [
                    TrinoQueryError("{'message': 'PERMISSION_DENIED', 'errorName': 'PERMISSION_DENIED'}")
                ],
            }
        )

        with pytest.raises(TrinoQueryError, match="PERMISSION_DENIED"):
            table_freshness(client, "job_info")


class TestTableFreshnessExcludesReplaceSnapshots:
    """Final review R2: Trino's weekly `OPTIMIZE` (`run-trino-maintenance`)
    commits a NEW Iceberg snapshot -- physically compacting files, adding NO
    new data -- recorded with `operation='replace'` (live-verified against
    this pin's real Trino 476, 2026-07-13: a scratch table with 6
    dlt/pyiceberg-style `INSERT`-then-`OPTIMIZE` commits showed
    `operation='append'` for every one of the 6 inserts and
    `operation='replace'` for the following `OPTIMIZE`, with `total-records`
    unchanged across the replace (5 -> 5) -- pure compaction, not new data;
    the SAME `operation='append'` was independently confirmed on the three
    real production core tables' own single ingest snapshot each
    (job_info/trace/mon_jdls_parsed), and `operation` was never NULL on any
    commit observed, dlt/pyiceberg or Trino). The OLD query (`SELECT
    max(committed_at) FROM ..."<table>$snapshots"`, no operation filter)
    therefore lets a Sunday `run-trino-maintenance` OPTIMIZE run silently
    refresh `check-freshness`'s signal even when `run-nightly` has not
    appended anything new in days -- the freshness gate goes blind exactly
    when it matters most. Fix: `table_freshness()` now filters `WHERE
    operation <> 'replace'` server-side, so `max(committed_at)` only ever
    reflects a real data-bearing commit. No `(operation IS NULL OR ...)`
    escape hatch is needed -- the live evidence above shows `operation` is
    populated on every commit this pin produces, dlt/pyiceberg or Trino
    alike; a bare `<> 'replace'` is correct and does not risk silently
    dropping a legitimate NULL-operation row that doesn't occur in
    practice."""

    def test_append_only_history_reports_fresh(self):
        """Baseline: a table with only append snapshots (no maintenance run
        yet) is unaffected by the fix -- same shape as the pre-fix
        behavior, just via the filtered query."""
        client = ScriptedTrinoClient(
            {
                'SELECT max(committed_at) FROM lake.alice."job_info$snapshots" '
                "WHERE operation <> 'replace'": [[["2026-07-13 02:00:00.000 UTC"]]],
            }
        )

        result = table_freshness(client, "job_info")

        assert result == FreshnessResult(
            table="job_info",
            max_committed_at=datetime(2026, 7, 13, 2, 0, 0, tzinfo=timezone.utc),
            missing=False,
        )
        assert result.is_stale(NOW, timedelta(hours=26)) is False

    def test_stale_appends_masked_by_a_fresh_replace_reports_stale(self):
        """The masking case this fix closes: the table's most RECENT
        Iceberg snapshot overall is a same-day Trino OPTIMIZE (`replace`),
        but its most recent APPEND -- the only kind of commit that means
        `run-nightly` actually landed new data -- is 3 days old, well
        outside the 26h default. The pre-fix unfiltered query would have
        picked up the replace's fresh `committed_at` and wrongly reported
        FRESH; the filtered query issued here excludes that replace
        server-side and returns the stale append's timestamp instead, which
        `table_freshness` must report as-is (staleness itself is
        `FreshnessResult.is_stale`'s job, exercised below)."""
        client = ScriptedTrinoClient(
            {
                'SELECT max(committed_at) FROM lake.alice."job_info$snapshots" '
                "WHERE operation <> 'replace'": [[["2026-07-10 02:00:00.000 UTC"]]],
            }
        )

        result = table_freshness(client, "job_info")

        assert result == FreshnessResult(
            table="job_info",
            max_committed_at=datetime(2026, 7, 10, 2, 0, 0, tzinfo=timezone.utc),
            missing=False,
        )
        assert result.is_stale(NOW, timedelta(hours=26)) is True

    def test_mixed_history_still_reports_the_most_recent_append(self):
        """A table with an interleaved append/replace history (several
        nightly appends, a weekly OPTIMIZE in between, then another recent
        append) reports the most recent APPEND's timestamp -- proving the
        filter does not just handle the all-replace-at-the-tail case but
        genuinely ignores every replace snapshot regardless of position in
        history. Trino's own `max(committed_at) WHERE operation <>
        'replace'` does this filtering server-side; this test pins the
        query text and confirms the code passes the filtered answer through
        unchanged."""
        client = ScriptedTrinoClient(
            {
                'SELECT max(committed_at) FROM lake.alice."job_info$snapshots" '
                "WHERE operation <> 'replace'": [[["2026-07-12 20:00:00.000 UTC"]]],
            }
        )

        result = table_freshness(client, "job_info")

        assert result == FreshnessResult(
            table="job_info",
            max_committed_at=datetime(2026, 7, 12, 20, 0, 0, tzinfo=timezone.utc),
            missing=False,
        )
        assert result.is_stale(NOW, timedelta(hours=26)) is False


class TestFreshnessResultIsStale:
    def test_missing_table_is_always_stale(self):
        result = FreshnessResult(table="job_info", max_committed_at=None, missing=True)

        assert result.is_stale(NOW, timedelta(hours=26)) is True

    def test_null_snapshot_max_is_always_stale(self):
        result = FreshnessResult(table="job_info", max_committed_at=None, missing=False)

        assert result.is_stale(NOW, timedelta(hours=26)) is True

    def test_within_max_age_is_fresh(self):
        result = FreshnessResult(table="job_info", max_committed_at=NOW - timedelta(hours=1), missing=False)

        assert result.is_stale(NOW, timedelta(hours=26)) is False

    def test_beyond_max_age_is_stale(self):
        result = FreshnessResult(table="job_info", max_committed_at=NOW - timedelta(hours=27), missing=False)

        assert result.is_stale(NOW, timedelta(hours=26)) is True

    def test_exactly_at_the_boundary_is_not_yet_stale(self):
        result = FreshnessResult(table="job_info", max_committed_at=NOW - timedelta(hours=26), missing=False)

        assert result.is_stale(NOW, timedelta(hours=26)) is False


class TestCheckFreshness:
    def test_one_result_per_core_table_in_order(self):
        client = ScriptedTrinoClient(
            {
                f'SELECT max(committed_at) FROM lake.alice."{t}$snapshots" WHERE operation <> \'replace\'': [
                    [["2026-07-13 02:00:00.000 UTC"]]
                ]
                for t in CORE_FRESHNESS_TABLES
            }
        )

        results = check_freshness(client, NOW)

        assert [r.table for r in results] == list(CORE_FRESHNESS_TABLES)

    def test_default_core_tables_are_job_info_trace_mon_jdls_parsed(self):
        assert CORE_FRESHNESS_TABLES == ("job_info", "trace", "mon_jdls_parsed")

    def test_default_max_age_is_26_hours(self):
        assert DEFAULT_FRESHNESS_MAX_AGE_HOURS == "26"


class TestFormatFreshnessSummary:
    def test_fresh_table_format(self):
        result = FreshnessResult(
            table="job_info", max_committed_at=NOW - timedelta(hours=1), missing=False
        )

        line = format_freshness_summary(result, NOW, timedelta(hours=26))

        assert line.startswith("FRESHNESS table=job_info status=FRESH")
        assert "age_hours=1.00" in line

    def test_stale_table_format(self):
        result = FreshnessResult(
            table="job_info", max_committed_at=NOW - timedelta(hours=30), missing=False
        )

        line = format_freshness_summary(result, NOW, timedelta(hours=26))

        assert line.startswith("FRESHNESS table=job_info status=STALE")
        assert "age_hours=30.00" in line

    def test_missing_table_format(self):
        result = FreshnessResult(table="job_info", max_committed_at=None, missing=True)

        line = format_freshness_summary(result, NOW, timedelta(hours=26))

        assert line == "FRESHNESS table=job_info status=MISSING"


class TestRunFreshnessCheck:
    def test_requires_trino_uri_env_var(self):
        with pytest.raises(SystemExit, match="TRINO_URI"):
            run_freshness_check({})

    def test_all_fresh_exits_zero(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        client = ScriptedTrinoClient(
            {
                f'SELECT max(committed_at) FROM lake.alice."{t}$snapshots" WHERE operation <> \'replace\'': [
                    [["2026-07-13 02:00:00.000 UTC"]]
                ]
                for t in CORE_FRESHNESS_TABLES
            }
        )
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_freshness_check({"TRINO_URI": "http://trino.trino.svc:8080"}, now=NOW)

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "FRESHNESS CHECK OK" in out
        assert out.count("status=FRESH") == 3

    def test_one_stale_table_exits_nonzero(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        responses = {
            f'SELECT max(committed_at) FROM lake.alice."{t}$snapshots" WHERE operation <> \'replace\'': [
                [["2026-07-13 02:00:00.000 UTC"]]
            ]
            for t in CORE_FRESHNESS_TABLES
        }
        responses['SELECT max(committed_at) FROM lake.alice."trace$snapshots" WHERE operation <> \'replace\''] = [
            [["2026-07-10 00:00:00.000 UTC"]]
        ]
        client = ScriptedTrinoClient(responses)
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_freshness_check({"TRINO_URI": "http://trino.trino.svc:8080"}, now=NOW)

        assert exit_code == 1
        out = capsys.readouterr().out
        assert "FRESHNESS CHECK FAILED" in out
        assert "table=trace status=STALE" in out

    def test_missing_table_exits_nonzero(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        responses = {
            f'SELECT max(committed_at) FROM lake.alice."{t}$snapshots" WHERE operation <> \'replace\'': [
                [["2026-07-13 02:00:00.000 UTC"]]
            ]
            for t in CORE_FRESHNESS_TABLES
        }
        responses['SELECT max(committed_at) FROM lake.alice."mon_jdls_parsed$snapshots" WHERE operation <> \'replace\''] = [
            TrinoQueryError(
                "{'message': \"Table 'lake.alice.mon_jdls_parsed' does not exist\", "
                "'errorName': 'TABLE_NOT_FOUND'}"
            )
        ]
        client = ScriptedTrinoClient(responses)
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_freshness_check({"TRINO_URI": "http://trino.trino.svc:8080"}, now=NOW)

        assert exit_code == 1
        out = capsys.readouterr().out
        assert "table=mon_jdls_parsed status=MISSING" in out

    def test_honors_freshness_max_age_hours_env_override(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        # 25h old is stale under a 1h override, fresh under the 26h default.
        responses = {
            f'SELECT max(committed_at) FROM lake.alice."{t}$snapshots" WHERE operation <> \'replace\'': [
                [["2026-07-12 02:00:00.000 UTC"]]  # 25h before NOW
            ]
            for t in CORE_FRESHNESS_TABLES
        }
        client = ScriptedTrinoClient(responses)
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_freshness_check(
            {"TRINO_URI": "http://trino.trino.svc:8080", "FRESHNESS_MAX_AGE_HOURS": "1"}, now=NOW
        )

        assert exit_code == 1

    def test_default_now_comes_from_the_now_seam_when_not_injected(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        client = ScriptedTrinoClient(
            {
                f'SELECT max(committed_at) FROM lake.alice."{t}$snapshots" WHERE operation <> \'replace\'': [
                    [["2026-07-13 02:00:00.000 UTC"]]
                ]
                for t in CORE_FRESHNESS_TABLES
            }
        )
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)
        monkeypatch.setattr(maintenance_module, "_now", lambda: NOW)

        exit_code = run_freshness_check({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0


# ---------------------------------------------------------------------------
# P3T3 review Important 1: nightly-vs-maintenance cross-workflow overlap
# guard. Production schedules: nightly `0 2 * * *` (activeDeadlineSeconds
# 21600 -- can run to 08:00), maintenance `0 4 * * 0` -- a real Sunday
# overlap window. Iceberg OCC + the 7d expiry floor already prevent
# CORRUPTION on a genuine concurrent-commit conflict between Trino's
# OPTIMIZE and an in-flight nightly write; this guard exists purely to avoid
# the AVAILABILITY cost (a conflict-and-retry, or an outright failed
# maintenance run) by deferring `run-trino-maintenance` to next Sunday when
# `ingest-nightly` is still Running. Every failure mode except an actual
# Running nightly resolves to `proceed=True` (fail-open): the guard is an
# optimization, not the safety layer -- see `check_nightly_overlap_guard`'s
# docstring in maintenance.py for the full reasoning.
# ---------------------------------------------------------------------------


class FakeWorkflowsClient:
    """Mirrors the real `WorkflowsClient.list_workflows()` interface shape
    (one method, `cron_workflow_name -> list[dict]`) without touching the
    filesystem or network -- `error`, when given, is raised instead of
    returning, standing in for `KubernetesApiError` (token unreadable, TLS/
    connection failure, non-2xx response -- module docstring's "API
    unreachable" case)."""

    def __init__(self, items: list[dict] | None = None, error: Exception | None = None):
        self._items = items if items is not None else []
        self._error = error
        self.list_workflows_calls: list[str] = []

    def list_workflows(self, cron_workflow_name: str) -> list[dict]:
        self.list_workflows_calls.append(cron_workflow_name)
        if self._error is not None:
            raise self._error
        return self._items


class TestIsCronWorkflowRunning:
    def test_true_when_any_item_has_running_phase(self):
        client = FakeWorkflowsClient(
            items=[{"status": {"phase": "Succeeded"}}, {"status": {"phase": "Running"}}]
        )

        assert is_cron_workflow_running(client, "ingest-nightly") is True
        assert client.list_workflows_calls == ["ingest-nightly"]

    def test_false_when_no_items(self):
        client = FakeWorkflowsClient(items=[])

        assert is_cron_workflow_running(client, "ingest-nightly") is False

    def test_false_when_every_item_is_terminal(self):
        client = FakeWorkflowsClient(
            items=[{"status": {"phase": "Succeeded"}}, {"status": {"phase": "Failed"}}]
        )

        assert is_cron_workflow_running(client, "ingest-nightly") is False

    def test_missing_status_or_phase_is_treated_as_not_running(self):
        client = FakeWorkflowsClient(items=[{}, {"status": {}}])

        assert is_cron_workflow_running(client, "ingest-nightly") is False


class TestCheckNightlyOverlapGuard:
    def test_running_nightly_defers(self):
        client = FakeWorkflowsClient(items=[{"status": {"phase": "Running"}}])

        result = check_nightly_overlap_guard(client, {})

        assert result == OverlapGuardResult(
            proceed=False, message="MAINTENANCE DEFERRED: ingest-nightly in flight"
        )

    def test_nightly_not_running_proceeds_with_no_message(self):
        client = FakeWorkflowsClient(items=[])

        result = check_nightly_overlap_guard(client, {})

        assert result == OverlapGuardResult(proceed=True, message=None)

    def test_maintenance_force_proceeds_despite_running_and_skips_the_api_call(self):
        client = FakeWorkflowsClient(items=[{"status": {"phase": "Running"}}])

        result = check_nightly_overlap_guard(client, {"MAINTENANCE_FORCE": "1"})

        assert result.proceed is True
        assert client.list_workflows_calls == []

    def test_api_unreachable_proceeds_with_a_warning_fail_open(self):
        client = FakeWorkflowsClient(error=KubernetesApiError("connection refused"))

        result = check_nightly_overlap_guard(client, {})

        assert result.proceed is True
        assert result.message is not None
        assert "connection refused" in result.message
        assert "proceeding" in result.message.lower()

    def test_uses_the_given_cron_workflow_name(self):
        client = FakeWorkflowsClient(items=[])

        check_nightly_overlap_guard(client, {}, cron_workflow_name="ingest-nightly")

        assert client.list_workflows_calls == ["ingest-nightly"]

    def test_default_cron_workflow_name_is_ingest_nightly(self):
        assert NIGHTLY_CRON_WORKFLOW_NAME == "ingest-nightly"


class FakeK8sResponse:
    def __init__(self, json_body: dict, status_code: int = 200):
        self._json_body = json_body
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._json_body


class RecordingGetRequests:
    """Fake `requests` module recording the single GET call
    `WorkflowsClient.list_workflows()` makes -- mirrors `FakeRequests` in
    test_views.py, GET-only since this client only ever lists (Important 1's
    brief: "get/list workflows")."""

    def __init__(self, response):
        self._response = response
        self.get_calls: list[dict] = []

    def get(self, url, headers=None, params=None, verify=None, timeout=None):
        self.get_calls.append(
            {"url": url, "headers": headers, "params": params, "verify": verify, "timeout": timeout}
        )
        return self._response


class TestWorkflowsClient:
    """The real REST client (in-cluster Kubernetes API, `requests`-based,
    same minimal-client pattern as views.py's TrinoClient rather than adding
    the full `kubernetes` PyPI package for one get/list call). Unit-tested
    against a monkeypatched `requests` module + a real tmp_path token file
    -- no live cluster needed."""

    def test_lists_workflows_with_the_cron_workflow_label_selector(self, monkeypatch, tmp_path):
        import alice_ingest.maintenance as maintenance_module

        token_path = tmp_path / "token"
        token_path.write_text("fake-token")
        fake = RecordingGetRequests(FakeK8sResponse({"items": [{"status": {"phase": "Running"}}]}))
        monkeypatch.setattr(maintenance_module, "requests", fake)

        client = WorkflowsClient(token_path=str(token_path), ca_cert_path=str(tmp_path / "ca.crt"))
        items = client.list_workflows("ingest-nightly")

        assert items == [{"status": {"phase": "Running"}}]
        call = fake.get_calls[0]
        assert call["url"] == (
            "https://kubernetes.default.svc/apis/argoproj.io/v1alpha1/"
            "namespaces/argo-workflows/workflows"
        )
        assert call["headers"] == {"Authorization": "Bearer fake-token"}
        assert call["params"] == {
            "labelSelector": "workflows.argoproj.io/cron-workflow=ingest-nightly"
        }
        assert call["verify"] == str(tmp_path / "ca.crt")

    def test_no_items_key_in_response_returns_empty_list(self, monkeypatch, tmp_path):
        import alice_ingest.maintenance as maintenance_module

        token_path = tmp_path / "token"
        token_path.write_text("fake-token")
        fake = RecordingGetRequests(FakeK8sResponse({}))
        monkeypatch.setattr(maintenance_module, "requests", fake)

        client = WorkflowsClient(token_path=str(token_path))

        assert client.list_workflows("ingest-nightly") == []

    def test_missing_token_file_raises_kubernetes_api_error(self):
        client = WorkflowsClient(token_path="/nonexistent/path/token")

        with pytest.raises(KubernetesApiError):
            client.list_workflows("ingest-nightly")

    def test_request_exception_raises_kubernetes_api_error(self, monkeypatch, tmp_path):
        import alice_ingest.maintenance as maintenance_module
        import requests as real_requests

        token_path = tmp_path / "token"
        token_path.write_text("fake-token")

        class RaisingRequests:
            exceptions = real_requests.exceptions

            def get(self, *args, **kwargs):
                raise real_requests.exceptions.ConnectionError("connection refused")

        monkeypatch.setattr(maintenance_module, "requests", RaisingRequests())

        client = WorkflowsClient(token_path=str(token_path))

        with pytest.raises(KubernetesApiError, match="connection refused"):
            client.list_workflows("ingest-nightly")


class TestRunTrinoOverlapGuardIntegration:
    """Wires `check_nightly_overlap_guard` into `run_trino()` -- the
    "Trino-maintenance entrypoint" the review Important names -- BEFORE any
    Trino call is made, so a deferred run never touches the Trino cluster at
    all (asserted below via an intentionally-empty `ScriptedTrinoClient`
    script: any Trino statement issued despite deferral raises
    AssertionError, ScriptedTrinoClient's own "unscripted statement" guard)."""

    def test_defers_when_nightly_is_running_and_never_touches_trino(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        workflows_client = FakeWorkflowsClient(items=[{"status": {"phase": "Running"}}])
        monkeypatch.setattr(maintenance_module, "WorkflowsClient", lambda: workflows_client)
        trino_client = ScriptedTrinoClient({})
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: trino_client)

        exit_code = run_trino({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0
        assert trino_client.statements == []
        assert "MAINTENANCE DEFERRED: ingest-nightly in flight" in capsys.readouterr().out

    def test_proceeds_when_nightly_not_running(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        workflows_client = FakeWorkflowsClient(items=[])
        monkeypatch.setattr(maintenance_module, "WorkflowsClient", lambda: workflows_client)
        client = ScriptedTrinoClient({"SHOW TABLES FROM lake.alice": [[]]})
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_trino({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0
        assert "TRINO MAINTENANCE SKIPPED" in capsys.readouterr().out
        assert "MAINTENANCE DEFERRED" not in capsys.readouterr().out

    def test_maintenance_force_proceeds_despite_running_nightly(self, monkeypatch, capsys):
        import alice_ingest.maintenance as maintenance_module

        workflows_client = FakeWorkflowsClient(items=[{"status": {"phase": "Running"}}])
        monkeypatch.setattr(maintenance_module, "WorkflowsClient", lambda: workflows_client)
        client = ScriptedTrinoClient({"SHOW TABLES FROM lake.alice": [[]]})
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_trino(
            {"TRINO_URI": "http://trino.trino.svc:8080", "MAINTENANCE_FORCE": "1"}
        )

        assert exit_code == 0
        assert workflows_client.list_workflows_calls == []
        assert "TRINO MAINTENANCE SKIPPED" in capsys.readouterr().out

    def test_api_unreachable_proceeds_with_warning_and_still_runs_maintenance(
        self, monkeypatch, capsys
    ):
        import alice_ingest.maintenance as maintenance_module

        workflows_client = FakeWorkflowsClient(error=KubernetesApiError("boom"))
        monkeypatch.setattr(maintenance_module, "WorkflowsClient", lambda: workflows_client)
        client = ScriptedTrinoClient({"SHOW TABLES FROM lake.alice": [[]]})
        monkeypatch.setattr(maintenance_module, "TrinoClient", lambda base_uri: client)

        exit_code = run_trino({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "MAINTENANCE WARNING" in out
        assert "boom" in out
        assert "TRINO MAINTENANCE SKIPPED" in out

    def test_still_requires_trino_uri_even_though_the_guard_runs_first(self):
        with pytest.raises(SystemExit, match="TRINO_URI"):
            run_trino({})
