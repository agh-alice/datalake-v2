"""TDD for alice_ingest.maintenance (Plan 2 Task 5).

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
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from alice_ingest.maintenance import (
    DEFAULT_MAINTENANCE_OLDER_THAN_DAYS,
    TableMaintenanceResult,
    expire_table_snapshots,
    format_table_summary,
    list_alice_tables,
    run,
)


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
