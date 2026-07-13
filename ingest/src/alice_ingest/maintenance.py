"""alice-ingest maintenance: weekly Iceberg snapshot expiry for every table
currently registered in the `alice` namespace (Plan 2 Task 5, design D12).

pyiceberg maintenance API -- verified LIVE, not assumed
--------------------------------------------------------
Checked directly against the pinned dependency (dlt[pyiceberg]==1.28.2 ->
pyiceberg==0.11.1) in a throwaway venv (2026-07-12, `python3.12 -m venv` +
`pip install -e ".[dev]"` from this package, then `inspect.getsource()` on
the live classes):

  - `Table.maintenance` IS a real property on pyiceberg 0.11.1's `Table`
    (`pyiceberg.table.maintenance.MaintenanceTable`), and its ONLY method is
    `expire_snapshots()` (verified via `dir(MaintenanceTable)` ==
    `['expire_snapshots']`) -- no `remove_orphan_files` or similar exists on
    this pin.
  - `table.maintenance.expire_snapshots()` returns an `ExpireSnapshots`
    builder (`pyiceberg.table.update.snapshot.ExpireSnapshots`) supporting
    `.older_than(datetime)`, `.by_id(int)`, `.by_ids(list[int])`, and
    `.commit()` (verified via `inspect.getsource(ExpireSnapshots)` and
    `dir(ExpireSnapshots)` == `['by_id', 'by_ids', 'commit', 'older_than']`).
    `.older_than(dt)` marks every UNPROTECTED snapshot (i.e. excluding
    branch/tag HEADs -- `_get_protected_snapshot_ids()`) with
    `timestamp_ms < dt` for removal; `.commit()` applies a
    `RemoveSnapshotsUpdate`.
  - The brief's OTHER named candidate, `table.manage_snapshots()`, is a
    DIFFERENT operation -- confirmed by reading its docstring live
    ("Shorthand to run snapshot management operations like create branch,
    create tag, etc.", returns `ManageSnapshots`) -- it is branch/tag
    management, not snapshot expiry, and is NOT used here.
  - `RemoveSnapshotsUpdate`'s registered apply-handler
    (`pyiceberg.table.update._apply_table_update`, `@...register
    (RemoveSnapshotsUpdate)`) was read live too: it filters BOTH
    `metadata.snapshots` (the live snapshot list -- what `table.snapshots()`
    returns) AND `metadata.snapshot_log` (the audit trail -- what
    `table.history()` returns) to drop every expired snapshot id. So both
    `table.snapshots()` and `table.history()` shrink after a successful
    expiry commit + `table.refresh()` -- the verification signal named in
    the brief (Step 3).

Conclusion: the exact API name/kwargs the brief asked to verify is
    table.maintenance.expire_snapshots().older_than(cutoff).commit()
It exists on the pin. This module therefore does NOT take the brief's
documented BLOCKED-alternative path (implement detect-and-report,
`MAINTENANCE SKIPPED: no expiry API on pyiceberg 0.11.1`, defer to Plan 3's
Trino OPTIMIZE) -- that path is reserved for a pin where no expiry API
exists at all, which is not the case here. `run()`'s only "SKIPPED" message
covers a genuinely different, mundane case: an empty `alice` namespace (no
tables registered yet).

Scope note: `expire_snapshots()` only removes SNAPSHOT METADATA entries; it
does not physically delete the data/manifest files an expired snapshot
referenced (no orphan-file-removal API exists on this pin's
`MaintenanceTable`, per the `dir()` check above). Physical file GC is out of
scope for this pin and this task -- Trino OPTIMIZE (Plan 3) owns compaction,
consistent with the plan's separation of concerns for this component (this
IS the same "Trino OPTIMIZE lands in Plan 3" ownership boundary the brief
names, just reached because physical-file GC has no API here, not because
snapshot expiry itself was blocked).

Which tables get maintained: every table currently registered in the
`alice` Iceberg namespace (`catalog.list_tables(DATASET_NAME)`), not a
hardcoded list -- so `site_sonar` (Task 4) and any future `alice.*` table
are covered automatically without a code change here.

MAINTENANCE_OLDER_THAN_DAYS: default 7 (brief: "older_than=<7d>"), overridable
via env for operational flexibility; not part of pipeline.py's required
env-var contract since a sane default always applies.

Plan 3 Task 3: Trino physical maintenance + the data-layer freshness gate
--------------------------------------------------------------------------
This module ALSO owns the physical-file-GC half of weekly maintenance that
the section above documents as out of scope for pyiceberg 0.11.1 (no
`remove_orphan_files`-equivalent API on this pin), and the nightly data-layer
staleness check (design D11 completion). Both reuse Task 2's `TrinoClient`/
`TrinoQueryError` (`alice_ingest.views`) rather than building a second REST
client (plan Self-Review: "Trino client built once (T2) reused (T3)").

Trino 476 procedure syntax -- verified LIVE, not assumed
--------------------------------------------------------------------------
Checked two ways, both agreeing (2026-07-12/13): (1) the version-PINNED docs
page `https://trino.io/docs/476/connector/iceberg.html` (confirmed to serve
476-exact content, not a redirect to `/docs/current/` -- HTTP 200 direct);
(2) a real probe pod on this kind cluster running each statement against the
live `lake.alice.job_info` table via the same REST protocol `TrinoClient`
implements. Exact forms, both sources agreeing:

    ALTER TABLE lake.alice.<table> EXECUTE optimize
    ALTER TABLE lake.alice.<table> EXECUTE expire_snapshots(retention_threshold => '7d')
    ALTER TABLE lake.alice.<table> EXECUTE remove_orphan_files(retention_threshold => '7d')

Metadata tables for per-table file/snapshot counts and freshness:
`lake.alice."<table>$files"` (row count = physically-referenced data file
count for the table's current snapshot) and `lake.alice."<table>$snapshots"`
(has a `committed_at` column -- `SELECT max(committed_at) FROM lake.alice.
"job_info$snapshots"` returned `[['2026-07-12 19:40:03.327 UTC']]` live).

Retention floor -- verified LIVE, closes the brief's open question
--------------------------------------------------------------------------
Trino enforces a MINIMUM retention via two connector config properties,
`iceberg.expire-snapshots.min-retention` and `iceberg.remove-orphan-files.
min-retention`, BOTH defaulting to `7d` on this Trino (confirmed both by the
pinned docs page and live: `retention_threshold => '0s'` fails with
`INVALID_PROCEDURE_ARGUMENT` -- "Retention specified (0.00s) is shorter than
the minimum retention configured in the system (7.00d)" -- while `'7d'`
(exactly at the floor) succeeds). DEFAULT_TRINO_MAINTENANCE_RETENTION_
THRESHOLD is therefore `'7d'`: the plan's chosen threshold sits exactly AT
the live floor, so production maintenance needs no config change and no
session-property override. The override mechanism DOES exist and was proven
live too (for the one-off scratch-table physical-GC proof described below):
catalog-scoped session properties `SET SESSION lake.expire_snapshots_
min_retention = '<duration>'` and `SET SESSION lake.remove_orphan_files_
min_retention = '<duration>'` (property name uses the CATALOG name `lake` as
its prefix, not the connector name `iceberg`, since that's how Trino scopes
session properties to a specific catalog instance) -- recorded here for any
future operator who needs a below-floor run, not wired into this module.

Vended-credentials S3 LIST permission -- verified LIVE (was unproven)
--------------------------------------------------------------------------
The brief flagged this as unproven: write-vending was proven in P2T1, but
`remove_orphan_files` additionally needs to LIST the table's S3 data
directory to find files no snapshot references, and Lakekeeper's vended STS
credentials might not include `s3:ListBucket`. Proven live two ways: (1)
`ALTER TABLE lake.alice.job_info EXECUTE remove_orphan_files(retention_
threshold => '7d')` against the real production table succeeded with no
`AccessDenied`/`403` (a LIST-permission failure would surface as a Trino
query error at this exact procedure call); (2) a dedicated scratch table
(schema `lake.gc_probe`, dropped after the probe -- brief: "test on a
scratch table first") with 5 separate small INSERTs (5 data files), then
`OPTIMIZE` (compacted to 1 new file, old 5 still referenced by the pre-
optimize snapshot) then `expire_snapshots('0s')` then `remove_orphan_files
('0s')` (both session-property-floor-overridden per above, to avoid a real
7-day wait), with the PHYSICAL object count independently verified via
`s3fs` against MinIO directly using the harness's root S3 credentials at
each step: 8 real `.parquet` objects before `remove_orphan_files`, STILL 8
after `expire_snapshots` alone (proving `expire_snapshots` is metadata-only,
confirming for Trino's own procedure the same claim already established for
pyiceberg's `expire_snapshots()` above), down to 3 after `remove_orphan_
files` (2 of those 3 were "current"/still-referenced files from other
manual verification runs of this same probe, not orphans -- the true
before/after for THIS run's own files_metadata count, the number
`run_trino_table_maintenance()` below actually reports per production table,
was `$files`: 5 -> 1). Conclusion: vended credentials DO permit LIST.

Which tables get Trino maintenance: `SHOW TABLES FROM lake.alice`, dynamic
(`list_trino_alice_tables()`) -- same "not hardcoded" principle as pyiceberg
maintenance above, so `site_sonar` and any future `alice.*` table are
covered without a code change.

Data-layer freshness gate (design D11 completion)
--------------------------------------------------------------------------
The nightly workflow's final `check-freshness` step: `max(committed_at)`
from each CORE_FRESHNESS_TABLES table's `"<table>$snapshots"` metadata
table, stale if older than DEFAULT_FRESHNESS_MAX_AGE_HOURS (26h -- 24h
nightly cadence + 2h buffer, the same window `IcebergIngestStale` in
datalake-alerts.yaml already uses for the CronWorkflow-trigger side of
staleness). This is the DATA-layer half of D11 staleness: IcebergIngestStale
detects "the nightly CronWorkflow stopped firing"; this check detects "the
CronWorkflow fired and reported success, but the data it wrote never
actually landed a fresh Iceberg snapshot" -- a gap IcebergIngestStale cannot
see (design D11, Plan 2 Task 5's report: "D11 snapshot-staleness: documented
as composite; data-layer check -> Plan 3"). A missing table (freshness
checked before any table exists, or a table dropped/renamed) counts as
stale, not as an error -- `run_freshness_check()` returns exit code 1 either
way, which fails the `check-freshness` container, which fails the owning
ingest-nightly Workflow, surfacing through the existing WorkflowFailed alert
(same no-new-components pattern as retention.py's unverified>0 guard,
RetentionUnverifiedRows' annotation in datalake-alerts.yaml).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping

from alice_ingest.pipeline import DATASET_NAME
from alice_ingest.retention import load_iceberg_catalog
from alice_ingest.views import CATALOG, SOURCE_SCHEMA, TrinoClient, TrinoQueryError

logger = logging.getLogger(__name__)

DEFAULT_MAINTENANCE_OLDER_THAN_DAYS = "7"
DEFAULT_TRINO_MAINTENANCE_RETENTION_THRESHOLD = "7d"
CORE_FRESHNESS_TABLES = ("job_info", "trace", "mon_jdls_parsed")
DEFAULT_FRESHNESS_MAX_AGE_HOURS = "26"


def _now() -> datetime:
    """Seam for tests to freeze "now" (monkeypatched in test_maintenance.py)
    rather than reaching for a fake clock library for one call site."""
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class TableMaintenanceResult:
    """Outcome of expiring one table's old snapshots. `expired` is derived
    (before - after) rather than stored, so it can never drift from the two
    counts it summarizes."""

    table: str
    snapshots_before: int
    snapshots_after: int

    @property
    def expired(self) -> int:
        return self.snapshots_before - self.snapshots_after


def list_alice_tables(catalog, dataset_name: str = DATASET_NAME) -> list[str]:
    """Every table currently registered under `dataset_name`, as dotted
    identifiers (e.g. "alice.job_info") -- the form `catalog.load_table()`
    expects. Dynamic, not hardcoded, so a future table added to the
    namespace is picked up automatically (module docstring)."""
    identifiers = catalog.list_tables(dataset_name)
    return [".".join(identifier) for identifier in identifiers]


def expire_table_snapshots(
    catalog, table_identifier: str, older_than: datetime
) -> TableMaintenanceResult:
    """Expire snapshot metadata older than `older_than` on ONE table, using
    the verified `table.maintenance.expire_snapshots().older_than(...)
    .commit()` form (module docstring). Refreshes the table handle after
    commit before recounting -- pyiceberg table objects cache metadata
    locally; without refresh(), snapshots() would still report the
    pre-commit count."""
    table = catalog.load_table(table_identifier)
    snapshots_before = len(table.snapshots())
    table.maintenance.expire_snapshots().older_than(older_than).commit()
    table.refresh()
    snapshots_after = len(table.snapshots())
    return TableMaintenanceResult(
        table=table_identifier,
        snapshots_before=snapshots_before,
        snapshots_after=snapshots_after,
    )


def format_table_summary(result: TableMaintenanceResult) -> str:
    """Per-table log line (brief, Step 1: "Log per-table summary")."""
    return (
        f"MAINTENANCE table={result.table} "
        f"snapshots_before={result.snapshots_before} "
        f"snapshots_after={result.snapshots_after} "
        f"expired={result.expired}"
    )


def run(env: Mapping[str, str] | None = None) -> int:
    """Entry point wired from pipeline.py's `run_maintenance` CLI command."""
    env = env if env is not None else os.environ
    older_than_days = int(
        env.get("MAINTENANCE_OLDER_THAN_DAYS", DEFAULT_MAINTENANCE_OLDER_THAN_DAYS)
    )
    cutoff = _now() - timedelta(days=older_than_days)

    catalog = load_iceberg_catalog(env)
    identifiers = list_alice_tables(catalog)
    if not identifiers:
        print(f"MAINTENANCE SKIPPED: no tables found in namespace {DATASET_NAME}")
        return 0

    total_expired = 0
    for table_identifier in sorted(identifiers):
        result = expire_table_snapshots(catalog, table_identifier, cutoff)
        print(format_table_summary(result))
        total_expired += result.expired

    print(f"MAINTENANCE tables={len(identifiers)} total_expired={total_expired}")
    return 0


# --------------------------------------------------------------------------
# Plan 3 Task 3: Trino physical maintenance (module docstring, "Trino 476
# procedure syntax"). Reuses Task 2's TrinoClient (`alice_ingest.views`).
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class TrinoMaintenanceResult:
    """Outcome of physically compacting/expiring/GC-ing one table via Trino.
    `files_removed` is derived (before - after), same reasoning as
    TableMaintenanceResult.expired above: it can never drift from the two
    counts it summarizes. `files_before`/`files_after` count rows in
    `"<table>$files"` -- the physically-referenced data files for the
    table's CURRENT snapshot (module docstring)."""

    table: str
    files_before: int
    files_after: int

    @property
    def files_removed(self) -> int:
        return self.files_before - self.files_after


def list_trino_alice_tables(client) -> list[str]:
    """`SHOW TABLES FROM lake.alice`, sorted -- dynamic, not hardcoded (module
    docstring), mirroring `list_alice_tables()`'s pyiceberg-side reasoning."""
    rows = client.run(f"SHOW TABLES FROM {CATALOG}.{SOURCE_SCHEMA}")
    return sorted(row[0] for row in rows)


def _trino_files_count(client, table: str) -> int:
    rows = client.run(f'SELECT count(*) FROM {CATALOG}.{SOURCE_SCHEMA}."{table}$files"')
    return int(rows[0][0])


def run_trino_table_maintenance(
    client,
    table: str,
    retention_threshold: str = DEFAULT_TRINO_MAINTENANCE_RETENTION_THRESHOLD,
) -> TrinoMaintenanceResult:
    """Runs the verified `optimize` -> `expire_snapshots` -> `remove_orphan_
    files` sequence on ONE table (module docstring, "Trino 476 procedure
    syntax"), counting `"<table>$files"` before and after as the physical-GC
    proof (before/after file counts -- brief's acceptance line)."""
    qualified = f"{CATALOG}.{SOURCE_SCHEMA}.{table}"
    files_before = _trino_files_count(client, table)
    client.run(f"ALTER TABLE {qualified} EXECUTE optimize")
    client.run(
        f"ALTER TABLE {qualified} EXECUTE "
        f"expire_snapshots(retention_threshold => '{retention_threshold}')"
    )
    client.run(
        f"ALTER TABLE {qualified} EXECUTE "
        f"remove_orphan_files(retention_threshold => '{retention_threshold}')"
    )
    files_after = _trino_files_count(client, table)
    return TrinoMaintenanceResult(table=table, files_before=files_before, files_after=files_after)


def format_trino_maintenance_summary(result: TrinoMaintenanceResult) -> str:
    """Per-table log line, same shape as format_table_summary() above."""
    return (
        f"TRINO MAINTENANCE table={result.table} "
        f"files_before={result.files_before} "
        f"files_after={result.files_after} "
        f"files_removed={result.files_removed}"
    )


def run_trino_maintenance(
    client,
    tables: list[str],
    retention_threshold: str = DEFAULT_TRINO_MAINTENANCE_RETENTION_THRESHOLD,
) -> list[TrinoMaintenanceResult]:
    """Runs `run_trino_table_maintenance()` over every table, in sorted
    order, printing a per-table summary line as it goes (brief, Step 2:
    "with per-table summary lines")."""
    results = []
    for table in sorted(tables):
        result = run_trino_table_maintenance(client, table, retention_threshold=retention_threshold)
        print(format_trino_maintenance_summary(result))
        results.append(result)
    return results


def run_trino(env: Mapping[str, str] | None = None) -> int:
    """Entry point wired from pipeline.py's `run-trino-maintenance` CLI
    command -- the weekly ingest-maintenance CronWorkflow's second DAG step,
    after the existing pyiceberg `run-maintenance` step (brief, Step 3:
    "maintenance: after the pyiceberg step")."""
    env = env if env is not None else os.environ
    trino_uri = env.get("TRINO_URI")
    if not trino_uri:
        raise SystemExit("alice-ingest: required env var TRINO_URI is not set")
    retention_threshold = env.get(
        "TRINO_MAINTENANCE_RETENTION_THRESHOLD", DEFAULT_TRINO_MAINTENANCE_RETENTION_THRESHOLD
    )

    client = TrinoClient(base_uri=trino_uri.rstrip("/"))
    tables = list_trino_alice_tables(client)
    if not tables:
        print(f"TRINO MAINTENANCE SKIPPED: no tables found in {CATALOG}.{SOURCE_SCHEMA}")
        return 0

    results = run_trino_maintenance(client, tables, retention_threshold=retention_threshold)
    total_removed = sum(result.files_removed for result in results)
    print(f"TRINO MAINTENANCE tables={len(results)} total_files_removed={total_removed}")
    return 0


# --------------------------------------------------------------------------
# Plan 3 Task 3: data-layer freshness gate (module docstring, "Data-layer
# freshness gate"). Reuses Task 2's TrinoClient/TrinoQueryError.
# --------------------------------------------------------------------------


def _parse_trino_timestamp(value: str) -> datetime:
    """Trino's JSON REST wire format for a `committed_at` TIMESTAMP WITH TIME
    ZONE value (verified live, module docstring): `'2026-07-12
    19:40:03.327 UTC'`. Iceberg snapshot commit timestamps are always UTC
    (there is no code path that writes one in another zone), so the trailing
    zone name is checked and dropped rather than parsed generically, and the
    remainder is read as a naive datetime plus `timezone.utc`."""
    suffix = " UTC"
    if not value.endswith(suffix):
        raise ValueError(f"unexpected Trino timestamp format (not UTC): {value!r}")
    naive = datetime.strptime(value[: -len(suffix)], "%Y-%m-%d %H:%M:%S.%f")
    return naive.replace(tzinfo=timezone.utc)


def _looks_like_missing_table_error(exc: TrinoQueryError) -> bool:
    message = str(exc)
    return "TABLE_NOT_FOUND" in message or "does not exist" in message


@dataclass(frozen=True)
class FreshnessResult:
    """One core table's freshness. `missing` covers both "the table was
    never created" and "the table exists but has never had a snapshot
    committed" being indistinguishable from "stale" for alerting purposes --
    both mean the data-layer has nothing recent to show (module docstring,
    "a missing table ... counts as stale, not as an error")."""

    table: str
    max_committed_at: datetime | None
    missing: bool = False

    def is_stale(self, now: datetime, max_age: timedelta) -> bool:
        if self.missing or self.max_committed_at is None:
            return True
        return (now - self.max_committed_at) > max_age


def table_freshness(client, table: str) -> FreshnessResult:
    """`SELECT max(committed_at) FROM lake.alice."<table>$snapshots"` for
    ONE table. A `TrinoQueryError` that looks like "the table doesn't exist"
    becomes `missing=True` rather than propagating (module docstring); any
    OTHER Trino error (permissions, connectivity) is re-raised -- freshness
    checking must not silently swallow an unrelated failure as if it were
    "no data yet"."""
    try:
        rows = client.run(
            f'SELECT max(committed_at) FROM {CATALOG}.{SOURCE_SCHEMA}."{table}$snapshots"'
        )
    except TrinoQueryError as exc:
        if _looks_like_missing_table_error(exc):
            return FreshnessResult(table=table, max_committed_at=None, missing=True)
        raise
    value = rows[0][0] if rows and rows[0] else None
    if value is None:
        return FreshnessResult(table=table, max_committed_at=None, missing=False)
    return FreshnessResult(table=table, max_committed_at=_parse_trino_timestamp(value))


def check_freshness(
    client,
    now: datetime,
    tables: tuple[str, ...] = CORE_FRESHNESS_TABLES,
) -> list[FreshnessResult]:
    """One `FreshnessResult` per table, in `tables` order (default
    CORE_FRESHNESS_TABLES, module docstring)."""
    return [table_freshness(client, table) for table in tables]


def format_freshness_summary(result: FreshnessResult, now: datetime, max_age: timedelta) -> str:
    """Per-table log line. Missing tables have no age to report; fresh/stale
    tables report `age_hours` to two decimal places so a near-boundary case
    is legible without cross-referencing timestamps by hand."""
    if result.missing:
        return f"FRESHNESS table={result.table} status=MISSING"
    status = "STALE" if result.is_stale(now, max_age) else "FRESH"
    age_hours = (now - result.max_committed_at).total_seconds() / 3600
    return (
        f"FRESHNESS table={result.table} status={status} "
        f"max_committed_at={result.max_committed_at.isoformat()} "
        f"age_hours={age_hours:.2f}"
    )


def run_freshness_check(env: Mapping[str, str], now: datetime | None = None) -> int:
    """Entry point wired from pipeline.py's `check-freshness` CLI command --
    the nightly ingest-nightly CronWorkflow's third DAG step, after
    run-retention (brief, Step 3: "check-freshness AFTER run-retention").
    `now`: injected-clock seam for tests; falls back to the same `_now()`
    seam pyiceberg maintenance above uses when not given (module docstring:
    "Injected clock")."""
    trino_uri = env.get("TRINO_URI")
    if not trino_uri:
        raise SystemExit("alice-ingest: required env var TRINO_URI is not set")
    max_age_hours = int(env.get("FRESHNESS_MAX_AGE_HOURS", DEFAULT_FRESHNESS_MAX_AGE_HOURS))
    max_age = timedelta(hours=max_age_hours)
    now = now if now is not None else _now()

    client = TrinoClient(base_uri=trino_uri.rstrip("/"))
    results = check_freshness(client, now)

    any_stale = False
    for result in results:
        print(format_freshness_summary(result, now, max_age))
        if result.is_stale(now, max_age):
            any_stale = True

    if any_stale:
        print(
            f"FRESHNESS CHECK FAILED: one or more core tables stale "
            f"(max_age_hours={max_age_hours})"
        )
        return 1

    print("FRESHNESS CHECK OK")
    return 0
