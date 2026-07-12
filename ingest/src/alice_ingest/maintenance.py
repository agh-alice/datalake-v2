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
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping

from alice_ingest.pipeline import DATASET_NAME
from alice_ingest.retention import load_iceberg_catalog

logger = logging.getLogger(__name__)

DEFAULT_MAINTENANCE_OLDER_THAN_DAYS = "7"


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
