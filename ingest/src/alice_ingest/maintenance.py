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

P3T3 review Important 1: nightly-vs-maintenance overlap guard
--------------------------------------------------------------------------
Production schedules: `ingest-nightly` fires `0 2 * * *` with
`activeDeadlineSeconds: 21600` (chart/values.yaml), so a slow run can still
be executing at 08:00; `ingest-maintenance` (this module's Trino half)
fires `0 4 * * 0` -- Sunday 04:00 sits squarely inside that window, so a
real overlap is possible, not a theoretical edge case. Iceberg's optimistic
concurrency control (every commit is a compare-and-swap against the
table's current metadata pointer) already prevents CORRUPTION if Trino's
`OPTIMIZE`/`expire_snapshots`/`remove_orphan_files` and an in-flight
nightly write race for the same table -- one of the two commits simply
fails and (for Trino's procedures) surfaces as a query error. What OCC does
NOT prevent is the AVAILABILITY cost of that failure: a failed maintenance
run (or a failed nightly write, if nightly loses the race) that then has to
wait a full week (or a full day) for the next scheduled attempt. This guard
exists purely to avoid paying that cost when it's cheap to avoid --
`run_trino()` (wired from the `run-trino-maintenance` CLI command, the
weekly CronWorkflow's Trino-maintenance entrypoint) checks whether
`ingest-nightly`'s most recent Workflow is currently `Running` via the
Kubernetes API (`WorkflowsClient`, RBAC added in
chart/templates/workflows-rbac.yaml) BEFORE issuing any Trino statement,
and defers (`MAINTENANCE DEFERRED: ingest-nightly in flight`, exit 0 -- not
a failure, next Sunday's tick catches up) if one is found.

This is a best-effort OPTIMIZATION layered on top of a safety mechanism
that already works without it, which is why every failure mode below a
genuinely Running nightly resolves to `proceed=True` (`check_nightly_
overlap_guard()`'s `OverlapGuardResult`):
  - `MAINTENANCE_FORCE=1` (env) skips the Kubernetes API call entirely and
    proceeds -- an operator's explicit override, documented in
    docs/runbooks/ingestion-pipeline.md's env-var table, for a manual
    maintenance trigger the operator already knows is safe (e.g. they just
    confirmed nightly is NOT running, or they accept the OCC-mediated risk
    for a one-off run).
  - An unreachable Kubernetes API (RBAC misconfigured, token unreadable,
    apiserver connection failure -- anything `WorkflowsClient.
    list_workflows()` raises as `KubernetesApiError`) proceeds too, with a
    `MAINTENANCE WARNING` line rather than blocking. Reasoning: this guard
    is an optimization, not the safety layer (Iceberg OCC is), so a routine
    weekly job should not fail outright because a best-effort check
    couldn't run -- that would make the guard itself a new single point of
    failure for a job that worked fine before this guard existed.
Only a confirmed Running `ingest-nightly` Workflow defers. TDD coverage
(`ingest/tests/test_maintenance.py`, "P3T3 review Important 1" section)
exercises all four paths (running -> defer; none -> proceed; force ->
proceed despite running; API unreachable -> proceed with a warning) against
a fake k8s-API client -- no live overlap was fabricated on the shared kind
cluster to test this (there is exactly one shared cluster; deliberately
colliding a real Sunday maintenance tick with a real nightly run would risk
an actual production-shaped commit conflict for no verification benefit the
fakes don't already provide).

`WorkflowsClient` is a minimal in-cluster REST client (same pattern as
views.py's `TrinoClient`: a `requests`-based client scoped to exactly the
one call this module needs) rather than a dependency on the full
`kubernetes` PyPI package for a single GET. It reads the pod's mounted
ServiceAccount token (`pipeline-runner`, workflows-rbac.yaml) and CA cert
from the standard in-cluster paths at call time, and lists `Workflow`
custom objects (`argoproj.io/v1alpha1`) filtered by the
`workflows.argoproj.io/cron-workflow=<name>` label -- the same label
`docs/runbooks/ingestion-pipeline.md`'s existing manual `kubectl get
workflow -l workflows.argoproj.io/cron-workflow=ingest-nightly` recipe
already uses, so this guard queries exactly what a human operator would
check by hand.

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

import requests

from alice_ingest.pipeline import DATASET_NAME
from alice_ingest.retention import load_iceberg_catalog
from alice_ingest.views import CATALOG, SOURCE_SCHEMA, TrinoClient, TrinoQueryError

logger = logging.getLogger(__name__)

DEFAULT_MAINTENANCE_OLDER_THAN_DAYS = "7"
DEFAULT_TRINO_MAINTENANCE_RETENTION_THRESHOLD = "7d"
CORE_FRESHNESS_TABLES = ("job_info", "trace", "mon_jdls_parsed")
DEFAULT_FRESHNESS_MAX_AGE_HOURS = "26"

# P3T3 review Important 1: nightly-vs-maintenance overlap guard (module
# docstring, "P3T3 review Important 1" section).
NIGHTLY_CRON_WORKFLOW_NAME = "ingest-nightly"
ARGO_WORKFLOWS_NAMESPACE = "argo-workflows"
DEFAULT_K8S_API_SERVER = "https://kubernetes.default.svc"
DEFAULT_K8S_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
DEFAULT_K8S_CA_CERT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


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


class KubernetesApiError(RuntimeError):
    """Raised by `WorkflowsClient.list_workflows()` for every failure mode
    (unreadable ServiceAccount token, TLS/connection failure, non-2xx
    response) -- one exception type so `check_nightly_overlap_guard()` has
    exactly one thing to catch for its fail-open path (module docstring,
    "P3T3 review Important 1")."""


@dataclass(frozen=True)
class WorkflowsClient:
    """Minimal in-cluster REST client for listing Argo `Workflow` custom
    objects (`argoproj.io/v1alpha1`) -- mirrors views.py's `TrinoClient`
    (a `requests`-based client scoped to exactly the one call this module
    needs) rather than adding the full `kubernetes` PyPI package as a
    dependency for a single GET. Reads the token/CA at CALL time, not
    construction time, so a client can be constructed in a unit test (or
    before the ServiceAccount volume is mounted) without either file
    existing yet."""

    api_server: str = DEFAULT_K8S_API_SERVER
    namespace: str = ARGO_WORKFLOWS_NAMESPACE
    token_path: str = DEFAULT_K8S_TOKEN_PATH
    ca_cert_path: str = DEFAULT_K8S_CA_CERT_PATH
    timeout: float = 10.0

    def list_workflows(self, cron_workflow_name: str) -> list[dict]:
        """GET .../workflows?labelSelector=workflows.argoproj.io/
        cron-workflow=<cron_workflow_name> -- the same label docs/runbooks/
        ingestion-pipeline.md's existing manual `kubectl get workflow -l
        ...` recipe already uses. Returns the raw `items` list (each a
        Workflow object dict); `is_cron_workflow_running()` below reads
        `.status.phase` from each."""
        try:
            with open(self.token_path, encoding="utf-8") as fh:
                token = fh.read().strip()
        except OSError as exc:
            raise KubernetesApiError(f"cannot read service account token: {exc}") from exc

        url = f"{self.api_server}/apis/argoproj.io/v1alpha1/namespaces/{self.namespace}/workflows"
        try:
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params={"labelSelector": f"workflows.argoproj.io/cron-workflow={cron_workflow_name}"},
                verify=self.ca_cert_path,
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as exc:
            raise KubernetesApiError(f"Kubernetes API request failed: {exc}") from exc
        return resp.json().get("items", [])


def is_cron_workflow_running(client, cron_workflow_name: str = NIGHTLY_CRON_WORKFLOW_NAME) -> bool:
    """True if any Workflow object `client.list_workflows(cron_workflow_
    name)` returns has `status.phase == "Running"` -- Argo's own phase
    value for an in-progress Workflow (Succeeded/Failed/Error are the
    terminal phases, none of which indicate an overlap risk). A missing
    `status` or `phase` key (a Workflow object that hasn't been scheduled
    onto a pod yet) is treated as not-running rather than raised -- this
    function only answers "is one currently executing", and an object
    without a phase yet is not."""
    items = client.list_workflows(cron_workflow_name)
    return any(item.get("status", {}).get("phase") == "Running" for item in items)


@dataclass(frozen=True)
class OverlapGuardResult:
    """`proceed=False` only for a confirmed Running nightly; `message` is
    `None` on the silent-proceed path (nothing worth logging) and set on
    every other path (deferred, forced, or fail-open-with-warning) -- see
    `check_nightly_overlap_guard()`'s docstring for which is which."""

    proceed: bool
    message: str | None = None


def check_nightly_overlap_guard(
    client,
    env: Mapping[str, str],
    cron_workflow_name: str = NIGHTLY_CRON_WORKFLOW_NAME,
) -> OverlapGuardResult:
    """The P3T3 review Important 1 guard (module docstring has the full
    reasoning). Order of checks:

    1. `MAINTENANCE_FORCE=1` (env) -- skip the API call entirely, proceed.
       An operator's explicit override (docs/runbooks/ingestion-pipeline.md).
    2. Query `client.list_workflows(cron_workflow_name)` via
       `is_cron_workflow_running()`:
       - `KubernetesApiError` (API unreachable, RBAC misconfigured, token
         unreadable) -- proceed anyway, with a `MAINTENANCE WARNING`
         message. Fail-open: this guard is an optimization (avoid an
         OPTIMIZE-vs-in-flight-write commit conflict's availability cost),
         NOT the safety layer -- Iceberg's optimistic concurrency control
         already prevents corruption on a real conflict, so a best-effort
         check that can't run should not block a routine weekly job.
       - Running -- defer (`proceed=False`, `MAINTENANCE DEFERRED: <name>
         in flight`). Next Sunday's tick catches up; this is not a failure.
       - Not running -- proceed silently (`message=None`).
    """
    if env.get("MAINTENANCE_FORCE") == "1":
        return OverlapGuardResult(proceed=True)
    try:
        running = is_cron_workflow_running(client, cron_workflow_name)
    except KubernetesApiError as exc:
        return OverlapGuardResult(
            proceed=True,
            message=(
                f"MAINTENANCE WARNING: nightly-overlap guard check failed ({exc}); "
                f"proceeding (fail-open -- Iceberg OCC is the safety layer, "
                f"this guard is an optimization)"
            ),
        )
    if running:
        return OverlapGuardResult(
            proceed=False, message=f"MAINTENANCE DEFERRED: {cron_workflow_name} in flight"
        )
    return OverlapGuardResult(proceed=True)


def run_trino(env: Mapping[str, str] | None = None) -> int:
    """Entry point wired from pipeline.py's `run-trino-maintenance` CLI
    command -- the weekly ingest-maintenance CronWorkflow's second DAG step,
    after the existing pyiceberg `run-maintenance` step (brief, Step 3:
    "maintenance: after the pyiceberg step"). Runs the P3T3 review
    Important 1 nightly-overlap guard (`check_nightly_overlap_guard()`)
    before issuing any Trino statement -- a deferred run returns 0 without
    ever constructing a `TrinoClient`."""
    env = env if env is not None else os.environ
    trino_uri = env.get("TRINO_URI")
    if not trino_uri:
        raise SystemExit("alice-ingest: required env var TRINO_URI is not set")

    guard_result = check_nightly_overlap_guard(WorkflowsClient(), env)
    if guard_result.message:
        print(guard_result.message)
    if not guard_result.proceed:
        return 0

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
