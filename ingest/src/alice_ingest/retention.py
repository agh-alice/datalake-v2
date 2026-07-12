"""alice-ingest retention: delete landing-PostgreSQL rows older than
`RETENTION_DAYS`, but ONLY after verifying each row's `job_id` exists in the
corresponding Iceberg table (design D5 + owner decision 2026-07-12). This
guards against ever deleting a landing row before it has been successfully
replicated to Iceberg, even if ingestion is lagging, failed mid-run, or a
row's incremental cursor hasn't advanced past it yet.

Per-table cutoff predicate (brief, Plan 2 Task 4 "Context" section):

  - `job_info`: its own `last_update` column directly. Production's column
    is `timestamp WITHOUT time zone` (naive) -- see pipeline.py's module
    docstring -- so the cutoff bound must be a naive Python datetime.

  - `trace`: has NO `last_update` column in production (research/
    2026-07-12_ml-consumer-data-contract.md, "Production schema ground
    truth"). Uses its own `laststatuschangetimestamp` bigint-epoch column
    instead. Epoch UNIT: MILLISECONDS -- resolved by querying PRODUCTION
    live, not from the brief (the brief documents no unit at all; an
    earlier version of this docstring and of hack/seed-fixture.sh's
    comment incorrectly attributed "bigint ms per gen-1 Java convention"
    to the brief, which a P2T4 review flagged as citation-free and
    fabricated). Ground truth: research/2026-07-12_ml-consumer-data-
    contract.md, "Production schema ground truth" section, epoch-unit
    bullet (controller-resolved live against production 2026-07-12):
    production sample `laststatuschangetimestamp`/`startedtimestamp`/
    `finaltimestamp` = e.g. `1783848905000` vs `extract(epoch from now())`
    = `1783869017` -- three orders of magnitude apart, i.e. production
    stores these trace epoch columns in MILLISECONDS. `runningtimestamp`/
    `savingtimestamp` are treated the same way by the same system's
    convention (all five are populated by the same gen-1 monitoring writer
    per column naming/shape), though only the three above were directly
    sampled live. Separately, the live kind FIXTURE was found generating
    epoch SECONDS at the same ~1.78e9 magnitude as `extract(epoch FROM
    now())` -- a fixture bug, not a units question about production --
    fixed at the source across all five trace epoch columns
    (hack/seed-fixture.sh, `* 1000`) rather than papered over with a
    magnitude-sniffing heuristic here, so this module's SQL matches real
    production semantics unconditionally:
    `to_timestamp(laststatuschangetimestamp / 1000.0)`. `to_timestamp()`
    returns `timestamptz`, so the cutoff bound for this table must be a
    tz-aware Python datetime.

    Plausibility guard (review Fix 1c): `check_trace_plausibility()` below
    sanity-checks the ms->s decoded min/max of the trace candidate set
    against [2020-01-01, 2035-01-01) BEFORE any delete runs, and aborts
    loudly (distinct `RETENTION ABORT: implausible trace timestamps`
    message, non-zero exit) if it's ever wrong again -- the defense
    against a future unit-convention drift (production reverting to
    seconds, a fixture/migration regression, etc.) silently mass-deleting
    rows under a wrong divisor.

  - `mon_jdls`: has NO timestamp column of any kind (production ground
    truth: `job_id`, `lpmjobtypeid`, `full_jdl` only). Retention age is
    derived via a join to `job_info.last_update` on `job_id` (documented
    choice per the brief's explicit instruction) -- a `mon_jdls` row's
    "age" is its parent job's `last_update`. Naive cutoff, same as
    `job_info`. The join is a LEFT JOIN with an `OR j.job_id IS NULL`
    escape hatch (review Fix 2, important), not an INNER JOIN: an INNER
    JOIN makes a `mon_jdls` row permanently un-collectible once its parent
    `job_info` row has already been retained (deleted) by an *earlier*
    run -- the row would never again match the join and would accumulate
    forever (flagged as a concern in task-4-report.md). The LEFT JOIN form
    treats a `mon_jdls` row whose parent is already gone (orphaned) as
    eligible, same as one whose parent is old.

Iceberg presence verification: job_ids are checked in batches of
`BATCH_SIZE` (10_000, per the brief) using pyiceberg's `In` row-filter
expression -- verified LIVE against this cluster's Lakekeeper/MinIO
(Plan 2 Task 4, in-cluster probe pod, pinned pyiceberg 0.11.1):
`table.scan(row_filter=In("job_id", batch)).to_arrow()` returns exactly the
present subset of `batch`; job_ids not in that subset are "unverified"
(kept, not deleted, counted, logged -- the alerting signal for Task 5).

SQL discipline: every query is a fixed, hardcoded string with `%s`
placeholders; only the cutoff datetime and job_id batches travel as
psycopg2 execute() parameters. No f-string/`.format()` interpolation of
values into SQL text anywhere in this module ("that's a gen-1 disease" --
brief).

Exit code: `run()` returns non-zero when total `unverified > 0` across ALL
tables (brief's alerting contract, consumed by Task 5's monitoring).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping, Protocol, Sequence

from alice_ingest.pipeline import DEFAULT_RETENTION_DAYS, _require_env, iceberg_catalog_properties

logger = logging.getLogger(__name__)

# Presence-check AND delete batches are bounded at this size (brief, Step 2:
# "batch job_ids (10k) per the brief" / "delete only verified batches in
# bounded transactions (10k/batch)"). A module-level variable (not a
# function default) so tests can monkeypatch it to exercise multi-batch
# behavior without constructing 10_000 fake job_ids.
BATCH_SIZE = 10_000


# --------------------------------------------------------------------------
# Per-table SQL (parameterized; see module docstring's "SQL discipline").
# --------------------------------------------------------------------------

_JOB_INFO_AGE_SQL = "SELECT DISTINCT job_id FROM job_info WHERE last_update < %s"
# AND last_update < %s (final-review N1, important -- TOCTOU): the pre-fix
# form was `WHERE job_id = ANY(%s)` alone. A row whose `last_update`
# advances past the cutoff between the candidate SELECT and this DELETE
# (the job "revives") was deleted anyway -- silently discarding the
# revival's newest update, which never gets a chance to be ingested before
# its landing row disappears. Re-asserting the SAME age predicate here
# closes the window: only rows still genuinely older than `cutoff` at
# DELETE time are removed, cutoff bound as the second parameter. See
# tests/test_retention.py's TestDeleteReassertsAgePredicateTOCTOU for the
# behavioral proof (revived row survives; non-revived rows still deleted).
_JOB_INFO_DELETE_SQL = "DELETE FROM job_info WHERE job_id = ANY(%s) AND last_update < %s"

# / 1000.0 -- epoch MILLISECONDS -> seconds, per this module's docstring.
_TRACE_AGE_SQL = (
    "SELECT DISTINCT job_id FROM trace "
    "WHERE to_timestamp(laststatuschangetimestamp / 1000.0) < %s"
)
# Same N1 TOCTOU re-assertion as job_info's DELETE above, on trace's own
# epoch-ms age column.
_TRACE_DELETE_SQL = (
    "DELETE FROM trace WHERE job_id = ANY(%s) "
    "AND to_timestamp(laststatuschangetimestamp / 1000.0) < %s"
)

# mon_jdls has no timestamp of its own -- join to job_info.last_update.
# LEFT JOIN (not INNER) + a bounded orphan escape hatch (review Fix 2 /
# final-review N2, important): a mon_jdls row whose job_info parent was
# already retained (deleted) by a previous run is an ORPHAN, not "not old
# enough" -- an INNER JOIN would silently and permanently hide it from
# every future candidate set. The escape hatch used to be unconditional
# (`OR j.job_id IS NULL`), which made a BRAND NEW mon_jdls row an instant
# candidate too, whenever its own job_info parent insert simply hadn't
# landed yet (ordinary ingestion ordering, not retirement). Bounded here
# (N2): an orphan is only eligible when its job_id is BELOW the oldest
# currently-surviving job_info row -- since job_id is monotonically
# increasing in the source system (pipeline.py's module docstring), a
# parent that was genuinely retained away is always numerically below
# every survivor, while a parent that merely hasn't landed yet belongs to
# a job_id at or above the current maximum, never below the minimum.
# COALESCE(..., 0): an empty job_info table (fresh reset) pins the floor
# at 0 rather than leaving the bound NULL (which would make every
# `m.job_id < NULL` comparison false via SQL's three-valued logic). See
# this module's docstring and TestMonJdlsOrphanEligibility /
# TestMonJdlsOrphanEligibilityBounded in tests/test_retention.py for the
# behavioral proofs.
_MON_JDLS_AGE_SQL = (
    "SELECT DISTINCT m.job_id FROM mon_jdls m "
    "LEFT JOIN job_info j ON j.job_id = m.job_id "
    "WHERE j.last_update < %s OR (j.job_id IS NULL "
    "AND m.job_id < (SELECT COALESCE(MIN(job_id), 0) FROM job_info))"
)
# N1 TOCTOU re-assertion for mon_jdls, respecting N2's bounded orphan
# condition: a candidate job_id is only actually deleted if, AT DELETE
# TIME, it is either (a) still joined to a job_info row whose last_update
# is still older than cutoff, or (b) still a bounded orphan (no job_info
# row, AND below the current minimum surviving job_id). Either the parent
# revives (case a's subquery excludes it) or a parent shows up between
# SELECT and DELETE (case b's NOT IN excludes it) -- both TOCTOU windows
# are closed by the same two-branch re-assertion the age predicate uses.
_MON_JDLS_DELETE_SQL = (
    "DELETE FROM mon_jdls WHERE job_id = ANY(%s) AND ("
    "job_id IN (SELECT job_id FROM job_info WHERE last_update < %s) "
    "OR (job_id NOT IN (SELECT job_id FROM job_info) "
    "AND job_id < (SELECT COALESCE(MIN(job_id), 0) FROM job_info)))"
)


# --------------------------------------------------------------------------
# Trace-table plausibility guard (review Fix 1c, critical).
# --------------------------------------------------------------------------


class RetentionAbortError(RuntimeError):
    """Raised by check_trace_plausibility() when the trace table's decoded
    timestamps look implausible -- caught by run() and turned into a loud,
    distinctly-messaged (`RETENTION ABORT: implausible trace timestamps`),
    non-zero-exit abort BEFORE any delete executes. See this module's
    docstring for the defense this guards against."""


# Plausible calendar window for to_timestamp(laststatuschangetimestamp /
# 1000.0): [PLAUSIBLE_MIN, PLAUSIBLE_MAX). Wide on purpose -- this is a
# coarse sanity check against gross unit-convention errors (e.g. a value
# read as ms that's actually seconds decodes to 1970-something, comfortably
# outside this window), not a tight freshness check.
PLAUSIBLE_MIN = datetime(2020, 1, 1, tzinfo=timezone.utc)
PLAUSIBLE_MAX = datetime(2035, 1, 1, tzinfo=timezone.utc)

# Aggregates over exactly the candidate job_ids already collected by the
# two-phase pass's SELECT phase (run_retention_pass) -- cheap: one bounded
# query, not a full-table scan ("the candidate set", per the review ask).
_TRACE_PLAUSIBILITY_SQL = (
    "SELECT MIN(to_timestamp(laststatuschangetimestamp / 1000.0)), "
    "MAX(to_timestamp(laststatuschangetimestamp / 1000.0)) "
    "FROM trace WHERE job_id = ANY(%s)"
)


def check_trace_plausibility(conn, candidate_job_ids: Sequence[int]) -> None:
    """Plausibility guard (review Fix 1c): before ANY delete runs against
    `trace`, sanity-check that laststatuschangetimestamp/1000.0 decodes to
    timestamps inside [PLAUSIBLE_MIN, PLAUSIBLE_MAX). Aborts loudly
    (RetentionAbortError, distinct `RETENTION ABORT: implausible trace
    timestamps` message) rather than proceeding to delete under a
    possibly-wrong unit conversion -- the defense against a future
    epoch-unit drift (production reverting to seconds, a fixture/migration
    regression, etc.) silently mass-deleting rows.

    No-ops on an empty candidate set: nothing would be deleted, so there is
    nothing to guard.
    """
    candidate_job_ids = list(candidate_job_ids)
    if not candidate_job_ids:
        return

    with conn.cursor() as cur:
        cur.execute(_TRACE_PLAUSIBILITY_SQL, (candidate_job_ids,))
        row = cur.fetchone()
    if not row:
        return
    lo, hi = row
    if lo is None or hi is None:
        return

    if lo < PLAUSIBLE_MIN or hi >= PLAUSIBLE_MAX:
        raise RetentionAbortError(
            f"RETENTION ABORT: implausible trace timestamps "
            f"(min={lo!r}, max={hi!r}; expected within "
            f"[{PLAUSIBLE_MIN!r}, {PLAUSIBLE_MAX!r})) -- refusing to delete "
            f"{len(candidate_job_ids)} candidate row(s) from `trace`. This "
            f"is almost certainly an epoch-unit regression (seconds "
            f"mistaken for milliseconds, or vice versa) -- investigate "
            f"before rerunning retention."
        )


@dataclass(frozen=True)
class RetentionTable:
    """One landing table's retention wiring: which PG table, which Iceberg
    table job_ids get verified against, which cutoff type its age
    predicate needs ("naive" | "aware"), and its two SQL statements."""

    landing_table: str
    iceberg_table: str
    cutoff_kind: str  # "naive" | "aware" -- selects cutoffs[cutoff_kind]
    age_sql: str
    delete_sql: str


# DATASET_NAME ("alice") and the mon_jdls -> mon_jdls_parsed table_name
# mapping mirror pipeline.py's build_*_source()/build_mon_jdls_resource()
# apply_hints() calls exactly -- these must stay in sync with wherever dlt
# actually lands the Iceberg tables.
TABLES: tuple[RetentionTable, ...] = (
    RetentionTable("job_info", "alice.job_info", "naive", _JOB_INFO_AGE_SQL, _JOB_INFO_DELETE_SQL),
    RetentionTable("trace", "alice.trace", "aware", _TRACE_AGE_SQL, _TRACE_DELETE_SQL),
    RetentionTable(
        "mon_jdls", "alice.mon_jdls_parsed", "naive", _MON_JDLS_AGE_SQL, _MON_JDLS_DELETE_SQL
    ),
)


@dataclass(frozen=True)
class TableResult:
    """Outcome of retaining one table. `kept` and `unverified` are
    currently always equal (the only reason an old row is kept is failed
    Iceberg-presence verification) -- both are reported because the brief's
    summary line format names them separately (`unverified` is Task 5's
    specific alerting field)."""

    kept: int
    deleted: int
    unverified: int

    def __add__(self, other: "TableResult") -> "TableResult":
        return TableResult(
            kept=self.kept + other.kept,
            deleted=self.deleted + other.deleted,
            unverified=self.unverified + other.unverified,
        )


class IcebergPresenceCatalog(Protocol):
    """Fake-able seam between retention's business logic and Iceberg.
    Production adapter: PyIcebergPresenceCatalog below. Tests fake this
    protocol directly (brief, Step 1: 'fake catalog interface')."""

    def present_job_ids(self, iceberg_table: str, job_ids: Sequence[int]) -> set[int]:
        """Return the subset of job_ids present in iceberg_table."""
        ...


@dataclass
class PyIcebergPresenceCatalog:
    """Production `IcebergPresenceCatalog`: wraps a live pyiceberg REST
    `Catalog` (Lakekeeper). Uses the `In` row-filter form verified live
    against this cluster (Plan 2 Task 4, in-cluster probe pod, pinned
    pyiceberg 0.11.1) -- `table.scan(row_filter=In("job_id", batch))
    .to_arrow()` returns exactly the present subset of `batch`, batched at
    BATCH_SIZE so a single retention run never asks Lakekeeper to plan a
    scan against an unbounded IN-list."""

    catalog: object  # pyiceberg.catalog.Catalog; loosely typed so this
    # module never needs to import pyiceberg just to be imported (unit
    # tests exercise the pure logic via the Protocol's fakes instead).

    def present_job_ids(self, iceberg_table: str, job_ids: Sequence[int]) -> set[int]:
        from pyiceberg.expressions import In

        job_ids = list(job_ids)
        present: set[int] = set()
        table = self.catalog.load_table(iceberg_table)  # type: ignore[attr-defined]
        for i in range(0, len(job_ids), BATCH_SIZE):
            batch = job_ids[i : i + BATCH_SIZE]
            arrow_table = table.scan(row_filter=In("job_id", batch)).to_arrow()
            present.update(arrow_table.column("job_id").to_pylist())
        return present


def load_iceberg_catalog(env: Mapping[str, str]):
    """Production wiring: `pyiceberg.catalog.load_catalog(...)` using the
    same flat-key properties dlt's filesystem destination uses
    (pipeline.py's `iceberg_catalog_properties()`), matching the pattern
    already verified working in hack/kind-verify.sh's iceberg-contents-probe."""
    from pyiceberg.catalog import load_catalog

    warehouse = _require_env(env, "LAKEKEEPER_WAREHOUSE")
    return load_catalog(warehouse, **iceberg_catalog_properties(env))


def select_old_job_ids(conn, table: RetentionTable, cutoffs: Mapping[str, datetime]) -> list[int]:
    """Distinct job_ids in `table.landing_table` older than the cutoff for
    `table.cutoff_kind`. Parameterized: the cutoff travels as a bound
    query parameter, never formatted into the SQL text."""
    cutoff = cutoffs[table.cutoff_kind]
    with conn.cursor() as cur:
        cur.execute(table.age_sql, (cutoff,))
        return [row[0] for row in cur.fetchall()]


def classify_and_delete(
    conn,
    presence_catalog: IcebergPresenceCatalog,
    table: RetentionTable,
    old_job_ids: Sequence[int],
    cutoff: datetime,
) -> TableResult:
    """Core presence-check + delete orchestration (brief, Step 1's three
    cases). `old_job_ids` are job_ids already known to be older than
    `cutoff` as of the candidate SELECT (select_old_job_ids's job); this
    function only decides, for each, whether it is safe to delete.

    `cutoff` is re-bound into every DELETE statement (final-review N1,
    important -- TOCTOU): each table's `delete_sql` now re-asserts the same
    age predicate the SELECT used, so a row that was old at SELECT time but
    has since revived (its age column advanced past `cutoff` in the window
    between the SELECT and this DELETE) is excluded at DELETE time rather
    than removed anyway. See retention.py's per-table SQL comments and
    tests/test_retention.py's TestDeleteReassertsAgePredicateTOCTOU."""
    old_job_ids = list(old_job_ids)
    if not old_job_ids:
        return TableResult(kept=0, deleted=0, unverified=0)

    present = presence_catalog.present_job_ids(table.iceberg_table, old_job_ids)
    verified = [jid for jid in old_job_ids if jid in present]
    unverified = [jid for jid in old_job_ids if jid not in present]

    deleted = 0
    for i in range(0, len(verified), BATCH_SIZE):
        batch = verified[i : i + BATCH_SIZE]
        with conn.cursor() as cur:
            cur.execute(table.delete_sql, (batch, cutoff))
            deleted += cur.rowcount
        conn.commit()  # bounded transaction: one commit per <=BATCH_SIZE batch

    if unverified:
        logger.warning(
            "RETENTION table=%s: %d job_id(s) older than cutoff but NOT "
            "verified present in Iceberg table %s -- kept, unverified: %s",
            table.landing_table,
            len(unverified),
            table.iceberg_table,
            sorted(unverified)[:20],
        )

    return TableResult(kept=len(unverified), deleted=deleted, unverified=len(unverified))


def retain_table(
    conn,
    presence_catalog: IcebergPresenceCatalog,
    table: RetentionTable,
    cutoffs: Mapping[str, datetime],
) -> TableResult:
    """select_old_job_ids + classify_and_delete for a SINGLE table in
    isolation. Do not loop this over TABLES to retain multiple tables in
    one run -- see run_retention_pass()'s docstring for why that ordering
    is unsafe when one table's age predicate joins to another's."""
    old_job_ids = select_old_job_ids(conn, table, cutoffs)
    return classify_and_delete(
        conn, presence_catalog, table, old_job_ids, cutoffs[table.cutoff_kind]
    )


def run_retention_pass(
    conn,
    presence_catalog: IcebergPresenceCatalog,
    cutoffs: Mapping[str, datetime],
) -> dict[str, TableResult]:
    """Two-phase retention across every table in TABLES: first collect
    EVERY table's old-job-id candidate set (SELECTs only, no deletes),
    THEN verify+delete each table.

    This ordering is load-bearing, not cosmetic. `mon_jdls` has no
    timestamp of its own -- its age predicate JOINs to `job_info.
    last_update` (module docstring). Found empirically (Plan 2 Task 4,
    first live nightly CronWorkflow trigger against the kind fixture):
    processing tables one-at-a-time end-to-end (select THEN delete THEN
    move to the next table) deleted job_info's 528 old+verified rows
    correctly, but then mon_jdls's join-based SELECT ran against a
    job_info table that NO LONGER HAD those rows -- the join silently
    lost its basis, mon_jdls's candidate set came back empty, and 528
    equally-old mon_jdls rows were never even considered (not deleted,
    not counted as kept/unverified -- simply invisible to the run).
    Collecting every table's candidates BEFORE any table's deletes run
    closes this regardless of TABLES' order or which table's predicate
    happens to reference which.

    Also runs the trace-table plausibility guard (review Fix 1c) right
    after collection, still BEFORE any table's delete loop starts:
    check_trace_plausibility() raises RetentionAbortError if trace's
    candidate set decodes to an implausible calendar range, aborting the
    whole pass -- no table's delete (not just trace's) executes in that
    case.
    """
    old_job_ids_by_table = {
        table.landing_table: select_old_job_ids(conn, table, cutoffs) for table in TABLES
    }
    check_trace_plausibility(conn, old_job_ids_by_table["trace"])
    results: dict[str, TableResult] = {}
    for table in TABLES:
        results[table.landing_table] = classify_and_delete(
            conn,
            presence_catalog,
            table,
            old_job_ids_by_table[table.landing_table],
            cutoffs[table.cutoff_kind],
        )
    return results


def format_summary(total: TableResult) -> str:
    """The alerting-grep-friendly summary line (brief, Step 2)."""
    return f"RETENTION kept={total.kept} deleted={total.deleted} unverified={total.unverified}"


def exit_code(total: TableResult) -> int:
    """Non-zero when any row was kept for lack of Iceberg verification --
    the alarm signal Task 5's monitoring consumes."""
    return 0 if total.unverified == 0 else 1


def run(env: Mapping[str, str] | None = None) -> int:
    """Entry point wired from pipeline.py's `run_retention` CLI command."""
    env = env if env is not None else os.environ
    import psycopg2

    retention_days = int(env.get("RETENTION_DAYS", DEFAULT_RETENTION_DAYS))
    now_aware = datetime.now(timezone.utc)
    cutoffs: dict[str, datetime] = {
        "naive": now_aware.replace(tzinfo=None) - timedelta(days=retention_days),
        "aware": now_aware - timedelta(days=retention_days),
    }

    pg_url = _require_env(env, "PG_URL")
    catalog = load_iceberg_catalog(env)
    presence_catalog = PyIcebergPresenceCatalog(catalog)

    total = TableResult(kept=0, deleted=0, unverified=0)
    conn = psycopg2.connect(pg_url)
    try:
        # Two-phase across ALL tables (run_retention_pass's docstring):
        # never delete-then-select table-by-table here -- mon_jdls's
        # job_info-join predicate depends on job_info's old rows still
        # being present when mon_jdls's own SELECT runs.
        try:
            results_by_table = run_retention_pass(conn, presence_catalog, cutoffs)
        except RetentionAbortError as exc:
            # Plausibility guard tripped (review Fix 1c) -- no delete has
            # run for ANY table (run_retention_pass's docstring). Print the
            # distinct, grep-friendly abort message and exit non-zero
            # instead of the normal RETENTION summary line.
            print(str(exc))
            return 1
        for table in TABLES:
            result = results_by_table[table.landing_table]
            print(
                f"RETENTION table={table.landing_table} kept={result.kept} "
                f"deleted={result.deleted} unverified={result.unverified}"
            )
            total = total + result
    finally:
        conn.close()

    print(format_summary(total))
    return exit_code(total)
