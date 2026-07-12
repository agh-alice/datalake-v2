"""alice-ingest apply-views: idempotent `CREATE OR REPLACE VIEW` DDL that
restores the ML consumer's dtypes-contract column spellings over the
dlt-normalized `alice.*` Iceberg tables (Plan 3 Task 2, design D-served-
contract).

View-storage decision (Step 1, decided by evidence -- brief: "Determine the
view-storage path... RECORD the evidence and chosen path")
--------------------------------------------------------------------------
CHOSEN: Iceberg REST-catalog views, stored in a dedicated schema
`lake.contract`, applied by this idempotent CLI subcommand. NO fallback
(nightly-CronWorkflow-step re-apply) is needed.

Evidence (verified live against this kind cluster, Lakekeeper 0.12.2 +
Trino 476, 2026-07-12, via a throwaway in-cluster probe pod using this same
image + the REST protocol this module implements):

  1. `CREATE SCHEMA IF NOT EXISTS lake.contract` succeeded.
  2. `CREATE OR REPLACE VIEW lake.contract.probe AS SELECT 1 AS x` succeeded.
  3. `SELECT * FROM lake.contract.probe` (same pod, same session) returned
     `[[1]]`.
  4. Persistence proof (the actual bar the brief sets: "create + select-back
     AFTER Trino restart"): `kubectl -n trino rollout restart
     deploy/trino-coordinator` (full pod replacement, fresh JVM, no
     in-memory session cache), THEN a BRAND NEW probe pod ran
     `SELECT * FROM lake.contract.probe` and again got `[[1]]`; `SHOW
     SCHEMAS FROM lake` listed `contract` alongside `alice`. This proves the
     view definition is held server-side by Lakekeeper's REST catalog (the
     only thing that survived the restart), not by anything cached in the
     old coordinator process.
  5. The probe schema/view were dropped afterward (`DROP VIEW IF EXISTS
     lake.contract.probe`) -- `apply_views()` below creates the real
     contract views fresh.

Conclusion: Lakekeeper 0.12.2 DOES implement the Iceberg REST view
endpoints Trino's Iceberg connector needs, contrary to the plan's stated
risk ("Lakekeeper 0.12.2 view support unknown"). The Global Constraints
fallback path (views applied as a nightly CronWorkflow step) is documented
here as NOT taken, and is not implemented.

Trino REST client (Step 3)
--------------------------------------------------------------------------
Minimal statement-polling client: `POST /v1/statement`, then follow
`nextUri` (GET) until the response has no `nextUri` left, collecting `data`
along the way; an `error` field at any point raises `TrinoQueryError`. Same
protocol shape already proven working by hack/kind-verify.sh's existing
Trino probes (https://trino.io/docs/current/develop/client-protocol.html).
No `trino-python-client` dependency -- `requests` is already pinned
(ingest/pyproject.toml), keeping the image's dependency surface unchanged.
This client is built once here and is the seam Task 3's `run_trino_
maintenance()` reuses (plan's Self-Review: "Trino client built once (T2)
reused (T3)").

DDL builder (Step 3)
--------------------------------------------------------------------------
`build_view_ddl()` renders one `CREATE OR REPLACE VIEW` from a
`{contract_name: dlt_name_or_None}` mapping (`alice_ingest.contract_columns`
-- see that module's docstring for how the mapping itself was derived and
verified). EVERY identifier is double-quoted, including already-lowercase
ones (job_info/trace's identity columns): contract names are mixed-case
(brief: "Mixed-case contract identifiers need double-quoting in DDL") and
quoting the lowercase ones too keeps the builder's logic uniform rather than
branching on "does this one actually need it". A `None` mapping value
renders as `CAST(NULL AS <null_type>) AS "<ContractName>"` with an inline
`--` comment (no live dlt counterpart); trailing `passthrough` columns are
appended, unaliased, under their own (already-valid) dlt names.

Drift detection (review fix -- "the silent-NULL trap")
--------------------------------------------------------------------------
`contract_columns.py` is a STATIC mapping, generated once against the kind
fixture's live schema (2026-07-12): only 12 of `mon_jdls_parsed`'s 66 JDL
contract fields have a live `jdl__*` counterpart today, so 54 render as
typed NULLs. At Plan-4 cutover, dlt's `schema_contract={"columns": "evolve",
...}` (pipeline.py) will let production JDLs grow NEW `jdl__*` columns that
this static mapping has never seen. Without detection, `lake.contract.
mon_jdls_parsed` would keep silently serving a typed NULL for a field whose
real, live data has started arriving on `lake.alice.mon_jdls_parsed` right
next to it -- wrong, and silent.

`check_table_drift()`/`check_drift()` run `SHOW COLUMNS FROM lake.alice.
<table>` (the SOURCE table apply_views() selects FROM, not the contract
VIEW -- the view's own column set is fixed by construction and would never
show drift against itself) and diff the live column names against
everything this module already accounts for: every mapped `dlt_name` in
`TABLE_SPECS[table].columns`, that table's `passthrough` tuple,
`DLT_METADATA_COLUMNS` (`_dlt_load_id`/`_dlt_id` -- checked independently of
any one table's passthrough tuple, so tables with an empty passthrough
today, like job_info/trace, still get a correct baseline), and
`KNOWN_NON_CONTRACT_HELPERS` (`jdl_parse_ok`/`full_jdl_raw`/`full_jdl` --
`parse_jdl`/pipeline bookkeeping columns, same reasoning). Anything left
over is classified by name: a live `jdl__*` column is ACTIONABLE drift
(contract_columns.py needs regenerating -- real base data exists with no
contract mapping to surface it); anything else is informational only, e.g.
some future dlt-internal helper column that was never going to be a
contract field regardless (brief: "never strict-fails").

`run()` calls `check_drift()` once, after `apply_views()` has applied all
three views, and prints one line per table with actionable drift:
`WARNING: unmapped jdl__ columns present live in lake.alice.<table>
(contract regeneration needed): [name:type, ...]` to stderr -- INCLUDING
each column's live TYPE (so a future non-varchar arrival, e.g. `bigint`
where every column mapped today is `varchar`, is visible without a second
query). Informational-only columns print as `INFO: ...` to stderr and never
affect the exit code. On kind today this prints nothing: the fixture's 12
live `jdl__*` columns are all mapped, so `check_drift()` finds zero
actionable drift on any of the three tables -- the warning path only fires
on real drift (verified live, this task).

`--strict` (CLI: `alice-ingest apply-views --strict`, wired through
pipeline.py's argparse, `run(env, strict=True)` here): identical detection,
but any actionable jdl__ drift makes `run()` return exit code 2 instead of
0. Plan 4's cutover will run `apply-views --strict` as its gate -- a fresh
production JDL vocabulary silently NULL-ing a real field must block the
cutover, not just log a warning nobody reads. Non-strict `apply-views` (the
default, and everything before Plan 4) never fails the process over drift;
it only reports it.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import Any, Mapping

import requests

from alice_ingest.contract_columns import (
    JOB_INFO_COLUMNS,
    JOB_INFO_PASSTHROUGH,
    MON_JDLS_PARSED_COLUMNS,
    MON_JDLS_PARSED_NULL_TYPE,
    MON_JDLS_PARSED_PASSTHROUGH,
    TRACE_COLUMNS,
    TRACE_PASSTHROUGH,
)

CATALOG = "lake"
SOURCE_SCHEMA = "alice"
CONTRACT_SCHEMA = "contract"
DEFAULT_NULL_TYPE = "VARCHAR"


# --------------------------------------------------------------------------
# Minimal Trino REST statement-polling client.
# --------------------------------------------------------------------------


class TrinoQueryError(RuntimeError):
    """Raised when a Trino statement's JSON response carries an `error`
    field, at any point in the POST/GET(nextUri) chain."""


@dataclass(frozen=True)
class TrinoClient:
    """`base_uri` e.g. `http://trino.trino.svc:8080` (no trailing
    `/v1/statement` -- appended here), matching TRINO_URI's env-var
    contract (module docstring / pipeline.py's env-var table)."""

    base_uri: str
    user: str = "alice-ingest"
    timeout: float = 30.0

    def run(self, sql: str, poll_interval: float = 1.0) -> list[list[Any]]:
        headers = {"X-Trino-User": self.user, "Content-Type": "text/plain"}
        resp = requests.post(
            f"{self.base_uri}/v1/statement", data=sql, headers=headers, timeout=self.timeout
        )
        resp.raise_for_status()
        result = resp.json()
        rows: list[list[Any]] = []
        while True:
            if "error" in result:
                raise TrinoQueryError(str(result["error"]))
            rows.extend(result.get("data") or [])
            next_uri = result.get("nextUri")
            if not next_uri:
                return rows
            time.sleep(poll_interval)
            resp = requests.get(next_uri, timeout=self.timeout)
            resp.raise_for_status()
            result = resp.json()


# --------------------------------------------------------------------------
# DDL builder.
# --------------------------------------------------------------------------


def build_view_ddl(
    table: str,
    columns: Mapping[str, str | None],
    *,
    null_type: str = DEFAULT_NULL_TYPE,
    passthrough: tuple[str, ...] = (),
) -> str:
    """`CREATE OR REPLACE VIEW lake.contract.<table>` selecting from
    `lake.alice.<table>`, per this module's docstring. Raises `ValueError`
    if there is nothing to select at all (an empty mapping AND no
    passthrough columns -- a real view can't have zero columns).

    Comma placement is load-bearing here, not cosmetic: a trailing SQL `--`
    comment runs to end-of-line, so a comma placed AFTER the comment (e.g.
    joining lines with `",\\n"` when the expression itself already ends in
    a comment) is silently swallowed INTO the comment and never reaches the
    parser -- verified live against this cluster's Trino 476 (SYNTAX_ERROR:
    "mismatched input 'CAST'", the next line's leading token, because the
    previous line's separating comma had vanished). Every expression here is
    therefore built comma-first, comment-last, and lines are joined with a
    plain newline (no comma injected by the join itself).
    """
    exprs: list[str] = []
    for contract_name, dlt_name in columns.items():
        if dlt_name is None:
            exprs.append(
                (f'CAST(NULL AS {null_type}) AS "{contract_name}"', "no dlt counterpart in the live schema")
            )
        else:
            exprs.append((f'"{dlt_name}" AS "{contract_name}"', None))
    for dlt_name in passthrough:
        exprs.append((f'"{dlt_name}"', None))

    if not exprs:
        raise ValueError(f"{table}: no columns to select (empty mapping and no passthrough)")

    select_lines = []
    last = len(exprs) - 1
    for i, (expr, comment) in enumerate(exprs):
        line = f"    {expr}{',' if i != last else ''}"
        if comment:
            line += f" -- {comment}"
        select_lines.append(line)

    select_clause = "\n".join(select_lines)
    return (
        f"CREATE OR REPLACE VIEW {CATALOG}.{CONTRACT_SCHEMA}.{table} AS\n"
        f"SELECT\n{select_clause}\n"
        f"FROM {CATALOG}.{SOURCE_SCHEMA}.{table}"
    )


@dataclass(frozen=True)
class _TableSpec:
    columns: Mapping[str, str | None]
    passthrough: tuple[str, ...] = ()
    null_type: str = DEFAULT_NULL_TYPE


# Declaration order drives apply_views()'s CREATE VIEW order (and therefore
# run()'s per-table print order) -- job_info, trace, mon_jdls_parsed, matching
# the brief's Interfaces line and pipeline.py's run_nightly() table order.
TABLE_SPECS: dict[str, _TableSpec] = {
    "job_info": _TableSpec(columns=JOB_INFO_COLUMNS, passthrough=JOB_INFO_PASSTHROUGH),
    "trace": _TableSpec(columns=TRACE_COLUMNS, passthrough=TRACE_PASSTHROUGH),
    "mon_jdls_parsed": _TableSpec(
        columns=MON_JDLS_PARSED_COLUMNS,
        passthrough=MON_JDLS_PARSED_PASSTHROUGH,
        null_type=MON_JDLS_PARSED_NULL_TYPE,
    ),
}


def apply_views(client: TrinoClient) -> list[str]:
    """Idempotently (CREATE OR REPLACE) apply every contract view, after
    ensuring the `lake.contract` schema exists. Returns the list of
    CREATE-VIEW DDL statements executed, in `TABLE_SPECS` order (`client`
    is any object exposing `.run(sql)` -- a `TrinoClient` in production, a
    fake in tests, matching this repo's established Protocol-fake seam
    pattern, e.g. retention.py's `IcebergPresenceCatalog`)."""
    client.run(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CONTRACT_SCHEMA}")
    executed: list[str] = []
    for table, spec in TABLE_SPECS.items():
        ddl = build_view_ddl(
            table, spec.columns, null_type=spec.null_type, passthrough=spec.passthrough
        )
        client.run(ddl)
        executed.append(ddl)
    return executed


# --------------------------------------------------------------------------
# Drift detection (module docstring, "Drift detection -- the silent-NULL
# trap"). Diffs `SHOW COLUMNS FROM lake.alice.<table>` against everything
# TABLE_SPECS already accounts for; a live `jdl__*` column left over is
# actionable drift, anything else is informational only.
# --------------------------------------------------------------------------

# dlt's own per-load metadata columns -- checked independently of any one
# table's `passthrough` tuple so job_info/trace (empty passthrough today)
# still get a correct baseline if either ever grows these.
DLT_METADATA_COLUMNS: tuple[str, ...] = ("_dlt_load_id", "_dlt_id")

# Non-contract pipeline/parser bookkeeping columns that legitimately live on
# `lake.alice.*` tables without ever belonging in the ML consumer's dtypes
# contract (contract_columns.py's MON_JDLS_PARSED_PASSTHROUGH comment).
# Listed again here, independent of any one table's passthrough tuple, for
# the same reason as DLT_METADATA_COLUMNS above.
KNOWN_NON_CONTRACT_HELPERS: tuple[str, ...] = ("jdl_parse_ok", "full_jdl_raw", "full_jdl")


@dataclass(frozen=True)
class DriftResult:
    """One table's `SHOW COLUMNS FROM lake.alice.<table>` diffed against
    everything this module currently accounts for. `unmapped_jdl_columns` is
    the actionable one (contract_columns.py needs regenerating);
    `other_unmapped_columns` is informational only -- never causes
    `--strict` to fail (module docstring)."""

    table: str
    unmapped_jdl_columns: tuple[tuple[str, str], ...] = ()
    other_unmapped_columns: tuple[tuple[str, str], ...] = ()

    @property
    def has_jdl_drift(self) -> bool:
        return bool(self.unmapped_jdl_columns)


def _known_live_columns(spec: _TableSpec) -> set[str]:
    mapped = {dlt_name for dlt_name in spec.columns.values() if dlt_name is not None}
    return mapped | set(spec.passthrough) | set(DLT_METADATA_COLUMNS) | set(KNOWN_NON_CONTRACT_HELPERS)


def check_table_drift(client: Any, table: str, spec: _TableSpec) -> DriftResult:
    """Runs `SHOW COLUMNS FROM lake.alice.<table>` (the SOURCE table, not
    the contract view -- module docstring) and classifies every live column
    not already accounted for by `spec`: a `jdl__*` name is actionable
    drift, anything else is informational only."""
    rows = client.run(f"SHOW COLUMNS FROM {CATALOG}.{SOURCE_SCHEMA}.{table}")
    known = _known_live_columns(spec)
    unmapped_jdl: list[tuple[str, str]] = []
    other_unmapped: list[tuple[str, str]] = []
    for row in rows:
        name, col_type = row[0], row[1]
        if name in known:
            continue
        (unmapped_jdl if name.startswith("jdl__") else other_unmapped).append((name, col_type))
    return DriftResult(
        table=table,
        unmapped_jdl_columns=tuple(unmapped_jdl),
        other_unmapped_columns=tuple(other_unmapped),
    )


def check_drift(client: Any) -> list[DriftResult]:
    """One `DriftResult` per `TABLE_SPECS` entry, in declaration order."""
    return [check_table_drift(client, table, spec) for table, spec in TABLE_SPECS.items()]


def _report_drift(results: list[DriftResult]) -> bool:
    """Prints a `WARNING`/`INFO` line per table with unmapped live columns
    (module docstring's exact message shape) to stderr. Returns True iff any
    table has actionable jdl__ drift -- the only condition `--strict` acts
    on."""
    any_jdl_drift = False
    for result in results:
        if result.unmapped_jdl_columns:
            any_jdl_drift = True
            cols = ", ".join(f"{name}:{col_type}" for name, col_type in result.unmapped_jdl_columns)
            print(
                f"WARNING: unmapped jdl__ columns present live in "
                f"{CATALOG}.{SOURCE_SCHEMA}.{result.table} (contract regeneration "
                f"needed): [{cols}]",
                file=sys.stderr,
            )
        if result.other_unmapped_columns:
            cols = ", ".join(f"{name}:{col_type}" for name, col_type in result.other_unmapped_columns)
            print(
                f"INFO: unmapped non-jdl__ column(s) present live in "
                f"{CATALOG}.{SOURCE_SCHEMA}.{result.table} (not contract drift, "
                f"no action needed): [{cols}]",
                file=sys.stderr,
            )
    return any_jdl_drift


def run(env: Mapping[str, str], strict: bool = False) -> int:
    """Entry point wired from pipeline.py's `apply-views` CLI command
    (env-var contract: TRINO_URI, e.g. `http://trino.trino.svc:8080`,
    added to `ingest-env` by hack/kind-up.sh).

    `strict=True` (CLI: `apply-views --strict`) turns actionable jdl__
    drift (module docstring, "Drift detection") into exit code 2 instead of
    a warning -- the Plan 4 cutover gate."""
    trino_uri = env.get("TRINO_URI")
    if not trino_uri:
        raise SystemExit("alice-ingest: required env var TRINO_URI is not set")

    client = TrinoClient(base_uri=trino_uri.rstrip("/"))
    executed = apply_views(client)
    for table in TABLE_SPECS:
        print(f"CONTRACT VIEW applied: {CATALOG}.{CONTRACT_SCHEMA}.{table}")
    print(f"apply-views: {len(executed)} view(s) applied")

    any_jdl_drift = _report_drift(check_drift(client))
    if strict and any_jdl_drift:
        return 2
    return 0
