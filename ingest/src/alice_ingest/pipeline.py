"""alice-ingest CLI: nightly landing-PostgreSQL -> Iceberg (Lakekeeper REST) pipeline.

Config is assembled entirely from environment variables at process start --
no toml files ship in the image (brief requirement, Plan 2 Task 2 Step 3).
Every dlt/PyIceberg call below is written to match the verified forms in
`research/2026-07-12_dlt-iceberg-lakekeeper-api-verification.md` (Option A
architecture: dlt `destination="filesystem"` + `table_format="iceberg"` +
`[iceberg_catalog]` REST config pointed at Lakekeeper).

Env-var contract (brief, Task 2 Interfaces):
    PG_URL              landing PostgreSQL connection string (mon-data)
    S3_ENDPOINT         S3-compatible endpoint (MinIO on kind; Cyfronet S3 in prod)
    S3_ACCESS_KEY       S3 access key id
    S3_SECRET_KEY       S3 secret access key
    S3_BUCKET           bucket backing the Lakekeeper warehouse (e.g. "warehouse")
    LAKEKEEPER_URI      Lakekeeper base URI, e.g. http://lakekeeper.lakekeeper.svc:8181
                         (NOT including /catalog -- appended below, per the
                         research doc: "Iceberg REST at <base>/catalog")
    LAKEKEEPER_WAREHOUSE  Lakekeeper warehouse name (Task 1 bootstraps "default")
    RETENTION_DAYS      landing-row retention window in days (default "14";
                         consumed by retention.py, Plan 2 Task 4 -- read here
                         only for the env-var contract/CLI wiring)
    S3_REGION           optional, default "local-01" (not part of the brief's
                         listed contract; MinIO/kind ignores the value, any
                         non-empty string satisfies the S3 SDK -- documented
                         here rather than silently hardcoded)

    Backfill overrides (Plan 2 Task 6; all optional, absent = unchanged
    default behavior; mechanism + worked examples in docs/runbooks/
    backfill.md, this is the env-var contract only):
    INGEST_INITIAL_JOB_INFO  overrides job_info's `last_update` incremental
                         cursor start (default pendulum.naive(2026, 1, 1)).
                         ISO date (`2026-03-01`) or date-time
                         (`2026-03-01T00:00:00`) -- NAIVE only, no UTC
                         offset (SystemExit if one is given; see the
                         aware/naive warning above -- an offset here would
                         reproduce the exact bug this module already had to
                         fix once).
    INGEST_INITIAL_TRACE  overrides trace's `laststatuschangetimestamp`
                         incremental cursor start (default 0). Plain integer,
                         same epoch-MILLISECONDS unit as the column itself
                         (retention.py's module docstring) -- pipeline.py
                         does no unit conversion, the value is handed to
                         dlt's incremental cursor as-is.
    INGEST_INITIAL_MON_JDLS  overrides mon_jdls's `job_id` incremental
                         cursor start (default 0). Plain integer.

Watermark note (per-table, corrected against production ground truth --
research/2026-07-12_ml-consumer-data-contract.md, "Production schema ground
truth", verified live against information_schema on mon_data 2026-07-12):

  - `job_info`: HAS `last_update`, but production's column is
    `timestamp WITHOUT time zone` (naive), not `timestamptz`. The incremental
    initial value must therefore be a NAIVE `pendulum.naive(...)` literal --
    a tz-aware literal pushed down against a naive column forces a
    session-timezone cast in the SQL comparison, silently shifting the
    cursor (the mirror-image bug of the one fixed in a610069, which was for
    a timestamptz column).
  - `trace`: has NO `last_update` column in production (an earlier fixture
    incorrectly added one; corrected in Task 3's review). Its incremental
    cursor is `laststatuschangetimestamp` (bigint epoch), initial_value=0.
  - `mon_jdls`: no `last_update` column either. `job_id` is monotonically
    increasing in the source system, so incremental extraction on `job_id`
    (instead of a timestamp cursor) is the correct and only viable watermark
    for this table. Recorded per the brief's explicit instruction to
    document this choice.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from typing import Mapping

import dlt
import pendulum
from dlt.sources.sql_database import sql_database, sql_table

from alice_ingest.jdl import parse_jdl

DATASET_NAME = "alice"  # Iceberg namespace served to the ML consumer (design D-served-contract)
DEFAULT_RETENTION_DAYS = "14"  # owner decision 2026-07-12; consumed by retention.py (Task 4)
DEFAULT_S3_REGION = "local-01"

_REQUIRED_ENV_VARS = (
    "PG_URL",
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "S3_BUCKET",
    "LAKEKEEPER_URI",
    "LAKEKEEPER_WAREHOUSE",
)

# Backfill initial_value overrides (Plan 2 Task 6, docs/runbooks/
# backfill.md) -- optional, never required/validated by _check_required_env;
# absence means "use the table's normal default" (see build_job_info_source/
# build_trace_source/build_mon_jdls_resource below).
ENV_INITIAL_JOB_INFO = "INGEST_INITIAL_JOB_INFO"
ENV_INITIAL_TRACE = "INGEST_INITIAL_TRACE"
ENV_INITIAL_MON_JDLS = "INGEST_INITIAL_MON_JDLS"


def _require_env(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if not value:
        raise SystemExit(f"alice-ingest: required env var {name} is not set")
    return value


def _check_required_env(env: Mapping[str, str]) -> None:
    missing = [name for name in _REQUIRED_ENV_VARS if not env.get(name)]
    if missing:
        raise SystemExit(
            "alice-ingest: missing required env var(s): " + ", ".join(missing)
        )


def _resolve_naive_initial_value(
    env: Mapping[str, str], var_name: str, default: "pendulum.DateTime"
) -> "pendulum.DateTime":
    """Backfill env-var hook (docs/runbooks/backfill.md): override a
    naive-cursor table's dlt incremental `initial_value` from an ISO
    date/date-time string, no code edit required. Falls back to `default`
    when `var_name` is unset or empty.

    Parses with stdlib `datetime.fromisoformat`, NOT `pendulum.parse` --
    pendulum.parse's default `tz='UTC'` would silently attach a timezone to
    a value meant for a `timestamp WITHOUT time zone` column (see this
    module's docstring on why an aware literal against a naive column is a
    real bug, not a style choice: it forces a session-timezone cast in SQL
    pushdown and silently shifts the cursor -- the exact class of bug fixed
    once already in a610069). An explicit UTC offset in the input is
    therefore refused (SystemExit) rather than silently dropped or honored.
    """
    raw = env.get(var_name)
    if not raw:
        return default
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise SystemExit(
            f"alice-ingest: invalid {var_name}={raw!r}: expected an ISO "
            f"date (2026-03-01) or date-time (2026-03-01T00:00:00): {exc}"
        ) from exc
    if parsed.tzinfo is not None:
        raise SystemExit(
            f"alice-ingest: invalid {var_name}={raw!r}: must be naive (no "
            f"UTC offset) -- this cursor's column is `timestamp WITHOUT "
            f"time zone`; an aware literal would silently shift it "
            f"(see pipeline.py's module docstring)"
        )
    return pendulum.naive(
        parsed.year,
        parsed.month,
        parsed.day,
        parsed.hour,
        parsed.minute,
        parsed.second,
        parsed.microsecond,
    )


def _resolve_int_initial_value(env: Mapping[str, str], var_name: str, default: int) -> int:
    """Backfill env-var hook (docs/runbooks/backfill.md): override an
    integer-cursor table's (trace's epoch-ms `laststatuschangetimestamp`,
    mon_jdls's `job_id`) dlt incremental `initial_value` from a plain
    integer string. Falls back to `default` when `var_name` is unset or
    empty. No unit conversion is performed -- the parsed integer is handed
    straight to dlt's incremental cursor, same as the hardcoded defaults it
    replaces.
    """
    raw = env.get(var_name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SystemExit(
            f"alice-ingest: invalid {var_name}={raw!r}: must be an integer"
        ) from exc


def iceberg_catalog_properties(env: Mapping[str, str]) -> dict[str, str]:
    """Flat dotted-key pyiceberg REST catalog properties -- the same shape
    `pyiceberg.catalog.load_catalog(warehouse, **properties)` expects
    (verified live against this cluster, Plan 2 Task 1's kind-verify.sh
    probe and Task 4's retention.py). Shared by `configure_dlt()` (which
    wraps this dict for dlt's filesystem destination) and
    `retention.py`'s `load_iceberg_catalog()` (a direct pyiceberg REST
    client for the presence-verification scan) so the two call sites can
    never drift out of sync.

    Per-key dotted assignment into dlt's config tree (e.g.
    `dlt.secrets["...config.s3.endpoint"] = ...`) would nest: dlt's
    accessor turns each dotted path into nested dict levels, so
    "s3.endpoint" would land as {"s3": {"endpoint": ...}}. pyiceberg reads
    catalog properties as a flat dict of dotted STRING keys (see
    get_header_properties() etc.), so the nested form is silently ignored
    at runtime -- this function's dict keys are the flat dotted strings
    pyiceberg actually needs (correction recorded in
    research/2026-07-12_dlt-iceberg-lakekeeper-api-verification.md).
    """
    s3_endpoint = _require_env(env, "S3_ENDPOINT")
    s3_access_key = _require_env(env, "S3_ACCESS_KEY")
    s3_secret_key = _require_env(env, "S3_SECRET_KEY")
    s3_region = env.get("S3_REGION", DEFAULT_S3_REGION)
    warehouse = _require_env(env, "LAKEKEEPER_WAREHOUSE")
    catalog_uri = _require_env(env, "LAKEKEEPER_URI").rstrip("/") + "/catalog"
    return {
        "uri": catalog_uri,
        "type": "rest",
        "warehouse": warehouse,
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint": s3_endpoint,
        "s3.access-key-id": s3_access_key,
        "s3.secret-access-key": s3_secret_key,
        "s3.region": s3_region,
    }


def configure_dlt(env: Mapping[str, str]) -> None:
    """Assemble the filesystem destination + Iceberg REST catalog config.

    Programmatic equivalent of the research doc's verified TOML block, set
    via `dlt.secrets[...]` (brief's literal example style -- the explicit
    in-memory config provider dlt consults at these dotted paths does not
    distinguish config/secrets at lookup time; the split matters for
    toml-file hygiene only, which is moot since no toml ships in this image).
    """
    bucket = _require_env(env, "S3_BUCKET")
    s3_endpoint = _require_env(env, "S3_ENDPOINT")
    s3_access_key = _require_env(env, "S3_ACCESS_KEY")
    s3_secret_key = _require_env(env, "S3_SECRET_KEY")
    s3_region = env.get("S3_REGION", DEFAULT_S3_REGION)
    warehouse = _require_env(env, "LAKEKEEPER_WAREHOUSE")

    # [destination.filesystem]
    dlt.secrets["destination.filesystem.bucket_url"] = f"s3://{bucket}"
    # [destination.filesystem.credentials]
    dlt.secrets["destination.filesystem.credentials.aws_access_key_id"] = s3_access_key
    dlt.secrets["destination.filesystem.credentials.aws_secret_access_key"] = s3_secret_key
    dlt.secrets["destination.filesystem.credentials.endpoint_url"] = s3_endpoint
    dlt.secrets["destination.filesystem.credentials.region_name"] = s3_region

    # [iceberg_catalog] -- iceberg_catalog_name mirrors the research doc's
    # verified example, where it equals the warehouse name ("default" on kind).
    dlt.secrets["iceberg_catalog.iceberg_catalog_name"] = warehouse
    dlt.secrets["iceberg_catalog.iceberg_catalog_type"] = "rest"

    # [iceberg_catalog.iceberg_catalog_config] -- set as ONE dict literal
    # (see iceberg_catalog_properties()'s docstring for why: dlt's dotted
    # per-key accessor would nest what pyiceberg needs flat).
    dlt.secrets["iceberg_catalog.iceberg_catalog_config"] = iceberg_catalog_properties(env)

    # Gotcha carried from the research doc: keep purge-on-drop disabled --
    # Lakekeeper hard-deletes can purge recreated tables' files.
    dlt.config["destination.filesystem.iceberg_use_catalog_purge"] = False


def build_job_info_source(pg_url: str, env: Mapping[str, str] | None = None):
    """job_info: connectorx backend (fast, columnar; no per-row transform needed).

    Incremental cursor is `last_update`. Production's column is `timestamp
    WITHOUT time zone` (naive) -- research/2026-07-12_ml-consumer-data-
    contract.md, "Production schema ground truth" (verified live against
    information_schema on mon_data). `pendulum.naive(...)` (naive factory),
    NOT the tz-aware `pendulum.datetime(...)`: an aware literal pushed down
    against a naive `timestamp` column forces a session-timezone cast in the
    SQL comparison, which silently shifts the incremental cursor.

    `initial_value` defaults to `pendulum.naive(2026, 1, 1)` but is
    overridable via `INGEST_INITIAL_JOB_INFO` (docs/runbooks/backfill.md,
    Plan 2 Task 6) -- `_resolve_naive_initial_value` above.
    """
    env = env if env is not None else os.environ
    initial_value = _resolve_naive_initial_value(
        env, ENV_INITIAL_JOB_INFO, pendulum.naive(2026, 1, 1)
    )
    source = sql_database(
        credentials=pg_url, backend="connectorx", chunk_size=100_000
    ).with_resources("job_info")
    source.job_info.apply_hints(
        incremental=dlt.sources.incremental("last_update", initial_value=initial_value),
        primary_key="job_id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        schema_contract={"columns": "evolve", "data_type": "freeze"},
        table_format="iceberg",
    )
    return source


def build_trace_source(pg_url: str, env: Mapping[str, str] | None = None):
    """trace: connectorx backend. Production has NO `last_update` column on
    `trace` (research/2026-07-12_ml-consumer-data-contract.md, "Production
    schema ground truth" -- all 18 production columns enumerated there, none
    named `last_update`; an earlier fixture incorrectly added one, corrected
    in Task 3's review). Incremental cursor is `laststatuschangetimestamp`
    (bigint epoch, not a timestamp type), initial_value=0.

    `initial_value` is overridable via `INGEST_INITIAL_TRACE` (docs/
    runbooks/backfill.md, Plan 2 Task 6) -- `_resolve_int_initial_value`
    above.
    """
    env = env if env is not None else os.environ
    initial_value = _resolve_int_initial_value(env, ENV_INITIAL_TRACE, 0)
    source = sql_database(
        credentials=pg_url, backend="connectorx", chunk_size=100_000
    ).with_resources("trace")
    source.trace.apply_hints(
        incremental=dlt.sources.incremental(
            "laststatuschangetimestamp", initial_value=initial_value
        ),
        primary_key="job_id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        schema_contract={"columns": "evolve", "data_type": "freeze"},
        table_format="iceberg",
    )
    return source


def build_mon_jdls_resource(pg_url: str, env: Mapping[str, str] | None = None):
    """mon_jdls -> mon_jdls_parsed: MUST use the sqlalchemy backend.

    add_map(parse_jdl) requires row dicts; connectorx/pyarrow backends yield
    pyarrow.Table items and silently skip per-row maps (verified 2026-07-12,
    research doc). Watermark is `job_id` (see module docstring): mon_jdls has
    no last_update column in the gen-1 schema.

    `initial_value` is overridable via `INGEST_INITIAL_MON_JDLS` (docs/
    runbooks/backfill.md, Plan 2 Task 6) -- `_resolve_int_initial_value`
    above.

    `max_table_nesting = 1` (final-review N3, controller decision): without
    it, dlt spins JDL list fields (e.g. `Packages`) off into a CHILD TABLE
    (`mon_jdls_parsed__jdl__packages`, one row per list item) instead of
    landing them as a VALUE column on `mon_jdls_parsed` -- the ML
    consumer's data contract expects Packages as a column. Verified
    empirically against the pinned dlt 1.28.2 (ingest/tests/test_pipeline.py,
    TestMaxTableNestingSuppressesChildTables): this is a settable PROPERTY
    on `DltResource` (`dlt/extract/resource.py`), NOT an `apply_hints()`
    kwarg -- `apply_hints()` takes no `max_table_nesting` parameter at all
    in 1.28.2. `max_table_nesting=1` is the specific value that keeps
    ordinary dict flattening (`jdl__ttl`, `jdl__lpm_pass_name`, etc.)
    working while demoting list fields to JSON-typed columns instead of
    child tables; `max_table_nesting=0` would suppress the dict flattening
    too (collapses the whole `jdl` field into one JSON column), which is
    NOT what's wanted here.
    """
    env = env if env is not None else os.environ
    initial_value = _resolve_int_initial_value(env, ENV_INITIAL_MON_JDLS, 0)
    resource = sql_table(
        credentials=pg_url, table="mon_jdls", backend="sqlalchemy", chunk_size=20_000
    )
    resource.add_map(parse_jdl)
    resource.apply_hints(
        table_name="mon_jdls_parsed",
        incremental=dlt.sources.incremental("job_id", initial_value=initial_value),
        primary_key="job_id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        schema_contract={"columns": "evolve", "data_type": "freeze"},
        table_format="iceberg",
    )
    resource.max_table_nesting = 1
    return resource


def run_nightly(env: Mapping[str, str] | None = None) -> int:
    env = env if env is not None else os.environ
    _check_required_env(env)
    configure_dlt(env)
    pg_url = env["PG_URL"]

    pipeline = dlt.pipeline(
        pipeline_name="alice_ingest_nightly",
        destination="filesystem",
        dataset_name=DATASET_NAME,
    )

    job_info_source = build_job_info_source(pg_url, env)
    load_info = pipeline.run(job_info_source)
    print(load_info)

    trace_source = build_trace_source(pg_url, env)
    load_info = pipeline.run(trace_source)
    print(load_info)

    mon_jdls_resource = build_mon_jdls_resource(pg_url, env)
    load_info = pipeline.run(mon_jdls_resource)
    print(load_info)

    return 0


def run_sitesonar(env: Mapping[str, str] | None = None, limit: int | None = None) -> int:
    env = env if env is not None else os.environ
    try:
        from alice_ingest.sitesonar import run as _run
    except ModuleNotFoundError:
        print(
            "alice-ingest run-sitesonar: not implemented yet "
            "(lands in Plan 2 Task 4, ingest/src/alice_ingest/sitesonar.py)",
            file=sys.stderr,
        )
        return 1
    return _run(env, limit=limit)


def run_retention(env: Mapping[str, str] | None = None) -> int:
    env = env if env is not None else os.environ
    try:
        from alice_ingest.retention import run as _run
    except ModuleNotFoundError:
        print(
            "alice-ingest run-retention: not implemented yet "
            "(lands in Plan 2 Task 4, ingest/src/alice_ingest/retention.py); "
            f"RETENTION_DAYS={env.get('RETENTION_DAYS', DEFAULT_RETENTION_DAYS)}",
            file=sys.stderr,
        )
        return 1
    return _run(env)


def run_maintenance(env: Mapping[str, str] | None = None) -> int:
    env = env if env is not None else os.environ
    try:
        from alice_ingest.maintenance import run as _run
    except ModuleNotFoundError:
        print(
            "alice-ingest run-maintenance: not implemented yet "
            "(lands in Plan 2 Task 5, ingest/src/alice_ingest/maintenance.py)",
            file=sys.stderr,
        )
        return 1
    return _run(env)


_COMMANDS = {
    "run-nightly": run_nightly,
    "run-sitesonar": run_sitesonar,
    "run-retention": run_retention,
    "run-maintenance": run_maintenance,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="alice-ingest")
    parser.add_argument("command", choices=sorted(_COMMANDS))
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "run-sitesonar only: cap fetching to the N most-recent "
            "site-sonar .out.xz files (Plan 2 Task 4's bounded e2e probe: "
            "`alice-ingest run-sitesonar --limit 1`). Ignored by other commands."
        ),
    )
    args = parser.parse_args(argv)
    if args.command == "run-sitesonar":
        return run_sitesonar(limit=args.limit)
    return _COMMANDS[args.command]()


if __name__ == "__main__":
    sys.exit(main())
