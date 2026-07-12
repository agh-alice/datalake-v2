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


def build_job_info_source(pg_url: str):
    """job_info: connectorx backend (fast, columnar; no per-row transform needed).

    Incremental cursor is `last_update`. Production's column is `timestamp
    WITHOUT time zone` (naive) -- research/2026-07-12_ml-consumer-data-
    contract.md, "Production schema ground truth" (verified live against
    information_schema on mon_data). `pendulum.naive(...)` (naive factory),
    NOT the tz-aware `pendulum.datetime(...)`: an aware literal pushed down
    against a naive `timestamp` column forces a session-timezone cast in the
    SQL comparison, which silently shifts the incremental cursor.
    """
    source = sql_database(
        credentials=pg_url, backend="connectorx", chunk_size=100_000
    ).with_resources("job_info")
    source.job_info.apply_hints(
        incremental=dlt.sources.incremental(
            "last_update", initial_value=pendulum.naive(2026, 1, 1)
        ),
        primary_key="job_id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        schema_contract={"columns": "evolve", "data_type": "freeze"},
        table_format="iceberg",
    )
    return source


def build_trace_source(pg_url: str):
    """trace: connectorx backend. Production has NO `last_update` column on
    `trace` (research/2026-07-12_ml-consumer-data-contract.md, "Production
    schema ground truth" -- all 18 production columns enumerated there, none
    named `last_update`; an earlier fixture incorrectly added one, corrected
    in Task 3's review). Incremental cursor is `laststatuschangetimestamp`
    (bigint epoch, not a timestamp type), initial_value=0.
    """
    source = sql_database(
        credentials=pg_url, backend="connectorx", chunk_size=100_000
    ).with_resources("trace")
    source.trace.apply_hints(
        incremental=dlt.sources.incremental(
            "laststatuschangetimestamp", initial_value=0
        ),
        primary_key="job_id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        schema_contract={"columns": "evolve", "data_type": "freeze"},
        table_format="iceberg",
    )
    return source


def build_mon_jdls_resource(pg_url: str):
    """mon_jdls -> mon_jdls_parsed: MUST use the sqlalchemy backend.

    add_map(parse_jdl) requires row dicts; connectorx/pyarrow backends yield
    pyarrow.Table items and silently skip per-row maps (verified 2026-07-12,
    research doc). Watermark is `job_id` (see module docstring): mon_jdls has
    no last_update column in the gen-1 schema.
    """
    resource = sql_table(
        credentials=pg_url, table="mon_jdls", backend="sqlalchemy", chunk_size=20_000
    )
    resource.add_map(parse_jdl)
    resource.apply_hints(
        table_name="mon_jdls_parsed",
        incremental=dlt.sources.incremental("job_id", initial_value=0),
        primary_key="job_id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        schema_contract={"columns": "evolve", "data_type": "freeze"},
        table_format="iceberg",
    )
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

    job_info_source = build_job_info_source(pg_url)
    load_info = pipeline.run(job_info_source)
    print(load_info)

    trace_source = build_trace_source(pg_url)
    load_info = pipeline.run(trace_source)
    print(load_info)

    mon_jdls_resource = build_mon_jdls_resource(pg_url)
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
