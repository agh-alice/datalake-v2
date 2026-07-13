#!/usr/bin/env python3
"""Bulk-extract ALICE datalake tables to Parquet for offline ML training.

Replaces `alice_data_downloader.py` (the ML consumer's current script --
see research/2026-07-12_ml-consumer-data-contract.md in the redesign repo):
no Apache Arrow Flight, no Dremio, no `job_id`-percentile shard/bisection
retry dance, no hardcoded admin credentials. This script attaches
Lakekeeper's Iceberg REST catalog directly with DuckDB and does one
`COPY (SELECT ...) TO ... (FORMAT parquet)` per table.

Standalone, stdlib + duckdb ONLY -- deliberately NOT part of the `alice-
ingest` package or its container image (Plan 3 Task 4 brief): consumers
`pip install duckdb` and run this file, nothing else. See
`docs/runbooks/ml-extraction.md` for the PhD-student quickstart, the
Athena/SLURM 1:1 mapping from the old `sbatch` pattern, and the interim
(kind/MinIO) vs. cyfronet-cutover story.

--------------------------------------------------------------------------
ATTACH incantation (verified live 2026-07-13, duckdb 1.5.4 / duckdb-iceberg
75726455 extension, against this repo's kind cluster: Lakekeeper 0.12.2,
Trino 476, MinIO -- both port-forwarded to localhost from OUTSIDE the
cluster, matching how a real consumer reaches cyfronet)
--------------------------------------------------------------------------

    INSTALL iceberg; LOAD iceberg;
    INSTALL httpfs; LOAD httpfs;

    CREATE SECRET lakekeeper_secret (
        TYPE iceberg,
        TOKEN '<token>'                   -- Bearer token; this kind cluster's
    );                                    -- Lakekeeper accepts any non-empty
                                           -- string ("Bearer-anything").

    -- Only when a manual S3 endpoint override is supplied (--s3-endpoint;
    -- see "S3 endpoint discovery" below) -- omitted entirely otherwise:
    CREATE SECRET minio_s3 (
        TYPE S3,
        KEY_ID '<access key>',
        SECRET '<secret key>',
        ENDPOINT '<host:port>',           -- e.g. 127.0.0.1:19000 for a
        URL_STYLE 'path',                 -- port-forwarded MinIO
        USE_SSL false,
        REGION '<region>'
    );

    ATTACH '<warehouse>' AS lake (
        TYPE iceberg,
        ENDPOINT '<catalog-uri>',         -- e.g. http://127.0.0.1:18181/catalog
        SECRET lakekeeper_secret,
        ACCESS_DELEGATION_MODE '<none|vended_credentials>'
        -- 'vended_credentials' (DuckDB's own default) when no --s3-endpoint
        -- override is given; 'none' when it is (see below).
    );

    SELECT * FROM lake.alice.job_info;    -- or lake.contract.job_info, see
                                           -- "contract vs alice" below.

S3 endpoint discovery (the recipe's cyfronet-relevant core)
--------------------------------------------------------------------------
Lakekeeper's per-table "vended credentials" (`GET .../tables/{table}/
credentials`, fetched automatically by duckdb-iceberg under the default
`ACCESS_DELEGATION_MODE 'vended_credentials'`) carry whatever `s3.endpoint`
the server's OWN storage profile is configured with. On this kind cluster
that is `http://minio.minio.svc:9000/` -- Kubernetes-internal DNS,
unreachable from a laptop/Athena node outside the cluster even when the
catalog itself is reachable via `kubectl port-forward`. Verified live: with
vended credentials, `SELECT * FROM lake.alice.job_info` failed with
`IOException: Could not resolve hostname error for HTTP GET to
'http://minio.minio.svc:9000/warehouse/...'`.

Fix (this script's `--s3-endpoint`/`--s3-access-key`/`--s3-secret-key`):
create your OWN `TYPE S3` secret pointed at a reachable endpoint (a
port-forwarded MinIO, or cyfronet's real object-storage endpoint if it
differs from what vended credentials would report) and set
`ACCESS_DELEGATION_MODE 'none'` on ATTACH, so DuckDB never asks the catalog
for storage credentials at all and instead uses the client-supplied S3
secret unconditionally. There is no ATTACH-level "s3 endpoint override"
option that coexists with vended credentials (checked: DuckDB's Iceberg
REST catalog ATTACH options -- ENDPOINT_TYPE, ENDPOINT, SECRET, CLIENT_ID,
CLIENT_SECRET, DEFAULT_REGION, OAUTH2_SERVER_URI, AUTHORIZATION_TYPE,
ACCESS_DELEGATION_MODE, EXTRA_HTTP_HEADERS, SUPPORT_NESTED_NAMESPACES,
STAGE_CREATE_TABLES, DISABLE_MULTI_TABLE_COMMIT,
SKIP_CREATE_TABLE_METADATA_UPDATES, REMOVE_FILES_ON_DELETE,
MAX_TABLE_STALENESS, PURGE_REQUESTED, DEFAULT_SCHEMA,
ENCODE_ENTIRE_PREFIX -- none of these override just the endpoint while
keeping delegation on; `ACCESS_DELEGATION_MODE 'none'` + a manual S3 secret
is the only working combination found). Verified live end to end this way:
`SELECT job_id, status, site FROM lake.alice.job_info LIMIT 3` returned
real rows, and `SELECT count(*)` returned 1000, matching the fixture.

At cyfronet cutover (Plan 4), if the storage endpoint vended credentials
report is externally routable (a real object-storage URL, not a
Kubernetes-internal one), `--s3-endpoint` should simply be omitted --
`ACCESS_DELEGATION_MODE 'vended_credentials'` (DuckDB's own default) is the
right and simpler path once nothing is hiding behind in-cluster DNS. This
kind-only workaround is expected to become unnecessary then, not to be the
long-term recipe.

contract vs. alice: DuckDB cannot read Iceberg REST-catalog VIEWS (verified)
--------------------------------------------------------------------------
The plan's design keeps `alice-ingest apply-views` restoring the ML
consumer's dtypes-contract column spellings (`LPMPassName`, `TTL`,
`Packages`, ...) as Iceberg REST-catalog VIEWS under `lake.contract.*`
(see `ingest/src/alice_ingest/views.py`). This script tries that schema
FIRST for every table, and live-verified (2026-07-13) that it always
currently fails: `lake.contract.job_info` (etc.) raises
`CatalogException: Table with name job_info does not exist!`, even though
`SHOW SCHEMAS FROM lake` / `duckdb_schemas()` lists `contract` right
alongside `alice`, and Trino reads the exact same views fine
(`hack/kind-verify.sh`'s Trino probes). The reason is NOT "the views are
missing" (the brief's original fallback trigger) -- Lakekeeper's REST
catalog genuinely holds them (Trino restart-persistence already proved
that, `views.py`'s module docstring) -- it is that duckdb-iceberg's REST
catalog client only discovers/reads TABLES, not VIEWS, as of this version
(no view-read support is documented in the duckdb-iceberg repository, and
none was observed here).

`resolve_source()` below therefore always falls back to `lake.alice.<table>`
today (dlt-normalized column names, e.g. `jdl__lpm_pass_name` instead of
`LPMPassName`) and prints a WARNING explaining exactly this. The fallback
is implemented generically (try, catch, fall back) rather than as a
special-cased "views are absent" check, so it will start serving the
contract-spelled columns automatically, with no script change, the day
duckdb-iceberg adds Iceberg view read support -- or a future revision of
this script could translate the view's stored SQL definition itself.
Until then, `docs/runbooks/ml-extraction.md` documents this column-naming
gap explicitly so consumers aren't surprised by `jdl__*` spellings.

fallback_reason disambiguation (P3T4 review Important 1)
--------------------------------------------------------------------------
DuckDB raises the exact same `CatalogException: Table with name <table>
does not exist!` whether the contract view genuinely exists server-side
(today's true case, above) OR the view was never created in the first
place (e.g. `alice-ingest apply-views` was never run after `kind-up` --
an ops mistake, not a client limitation). A `fallback_reason` that always
claims the former would misattribute the latter. `resolve_source()`
disambiguates by issuing a direct REST call against the Iceberg REST
catalog itself (`rest_catalog_prefix()` + `check_contract_view_exists()`,
stdlib `urllib` only -- no new dependency) whenever the contract read
fails and a usable `alice` fallback exists:

- `GET <catalog-uri>/v1/config?warehouse=<warehouse>` first, to resolve
  Lakekeeper's real REST-catalog prefix -- mirroring the discovery DuckDB's
  own Iceberg REST-catalog client already performs at ATTACH time. This is
  NOT the `--warehouse` name itself: verified live 2026-07-13 against this
  repo's kind cluster, `/v1/config?warehouse=default` returned
  `overrides.prefix` as a warehouse UUID (`2b6351e0-7e14-...`), distinct
  from the literal `"default"` ATTACH's own `warehouse` argument uses.
- `GET <catalog-uri>/v1/<prefix>/namespaces/contract/views`, then checks
  whether the failing table is in the returned `identifiers` list.
  Verified live 2026-07-13: for this cluster's `contract` namespace, the
  three views (`job_info`, `trace`, `mon_jdls_parsed`) ARE listed -- i.e.
  today's real cause genuinely is the duckdb-iceberg client limitation
  described above, not a missing `apply-views` run.

Three `fallback_reason` outcomes result: the view is listed (today's true
case -- a duckdb-iceberg client limitation), the view is absent or the
namespace itself 404s (an ops mistake -- `apply-views` likely was not run;
see `docs/runbooks/ingestion-pipeline.md`), or the REST check itself fails
(network/auth/malformed-response -- cause genuinely undetermined, said so
plainly rather than guessing). If the `alice` table is ALSO unreadable,
`resolve_source()` raises immediately (no usable source at all) without
attempting the REST check -- see the function's own docstring for why that
check runs before REST disambiguation.

Snapshot IDs (manifest provenance)
--------------------------------------------------------------------------
Via DuckDB's own `iceberg_snapshots(<table>)` table function -- no direct
REST catalog call and no Trino round-trip needed (verified available on
this duckdb-iceberg pin). Always queried against the physical
`lake.alice.<table>` source, never the `contract` view: a view has no
snapshot of its own, and views.py's own contract views SELECT FROM the
`alice` table anyway, so that table's current snapshot is the correct
provenance pointer regardless of which schema the actual row data came
from.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

DEFAULT_TABLES = ("job_info", "trace", "mon_jdls_parsed")
CATALOG_NAME = "lake"
CONTRACT_SCHEMA = "contract"
SOURCE_SCHEMA = "alice"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
REST_TIMEOUT_SECONDS = 10.0


# --------------------------------------------------------------------------
# Pure logic: arg parsing / validation, table-fallback decision shape, and
# manifest assembly. Unit-tested WITHOUT a cluster (tools/tests/).
# --------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="extract_training_data.py",
        description=(
            "Extract ALICE datalake tables to Parquet via DuckDB's Iceberg "
            "REST-catalog reader (replacement for alice_data_downloader.py's "
            "Dremio Arrow Flight path)."
        ),
    )
    parser.add_argument(
        "--tables",
        default=",".join(DEFAULT_TABLES),
        help=f"Comma-separated table names (default: {','.join(DEFAULT_TABLES)})",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write <table>.parquet files + manifest.json into (created if missing)",
    )
    parser.add_argument(
        "--catalog-uri",
        required=True,
        help=(
            "Lakekeeper REST catalog URL, e.g. http://127.0.0.1:18181/catalog "
            "(kind: kubectl -n lakekeeper port-forward svc/lakekeeper 18181:8181)"
        ),
    )
    parser.add_argument(
        "--token",
        required=True,
        help="Bearer token for the catalog. kind/dev Lakekeeper accepts any non-empty string.",
    )
    parser.add_argument(
        "--warehouse",
        default="default",
        help="Lakekeeper warehouse name (default: default)",
    )
    parser.add_argument(
        "--s3-endpoint",
        default=None,
        help=(
            "Override S3 endpoint (host:port, or scheme://host:port). Required on kind: "
            "vended credentials carry the in-cluster DNS name (e.g. minio.minio.svc:9000), "
            "unreachable from outside the cluster. Omit once the storage endpoint vended "
            "credentials report is externally routable (the cyfronet-cutover expectation) "
            "-- vended credentials (DuckDB's own default) are then correct on their own."
        ),
    )
    parser.add_argument("--s3-access-key", default=None, help="S3 access key. Required with --s3-endpoint.")
    parser.add_argument("--s3-secret-key", default=None, help="S3 secret key. Required with --s3-endpoint.")
    parser.add_argument(
        "--s3-region",
        default="us-east-1",
        help="S3 region for the --s3-endpoint override (default: us-east-1; kind/MinIO uses local-01)",
    )
    parser.add_argument(
        "--s3-url-style",
        default="path",
        choices=["path", "vhost"],
        help="S3 URL style for the --s3-endpoint override (default: path; MinIO requires path)",
    )
    return parser


def parse_tables(raw: str) -> list[str]:
    return [t.strip() for t in raw.split(",") if t.strip()]


def validate_args(args: argparse.Namespace) -> list[str]:
    """Pure validation returning a list of error strings (empty = valid).
    Kept separate from `build_arg_parser()`/argparse's own required-arg
    checks so table-fallback-adjacent cross-field rules (S3 override
    completeness) are unit-testable directly against a Namespace, without
    invoking argparse's SystemExit-on-error machinery."""
    errors: list[str] = []

    tables = parse_tables(args.tables)
    if not tables:
        errors.append("--tables must name at least one table")
    for t in tables:
        if not IDENTIFIER_RE.match(t):
            errors.append(f"--tables: {t!r} is not a valid SQL identifier")

    if args.s3_endpoint and not (args.s3_access_key and args.s3_secret_key):
        errors.append("--s3-access-key and --s3-secret-key are required when --s3-endpoint is given")
    if not args.s3_endpoint and (args.s3_access_key or args.s3_secret_key):
        errors.append(
            "--s3-access-key/--s3-secret-key have no effect without --s3-endpoint "
            "(vended credentials would be used instead); pass --s3-endpoint too, or drop these flags"
        )

    return errors


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    errors = validate_args(args)
    if errors:
        parser.error("; ".join(errors))
    return args


def split_s3_endpoint(raw: str) -> tuple[str, bool]:
    """Returns (host_port, use_ssl). DuckDB's S3 secret ENDPOINT wants a
    bare host:port, not a URL scheme -- this accepts either form so
    `--s3-endpoint http://127.0.0.1:19000` and `--s3-endpoint 127.0.0.1:19000`
    both work (the former inferring USE_SSL false from the http:// scheme,
    the latter defaulting to true, matching DuckDB's own s3_use_ssl default)."""
    if raw.startswith("https://"):
        return raw[len("https://") :], True
    if raw.startswith("http://"):
        return raw[len("http://") :], False
    return raw, True


def escape_sql_literal(value: str) -> str:
    """DuckDB's ATTACH statement does not accept `?` parameter placeholders
    in its option list (verified: `ParserException: syntax error at or near
    "?"`), unlike CREATE SECRET (which does, and is used for every
    credential value below). ATTACH's own literals -- warehouse name and
    catalog URI -- are escaped by hand instead."""
    return value.replace("'", "''")


@dataclass
class TableResult:
    table: str
    source: str  # "lake.alice.<table>" or "lake.contract.<table>"
    source_schema: str  # "alice" or "contract"
    row_count: int
    snapshot_id: int
    output_file: str
    fallback_reason: str | None = None


def build_manifest(
    results: list[TableResult],
    *,
    extracted_at: datetime,
    catalog_uri: str,
    warehouse: str,
) -> dict[str, Any]:
    """Provenance the old Dremio script never had (brief): per-table row
    count, Iceberg snapshot ID, and one shared extraction timestamp."""
    return {
        "extracted_at": extracted_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "catalog_uri": catalog_uri,
        "warehouse": warehouse,
        "tables": {
            r.table: {
                "source": r.source,
                "source_schema": r.source_schema,
                "row_count": r.row_count,
                "snapshot_id": r.snapshot_id,
                "output_file": r.output_file,
                **({"fallback_reason": r.fallback_reason} if r.fallback_reason else {}),
            }
            for r in results
        },
    }


# --------------------------------------------------------------------------
# DuckDB-touching functions. Integration-only -- exercised by the live kind
# run (Step 3 of the brief), not by unit tests; see
# tools/tests/test_extract_training_data.py's module docstring for why.
# --------------------------------------------------------------------------


def connect_catalog(args: argparse.Namespace):
    import duckdb  # imported lazily: --help / arg validation work without duckdb installed

    con = duckdb.connect()
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("CREATE SECRET lakekeeper_secret (TYPE iceberg, TOKEN ?)", [args.token])

    delegation_mode = "vended_credentials"
    if args.s3_endpoint:
        host_port, use_ssl = split_s3_endpoint(args.s3_endpoint)
        con.execute(
            "CREATE SECRET minio_s3 (TYPE S3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE ?, USE_SSL ?, REGION ?)",
            [args.s3_access_key, args.s3_secret_key, host_port, args.s3_url_style, use_ssl, args.s3_region],
        )
        delegation_mode = "none"

    warehouse = escape_sql_literal(args.warehouse)
    endpoint = escape_sql_literal(args.catalog_uri)
    con.execute(
        f"ATTACH '{warehouse}' AS {CATALOG_NAME} (TYPE iceberg, ENDPOINT '{endpoint}', "
        f"SECRET lakekeeper_secret, ACCESS_DELEGATION_MODE '{delegation_mode}')"
    )
    return con


def _rest_get_json(url: str, token: str, *, timeout: float = REST_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Stdlib-only GET-JSON helper (`urllib.request`) -- the single network
    transport function used by the fallback-cause REST check below.
    Isolated as its own module-level function so unit tests can monkeypatch
    just this network boundary (a fake REST responder keyed by URL) without
    a real HTTP server or touching `urllib` itself. Raises
    `urllib.error.HTTPError` / `urllib.error.URLError` / `json.JSONDecodeError`
    on failure -- callers decide what each means."""
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 -- fixed http(s) catalog-uri, not user-controlled
        return json.loads(resp.read().decode("utf-8"))


def rest_catalog_prefix(catalog_uri: str, warehouse: str, token: str) -> str:
    """Mirrors the discovery DuckDB's own Iceberg REST-catalog client
    already performs at ATTACH time: `GET /v1/config?warehouse=<warehouse>`,
    to resolve Lakekeeper's real per-warehouse REST prefix -- NOT the
    `--warehouse` name itself. Verified live 2026-07-13 against this repo's
    kind cluster: `/v1/config?warehouse=default` returned `overrides.prefix`
    as a warehouse UUID (`2b6351e0-7e14-...`), distinct from the literal
    `"default"` ATTACH's own `warehouse` argument uses (module docstring's
    "fallback_reason disambiguation" section). `overrides` wins over
    `defaults` per the Iceberg REST OpenAPI spec (server-supplied canonical
    values override client-requested ones)."""
    url = f"{catalog_uri.rstrip('/')}/v1/config?warehouse={urllib.parse.quote(warehouse, safe='')}"
    config = _rest_get_json(url, token)
    overrides = config.get("overrides") or {}
    defaults = config.get("defaults") or {}
    prefix = overrides.get("prefix") or defaults.get("prefix")
    if not prefix:
        raise RuntimeError(f"/v1/config response has no usable 'prefix' (overrides or defaults): {config!r}")
    return prefix


def check_contract_view_exists(catalog_uri: str, warehouse: str, token: str, namespace: str, table: str) -> bool:
    """Direct REST check disambiguating WHY `resolve_source()`'s contract-
    schema read failed (P3T4 review Important 1): duckdb-iceberg cannot
    read Iceberg REST-catalog views at all (today's true case -- the view
    genuinely exists server-side) versus the view never having been
    created (e.g. `alice-ingest apply-views` was never run after
    `kind-up`) -- DuckDB raises the byte-identical `CatalogException` for
    both, so this REST call is the only way to tell them apart.

    `GET /v1/<prefix>/namespaces/<namespace>/views` and looks for `table`
    in the returned `identifiers` list. A 404 (namespace itself absent) and
    an empty/non-matching identifier list both mean "not found" -- collapsed
    to a single bool here since `fallback_reason` only needs to distinguish
    "exists" from "doesn't"; any other HTTP error is re-raised so the caller
    can report "REST check failed" rather than silently treating it as
    "not found"."""
    prefix = rest_catalog_prefix(catalog_uri, warehouse, token)
    url = f"{catalog_uri.rstrip('/')}/v1/{urllib.parse.quote(prefix, safe='')}/namespaces/{namespace}/views"
    try:
        payload = _rest_get_json(url, token)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise
    identifiers = payload.get("identifiers") or []
    return any(entry.get("name") == table for entry in identifiers)


def _fallback_reason(
    *,
    contract_ref: str,
    alice_ref: str,
    contract_exc: Exception,
    catalog_uri: str,
    warehouse: str,
    token: str,
    namespace: str,
    table: str,
) -> str:
    """Builds `fallback_reason`'s text for the three REST-disambiguated
    outcomes (P3T4 review Important 1; module docstring's
    "fallback_reason disambiguation" section has the full rationale)."""
    try:
        exists = check_contract_view_exists(catalog_uri, warehouse, token, namespace, table)
    except Exception as rest_exc:  # noqa: BLE001 -- REST check itself failed; report that plainly, don't guess
        return (
            f"{contract_ref} not queryable via DuckDB's Iceberg REST-catalog client "
            f"(original DuckDB error: {contract_exc}). contract read failed; could not "
            f"determine cause (REST check failed: {rest_exc}). Falling back to {alice_ref} "
            f"(dlt-normalized column names, not the ML-consumer contract spellings)."
        )
    if exists:
        return (
            f"{contract_ref} not queryable via DuckDB's Iceberg REST-catalog client "
            f"(original DuckDB error: {contract_exc}). Disambiguated via a direct REST "
            f"check against the catalog (GET .../namespaces/{namespace}/views lists "
            f"{table!r}): duckdb-iceberg cannot read REST-catalog views (view EXISTS "
            f"server-side) -- a duckdb-iceberg client limitation, not an ops mistake; see "
            f"docs/runbooks/ml-extraction.md. Falling back to {alice_ref} (dlt-normalized "
            f"column names, not the ML-consumer contract spellings)."
        )
    return (
        f"{contract_ref} not queryable via DuckDB's Iceberg REST-catalog client "
        f"(original DuckDB error: {contract_exc}). Disambiguated via a direct REST check "
        f"against the catalog (GET .../namespaces/{namespace}/views does not list "
        f"{table!r}): contract view NOT FOUND server-side -- was apply-views run? (see "
        f"the ingestion-pipeline runbook, docs/runbooks/ingestion-pipeline.md). Falling "
        f"back to {alice_ref} (dlt-normalized column names, not the ML-consumer contract "
        f"spellings)."
    )


def resolve_source(
    con: Any,
    table: str,
    *,
    catalog_uri: str,
    warehouse: str,
    token: str,
) -> tuple[str, str, str | None]:
    """Returns (schema, fully_qualified_source, fallback_reason). Tries the
    `contract` schema's view first (ML-consumer column spellings); falls
    back to the raw `alice` table if it isn't queryable -- see the module
    docstring's "contract vs. alice" section for why this currently always
    falls back on duckdb-iceberg's current REST-catalog view support.

    On fallback, `alice`'s own queryability is checked BEFORE any REST
    disambiguation is attempted (raising immediately if it, too, is
    unreadable -- "no usable source at all" is a hard error, not a
    fallback-reason nuance) -- and only then does a REST call resolve
    *why* the contract read failed (`_fallback_reason()`, P3T4 review
    Important 1). This ordering fails fast on the truly-broken case
    without spending a network round trip on it."""
    contract_ref = f"{CATALOG_NAME}.{CONTRACT_SCHEMA}.{table}"
    contract_exc: Exception | None = None
    try:
        con.execute(f"SELECT 1 FROM {contract_ref} LIMIT 0")
        return CONTRACT_SCHEMA, contract_ref, None
    except Exception as exc:  # noqa: BLE001 -- any failure means "not readable this way", fall back
        contract_exc = exc

    alice_ref = f"{CATALOG_NAME}.{SOURCE_SCHEMA}.{table}"
    try:
        con.execute(f"SELECT 1 FROM {alice_ref} LIMIT 0")
    except Exception as alice_exc:  # noqa: BLE001 -- neither source is readable: hard error, no fallback possible
        raise RuntimeError(
            f"Both {contract_ref} and {alice_ref} are unreadable via DuckDB's Iceberg "
            f"REST-catalog client -- no usable source for table {table!r}. "
            f"contract error: {contract_exc}. alice error: {alice_exc}."
        ) from alice_exc

    reason = _fallback_reason(
        contract_ref=contract_ref,
        alice_ref=alice_ref,
        contract_exc=contract_exc,
        catalog_uri=catalog_uri,
        warehouse=warehouse,
        token=token,
        namespace=CONTRACT_SCHEMA,
        table=table,
    )
    return SOURCE_SCHEMA, alice_ref, reason


def get_snapshot_id(con: Any, table: str) -> int:
    """Current snapshot ID of the physical `alice.<table>` Iceberg table --
    always read from the SOURCE table (module docstring, "Snapshot IDs")."""
    row = con.execute(
        f"SELECT snapshot_id FROM iceberg_snapshots({CATALOG_NAME}.{SOURCE_SCHEMA}.{table}) "
        "ORDER BY sequence_number DESC LIMIT 1"
    ).fetchone()
    if row is None:
        raise RuntimeError(f"{CATALOG_NAME}.{SOURCE_SCHEMA}.{table} has no snapshots")
    return row[0]


def extract_table(
    con: Any,
    table: str,
    output_dir: Path,
    *,
    catalog_uri: str,
    warehouse: str,
    token: str,
) -> TableResult:
    schema, source_ref, fallback_reason = resolve_source(
        con, table, catalog_uri=catalog_uri, warehouse=warehouse, token=token
    )
    if fallback_reason:
        print(f"WARNING: {fallback_reason}", file=sys.stderr)

    output_file = f"{table}.parquet"
    output_path = output_dir / output_file
    copy_result = con.execute(
        f"COPY (SELECT * FROM {source_ref}) TO '{escape_sql_literal(str(output_path))}' (FORMAT parquet)"
    ).fetchone()
    row_count = copy_result[0] if copy_result else 0
    snapshot_id = get_snapshot_id(con, table)

    return TableResult(
        table=table,
        source=source_ref,
        source_schema=schema,
        row_count=row_count,
        snapshot_id=snapshot_id,
        output_file=output_file,
        fallback_reason=fallback_reason,
    )


def run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    con = connect_catalog(args)
    tables = parse_tables(args.tables)
    results = [
        extract_table(
            con,
            t,
            output_dir,
            catalog_uri=args.catalog_uri,
            warehouse=args.warehouse,
            token=args.token,
        )
        for t in tables
    ]

    manifest = build_manifest(
        results,
        extracted_at=datetime.now(timezone.utc),
        catalog_uri=args.catalog_uri,
        warehouse=args.warehouse,
    )
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    for r in results:
        print(f"{r.table}: {r.row_count} rows from {r.source} (snapshot {r.snapshot_id}) -> {r.output_file}")
    print(f"manifest: {manifest_path}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
