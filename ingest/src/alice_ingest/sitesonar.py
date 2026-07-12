"""alice-ingest site-sonar: dlt resource ingesting ALICE Site Sonar dumps
(alimonitor.cern.ch) into Iceberg table `site_sonar` (Plan 2 Task 4 -- the
E3 fix: gen-1's Spark driver OOMKilled nightly doing this same job with
driver-side JSON parsing, deliverables/2026-07-12-datalake-v2-design.md).

Source: HTTP directory listing at SITESONAR_URL (default
"http://alimonitor.cern.ch/download/kalana/", the gen-1 script's URL --
research/2026-07-12_ml-consumer-data-contract.md: "Site Sonar data comes
separately: HTTP scrape of alimonitor.cern.ch/download/kalana/*.out.xz").
Filenames are `site-sonar-<epoch>.out.xz`, one per generation run; each is
XZ/LZMA-compressed JSON-lines. Verified LIVE against the real public URL
(2026-07-12, Plan 2 Task 4 -- reachable from this VM: 1740 files, most
recent within the current day). Sample line shape:

    {"host_id": <int>, "hostname": <str>, "ce_name": <str>, "addr": <str>,
     "last_updated": <epoch SECONDS>, "test_results_json": {<nested,
     evolving per-test keys, e.g. "os", "cgroups2_checking", "uname", ...>}}

`last_updated` + `hostname` match the ML consumer's expected join keys
(research/2026-07-12_ml-consumer-data-contract.md: "temporal join to Site
Sonar on hostname with last_updated < startedtimestamp"). `test_results_json`
genuinely schema-drifts (per-test sub-keys vary by host/run and by which
tests a given site's probe supports) -- exactly what
`schema_contract={"columns":"evolve","data_type":"freeze"}` (brief) is for:
new test columns may appear over time, but an existing column's TYPE is
frozen (a drifting type is a hard failure surfaced by dlt, not silently
coerced).

Incremental behavior: dlt's incremental cursor on the row-level
`last_updated` field (same shape as pipeline.py's per-table cursors) makes
reruns over the SAME file idempotent (append-only, but a repeat run yields
no new rows once the watermark has advanced past them).

File-level high-water mark (Plan 2 Task 5 -- scoped in from the P2T4
review): downloading and decompressing the full historical backlog (1740
files, tens of GB decompressed) every night was Task 4's documented gap.
Closed here via `dlt.current.resource_state()` -- the idiomatic dlt pattern
for exactly this ("skip already-processed archives across runs"; see dlt's
own `resource_state()` docstring example, which is this same shape: track
processed identifiers in resource-scoped state, persisted to the
destination alongside the pipeline's data on a successful run, and
restored on the next run). `select_files_to_fetch()` filters the parsed
listing down to files whose filename-embedded epoch is strictly greater
than `state["high_water_mark"]`; `_iter_rows()` fetches only that filtered
set and advances the high-water mark to the max epoch actually fetched --
only after the fetch loop completes without raising, so a run that fails
partway through never advances the mark past a file it didn't finish
(dlt's own state-commit semantics reinforce this at the pipeline level: a
failed `pipeline.run()` does not persist updated state to the destination
at all, so a hard failure retries the same files next run too). The
`--limit` CLI flag still applies -- now to the NEWLY-selected files, not
the full listing -- and still doubles as an operational safety valve /
Task 4's e2e probe mechanism.
"""

from __future__ import annotations

import json
import lzma
import os
import re
from typing import Iterable, Iterator, Mapping, MutableMapping, Sequence
from urllib.parse import urlsplit, urlunsplit

import dlt
import requests

from alice_ingest.pipeline import DATASET_NAME, configure_dlt

DEFAULT_SITESONAR_URL = "http://alimonitor.cern.ch/download/kalana/"

# Apache-style directory listing: <a href="/download/kalana/site-sonar-<epoch>.out.xz">.
# Regex per the gen-1 script's approach (brief Step 3) -- the listing is a
# flat, uniform table; a full HTML parser is unwarranted. Verified live
# against the real page, 2026-07-12.
_FILE_LINK_RE = re.compile(r'href="([^"]*?site-sonar-(\d+)\.out\.xz)"')


def _resolve_url(base_url: str, href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        parts = urlsplit(base_url)
        return urlunsplit((parts.scheme, parts.netloc, href, "", ""))
    return base_url.rstrip("/") + "/" + href


def list_source_files(listing_html: str, base_url: str) -> list[tuple[int, str]]:
    """Parse the directory listing HTML; return (epoch, absolute_url) pairs
    for every `*.out.xz` entry, most-recent (highest epoch) first."""
    files = [
        (int(epoch_str), _resolve_url(base_url, href))
        for href, epoch_str in _FILE_LINK_RE.findall(listing_html)
    ]
    files.sort(key=lambda pair: pair[0], reverse=True)
    return files


def fetch_and_parse(url: str, session: requests.Session) -> Iterator[dict]:
    """Download one `.out.xz` file, LZMA-decode, yield each JSON-line as a
    dict. A single malformed line is skipped, not fatal -- these dumps are
    third-party generated (CERN Site Sonar), and one bad line must not
    abort an entire file's worth of otherwise-good rows. An unreachable
    file (network/HTTP error) DOES raise -- that is a hard CronWorkflow
    failure, not something to silently skip."""
    response = session.get(url, timeout=60)
    response.raise_for_status()
    raw = lzma.decompress(response.content)
    for line in raw.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def select_files_to_fetch(
    files: Sequence[tuple[int, str]], high_water_mark: int
) -> list[tuple[int, str]]:
    """Filter `files` (epoch, url pairs, as returned by list_source_files --
    already sorted most-recent-first) down to those NOT YET processed: a
    filename-embedded epoch strictly greater than `high_water_mark`.
    Strictly-greater-than (not >=): a file AT the high-water mark was the
    last one a prior run already fetched, so it is excluded, not
    reselected. Order is preserved (module docstring: high-water mark
    tracking; `_iter_rows` relies on the max of what THIS call selects)."""
    return [(epoch, url) for epoch, url in files if epoch > high_water_mark]


def _iter_rows(
    base_url: str,
    limit: int | None,
    session: requests.Session,
    state: MutableMapping[str, int],
) -> Iterable[dict]:
    """Fetch+parse only files newer than `state["high_water_mark"]` (module
    docstring's high-water-mark skip), then advance the mark to the max
    epoch actually fetched -- but only once the whole selected batch has
    been consumed without raising, so a mid-run failure never advances the
    mark past a file whose rows didn't make it into this run's yield."""
    listing = session.get(base_url, timeout=30)
    listing.raise_for_status()
    files = list_source_files(listing.text, base_url)

    high_water_mark = state.get("high_water_mark", 0)
    new_files = select_files_to_fetch(files, high_water_mark)
    already_processed_skipped = len(files) - len(new_files)
    if limit is not None:
        new_files = new_files[:limit]

    print(
        f"SITESONAR high_water_mark={high_water_mark} files_listed={len(files)} "
        f"files_already_processed_skipped={already_processed_skipped} "
        f"files_to_fetch={len(new_files)}"
    )

    max_epoch_fetched = high_water_mark
    for epoch, url in new_files:
        yield from fetch_and_parse(url, session)
        if epoch > max_epoch_fetched:
            max_epoch_fetched = epoch

    if max_epoch_fetched > high_water_mark:
        state["high_water_mark"] = max_epoch_fetched


def build_site_sonar_resource(env: Mapping[str, str], limit: int | None = None):
    """dlt resource: yields site-sonar rows across the `limit` most-recent
    NEW `.out.xz` files (or every new file if `limit` is None) -> table
    `site_sonar`, append, schema_contract evolve/freeze, table_format
    iceberg (brief, Step 3). "New" is decided by the high-water-mark skip
    (module docstring) against `dlt.current.resource_state()` -- dlt's own
    resource-scoped, destination-persisted state store, the same mechanism
    `last_updated`'s row-level incremental cursor rides on."""
    base_url = env.get("SITESONAR_URL", DEFAULT_SITESONAR_URL)

    @dlt.resource(
        name="site_sonar",
        table_name="site_sonar",
        write_disposition="append",
        schema_contract={"columns": "evolve", "data_type": "freeze"},
        table_format="iceberg",
    )
    def site_sonar(
        last_updated=dlt.sources.incremental("last_updated", initial_value=0),
    ):
        session = requests.Session()
        state = dlt.current.resource_state()
        yield from _iter_rows(base_url, limit, session, state)

    return site_sonar


def run(env: Mapping[str, str] | None = None, limit: int | None = None) -> int:
    """Entry point wired from pipeline.py's `run_sitesonar` CLI command."""
    env = env if env is not None else os.environ
    configure_dlt(env)

    pipeline = dlt.pipeline(
        pipeline_name="alice_ingest_sitesonar",
        destination="filesystem",
        dataset_name=DATASET_NAME,
    )
    resource = build_site_sonar_resource(env, limit=limit)
    load_info = pipeline.run(resource)
    print(load_info)
    return 0
