"""Contract column mapping: ML-consumer dtypes-contract spelling -> the live
dlt-produced Iceberg column, per `alice.*` table (Plan 3 Task 2 Step 2).

Consumed by `views.py`'s `apply-views` subcommand to build the `lake.contract`
schema's `CREATE OR REPLACE VIEW` DDL. This module is the reviewable
artifact -- generated once against verified sources below, reviewed forever;
it does not call Trino or dlt at import time.

Sources (both consulted, cross-checked against each other and against the
LIVE schema -- brief, Step 2):

1. The dtypes contract itself: `alice_jobs_package`'s
   `src/alice_jobs_package/resources/dtypes/{job_info,trace,mon_jdls_parsed}.json`
   (fetched live via `gh api repos/agh-alice/alice_jobs_package/contents/...`,
   2026-07-12 -- these are the ACTUAL files the ML consumer's `data_loader.py`
   validates against, not the research doc's abbreviated "…" example list).
   `job_info.json`: 7 keys. `trace.json`: 18 keys. `mon_jdls_parsed.json`: 67
   keys (`job_id` + 66 named JDL fields) -- the research doc's "~70 cols" was
   an approximation of this exact count.

2. The LIVE dlt-produced schema: `SHOW COLUMNS FROM lake.alice.<table>` run
   against this kind cluster 2026-07-12 (Trino 476 / Lakekeeper 0.12.2, after
   `hack/seed-fixture.sh` + `hack/run-ingest-once.sh`):
     job_info (7): job_id, jdl_set, trace_set, status, job_submit_timestamp,
       last_update, site -- IDENTICAL spelling to the dtypes contract (both
       tables "predate dlt renaming", per the plan: no dlt naming-convention
       transform ever touches these top-level PostgreSQL column names).
       Notably ALSO has no `_dlt_id`/`_dlt_load_id` metadata columns (unlike
       mon_jdls_parsed below) -- reflected here as-is, not invented.
     trace (18): job_id, aliencpuefficiency, cputime, host, maxrss,
       cpuefficiency, finaltimestamp, masterjobid, pid, requestedcpus,
       requestedttl, runningtimestamp, savingtimestamp, startedtimestamp,
       walltime, maxvirt, site, laststatuschangetimestamp -- IDENTICAL to the
       dtypes contract too, same reasoning.
     mon_jdls_parsed (19): job_id, lpmjobtypeid, full_jdl, jdl_parse_ok,
       jdl__pwg, jdl__ttl, jdl__user, jdl__job_tag, jdl__cpu_cores,
       jdl__cpu_limit, jdl__packages, jdl__executable, jdl__memory_size,
       jdl__requirements, jdl__collision_system, jdl__lpm_pass_name,
       _dlt_load_id, _dlt_id, full_jdl_raw -- ONLY 12 of the 66 JDL-derived
       contract fields have a live `jdl__*` counterpart. This is the kind
       fixture's JDL sample data being narrower than the full production JDL
       vocabulary (`ingest/tests/fixtures/jdl_samples.json`), not a mapping
       bug -- production JDLs are expected to populate more of the 66 over
       time as dlt's schema evolves (`schema_contract={"columns": "evolve",
       ...}`, pipeline.py). The 54 contract fields with no live counterpart
       are rendered as typed NULLs (see MON_JDLS_PARSED_NULL_TYPE below), NOT
       omitted -- the view's column set matches the FULL contract regardless
       of which fields today's fixture happens to populate.

dlt's naming convention (which contract field -> which `jdl__*` column) was
NOT hand-simulated: verified by running the actual pinned dependency,
`dlt.common.normalizers.naming.snake_case.NamingConvention().normalize_identifier()`,
against all 66 JDL field names in a throwaway venv (dlt==1.28.2, matching
`ingest/pyproject.toml`'s pin), 2026-07-12. Every one of the 12 live
`jdl__*` columns matches its contract field's normalized form exactly (e.g.
`CPUCores` -> `cpu_cores`, `LPMPassName` -> `lpm_pass_name`), confirming both
directions: the fixture's populated fields are exactly this set of 12, and
dlt's transform is exactly the invertible one this module assumes.

LPM casing (brief invariant, design spec section 4): gen-1 JDLs carried BOTH
`LPMPassName` and `LPMPASSNAME` as case-variant duplicate keys; the upstream
dtypes.json still literally lists the split-casing spelling `LPMPASSNAME`
(legacy, not yet updated for the redesign). `jdl.py`'s `_merge_lpm_casing()`
coalesces both into the canonical `LPMPassName` BEFORE dlt ever sees the
record (verified: `jdl__lpm_pass_name` exists live, `jdl__lpmpassname` does
NOT -- kind-verify.sh's existing iceberg-contents-probe hard-gates this).
This module therefore maps the CANONICAL key `LPMPassName` (not the raw
dtypes.json's `LPMPASSNAME`) to `jdl__lpm_pass_name` -- per the brief's own
worked example (`jdl__lpm_pass_name -> LPMPassName`) and per Step 2's
explicit invariant ("`jdl__lpmpassname` must NOT appear").

Packages: `Packages` (contract) -> `jdl__packages` (live). Physically a
`varchar` column in Trino's Iceberg-connector view (Iceberg has no native
JSON type), but dlt's OWN schema records its `data_type` as `"json"`
(verified live, `ingest/tests/test_pipeline.py`'s
`TestMaxTableNestingSuppressesChildTables` -- `max_table_nesting=1` demotes
the JDL's `Packages` list field to a JSON-serialized value column instead of
a child table). The view maps it as an ordinary passthrough column (no
special CAST) -- the consumer already expects to receive+parse a JSON string
under this field (dtypes.json declares it pandas `"object"` dtype, matching
a string column).

Typed-NULL type choice (MON_JDLS_PARSED_NULL_TYPE): the dtypes.json declares
mixed pandas dtypes per field (`float64`, `object`, `bool`) -- but EVERY ONE
of the 12 live `jdl__*` columns is `varchar` (JDL is a text-based classad
format; dlt/parse_jdl never casts values, so even numeric-looking fields
like `TTL`/`CPUCores` land as strings). VARCHAR is therefore the type that
is actually consistent with this table's real, live sibling columns; casting
the 54 absent fields' NULLs to their nominal pandas dtype would be a
type-consistency lie this module has no live evidence for. The ML consumer's
own `data_loader.py` already owns dtype casting against the contract
(research/2026-07-12_ml-consumer-data-contract.md) -- that responsibility is
not duplicated here.
"""

from __future__ import annotations

# --------------------------------------------------------------------------
# job_info: identity mapping (predates dlt renaming -- dtypes.json's 7 keys
# match `SHOW COLUMNS FROM lake.alice.job_info` verbatim). No dlt-only extra
# columns live on this table (verified: no _dlt_id/_dlt_load_id present).
# --------------------------------------------------------------------------
JOB_INFO_COLUMNS: dict[str, str | None] = {
    "job_id": "job_id",
    "jdl_set": "jdl_set",
    "trace_set": "trace_set",
    "status": "status",
    "job_submit_timestamp": "job_submit_timestamp",
    "last_update": "last_update",
    "site": "site",
}
JOB_INFO_PASSTHROUGH: tuple[str, ...] = ()

# --------------------------------------------------------------------------
# trace: identity mapping, same reasoning as job_info. No dlt-only extras.
# --------------------------------------------------------------------------
TRACE_COLUMNS: dict[str, str | None] = {
    "job_id": "job_id",
    "aliencpuefficiency": "aliencpuefficiency",
    "cputime": "cputime",
    "host": "host",
    "maxrss": "maxrss",
    "cpuefficiency": "cpuefficiency",
    "finaltimestamp": "finaltimestamp",
    "masterjobid": "masterjobid",
    "pid": "pid",
    "requestedcpus": "requestedcpus",
    "requestedttl": "requestedttl",
    "runningtimestamp": "runningtimestamp",
    "savingtimestamp": "savingtimestamp",
    "startedtimestamp": "startedtimestamp",
    "walltime": "walltime",
    "maxvirt": "maxvirt",
    "site": "site",
    "laststatuschangetimestamp": "laststatuschangetimestamp",
}
TRACE_PASSTHROUGH: tuple[str, ...] = ()

# --------------------------------------------------------------------------
# mon_jdls_parsed: `job_id` identity + 66 JDL-derived fields. Order below
# matches the upstream dtypes.json field order (module docstring, source 1),
# with `LPMPASSNAME` renamed to its canonical spelling `LPMPassName` (module
# docstring, "LPM casing"). A `None` value means: no live `jdl__*` column
# exists for this contract field today -- rendered as a typed NULL by
# views.py, using MON_JDLS_PARSED_NULL_TYPE.
# --------------------------------------------------------------------------
MON_JDLS_PARSED_COLUMNS: dict[str, str | None] = {
    "job_id": "job_id",
    "Activity": None,
    "Arguments": None,
    "CPUCores": "jdl__cpu_cores",
    "CPULimit": "jdl__cpu_limit",
    "CollisionSystem": "jdl__collision_system",
    "DataframeSize": None,
    "DirectAccess": None,
    "Executable": "jdl__executable",
    "FilesToCheck": None,
    "HYJobID": None,
    "HYRun": None,
    "HYTrain": None,
    "HardBins": None,
    "InputData": None,
    "InputDataList": None,
    "InputDataListFormat": None,
    "InputDataType": None,
    "InputFile": None,
    "IterationTimestamp": None,
    "JDLArguments": None,
    "JDLPath": None,
    "JDLProcessor": None,
    "JDLVariables": None,
    "JobTag": "jdl__job_tag",
    "LPMActivity": None,
    "LPMAnchorPassName": None,
    "LPMAnchorProduction": None,
    "LPMAnchorRun": None,
    "LPMAnchorYear": None,
    "LPMChainID": None,
    "LPMCollectionEntity": None,
    "LPMHighPriority": None,
    "LPMInteractionType": None,
    "LPMJobTypeID": None,
    "LPMMaxResubmissions": None,
    "LPMMetaData": None,
    "LPMParentPID": None,
    "LPMProductionTag": None,
    "LPMProductionType": None,
    "LPMRunNumber": None,
    "LegoResubmitZombies": None,
    "MCAnchor": None,
    "MasterJobID": None,
    "MasterResubmitThreshold": None,
    "MaxFailFraction": None,
    "MaxOutputSize": None,
    "MaxResubmitFraction": None,
    "MaxWaitingTime": None,
    "MemorySize": "jdl__memory_size",
    "OrigRequirements": None,
    "Output": None,
    "OutputDir": None,
    "OutputErrorE": None,
    "OutputFileType": None,
    "PWG": "jdl__pwg",
    "Packages": "jdl__packages",
    "Price": None,
    "Requirements": "jdl__requirements",
    "SeNumber": None,
    "Splitted": None,
    "TTL": "jdl__ttl",
    "Type": None,
    "User": "jdl__user",
    "ValidationCommand": None,
    "WorkDirectorySize": None,
    # Canonical spelling (module docstring, "LPM casing") -- NOT the upstream
    # dtypes.json's literal (legacy) key `LPMPASSNAME`.
    "LPMPassName": "jdl__lpm_pass_name",
}

# All 54 currently-NULLed fields share this type (module docstring, "Typed-
# NULL type choice").
MON_JDLS_PARSED_NULL_TYPE = "VARCHAR"

# Live dlt columns with NO contract counterpart at all -- passed straight
# through under their own dlt names, in a trailing section after every
# contract column (brief, Step 2). `lpmjobtypeid`/`full_jdl` are landing-
# table/pipeline artifacts (production ground truth: mon_jdls's own 3
# PostgreSQL columns are job_id/lpmjobtypeid/full_jdl -- see pipeline.py's
# module docstring); `jdl_parse_ok`/`full_jdl_raw` are `parse_jdl`'s own
# bookkeeping (jdl.py); `_dlt_load_id`/`_dlt_id` are dlt's own metadata
# columns. None of these six appear in the ML consumer's dtypes contract.
MON_JDLS_PARSED_PASSTHROUGH: tuple[str, ...] = (
    "lpmjobtypeid",
    "full_jdl",
    "jdl_parse_ok",
    "full_jdl_raw",
    "_dlt_load_id",
    "_dlt_id",
)
