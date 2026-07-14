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

2. The LIVE dlt-produced schema. REGENERATED against REAL PRODUCTION DATA
   by the 2026-07-13 production-data dress rehearsal (research/2026-07-13_
   production-data-dress-rehearsal.md): `SHOW COLUMNS FROM lake.alice.
   <table>` after ingesting a 166,019-job / 146,896-JDL production sample
   (job_id window 3593645502..3593845502, 43 lpmjobtypeid values) on this
   kind cluster (Trino 476 / Lakekeeper 0.12.2):
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
     mon_jdls_parsed (108): job_id, lpmjobtypeid, full_jdl, jdl_parse_ok,
       _dlt_load_id, _dlt_id, and 102 `jdl__*` columns -- EVERY ONE varchar
       (jdl.py's value canonicalization, a dress-rehearsal fix: values are
       served as strings by construction, so the column set equals the
       production JDL field-name vocabulary and never depends on value
       types or row order). 64 of the 66 JDL-derived contract fields have
       a live `jdl__*` counterpart in this window; the remaining 2
       (`HardBins`, `MasterResubmitThreshold`) appeared in none of the
       146,896 sampled JDLs and are rendered as typed NULLs (see
       MON_JDLS_PARSED_NULL_TYPE below), NOT omitted -- the view's column
       set matches the FULL contract regardless. The other 38 live
       `jdl__*` columns are real production fields OUTSIDE the contract,
       served via MON_JDLS_PARSED_PASSTHROUGH (see below). (full_jdl_raw
       was absent from this window's live schema -- all JDLs parsed -- and
       is hint-declared in pipeline.py since; see the passthrough comment.)

dlt's naming convention (which contract field -> which `jdl__*` column) was
NOT hand-simulated: verified by running the actual pinned dependency,
`dlt.common.normalizers.naming.snake_case.NamingConvention().normalize_identifier()`,
against all 66 JDL field names (dlt==1.28.2, matching `ingest/
pyproject.toml`'s pin) -- first on 2026-07-12 for the fixture-era 12, then
re-run 2026-07-13 against the full 103-field production census (102 after
the LPM merge). Every live `jdl__*` column matches its field's normalized
form exactly (e.g. `CPUCores` -> `cpu_cores`, `O2DPG_ASYNC_RECO_TAG` ->
`o2_dpg_async_reco_tag`), with ZERO normalization collisions across the
production vocabulary, confirming dlt's transform is exactly the
invertible one this module assumes.

LPM casing (brief invariant, design spec section 4): gen-1 JDLs carried BOTH
`LPMPassName` and `LPMPASSNAME` as case-variant duplicate keys; the upstream
dtypes.json still literally lists the split-casing spelling `LPMPASSNAME`
(legacy, not yet updated for the redesign). `jdl.py`'s `_merge_lpm_casing()`
coalesces both into the canonical `LPMPassName` BEFORE dlt ever sees the
record (verified: `jdl__lpm_pass_name` exists live, `jdl__lpmpassname` does
NOT -- kind-verify.sh's existing iceberg-contents-probe hard-gates this).
PROVEN AT SCALE on real data by the dress rehearsal: 36,217 sampled JDLs
carried the canonical casing and 13,243 the legacy one (the `33022`
MC-production JDL family); live `jdl__lpm_pass_name` is populated in
exactly 49,460 = 36,217 + 13,243 rows, and `jdl__lpmpassname` never
appeared.
This module therefore maps the CANONICAL key `LPMPassName` (not the raw
dtypes.json's `LPMPASSNAME`) to `jdl__lpm_pass_name` -- per the brief's own
worked example (`jdl__lpm_pass_name -> LPMPassName`) and per Step 2's
explicit invariant ("`jdl__lpmpassname` must NOT appear").

Packages: `Packages` (contract) -> `jdl__packages` (live varchar). Since
the dress rehearsal, `jdl.py`'s value canonicalization serializes list
values (like Packages) to compact JSON text at parse time, so the column
is a plain string end-to-end (pre-rehearsal, `max_table_nesting=1` demoted
it to a dlt `"json"`-typed value column instead -- `ingest/tests/
test_pipeline.py`'s `TestMaxTableNestingSuppressesChildTables` documents
that dlt mechanism, which remains load-bearing for flattening the `jdl`
dict itself). The view maps it as an ordinary column (no special CAST) --
the consumer already expects to receive+parse a JSON string under this
field (dtypes.json declares it pandas `"object"` dtype, matching a string
column).

Typed-NULL type choice (MON_JDLS_PARSED_NULL_TYPE): the dtypes.json declares
mixed pandas dtypes per field (`float64`, `object`, `bool`) -- but EVERY ONE
of the 102 live `jdl__*` columns is `varchar`, BY CONSTRUCTION since the
dress rehearsal (jdl.py's `_canonicalize_values`: strings pass through,
everything else becomes compact JSON text -- real production JDLs mix JSON
types for the same field across rows, which otherwise forces dlt variant
columns against the frozen data-type contract). VARCHAR is therefore the
type that is actually consistent with this table's real, live sibling
columns; casting the 2 absent fields' NULLs to their nominal pandas dtype
would be a type-consistency lie this module has no live evidence for. The
ML consumer's own `data_loader.py` already owns dtype casting against the
contract (research/2026-07-12_ml-consumer-data-contract.md) -- that
responsibility is not duplicated here.

How to regenerate this mapping
--------------------------------------------------------------------------
This module is a STATIC snapshot, not computed at import/run time (module
docstring, line 1). It goes stale in BOTH directions -- the second was
proven live by the 2026-07-13 production-data dress rehearsal (research/
2026-07-13_production-data-dress-rehearsal.md), which this procedure's
first execution against real data also debugged:

  - ADDITIVE: dlt evolves a new `jdl__*` column from production JDL data
    this file has never seen -> `apply-views` prints `WARNING: unmapped
    jdl__ columns present live ... (contract regeneration needed)`
    (non-strict exits 0; `--strict` exits 2). NOTE: ALL unmapped live
    `jdl__*` columns land in this one WARNING, whether or not they have a
    contract counterpart -- the `INFO:` line is only ever for non-`jdl__`
    columns. Which bucket a warned column belongs in (a mapping value vs
    `*_PASSTHROUGH`) is decided by step 3's normalization match, not by
    which stderr line it printed on.

  - SUBTRACTIVE: a column this mapping references does NOT exist on the
    live table (a fixture-era mapping meeting a freshly-reset,
    production-fed schema is the proven case) -> the view DDL is
    un-appliable outright, so `apply-views` reports `WARNING: mapped
    columns missing from live schema ... (contract regeneration needed;
    view DDL cannot be applied)` and exits 2 in BOTH modes, without
    touching any DDL.

When either WARNING appears, regenerate this file:

  1. Re-fetch the upstream dtypes contract (source 1 above): `gh api
     repos/agh-alice/alice_jobs_package/contents/src/alice_jobs_package/
     resources/dtypes/{job_info,trace,mon_jdls_parsed}.json` -- confirm the
     67-key `mon_jdls_parsed` shape (`job_id` + 66 JDL fields) hasn't itself
     changed; if it has, `MON_JDLS_PARSED_COLUMNS`'s key set changes too,
     not just its values.
  2. Re-run `SHOW COLUMNS FROM lake.alice.<table>` live (via a Trino probe
     pod/port-forward, same protocol `views.TrinoClient` implements) for all
     three tables -- this is the actual trigger: the WARNING/exit-2 message
     already names exactly which new `jdl__*` column(s) appeared and their
     live type.
  3. For each newly-live `jdl__*` column, find its contract field: run dlt's
     real naming convention (NOT hand-simulated -- this module's earlier
     verification note above), `dlt.common.normalizers.naming.snake_case.
     NamingConvention().normalize_identifier()`, against the dtypes.json
     field names from step 1, and match against the new live column name.
     Apply the same LPM-casing rule (canonical `LPMPassName`, never the
     legacy split-casing `LPMPASSNAME`) if relevant.
  4. Update `MON_JDLS_PARSED_COLUMNS` (flip the matched field's value from
     `None` to its live `jdl__*` name) -- or `JOB_INFO_COLUMNS`/
     `TRACE_COLUMNS`/the relevant `*_PASSTHROUGH` tuple if the drift is on
     one of those tables instead. If a warned live `jdl__*` column matches
     NO contract field under step 3's normalization, add it to that table's
     `*_PASSTHROUGH` tuple instead of leaving it unmapped and unexplained.
     (An earlier version of this step claimed no-counterpart columns arrive
     as `INFO`-level results -- wrong: `INFO` is only for non-`jdl__` names;
     see the ADDITIVE note above.) For SUBTRACTIVE drift, do the reverse:
     flip the affected field's value back to `None` (or remove the entry
     from `*_PASSTHROUGH`) so the view stops referencing a column the live
     schema does not have.
  5. Run `alice-ingest apply-views --strict` again -- exit 0 with no
     WARNING/stderr output confirms the regeneration closed the gap
     `check_drift()` found. (On kind: rebuild+push the ingest image first,
     per this repo's normal `chore: pin ingest image digest` flow, since the
     cluster runs the pinned image, not this working tree.)
  6. Update this module's test companion, `ingest/tests/test_views.py`'s
     `TestContractColumnsInvariants` (mapped/nulled counts, any new
     column-specific invariant) -- the counts in this docstring (currently
     `65 mapped` incl. job_id, `2 NULLed`, `44 passthrough`) and in those
     tests will both be stale after any regeneration and must move
     together.
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
# views.py, using MON_JDLS_PARSED_NULL_TYPE. Values REGENERATED against the
# 2026-07-13 production sample (module docstring, source 2): 64 of 66
# fields live (was 12 fixture-era); only `HardBins` and
# `MasterResubmitThreshold` remain NULL (absent from all 146,896 sampled
# production JDLs).
# --------------------------------------------------------------------------
MON_JDLS_PARSED_COLUMNS: dict[str, str | None] = {
    "job_id": "job_id",
    "Activity": "jdl__activity",
    "Arguments": "jdl__arguments",
    "CPUCores": "jdl__cpu_cores",
    "CPULimit": "jdl__cpu_limit",
    "CollisionSystem": "jdl__collision_system",
    "DataframeSize": "jdl__dataframe_size",
    "DirectAccess": "jdl__direct_access",
    "Executable": "jdl__executable",
    "FilesToCheck": "jdl__files_to_check",
    "HYJobID": "jdl__hy_job_id",
    "HYRun": "jdl__hy_run",
    "HYTrain": "jdl__hy_train",
    "HardBins": None,
    "InputData": "jdl__input_data",
    "InputDataList": "jdl__input_data_list",
    "InputDataListFormat": "jdl__input_data_list_format",
    "InputDataType": "jdl__input_data_type",
    "InputFile": "jdl__input_file",
    "IterationTimestamp": "jdl__iteration_timestamp",
    "JDLArguments": "jdl__jdl_arguments",
    "JDLPath": "jdl__jdl_path",
    "JDLProcessor": "jdl__jdl_processor",
    "JDLVariables": "jdl__jdl_variables",
    "JobTag": "jdl__job_tag",
    "LPMActivity": "jdl__lpm_activity",
    "LPMAnchorPassName": "jdl__lpm_anchor_pass_name",
    "LPMAnchorProduction": "jdl__lpm_anchor_production",
    "LPMAnchorRun": "jdl__lpm_anchor_run",
    "LPMAnchorYear": "jdl__lpm_anchor_year",
    "LPMChainID": "jdl__lpm_chain_id",
    "LPMCollectionEntity": "jdl__lpm_collection_entity",
    "LPMHighPriority": "jdl__lpm_high_priority",
    "LPMInteractionType": "jdl__lpm_interaction_type",
    "LPMJobTypeID": "jdl__lpm_job_type_id",
    "LPMMaxResubmissions": "jdl__lpm_max_resubmissions",
    "LPMMetaData": "jdl__lpm_meta_data",
    "LPMParentPID": "jdl__lpm_parent_pid",
    "LPMProductionTag": "jdl__lpm_production_tag",
    "LPMProductionType": "jdl__lpm_production_type",
    "LPMRunNumber": "jdl__lpm_run_number",
    "LegoResubmitZombies": "jdl__lego_resubmit_zombies",
    "MCAnchor": "jdl__mc_anchor",
    "MasterJobID": "jdl__master_job_id",
    "MasterResubmitThreshold": None,
    "MaxFailFraction": "jdl__max_fail_fraction",
    "MaxOutputSize": "jdl__max_output_size",
    "MaxResubmitFraction": "jdl__max_resubmit_fraction",
    "MaxWaitingTime": "jdl__max_waiting_time",
    "MemorySize": "jdl__memory_size",
    "OrigRequirements": "jdl__orig_requirements",
    "Output": "jdl__output",
    "OutputDir": "jdl__output_dir",
    "OutputErrorE": "jdl__output_error_e",
    "OutputFileType": "jdl__output_file_type",
    "PWG": "jdl__pwg",
    "Packages": "jdl__packages",
    "Price": "jdl__price",
    "Requirements": "jdl__requirements",
    "SeNumber": "jdl__se_number",
    "Splitted": "jdl__splitted",
    "TTL": "jdl__ttl",
    "Type": "jdl__type",
    "User": "jdl__user",
    "ValidationCommand": "jdl__validation_command",
    "WorkDirectorySize": "jdl__work_directory_size",
    # Canonical spelling (module docstring, "LPM casing") -- NOT the upstream
    # dtypes.json's literal (legacy) key `LPMPASSNAME`.
    "LPMPassName": "jdl__lpm_pass_name",
}

# Both currently-NULLed fields (`HardBins`, `MasterResubmitThreshold` --
# absent from the rehearsal window's 146,896 real JDLs) share this type
# (module docstring, "Typed-NULL type choice").
MON_JDLS_PARSED_NULL_TYPE = "VARCHAR"

# Live dlt columns with NO contract counterpart at all -- passed straight
# through under their own dlt names, in a trailing section after every
# contract column (brief, Step 2). `lpmjobtypeid`/`full_jdl` are landing-
# table/pipeline artifacts (production ground truth: mon_jdls's own 3
# PostgreSQL columns are job_id/lpmjobtypeid/full_jdl -- see pipeline.py's
# module docstring); `jdl_parse_ok`/`full_jdl_raw` are `parse_jdl`'s own
# bookkeeping (jdl.py; full_jdl_raw is hint-declared in pipeline.py since
# the dress rehearsal, so it exists even when every JDL parses); `_dlt_
# load_id`/`_dlt_id` are dlt's own metadata columns. None of these appear
# in the ML consumer's dtypes contract.
#
# The `jdl__*` entries below are REAL production JDL fields outside the
# 66-key dtypes contract, discovered by the 2026-07-13 dress rehearsal's
# 146,896-JDL sample (each matched against dlt's real normalizer per
# regeneration step 3 -- none corresponds to any contract field). They are
# served under their dlt names so the data stays reachable without
# pretending it belongs to the consumer contract. Notable: the ALL-CAPS
# O2/QC calibration flags (jdl__addtimeseriesinmc ... jdl__usethrottling)
# come from MC-production and a 2-job QC-special; the jdl__lpm_pass /
# jdl__lpm_raw_pass / jdl__lpmc_pass_mode / jdl__lpmraw_pass_id family are
# LPM fields the contract simply never included (distinct from
# LPMPassName, which IS contract-mapped above).
MON_JDLS_PARSED_PASSTHROUGH: tuple[str, ...] = (
    "lpmjobtypeid",
    "full_jdl",
    "jdl_parse_ok",
    "full_jdl_raw",
    "_dlt_load_id",
    "_dlt_id",
    "jdl__addtimeseriesinmc",
    "jdl__anchor_sim_options",
    "jdl__aodfilesize",
    "jdl__doemccalib",
    "jdl__dotpcresidualextraction",
    "jdl__dplreportprocessing",
    "jdl__enablemonitoring",
    "jdl__enablepermilfulltrackqc",
    "jdl__enableunbinnedtimeseries",
    "jdl__estimated_throughput",
    "jdl__extracttimeseries",
    "jdl__forced_kill_timeout",
    "jdl__hy_train_type",
    "jdl__hyx_run_merge_id",
    "jdl__initial_ttl",
    "jdl__keep_logs",
    "jdl__keeptofmatchoutput",
    "jdl__lpm_anchored_pass_number",
    "jdl__lpm_pass",
    "jdl__lpm_production_cycle",
    "jdl__lpm_raw_pass",
    "jdl__lpmc_pass_mode",
    "jdl__lpmraw_pass_id",
    "jdl__merging_stage",
    "jdl__nkeep",
    "jdl__notfdelay",
    "jdl__o2_dpg_async_reco_tag",
    "jdl__proxy_ttl",
    "jdl__run3_chunk",
    "jdl__runanalysisqc",
    "jdl__split_max_input_file_number",
    "jdl__subjob_count",
    "jdl__thinaods",
    "jdl__tpcscalingsource",
    "jdl__trackqcfraction",
    "jdl__ttl_optimization_type",
    "jdl__ttl_scaling",
    "jdl__usethrottling",
)
