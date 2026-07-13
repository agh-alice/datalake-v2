# Runbook: bounded historical backfill / backlog drain

How to re-run `alice-ingest run-nightly` over a bounded historical window,
rather than from each table's current incremental watermark. This is the
gen-1 "backlog-drain" replacement procedure Plan 4's migration will use to
pull historical rows gen-1 never processed (or processed under a bug this
pipeline has since fixed) into gen-2's Iceberg tables, without touching
gen-1 itself and without waiting for the nightly schedule to slowly catch
up one day at a time.

Read `docs/runbooks/ingestion-pipeline.md` first (architecture, env-var
contract, watermark state location) if this is unfamiliar territory --
this doc only covers the backfill-specific mechanism on top of that.

## 1. The mechanism: `INGEST_INITIAL_*` env overrides

`pipeline.py`'s three incremental cursors each have a hardcoded default
`initial_value` (job_info: `2026-01-01`; trace and mon_jdls: `0`). Plan 2
Task 6 added an env-var override for each, resolved once per
`build_*_source()`/`build_mon_jdls_resource()` call (`env` defaults to
`os.environ`, same pattern as every other `alice-ingest` entry point) --
no code change needed for a one-off backfill window:

| Var | Table / cursor | Format |
|---|---|---|
| `INGEST_INITIAL_JOB_INFO` | `job_info.last_update` | ISO date/date-time, naive -- `2026-03-01` or `2026-03-01T00:00:00` |
| `INGEST_INITIAL_TRACE` | `trace.laststatuschangetimestamp` | integer, epoch **milliseconds** |
| `INGEST_INITIAL_MON_JDLS` | `mon_jdls.job_id` | integer |

Unset (or empty-string) means "use the normal default" -- a backfill run
only needs to set the var(s) for the table(s) actually being backfilled;
the others fall through to their usual incremental behavior.

**job_info's format is naive, deliberately.** Production's `last_update`
column is `timestamp WITHOUT time zone` -- passing a value with a UTC
offset (`2026-03-01T00:00:00+02:00`) is refused with a `SystemExit`
(`_resolve_naive_initial_value` in `pipeline.py`), not silently accepted,
because an aware literal pushed down against a naive column forces a
session-timezone cast in the SQL comparison and silently shifts the cursor
-- exactly the bug class already fixed once in this codebase (commit
`a610069`, mirror-image case). If you have a wall-clock cutoff in mind,
write it as a plain local timestamp with no `+HH:MM`/`Z` suffix.

**trace and mon_jdls take raw integers, no unit conversion.** `pipeline.py`
hands the parsed integer straight to dlt's incremental cursor; it is your
responsibility to supply it in the column's own unit (epoch
**milliseconds** for `trace`, plain `job_id` for `mon_jdls` -- see
`retention.py`'s docstring for the epoch-unit ground truth, since the same
`laststatuschangetimestamp` column is read by both modules).

## 2. Running a backfill window

Same pod pattern as `docs/runbooks/ingestion-pipeline.md` section 3 (a
one-shot `Workflow`, `envFrom: ingest-env`, `pipeline-runner` SA), with the
override(s) added as extra `env` entries **after** `envFrom` (later entries
win on a name collision, so this cleanly layers on top of the Secret's
values without needing a modified Secret):

```bash
IMAGE=$(yq -r '.images.ingest' chart/values.yaml)
kubectl -n argo-workflows create -o name -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: {generateName: ingest-backfill-}
spec:
  serviceAccountName: pipeline-runner
  entrypoint: main
  ttlStrategy: {secondsAfterCompletion: 86400}
  templates:
    - name: main
      container:
        image: $IMAGE
        envFrom: [{secretRef: {name: ingest-env}}]
        env:
          - {name: INGEST_INITIAL_JOB_INFO, value: "2026-01-01"}
          - {name: INGEST_INITIAL_TRACE, value: "0"}
        args: ["run-nightly"]
EOF
```

This example backfills `job_info` and `trace` from the very start of the
fixture's window while leaving `mon_jdls` on its normal watermark. Adjust
the `env` entries and values to the window actually needed; poll/logs the
same way `hack/run-ingest-once.sh` does.

**`run-nightly` always processes all three resources in one invocation**
(`pipeline.py`'s `run_nightly()`) -- there is no per-table subcommand. A
backfill that should touch only one table still runs all three; the other
two simply continue from their own current watermark (unaffected, since
each resource's `initial_value` only matters the FIRST time dlt has no
prior state for that resource -- see section 3).

## 3. Why upsert makes re-running safe

`initial_value` only controls where dlt's incremental cursor starts **the
first time** a given resource has no prior watermark state (i.e., right
after `hack/reset-pipeline.sh`, or the very first run ever). On every
subsequent run, dlt reads its own persisted high-water mark from the
bucket-side `_dlt_pipeline_state/` object (`docs/runbooks/
ingestion-pipeline.md` section 4) and resumes from there, **ignoring**
`initial_value` -- so setting `INGEST_INITIAL_JOB_INFO` on a run against a
pipeline that already has state does nothing by itself.

To force a real re-scan of an already-ingested window (the actual
backlog-drain use case: pull rows gen-1 dropped, gen-2 has never seen), the
override alone is not enough -- there are two supported paths, pick based
on scope:

- **Full reset, then backfill from a chosen start** (safe, simple, but
  reprocesses everything from that point forward): `hack/reset-pipeline.sh`
  (wipes the catalog + bucket-side dlt state for the WHOLE `alice`
  namespace, all tables), then run with the `INGEST_INITIAL_*` override(s)
  set to the desired start. Every row from that point on gets re-fetched
  and re-merged. Safe to do even against tables you don't intend to
  backfill, since a full re-ingest of an unmodified table just re-merges
  identical rows (see below) -- but it does mean a larger single run
  (re-scans everything, not just the gap).
- **Targeted re-run of a bounded window, no reset**: not directly supported
  by `initial_value` alone (per the ignored-after-first-run behavior
  above). If gen-1's backlog only covers a KNOWN bounded historical range
  and gen-2's tables already have later data, a full reset + backfill from
  the historical start is still the straightforward option (it will
  re-touch the later, already-correct rows too, harmlessly -- see below)
  unless the reprocessing cost of the full range is prohibitive, in which
  case this needs a scoped SQL predicate change in `pipeline.py`'s
  `build_*` functions rather than an env-only mechanism -- out of scope
  for this env-var hook, flag it if it comes up.

Either way, **re-running over rows already in Iceberg is safe and
idempotent for the three tables `run-nightly` touches**: `job_info`,
`trace`, and `mon_jdls_parsed` all use `write_disposition=
{"disposition": "merge", "strategy": "upsert"}` on `primary_key="job_id"`.
Verified live (Task 3): running the same ingest twice back to back
produced identical row counts, identical distinct-`job_id` counts (no
duplicates), and identical Iceberg snapshot IDs on the second run. A
backfill window that overlaps already-ingested data merges cleanly -- it
does not duplicate rows, and a window with zero actually-new/changed rows
commits no new snapshot at all.

(This backfill mechanism does not apply to `site_sonar` in the first
place -- `run-sitesonar` is a separate subcommand from `run-nightly`, has
no `INGEST_INITIAL_*` override, and is append-only with its own
file-level high-water mark, not merge/upsert; see `docs/runbooks/
ingestion-pipeline.md` section 7 for its weaker crash-retry semantics
-- final-review N4, correcting this section's prior blanket "every table"
claim.)

## 4. After a backfill: watermark cleanup

A backfill Workflow's `pipeline.run()` calls persist their OWN
`_dlt_pipeline_state` entry, same as any other run -- the NEXT scheduled
`ingest-nightly` tick resumes from wherever the backfill run left the
cursor (i.e., the backfill's own high-water mark, not the pre-backfill
one). No manual watermark cleanup step is needed after a normal
(non-reset) backfill run; only the reset path (section 3, first bullet)
requires a deliberate `INGEST_INITIAL_*` value, since reset removes all
prior state.

## Snapshot retention vs migration (Plan 4)

The weekly `ingest-maintenance` CronWorkflow (`0 4 * * 0`,
`ingest/src/alice_ingest/maintenance.py`) expires Iceberg snapshots older
than **7 days** on both its pyiceberg pass (`MAINTENANCE_OLDER_THAN_DAYS`,
default `7`) and its Trino physical pass (`TRINO_MAINTENANCE_RETENTION_
THRESHOLD`, default `7d` -- `expire_snapshots`/`remove_orphan_files`). Once
a snapshot expires, Iceberg time-travel (`FOR VERSION AS OF` / `FOR
TIMESTAMP AS OF` queries, or a direct `catalog.load_table(...).scan(
snapshot_id=...)`) can no longer read it -- expiry is metadata deletion
(pyiceberg pass) followed by physical file GC (Trino's `remove_orphan_
files`), not a soft delete.

**This matters for Plan 4's dual-run comparison.** If the migration's
gen-1-vs-gen-2 comparison needs to time-travel across a window longer than
7 days (e.g. comparing gen-2's state as of the cutover date against an
earlier gen-2 snapshot from before the cutover, once more than a week has
elapsed), the routine weekly expiry will have already removed the
snapshots that comparison needs. Two supported options, pick one **before**
starting the dual-run window:

- **Pause the maintenance CronWorkflow for the migration period.** Suspend
  `ingest-maintenance` (`kubectl -n argo-workflows patch cronworkflow
  ingest-maintenance --type merge -p '{"spec":{"suspend":true}}'`, unsuspend
  the same way when the comparison window closes) -- simplest, no config
  drift to remember to revert, but also means NO maintenance runs at all
  during the pause (snapshot accumulation, not just the ones Plan 4 cares
  about, continues unbounded for the pause's duration -- fine for a bounded
  migration window, not for an indefinite pause).
- **Raise `TRINO_MAINTENANCE_RETENTION_THRESHOLD` beforehand** (env var,
  already supported by `maintenance.py`'s `run_trino()` -- see
  `docs/runbooks/ingestion-pipeline.md`'s env-var table) to a value that
  comfortably covers the comparison window, e.g. `30d`, set on the
  `ingest-env` Secret/ExternalSecret before the window opens and reverted
  to `7d` (or unset, since `7d` is the default) after. This keeps
  maintenance running on its normal schedule while simply expiring less
  aggressively -- preferable to a full pause if the migration window is
  long or its exact end date is uncertain. Note this env var only
  overrides the TRINO-side `TRINO_MAINTENANCE_RETENTION_THRESHOLD` argument
  passed to `expire_snapshots`/`remove_orphan_files` -- it does not touch
  the PYICEBERG-side `MAINTENANCE_OLDER_THAN_DAYS` cutoff, which has its own
  separate env var and defaults to the same `7` if not also raised. Raise
  both together if the pyiceberg-side expiry pass also needs to preserve
  the longer window.

**Going the other direction never applies here, but is worth knowing
about.** `TRINO_MAINTENANCE_RETENTION_THRESHOLD` can only be raised freely
-- Trino's Iceberg connector enforces a SERVER-SIDE minimum retention floor
via two connector config properties, `iceberg.expire-snapshots.min-
retention` and `iceberg.remove-orphan-files.min-retention`, both defaulting
to `7d` on this cluster's pinned Trino 476 (verified live: `retention_
threshold => '0s'` fails with `INVALID_PROCEDURE_ARGUMENT`, `'7d'` -- 
exactly at the floor -- succeeds; see `maintenance.py`'s module docstring,
"Retention floor" section, for the full verification trail). Production's
default `7d` threshold already sits exactly at that floor, so this doesn't
affect either option above. It would only matter if some future need went
BELOW 7 days: that requires a matching catalog-config bump (raising the
connector properties, a Trino chart/values change, not an env var this
codebase exposes) alongside a per-session override for a one-off run
(`SET SESSION lake.expire_snapshots_min_retention = '<duration>'` and `SET
SESSION lake.remove_orphan_files_min_retention = '<duration>'` --
catalog-scoped session properties, property name prefixed with the
CATALOG name `lake`, not the connector name `iceberg`; proven live during
Plan 3 Task 3's physical-GC verification, not wired into any code path).
Not needed for Plan 4's dual-run comparison (which only ever needs to
RAISE retention, never lower it), documented here only because raising and
lowering the floor look symmetric at first glance and are not.
