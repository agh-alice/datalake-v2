#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Fixture seed for Plan 2 Task 3's kind integration proof: creates job_info,
# mon_jdls, trace in the kind mon-data (landing) DB with ~1000 synthetic rows
# per table, matching PRODUCTION'S ACTUAL SHAPES per research/2026-07-12_
# ml-consumer-data-contract.md's "Production schema ground truth" section
# (verified live 2026-07-12 against information_schema on mon_data --
# authoritative over the earlier de-facto-contract-only column list):
#   job_info (7 cols): job_id bigint, jdl_set bool, trace_set bool,
#     status text, job_submit_timestamp bigint (epoch), last_update
#     timestamp WITHOUT time zone (naive -- NOT timestamptz; see
#     ingest/src/alice_ingest/pipeline.py's module docstring for why the
#     aware/naive distinction matters for incremental pushdown), site
#     varchar.
#   trace (EXACTLY the 18 production columns, review fix -- a prior version
#     of this fixture added a fictitious `last_update timestamptz` column
#     that does not exist in production and does not match the pipeline's
#     actual incremental hint): job_id, aliencpuefficiency numeric, cputime
#     numeric, host text, maxrss bigint, cpuefficiency text, finaltimestamp
#     bigint (epoch), masterjobid bigint, pid bigint, requestedcpus integer,
#     requestedttl bigint, runningtimestamp bigint (epoch), savingtimestamp
#     bigint (epoch), startedtimestamp bigint (epoch), walltime integer,
#     maxvirt bigint, site text, laststatuschangetimestamp bigint (epoch --
#     this is trace's incremental cursor per pipeline.py's build_trace_source;
#     values spread over the same 30-day window as the other timestamps so
#     the incremental extraction actually has variance to exercise across
#     reruns/retention, not a single constant. UNIT FIX (Plan 2 Task 4):
#     laststatuschangetimestamp is stored as epoch MILLISECONDS in
#     production (gen-1 Java convention, per the Task 4 brief) -- this
#     fixture previously emitted `extract(epoch FROM ts)::bigint`, epoch
#     SECONDS, verified empirically against the live kind fixture (sample
#     value ~1.78e9, same magnitude as `extract(epoch FROM now())`, not
#     ~1.78e12 as a true ms value would be). retention.py's trace-table
#     cutoff predicate must match real production semantics
#     (`to_timestamp(laststatuschangetimestamp / 1000.0)`), so the
#     mismatch is fixed at the source here (x1000) rather than papered
#     over with a magnitude-sniffing heuristic in retention code that
#     would never see real production data.).
#   mon_jdls (3 cols, not 2 -- review fix, ground truth doc corrected the
#     brief's original 2-col assumption): job_id bigint, lpmjobtypeid
#     varchar, full_jdl text -- full_jdl includes JDL text with BOTH
#     LPMPassName and LPMPASSNAME casings (contract's documented mixed-case
#     quality defect, now merged at ingestion by ingest/src/alice_ingest/
#     jdl.py's LPM-casing coalesce -- design spec section 4) and 2
#     deliberately corrupt (unparseable) JDLs, so jdl.py's never-drop-a-row
#     behavior gets exercised end to end.
#
# Runs via `kubectl exec -i` into the mon-data-1 CNPG pod as the `postgres`
# superuser over the pod's local unix socket (PGHOST=/controller/run baked
# into the CNPG image's env -- verified interactively, Task 3): no password
# needed, and no credential ever touches this host script, matching the
# in-cluster-only credential flow convention already used by
# hack/lakekeeper-warehouse.sh. Tables are created/owned by mon_user (the
# ingest job's PG_URL role, per landing-db.yaml's bootstrap.initdb.owner) so
# the ingestion Workflow can read them without extra GRANTs.
#
# Idempotent: CREATE TABLE IF NOT EXISTS, then TRUNCATE + deterministic
# INSERT ... SELECT FROM generate_series (setseed() fixes the random() draws)
# -- reruns converge to the identical 1000-row fixture rather than
# accumulating duplicates. NOTE: idempotent means "converges to the same
# rows", not "column set is fixed" -- IF NOT EXISTS does not migrate an
# existing table's columns. A cluster seeded by a pre-review version of this
# script (with trace.last_update, mon_jdls's old 2-col shape) must have its
# PG fixture tables dropped before rerunning this script so CREATE TABLE
# actually re-lays-out the production-parity schema.
MD_PRIMARY=$(kubectl -n landing-db get cluster mon-data -o jsonpath='{.status.currentPrimary}')
kubectl -n landing-db exec -i "$MD_PRIMARY" -- psql -U postgres -d mon_data -v ON_ERROR_STOP=1 <<'SQL'
SELECT setseed(0.42);

CREATE TABLE IF NOT EXISTS job_info (
  job_id                 bigint PRIMARY KEY,
  jdl_set                boolean,
  trace_set              boolean,
  status                 text,
  job_submit_timestamp   bigint,
  last_update            timestamp,
  site                   varchar
);

CREATE TABLE IF NOT EXISTS trace (
  job_id                       bigint PRIMARY KEY,
  aliencpuefficiency           numeric,
  cputime                      numeric,
  host                         text,
  maxrss                       bigint,
  cpuefficiency                text,
  finaltimestamp               bigint,
  masterjobid                  bigint,
  pid                          bigint,
  requestedcpus                integer,
  requestedttl                 bigint,
  runningtimestamp             bigint,
  savingtimestamp              bigint,
  startedtimestamp             bigint,
  walltime                     integer,
  maxvirt                      bigint,
  site                         text,
  laststatuschangetimestamp    bigint
);

CREATE TABLE IF NOT EXISTS mon_jdls (
  job_id        bigint,
  lpmjobtypeid  varchar,
  full_jdl      text
);

ALTER TABLE job_info OWNER TO mon_user;
ALTER TABLE trace    OWNER TO mon_user;
ALTER TABLE mon_jdls OWNER TO mon_user;

TRUNCATE job_info, trace, mon_jdls;

INSERT INTO job_info (job_id, jdl_set, trace_set, status, job_submit_timestamp, last_update, site)
SELECT
  gs,
  true,
  (gs % 20 <> 0),
  (ARRAY['DONE','ERROR_V','ERROR_E','RUNNING','SAVED','KILLED'])[1 + gs % 6],
  extract(epoch FROM ts - ((gs % 6) || ' hours')::interval)::bigint,
  ts::timestamp,
  'ALICE::SITE_' || lpad((1 + gs % 25)::text, 2, '0')
FROM (
  SELECT gs, now() - ((gs % 30) || ' days')::interval - ((gs * 7 % 24) || ' hours')::interval AS ts
  FROM generate_series(1, 1000) gs
) t;

INSERT INTO trace (job_id, aliencpuefficiency, cputime, host, maxrss, cpuefficiency,
  finaltimestamp, masterjobid, pid, requestedcpus, requestedttl, runningtimestamp,
  savingtimestamp, startedtimestamp, walltime, maxvirt, site, laststatuschangetimestamp)
SELECT
  gs,
  round((50 + random() * 50)::numeric, 2),
  round((100 + random() * 36000)::numeric, 2),
  'wn' || lpad((1 + gs % 200)::text, 4, '0') || '.site.alice',
  200000 + (gs % 500000),
  round((50 + random() * 50)::numeric, 2)::text,
  extract(epoch FROM ts - ((gs % 3) || ' hours')::interval)::bigint,
  90000000 + gs,
  10000 + gs % 50000,
  1 + gs % 8,
  3600 + (gs % 5) * 1800,
  extract(epoch FROM ts - ((gs % 5) || ' hours')::interval - interval '10 minutes')::bigint,
  extract(epoch FROM ts - ((gs % 3) || ' hours')::interval - interval '2 minutes')::bigint,
  extract(epoch FROM ts - ((gs % 5) || ' hours')::interval)::bigint,
  (100 + (random() * 36000))::integer,
  400000 + (gs % 800000),
  'ALICE::SITE_' || lpad((1 + gs % 25)::text, 2, '0'),
  (extract(epoch FROM ts) * 1000)::bigint
FROM (
  SELECT gs, now() - ((gs % 30) || ' days')::interval - ((gs * 7 % 24) || ' hours')::interval AS ts
  FROM generate_series(1, 1000) gs
) t;

-- mon_jdls: job_id 501 and 502 are deliberately corrupt (truncated/garbled
-- JSON -- jdl.py must flag jdl_parse_ok=false and preserve full_jdl_raw,
-- never drop the row). All others alternate LPMPassName (even job_id) /
-- LPMPASSNAME (odd job_id) casing per the contract's documented defect;
-- jdl.py's LPM-casing coalesce (design spec section 4) merges both into the
-- canonical `LPMPassName` key at parse time.
INSERT INTO mon_jdls (job_id, lpmjobtypeid, full_jdl)
SELECT
  gs,
  'LPMJT_' || lpad((1 + gs % 15)::text, 3, '0'),
  CASE
    WHEN gs = 501 THEN '{"TTL": "3600", "CPUCores": not valid json here'
    WHEN gs = 502 THEN '{"TTL": "1800", "CPUCores": "2", "Executable": "/alice/bin/aliroot"'
    ELSE jsonb_build_object(
      'TTL', (3600 + (gs % 5) * 600)::text,
      'CPUCores', (1 + gs % 8)::text,
      'CPULimit', (1.0 + (gs % 8))::text,
      'Executable', '/alice/bin/aliroot',
      'JobTag', 'prod_2026',
      'User', 'alidaq',
      'PWG', (ARRAY['PWGPP','PWGZZ','PWGCF','PWGHF'])[1 + gs % 4],
      'Packages', jsonb_build_array('AliPhysics::vAN-2026' || lpad((gs % 12 + 1)::text, 2, '0') || '01-1'),
      CASE WHEN gs % 2 = 0 THEN 'LPMPassName' ELSE 'LPMPASSNAME' END,
      'pass' || (1 + gs % 3),
      'CollisionSystem', (ARRAY['pp','PbPb','pPb'])[1 + gs % 3],
      'Requirements', 'member(other.GridPartitions,"alice")',
      'MemorySize', (2000 + gs % 4 * 500)::text
    )::text
  END
FROM generate_series(1, 1000) gs;

SELECT 'job_info' AS table_name, count(*) FROM job_info
UNION ALL SELECT 'trace', count(*) FROM trace
UNION ALL SELECT 'mon_jdls', count(*) FROM mon_jdls;
SQL
echo "seed-fixture.sh: OK"
