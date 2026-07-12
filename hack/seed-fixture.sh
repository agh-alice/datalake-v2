#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Fixture seed for Plan 2 Task 3's kind integration proof: creates job_info,
# mon_jdls, trace in the kind mon-data (landing) DB with ~1000 synthetic rows
# per table, shaped per the ML consumer's data contract (research/2026-07-12_
# ml-consumer-data-contract.md):
#   job_info (7 cols): job_id, jdl_set, trace_set, status, job_submit_timestamp,
#     last_update, site.
#   trace (contract lists 18 cols, none named last_update -- but
#     ingest/src/alice_ingest/pipeline.py's build_job_info_trace_source applies
#     the SAME `last_update` incremental hint to both job_info and trace. This
#     fixture therefore adds an explicit `last_update timestamptz` column to
#     trace beyond the contract's 18, as a deliberate redesign choice (a
#     generic incremental watermark instead of overloading
#     laststatuschangetimestamp) -- recorded here since it diverges from the
#     contract doc.
#   mon_jdls (2 cols, brief Step 1): job_id, full_jdl -- includes JDL text
#     with BOTH LPMPassName and LPMPASSNAME casings (contract's documented
#     mixed-case quality defect) and 2 deliberately corrupt (unparseable)
#     JDLs, so ingest/src/alice_ingest/jdl.py's never-drop-a-row behavior gets
#     exercised end to end.
# Timestamps spread over the last 30 days (last_update) so Task 4's retention
# job has old-enough rows to reap.
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
# accumulating duplicates.
MD_PRIMARY=$(kubectl -n landing-db get cluster mon-data -o jsonpath='{.status.currentPrimary}')
kubectl -n landing-db exec -i "$MD_PRIMARY" -- psql -U postgres -d mon_data -v ON_ERROR_STOP=1 <<'SQL'
SELECT setseed(0.42);

CREATE TABLE IF NOT EXISTS job_info (
  job_id                 bigint PRIMARY KEY,
  jdl_set                boolean,
  trace_set              boolean,
  status                 text,
  job_submit_timestamp   timestamptz,
  last_update            timestamptz,
  site                   text
);

CREATE TABLE IF NOT EXISTS trace (
  job_id                       bigint PRIMARY KEY,
  aliencpuefficiency           numeric,
  cputime                      numeric,
  host                         text,
  maxrss                       numeric,
  cpuefficiency                numeric,
  finaltimestamp               timestamptz,
  masterjobid                  bigint,
  pid                          integer,
  requestedcpus                integer,
  requestedttl                 integer,
  runningtimestamp             timestamptz,
  savingtimestamp              timestamptz,
  startedtimestamp             timestamptz,
  walltime                     numeric,
  maxvirt                      numeric,
  site                         text,
  laststatuschangetimestamp    timestamptz,
  last_update                  timestamptz  -- redesign addition, see header
);

CREATE TABLE IF NOT EXISTS mon_jdls (
  job_id    bigint,
  full_jdl  text
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
  ts - ((gs % 6) || ' hours')::interval,
  ts,
  'ALICE::SITE_' || lpad((1 + gs % 25)::text, 2, '0')
FROM (
  SELECT gs, now() - ((gs % 30) || ' days')::interval - ((gs * 7 % 24) || ' hours')::interval AS ts
  FROM generate_series(1, 1000) gs
) t;

INSERT INTO trace (job_id, aliencpuefficiency, cputime, host, maxrss, cpuefficiency,
  finaltimestamp, masterjobid, pid, requestedcpus, requestedttl, runningtimestamp,
  savingtimestamp, startedtimestamp, walltime, maxvirt, site, laststatuschangetimestamp, last_update)
SELECT
  gs,
  round((50 + random() * 50)::numeric, 2),
  round((100 + random() * 36000)::numeric, 2),
  'wn' || lpad((1 + gs % 200)::text, 4, '0') || '.site.alice',
  200000 + (gs % 500000),
  round((50 + random() * 50)::numeric, 2),
  ts - ((gs % 3) || ' hours')::interval,
  90000000 + gs,
  10000 + gs % 50000,
  1 + gs % 8,
  3600 + (gs % 5) * 1800,
  ts - ((gs % 5) || ' hours')::interval - interval '10 minutes',
  ts - ((gs % 3) || ' hours')::interval - interval '2 minutes',
  ts - ((gs % 5) || ' hours')::interval,
  round((100 + random() * 36000)::numeric, 2),
  400000 + (gs % 800000),
  'ALICE::SITE_' || lpad((1 + gs % 25)::text, 2, '0'),
  ts,
  ts
FROM (
  SELECT gs, now() - ((gs % 30) || ' days')::interval - ((gs * 7 % 24) || ' hours')::interval AS ts
  FROM generate_series(1, 1000) gs
) t;

-- mon_jdls: job_id 501 and 502 are deliberately corrupt (truncated/garbled
-- JSON -- jdl.py must flag jdl_parse_ok=false and preserve full_jdl_raw,
-- never drop the row). All others alternate LPMPassName (even job_id) /
-- LPMPASSNAME (odd job_id) casing per the contract's documented defect.
INSERT INTO mon_jdls (job_id, full_jdl)
SELECT
  gs,
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
