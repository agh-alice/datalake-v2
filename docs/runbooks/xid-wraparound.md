# Runbook: PostgreSQL transaction-ID (XID) wraparound

Incident this derives from: **E4** (design doc §1) — the Gen-1 landing PostgreSQL
hit transaction-ID wraparound protection around 2026-05-10 and refused all
writes for roughly two months before anyone noticed. Root cause: two small
orphaned temp tables (`pg_temp_22.ins_a`, `pg_temp_22.snap_a`) belonging to one
of MLClient's ~20 long-lived, never-disconnecting backend connections. A
session-owned temp table is only ever vacuumed by its own session or dropped
when that session ends; because the connection stayed open indefinitely, the
table's frozen-XID horizon never advanced, which pinned the whole database's
`datfrozenxid` and stalled autovacuum's freeze progress until the safety
limit was hit.

In this v2 platform, `landing-db` (CNPG cluster `mon-data`) is the equivalent
component. `LandingDBXidAgeHigh` (`chart/templates/datalake-alerts.yaml`)
fires at `cnpg_pg_database_xid_age > 1e9` for 30m specifically so this class
of incident is caught early instead of silently, as it was in Gen-1.

**Source:** The complete technical incident record and recovery procedure are documented in `alice-datalake-pepeline-redesign/research/2026-07-11_cluster-state-diagnosis.md`, section "The XID-wraparound incident and recovery (2026-07-12)". See also design doc E4 for context on the Gen-1 incident.

## Symptom

Writes (and eventually all new transactions) fail with:

```
ERROR:  database is not accepting commands to avoid wraparound data loss in database "<dbname>"
HINT:  Stop the postmaster and vacuum that database in single-user mode.
```

Before the hard stop is reached, PostgreSQL logs escalating warnings for
~2M transactions of runway:

```
WARNING:  database "<dbname>" must be vacuumed within <N> transactions
```

If the `LandingDBXidAgeHigh` alert fires first, treat it as this incident
starting — don't wait for the hard-stop error.

## Diagnosis

1. **Per-database XID age** — run on any reachable database (superuser reads
   still work even once writes are blocked):

   ```sql
   SELECT datname, age(datfrozenxid) AS xid_age
   FROM pg_database
   ORDER BY xid_age DESC;
   ```

   Cross-check against the metric backing the alert:

   ```
   max(cnpg_pg_database_xid_age)
   ```

   in Prometheus/Grafana (`monitoring` namespace).

2. **Orphaned temp-table check** — list every session-temp object still
   present, and see which backend (if any) owns its schema:

   ```sql
   SELECT n.nspname, c.relname, pg_size_pretty(pg_total_relation_size(c.oid))
   FROM pg_class c
   JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname LIKE 'pg_temp%'
   ORDER BY n.nspname, c.relname;
   ```

   ```sql
   SELECT pid, usename, application_name, state, backend_start, xact_start
   FROM pg_stat_activity
   WHERE backend_type = 'client backend'
   ORDER BY backend_start;
   ```

   A `pg_temp_N` schema whose owning backend PID is long-lived (or no longer
   present in `pg_stat_activity`, i.e. a leaked/orphaned temp table from a
   backend that died without a clean disconnect) is the pattern that caused
   E4.

3. **Other classes of blocker** — any of these can independently pin the
   freeze horizon and must be cleared too:

   ```sql
   -- Unresolved two-phase-commit transactions
   SELECT * FROM pg_prepared_xacts;

   -- Replication slots that stopped advancing
   SELECT slot_name, active, restart_lsn, xmin, catalog_xmin
   FROM pg_replication_slots;

   -- Long-running / idle-in-transaction sessions holding an old snapshot
   SELECT pid, usename, state, age(backend_xmin) AS xmin_age, xact_start, query
   FROM pg_stat_activity
   WHERE backend_xmin IS NOT NULL
   ORDER BY xmin_age DESC
   LIMIT 20;
   ```

## Recovery

**⚠️ CRITICAL FIRST STEP:** Pause or scale down the MLClient writing process (or any other persistent client creating session-scoped temp tables) BEFORE terminating its backend connection. Persistent connections will immediately reconnect and re-occupy the backend slot, masking the orphaned temp tables from autovacuum's cleanup and keeping them pinned to the old XID horizon.

1. **Terminate and drop the orphans.** If the offending backend is still
   connected, terminate it first, then drop its temp tables (superusers can
   drop another session's temp objects by schema-qualified name):

   ```sql
   SELECT pg_terminate_backend(<pid>);
   DROP TABLE pg_temp_22.ins_a, pg_temp_22.snap_a;
   ```

   Clear any prepared transactions or stale replication slots found in
   diagnosis step 3 the same way (`ROLLBACK PREPARED '<gid>';` /
   `SELECT pg_drop_replication_slot('<slot>');`) after confirming they are
   genuinely abandoned, not mid-flight.

2. **If the database is already stop-limited** (the hard-stop error above,
   not just the warning), normal connections that would consume a new XID
   are refused — this includes DROP/DELETE from a regular session. Stop the
   postmaster and connect in single-user mode, which is exempted from the
   limit specifically to allow this remediation:

   ```bash
   pg_ctl stop -D "$PGDATA"
   postgres --single -D "$PGDATA" <dbname>
   ```

   At the single-user prompt, drop the orphans (statement terminated by a
   blank line, no explicit `;` needed) and quit with Ctrl-D:

   ```
   DROP TABLE pg_temp_22.ins_a;
   DROP TABLE pg_temp_22.snap_a;
   ```

   Then start the postmaster normally again. On a CNPG-managed cluster,
   `pg_ctl`/`postgres --single` are run inside the primary pod
   (`kubectl -n landing-db exec -it <primary-pod> -- ...`); coordinate with
   CNPG's operator before stopping the postmaster directly, since it manages
   the process lifecycle.

3. **Freeze the whole cluster.** Once every blocker is cleared, force a
   cluster-wide freeze rather than waiting for autovacuum to catch up on its
   own schedule:

   ```bash
   vacuumdb --all --freeze
   ```

4. **Verify.** Re-run the per-database age query from Diagnosis step 1 and
   confirm ages have dropped by orders of magnitude (the E4 recovery took
   `datfrozenxid` age from ~2.14B down to single digits). Confirm the
   `LandingDBXidAgeHigh` alert is `inactive` in Prometheus/Alertmanager.
   Confirm application writes resume (e.g. `job_info.last_update` advancing,
   for the MLClient ingest path this incident traces to).

## Prevention

- `LandingDBXidAgeHigh` (critical, 30m) is the primary guard — it exists so
  this incident is caught within the hour, not two months later. Treat any
  firing of this alert as page-worthy, not a background warning.
- Investigate any client that holds long-lived connections and creates
  session-scoped temp tables (MLClient's pattern in the E4 incident); prefer
  connection pooling with bounded session lifetime, and set
  `idle_in_transaction_session_timeout` so a stuck session can't pin the
  freeze horizon indefinitely.
- Watch `pg_stat_activity` for backends with unusually large `xmin_age`
  during routine operations review, not just when the alert fires — it is a
  leading indicator before `datfrozenxid` age crosses the alert threshold.
