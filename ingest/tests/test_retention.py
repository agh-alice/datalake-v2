"""TDD for alice_ingest.retention (Plan 2 Task 4).

Written BEFORE alice_ingest/retention.py exists -- first run must be RED
(collection error: `ModuleNotFoundError: No module named 'alice_ingest.retention'`).

Scope: unit-test the PRESENCE-CHECK / delete-orchestration logic
(`classify_and_delete`) against a fake catalog interface (brief's explicit
ask) and a fake PG connection -- no live Postgres or Lakekeeper needed.
There is no local Postgres available in this test environment, so the age
predicate's actual SQL text is asserted structurally (parameterized, correct
comparison direction) rather than executed against a real cutoff boundary;
end-to-end date-boundary behavior is verified separately against the live
kind fixture (Plan 2 Task 4 acceptance run, see task-4-report.md).

The three cases from the brief map onto `classify_and_delete` as follows:
  1. "older than cutoff AND present in Iceberg -> deleted": job_id present
     in the fake catalog's `present` set is deleted (batched DELETE,
     bounded per-batch commit).
  2. "older but MISSING from Iceberg -> kept + counted + logged": job_id
     absent from the fake catalog's `present` set is left alone (no DELETE
     issued for it), counted into both `kept` and `unverified`, and a
     warning is logged (captured via caplog).
  3. "younger -> untouched": younger-than-cutoff rows never reach
     `classify_and_delete` in the first place -- `select_old_job_ids`'s SQL
     WHERE clause excludes them (structurally asserted below: parameterized
     cutoff, strict `<` comparison). From `classify_and_delete`'s side this
     is exactly the empty-candidate-list case: no catalog call, no cursor
     call, no commit -- asserted explicitly.
"""

from __future__ import annotations

import logging
from datetime import datetime

import pytest

from alice_ingest.retention import (
    BATCH_SIZE,
    TABLES,
    TableResult,
    classify_and_delete,
    exit_code,
    format_summary,
    retain_table,
    run_retention_pass,
    select_old_job_ids,
)


class FakePresenceCatalog:
    """Fake `IcebergPresenceCatalog`: canned present-set, records calls."""

    def __init__(self, present):
        self._present = set(present)
        self.calls = []

    def present_job_ids(self, iceberg_table, job_ids):
        job_ids = list(job_ids)
        self.calls.append((iceberg_table, job_ids))
        return self._present & set(job_ids)


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self._conn.executed.append((sql, params))
        # DELETE ... WHERE job_id = ANY(%s) form: params == (batch_list,).
        self.rowcount = len(params[0])


class FakeConnection:
    """Fake connection for classify_and_delete: records DELETE calls and
    commits, never touches a real database."""

    def __init__(self):
        self.executed = []
        self.commits = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1


class FakeSelectCursor(FakeCursor):
    def __init__(self, conn, rows):
        super().__init__(conn)
        self._rows = rows

    def execute(self, sql, params):
        # This fake connection is reused for both the SELECT
        # (select_old_job_ids, params == (cutoff,)) and the DELETE
        # (classify_and_delete, params == (batch_list,)) -- only compute a
        # DELETE-shaped rowcount when params[0] is actually a batch
        # (list/tuple); a SELECT's scalar cutoff just gets recorded, and
        # fetchall() below returns the canned rows for that path.
        self._conn.executed.append((sql, params))
        if params and isinstance(params[0], (list, tuple)):
            self.rowcount = len(params[0])

    def fetchall(self):
        return self._rows


class FakeSelectConnection(FakeConnection):
    """Fake connection for select_old_job_ids: cursor().fetchall() returns
    canned rows regardless of the real SQL text (no real DB available)."""

    def __init__(self, rows):
        super().__init__()
        self._rows = rows

    def cursor(self):
        return FakeSelectCursor(self, self._rows)


JOB_INFO_TABLE = next(t for t in TABLES if t.landing_table == "job_info")
TRACE_TABLE = next(t for t in TABLES if t.landing_table == "trace")
MON_JDLS_TABLE = next(t for t in TABLES if t.landing_table == "mon_jdls")


class TestClassifyAndDeletePresenceCheckCases:
    """The three cases named explicitly in the brief (Step 1)."""

    def test_older_and_present_in_iceberg_is_deleted(self):
        conn = FakeConnection()
        catalog = FakePresenceCatalog(present={1, 2, 3})

        result = classify_and_delete(conn, catalog, JOB_INFO_TABLE, [1, 2, 3])

        assert result == TableResult(kept=0, deleted=3, unverified=0)
        assert conn.commits == 1
        assert catalog.calls == [(JOB_INFO_TABLE.iceberg_table, [1, 2, 3])]

    def test_older_but_missing_from_iceberg_is_kept_counted_and_logged(self, caplog):
        conn = FakeConnection()
        catalog = FakePresenceCatalog(present=set())

        with caplog.at_level(logging.WARNING):
            result = classify_and_delete(conn, catalog, JOB_INFO_TABLE, [10, 11])

        assert result == TableResult(kept=2, deleted=0, unverified=2)
        assert conn.executed == []  # nothing deleted
        assert conn.commits == 0
        assert any(
            "unverified" in rec.message.lower() and "10" in rec.message and "11" in rec.message
            for rec in caplog.records
        )

    def test_younger_rows_never_enter_the_candidate_set_are_untouched(self):
        # Equivalent to "younger -> untouched" from classify_and_delete's
        # perspective: an empty candidate list (what select_old_job_ids
        # would return when every row is younger than cutoff) triggers no
        # catalog lookup, no cursor use, no commit.
        conn = FakeConnection()
        catalog = FakePresenceCatalog(present={1, 2, 3})

        result = classify_and_delete(conn, catalog, JOB_INFO_TABLE, [])

        assert result == TableResult(kept=0, deleted=0, unverified=0)
        assert catalog.calls == []
        assert conn.executed == []
        assert conn.commits == 0

    def test_mixed_batch_deletes_verified_and_keeps_unverified_separately(self):
        conn = FakeConnection()
        catalog = FakePresenceCatalog(present={1, 3})

        result = classify_and_delete(conn, catalog, JOB_INFO_TABLE, [1, 2, 3, 4])

        assert result == TableResult(kept=2, deleted=2, unverified=2)


class TestClassifyAndDeleteBatching:
    """Bounded transactions: one commit per <=BATCH_SIZE delete batch."""

    def test_default_batch_size_is_ten_thousand(self):
        assert BATCH_SIZE == 10_000

    def test_delete_batches_and_commits_per_batch(self, monkeypatch):
        import alice_ingest.retention as retention_module

        monkeypatch.setattr(retention_module, "BATCH_SIZE", 2)
        conn = FakeConnection()
        catalog = FakePresenceCatalog(present={1, 2, 3, 4, 5})

        result = classify_and_delete(conn, catalog, JOB_INFO_TABLE, [1, 2, 3, 4, 5])

        assert result == TableResult(kept=0, deleted=5, unverified=0)
        assert conn.commits == 3  # batches of 2, 2, 1
        assert len(conn.executed) == 3


class TestPerTableSqlDiscipline:
    """No f-string interpolation (brief: 'that's a gen-1 disease') --
    cutoff/job_id values must travel as query parameters, never formatted
    into the SQL text."""

    @pytest.mark.parametrize("table", TABLES, ids=[t.landing_table for t in TABLES])
    def test_age_sql_is_parameterized_and_strictly_less_than(self, table):
        assert "%s" in table.age_sql
        assert "<" in table.age_sql
        assert ">" not in table.age_sql  # strictly "older than", not "since"
        assert "{" not in table.age_sql and "}" not in table.age_sql

    @pytest.mark.parametrize("table", TABLES, ids=[t.landing_table for t in TABLES])
    def test_delete_sql_is_parameterized(self, table):
        assert "%s" in table.delete_sql
        assert "{" not in table.delete_sql and "}" not in table.delete_sql

    def test_select_old_job_ids_passes_cutoff_as_a_param_not_interpolated(self):
        rows = [(1,), (2,), (3,)]
        conn = FakeSelectConnection(rows)
        cutoff = datetime(2026, 1, 1)
        cutoffs = {"naive": cutoff, "aware": cutoff}

        result = select_old_job_ids(conn, JOB_INFO_TABLE, cutoffs)

        assert result == [1, 2, 3]
        sql, params = conn.executed[0]
        assert params == (cutoff,)
        assert "2026" not in sql  # cutoff never string-formatted into SQL text


class TestTableRegistry:
    """Per-table cutoff wiring documented in the brief's Context section."""

    def test_tables_map_landing_to_correct_iceberg_identifiers(self):
        mapping = {t.landing_table: t.iceberg_table for t in TABLES}
        assert mapping == {
            "job_info": "alice.job_info",
            "trace": "alice.trace",
            "mon_jdls": "alice.mon_jdls_parsed",  # dlt table_name mapping (pipeline.py)
        }

    def test_job_info_uses_naive_cutoff_on_its_own_last_update_column(self):
        assert JOB_INFO_TABLE.cutoff_kind == "naive"
        assert "last_update" in JOB_INFO_TABLE.age_sql

    def test_trace_uses_aware_cutoff_on_its_own_epoch_ms_column(self):
        # trace has NO last_update in production -- its own
        # laststatuschangetimestamp (epoch MILLISECONDS, verified against
        # both the live kind fixture and documented production convention;
        # see retention.py's module docstring) is the age signal.
        assert TRACE_TABLE.cutoff_kind == "aware"
        assert "laststatuschangetimestamp" in TRACE_TABLE.age_sql
        assert "1000" in TRACE_TABLE.age_sql  # ms -> s conversion

    def test_mon_jdls_has_no_timestamp_of_its_own_and_joins_to_job_info(self):
        # mon_jdls carries no timestamp column at all (production ground
        # truth: job_id, lpmjobtypeid, full_jdl) -- age is derived via a
        # join to job_info.last_update on job_id (documented choice).
        assert MON_JDLS_TABLE.cutoff_kind == "naive"
        assert "join" in MON_JDLS_TABLE.age_sql.lower()
        assert "job_info" in MON_JDLS_TABLE.age_sql.lower()
        assert "last_update" in MON_JDLS_TABLE.age_sql


class TestRetainTableComposesSelectAndClassify:
    def test_retain_table_end_to_end_with_fakes(self):
        rows = [(10,), (20,)]
        conn = FakeSelectConnection(rows)
        catalog = FakePresenceCatalog(present={10})
        cutoffs = {"naive": datetime(2026, 1, 1), "aware": datetime(2026, 1, 1)}

        result = retain_table(conn, catalog, JOB_INFO_TABLE, cutoffs)

        assert result == TableResult(kept=1, deleted=1, unverified=1)


class _FakeMultiTableCursor:
    """Routes SELECT results by matching the landing-table name embedded
    in the SQL text (each age_sql references its own FROM table by name);
    behaves like FakeCursor for DELETEs. Distinguishes mon_jdls's join SQL
    (which also contains the substring "job_info") by checking the more
    specific table names first."""

    _TABLE_NAME_PRIORITY = ("mon_jdls", "trace", "job_info")

    def __init__(self, conn):
        self._conn = conn
        self.rowcount = 0
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self._conn.executed.append((sql, params))
        if sql.strip().upper().startswith("SELECT"):
            for table_name in self._TABLE_NAME_PRIORITY:
                if table_name in sql:
                    self._rows = self._conn.rows_by_table.get(table_name, [])
                    return
            self._rows = []
        else:
            self.rowcount = len(params[0])

    def fetchall(self):
        return self._rows


class FakeMultiTableConnection:
    """Fake connection spanning all three landing tables, used ONLY to
    assert cross-table operation ORDERING in run_retention_pass -- a
    single-table fake can't exercise this (see the regression test
    below)."""

    def __init__(self, rows_by_table: dict[str, list[tuple]]):
        self.rows_by_table = rows_by_table
        self.executed: list[tuple] = []
        self.commits = 0

    def cursor(self):
        return _FakeMultiTableCursor(self)

    def commit(self):
        self.commits += 1


class TestRunRetentionPassTwoPhaseOrdering:
    """Regression for the bug found on Task 4's first live nightly
    CronWorkflow trigger: mon_jdls's age predicate JOINs to job_info.
    last_update (mon_jdls has no timestamp of its own). Deleting job_info's
    old rows before mon_jdls's SELECT ran made the join lose its basis --
    mon_jdls's candidate set silently came back empty, and its equally-old
    rows were never deleted (nor counted as kept/unverified). Two-phase
    execution (collect every table's candidates, THEN delete) fixes this
    regardless of table processing order."""

    def test_all_tables_selected_before_any_table_is_deleted(self):
        rows_by_table = {
            "job_info": [(1,), (2,)],
            "trace": [(1,), (2,)],
            "mon_jdls": [(1,), (2,)],
        }
        conn = FakeMultiTableConnection(rows_by_table)
        catalog = FakePresenceCatalog(present={1, 2})
        cutoffs = {"naive": datetime(2026, 1, 1), "aware": datetime(2026, 1, 1)}

        results = run_retention_pass(conn, catalog, cutoffs)

        assert results["job_info"] == TableResult(kept=0, deleted=2, unverified=0)
        assert results["trace"] == TableResult(kept=0, deleted=2, unverified=0)
        assert results["mon_jdls"] == TableResult(kept=0, deleted=2, unverified=0)

        # The first 3 executed statements are all SELECTs (one per table);
        # no DELETE appears before every table's SELECT has run.
        is_select = [sql.strip().upper().startswith("SELECT") for sql, _ in conn.executed]
        assert is_select[:3] == [True, True, True]
        assert False in is_select[3:]  # the DELETEs do eventually happen

    def test_mon_jdls_join_still_finds_rows_after_job_info_rows_are_gone(self):
        # The specific failure mode: mon_jdls's SELECT must be evaluated
        # against job_info as it stood BEFORE this pass's deletes, not
        # after -- simulated here by mon_jdls's fake rows being populated
        # independently of job_info's fake rows (a real join would break
        # if job_info's matching rows were deleted first; two-phase
        # collection means that never happens within one pass).
        rows_by_table = {
            "job_info": [(1,)],
            "trace": [],
            "mon_jdls": [(1,)],
        }
        conn = FakeMultiTableConnection(rows_by_table)
        catalog = FakePresenceCatalog(present={1})
        cutoffs = {"naive": datetime(2026, 1, 1), "aware": datetime(2026, 1, 1)}

        results = run_retention_pass(conn, catalog, cutoffs)

        assert results["mon_jdls"] == TableResult(kept=0, deleted=1, unverified=0)


class TestTableResultAndSummary:
    def test_table_result_addition_sums_fields(self):
        a = TableResult(kept=1, deleted=2, unverified=1)
        b = TableResult(kept=3, deleted=4, unverified=3)
        assert a + b == TableResult(kept=4, deleted=6, unverified=4)

    def test_format_summary_matches_brief_format(self):
        total = TableResult(kept=3, deleted=7, unverified=3)
        assert format_summary(total) == "RETENTION kept=3 deleted=7 unverified=3"

    def test_exit_code_zero_when_no_unverified(self):
        assert exit_code(TableResult(kept=0, deleted=5, unverified=0)) == 0

    def test_exit_code_nonzero_when_unverified_present(self):
        # The alarm signal (Task 5): any unverified row must fail the job.
        assert exit_code(TableResult(kept=2, deleted=5, unverified=2)) == 1
