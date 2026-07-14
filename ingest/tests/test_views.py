"""TDD for `alice_ingest.contract_columns` (mapping invariants) and
`alice_ingest.views` (DDL builder + Trino REST statement-polling client +
`apply-views` orchestration), Plan 3 Task 2.

Written BEFORE `alice_ingest/views.py` exists -- first run must be RED
(collection error: `ModuleNotFoundError: No module named 'alice_ingest.views'`).
`alice_ingest/contract_columns.py` already exists (it is pure data, reviewed
directly -- see its module docstring for the verification trail: the actual
upstream dtypes.json fetched live via `gh api`, cross-checked against `SHOW
COLUMNS` run against this kind cluster, and dlt's own naming-convention
function run against all 66 JDL field names in a throwaway venv). This file's
`TestContractColumnsInvariants` section asserts the properties the brief
calls out explicitly (Step 2): no duplicate targets, LPM merge honored,
Packages maps to the JSON-serialized column.

Scope for the Trino client / DDL builder: unit tests only, against fakes /
canned response sequences -- no live Trino needed (matches this repo's
established seam pattern, e.g. test_retention.py's IcebergPresenceCatalog
Protocol fakes, test_maintenance.py's FakeCatalog/FakeTable). The view-storage
DECISION itself (Iceberg REST-catalog views work on Lakekeeper 0.12.2 +
Trino 476, proven by create + select-back + select-back-after-coordinator-
restart) was verified live against this cluster, 2026-07-12 -- see
`views.py`'s module docstring for that evidence; it is not re-proven here.
"""

from __future__ import annotations

import pytest

from alice_ingest.contract_columns import (
    JOB_INFO_COLUMNS,
    JOB_INFO_PASSTHROUGH,
    MON_JDLS_PARSED_COLUMNS,
    MON_JDLS_PARSED_NULL_TYPE,
    MON_JDLS_PARSED_PASSTHROUGH,
    TRACE_COLUMNS,
    TRACE_PASSTHROUGH,
)


# ---------------------------------------------------------------------------
# contract_columns.py invariants (brief, Step 2).
# ---------------------------------------------------------------------------


class TestContractColumnsInvariants:
    def test_job_info_is_a_pure_identity_mapping_with_seven_columns(self):
        assert len(JOB_INFO_COLUMNS) == 7
        assert JOB_INFO_PASSTHROUGH == ()
        for contract_name, dlt_name in JOB_INFO_COLUMNS.items():
            assert dlt_name == contract_name

    def test_trace_is_a_pure_identity_mapping_with_eighteen_columns(self):
        assert len(TRACE_COLUMNS) == 18
        assert TRACE_PASSTHROUGH == ()
        for contract_name, dlt_name in TRACE_COLUMNS.items():
            assert dlt_name == contract_name

    def test_mon_jdls_parsed_has_the_full_67_column_contract(self):
        # job_id + 66 named JDL fields (upstream dtypes.json, fetched live --
        # contract_columns.py's module docstring, source 1).
        assert len(MON_JDLS_PARSED_COLUMNS) == 67

    def test_mon_jdls_parsed_mapped_vs_nulled_counts(self):
        # Regenerated against the 2026-07-13 production sample (146,896 real
        # JDLs, research/2026-07-13_production-data-dress-rehearsal.md):
        # 64 of 66 JDL fields live (was 12 fixture-era).
        mapped = [v for v in MON_JDLS_PARSED_COLUMNS.values() if v is not None]
        nulled = [v for v in MON_JDLS_PARSED_COLUMNS.values() if v is None]
        assert len(mapped) == 65  # job_id + 64 live jdl__* columns
        assert len(nulled) == 2

    def test_still_nulled_fields_are_exactly_the_two_absent_from_production(self):
        nulled = {k for k, v in MON_JDLS_PARSED_COLUMNS.items() if v is None}
        assert nulled == {"HardBins", "MasterResubmitThreshold"}

    def test_lpm_pass_name_maps_from_the_merged_canonical_column(self):
        assert MON_JDLS_PARSED_COLUMNS["LPMPassName"] == "jdl__lpm_pass_name"

    def test_split_casing_lpm_column_must_not_appear_anywhere(self):
        # The exact regression this invariant guards: dlt's naming
        # convention normalizes the ALL-CAPS legacy spelling `LPMPASSNAME`
        # (no camelCase boundary to split) to `jdl__lpmpassname` -- a
        # DIFFERENT column from the canonical `jdl__lpm_pass_name`. jdl.py's
        # _merge_lpm_casing() coalesces both before dlt ever sees a record,
        # so this split-casing spelling must never appear as a mapping
        # target, and "LPMPASSNAME" must never appear as a contract key.
        assert "jdl__lpmpassname" not in MON_JDLS_PARSED_COLUMNS.values()
        assert "jdl__lpmpassname" not in MON_JDLS_PARSED_PASSTHROUGH
        assert "LPMPASSNAME" not in MON_JDLS_PARSED_COLUMNS

    def test_packages_maps_to_the_json_serialized_column(self):
        assert MON_JDLS_PARSED_COLUMNS["Packages"] == "jdl__packages"

    def test_no_duplicate_mapping_targets_within_mon_jdls_parsed(self):
        mapped = [v for v in MON_JDLS_PARSED_COLUMNS.values() if v is not None]
        assert len(mapped) == len(set(mapped))

    def test_passthrough_columns_never_overlap_a_mapped_target(self):
        mapped = {v for v in MON_JDLS_PARSED_COLUMNS.values() if v is not None}
        assert mapped.isdisjoint(MON_JDLS_PARSED_PASSTHROUGH)

    def test_passthrough_contains_the_six_dlt_only_extras_plus_production_fields(self):
        # The six pipeline/dlt bookkeeping columns, plus the 38 real
        # production JDL fields outside the 66-key dtypes contract found by
        # the 2026-07-13 dress rehearsal (each verified against dlt's real
        # normalizer to match NO contract field).
        base = {
            "lpmjobtypeid",
            "full_jdl",
            "jdl_parse_ok",
            "full_jdl_raw",
            "_dlt_load_id",
            "_dlt_id",
        }
        assert base <= set(MON_JDLS_PARSED_PASSTHROUGH)
        extras = set(MON_JDLS_PARSED_PASSTHROUGH) - base
        assert len(extras) == 38
        assert all(name.startswith("jdl__") for name in extras)

    def test_null_type_is_varchar_matching_every_live_sibling_column(self):
        assert MON_JDLS_PARSED_NULL_TYPE == "VARCHAR"


# ---------------------------------------------------------------------------
# views.py: DDL builder.
# ---------------------------------------------------------------------------


class TestBuildViewDdl:
    def test_identity_table_selects_every_column_quoted_and_aliased(self):
        from alice_ingest.views import build_view_ddl

        ddl = build_view_ddl("job_info", {"job_id": "job_id", "site": "site"})

        assert ddl.startswith('CREATE OR REPLACE VIEW lake.contract.job_info AS')
        assert 'FROM lake.alice.job_info' in ddl
        assert '"job_id" AS "job_id"' in ddl
        assert '"site" AS "site"' in ddl

    def test_mixed_case_contract_name_is_double_quoted(self):
        from alice_ingest.views import build_view_ddl

        ddl = build_view_ddl("mon_jdls_parsed", {"TTL": "jdl__ttl"})

        assert '"jdl__ttl" AS "TTL"' in ddl

    def test_unmapped_column_renders_as_a_typed_null_with_a_comment(self):
        from alice_ingest.views import build_view_ddl

        ddl = build_view_ddl("mon_jdls_parsed", {"Activity": None}, null_type="VARCHAR")

        assert 'CAST(NULL AS VARCHAR) AS "Activity"' in ddl
        assert "--" in ddl  # inline comment noting no dlt counterpart

    def test_passthrough_columns_appended_after_mapped_columns_no_alias(self):
        from alice_ingest.views import build_view_ddl

        ddl = build_view_ddl(
            "mon_jdls_parsed",
            {"job_id": "job_id"},
            passthrough=("lpmjobtypeid", "_dlt_id"),
        )

        job_id_pos = ddl.index('"job_id" AS "job_id"')
        passthrough_pos = ddl.index('"lpmjobtypeid"')
        assert job_id_pos < passthrough_pos
        assert '"_dlt_id"' in ddl
        # Passthrough columns keep their own dlt name -- no re-aliasing.
        assert '"lpmjobtypeid" AS' not in ddl

    def test_ddl_is_idempotent_create_or_replace(self):
        from alice_ingest.views import build_view_ddl

        ddl = build_view_ddl("trace", {"job_id": "job_id"})

        assert ddl.startswith("CREATE OR REPLACE VIEW")

    def test_raises_on_empty_column_set(self):
        from alice_ingest.views import build_view_ddl

        with pytest.raises(ValueError):
            build_view_ddl("empty", {})

    def test_comma_is_not_swallowed_by_a_trailing_comment(self):
        # Regression test: verified live against this cluster's Trino 476
        # that a comma placed AFTER a trailing `--` comment is swallowed
        # into the comment (comments run to end-of-line) and never reaches
        # the SQL parser -- SYNTAX_ERROR on the NEXT line's leading token.
        # A NULLed column followed by another column is exactly the shape
        # that broke: the separating comma must appear BEFORE `--`, on the
        # NULLed column's own line, not after it.
        from alice_ingest.views import build_view_ddl

        ddl = build_view_ddl(
            "mon_jdls_parsed",
            {"Activity": None, "TTL": "jdl__ttl"},
            null_type="VARCHAR",
        )

        activity_line = next(
            line for line in ddl.splitlines() if "Activity" in line
        )
        assert activity_line.rstrip().endswith(
            "-- no dlt counterpart in the live schema"
        )
        # The comma must appear strictly before the comment marker.
        comma_index = activity_line.index(",")
        comment_index = activity_line.index("--")
        assert comma_index < comment_index
        # And the next column must still be present on its own line.
        assert '"jdl__ttl" AS "TTL"' in ddl

    def test_real_mon_jdls_parsed_mapping_builds_without_error(self):
        # Integration-shaped unit test: the REAL mapping module against the
        # REAL builder, still no network -- just proves the two modules
        # agree on shape (67 contract columns + 6 passthrough all render).
        from alice_ingest.contract_columns import (
            MON_JDLS_PARSED_COLUMNS,
            MON_JDLS_PARSED_NULL_TYPE,
            MON_JDLS_PARSED_PASSTHROUGH,
        )
        from alice_ingest.views import build_view_ddl

        ddl = build_view_ddl(
            "mon_jdls_parsed",
            MON_JDLS_PARSED_COLUMNS,
            null_type=MON_JDLS_PARSED_NULL_TYPE,
            passthrough=MON_JDLS_PARSED_PASSTHROUGH,
        )

        assert ddl.count(' AS "') == 67  # every contract column aliased to "ContractName"
        assert '"jdl__lpm_pass_name" AS "LPMPassName"' in ddl
        # Activity is contract-mapped since the 2026-07-13 production
        # regeneration; the two fields absent from the production sample
        # are the ones still rendered as typed NULLs.
        assert '"jdl__activity" AS "Activity"' in ddl
        assert 'CAST(NULL AS VARCHAR) AS "HardBins"' in ddl
        assert 'CAST(NULL AS VARCHAR) AS "MasterResubmitThreshold"' in ddl
        assert '"_dlt_id"' in ddl


# ---------------------------------------------------------------------------
# views.py: minimal Trino REST statement-polling client, against canned
# response sequences (brief, Step 3 -- no live Trino).
# ---------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, json_body: dict, status_code: int = 200):
        self._json_body = json_body
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._json_body


class FakeRequests:
    """Records calls; returns canned responses for POST (first call) and
    GET (nextUri polls) in the order given -- mirrors this repo's existing
    Trino-probe polling shape (hack/kind-verify.sh)."""

    def __init__(self, post_response: FakeResponse, get_responses: list[FakeResponse]):
        self._post_response = post_response
        self._get_responses = list(get_responses)
        self.post_calls: list[tuple[str, str]] = []
        self.get_calls: list[str] = []

    def post(self, url, data=None, headers=None, timeout=None):
        self.post_calls.append((url, data))
        return self._post_response

    def get(self, url, timeout=None):
        self.get_calls.append(url)
        return self._get_responses.pop(0)


class TestTrinoClient:
    def test_single_shot_response_with_no_next_uri_returns_data(self, monkeypatch):
        from alice_ingest import views as views_module

        fake = FakeRequests(
            post_response=FakeResponse({"data": [[1]]}),
            get_responses=[],
        )
        monkeypatch.setattr(views_module, "requests", fake)

        client = views_module.TrinoClient(base_uri="http://trino.trino.svc:8080")
        rows = client.run("SELECT 1")

        assert rows == [[1]]
        assert fake.post_calls == [("http://trino.trino.svc:8080/v1/statement", "SELECT 1")]

    def test_follows_next_uri_chain_until_absent(self, monkeypatch):
        from alice_ingest import views as views_module

        fake = FakeRequests(
            post_response=FakeResponse({"data": [], "nextUri": "http://trino/poll/1"}),
            get_responses=[
                FakeResponse({"data": [[1]], "nextUri": "http://trino/poll/2"}),
                FakeResponse({"data": [[2]]}),
            ],
        )
        monkeypatch.setattr(views_module, "requests", fake)
        monkeypatch.setattr(views_module.time, "sleep", lambda _s: None)

        client = views_module.TrinoClient(base_uri="http://trino.trino.svc:8080")
        rows = client.run("SELECT * FROM t")

        assert rows == [[1], [2]]
        assert fake.get_calls == ["http://trino/poll/1", "http://trino/poll/2"]

    def test_error_field_raises_trino_query_error(self, monkeypatch):
        from alice_ingest import views as views_module

        fake = FakeRequests(
            post_response=FakeResponse({"error": {"message": "COLUMN_NOT_FOUND"}}),
            get_responses=[],
        )
        monkeypatch.setattr(views_module, "requests", fake)

        client = views_module.TrinoClient(base_uri="http://trino.trino.svc:8080")
        with pytest.raises(views_module.TrinoQueryError, match="COLUMN_NOT_FOUND"):
            client.run("SELECT bogus FROM t")

    def test_error_field_arriving_mid_poll_also_raises(self, monkeypatch):
        from alice_ingest import views as views_module

        fake = FakeRequests(
            post_response=FakeResponse({"data": [], "nextUri": "http://trino/poll/1"}),
            get_responses=[FakeResponse({"error": {"message": "boom"}})],
        )
        monkeypatch.setattr(views_module, "requests", fake)
        monkeypatch.setattr(views_module.time, "sleep", lambda _s: None)

        client = views_module.TrinoClient(base_uri="http://trino.trino.svc:8080")
        with pytest.raises(views_module.TrinoQueryError, match="boom"):
            client.run("SELECT * FROM t")

    def test_sends_x_trino_user_header(self, monkeypatch):
        from alice_ingest import views as views_module

        captured = {}

        class RecordingRequests(FakeRequests):
            def post(self, url, data=None, headers=None, timeout=None):
                captured["headers"] = headers
                return super().post(url, data=data, headers=headers, timeout=timeout)

        fake = RecordingRequests(post_response=FakeResponse({"data": []}), get_responses=[])
        monkeypatch.setattr(views_module, "requests", fake)

        client = views_module.TrinoClient(base_uri="http://trino.trino.svc:8080", user="alice-ingest")
        client.run("SELECT 1")

        assert captured["headers"]["X-Trino-User"] == "alice-ingest"


# ---------------------------------------------------------------------------
# views.py: apply_views() orchestration (schema create + one CREATE VIEW per
# contract table, in TABLE_SPECS order).
# ---------------------------------------------------------------------------


class FakeTrinoClient:
    def __init__(self):
        self.statements: list[str] = []

    def run(self, sql: str, poll_interval: float = 1.0):
        self.statements.append(sql)
        return []


class TestApplyViews:
    def test_creates_the_contract_schema_first(self):
        from alice_ingest.views import apply_views

        client = FakeTrinoClient()
        apply_views(client)

        assert client.statements[0] == "CREATE SCHEMA IF NOT EXISTS lake.contract"

    def test_applies_one_create_or_replace_view_per_contract_table(self):
        from alice_ingest.views import TABLE_SPECS, apply_views

        client = FakeTrinoClient()
        apply_views(client)

        view_statements = client.statements[1:]
        assert len(view_statements) == len(TABLE_SPECS) == 3
        for table in TABLE_SPECS:
            assert any(
                stmt.startswith(f"CREATE OR REPLACE VIEW lake.contract.{table} AS")
                for stmt in view_statements
            )

    def test_returns_the_executed_ddl_statements(self):
        from alice_ingest.views import apply_views

        client = FakeTrinoClient()
        executed = apply_views(client)

        assert executed == client.statements[1:]


class TestRun:
    def test_requires_trino_uri_env_var(self):
        from alice_ingest.views import run

        with pytest.raises(SystemExit, match="TRINO_URI"):
            run({})

    def test_applies_views_and_prints_a_summary(self, monkeypatch, capsys):
        from alice_ingest import views as views_module
        from alice_ingest.views import TABLE_SPECS

        applied = []
        # Supply the exact no-drift live schema for each table. A prior
        # version of this test returned [] for every statement and relied
        # on "vacuously no drift" -- that vacuous truth is gone by design
        # (2026-07-13 dress-rehearsal fix): an EMPTY live column list now
        # correctly reads as "every mapped column missing", the un-appliable
        # DDL condition, and aborts with exit 2 instead of pretending to
        # apply views over a schema that cannot satisfy them.
        columns_by_table = {table: _known_live_rows(spec) for table, spec in TABLE_SPECS.items()}

        class RecordingClient:
            def __init__(self, base_uri, user="alice-ingest", timeout=30.0):
                self.base_uri = base_uri

            def run(self, sql, poll_interval=1.0):
                applied.append(sql)
                for table, rows in columns_by_table.items():
                    if sql == f"SHOW COLUMNS FROM lake.alice.{table}":
                        return rows
                return []

        monkeypatch.setattr(views_module, "TrinoClient", RecordingClient)

        exit_code = views_module.run({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "CONTRACT VIEW applied: lake.contract.job_info" in out
        assert "CONTRACT VIEW applied: lake.contract.trace" in out
        assert "CONTRACT VIEW applied: lake.contract.mon_jdls_parsed" in out
        assert "apply-views: 3 view(s) applied" in out
        assert "WARNING" not in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Drift detection (review fix, "the silent-NULL trap"): contract_columns.py
# is a static mapping frozen against the kind fixture's live jdl__* vocabulary
# (12 of 66 JDL contract fields). At Plan-4 cutover, dlt evolves NEW jdl__*
# columns from production JDLs whose base data would then exist live while
# the contract view kept silently emitting a typed NULL for it. This section
# diffs `SHOW COLUMNS FROM lake.alice.<table>` against everything
# contract_columns.py/TABLE_SPECS already accounts for -- see views.py's
# module docstring, "Drift detection" section, for the full design.
# ---------------------------------------------------------------------------


class FakeShowColumnsClient:
    """Records every statement; returns canned `SHOW COLUMNS FROM
    lake.alice.<table>` rows keyed by table name, `[]` for anything else
    (matches apply_views()'s CREATE SCHEMA/CREATE VIEW statements, which this
    fake is not exercising)."""

    def __init__(self, columns_by_table: dict[str, list[list]]):
        self.statements: list[str] = []
        self._columns_by_table = columns_by_table

    def run(self, sql: str, poll_interval: float = 1.0):
        self.statements.append(sql)
        for table, rows in self._columns_by_table.items():
            if sql == f"SHOW COLUMNS FROM lake.alice.{table}":
                return rows
        return []


def _known_live_rows(spec) -> list[list]:
    """Every column `spec` already accounts for (mapped + passthrough),
    rendered as canned `SHOW COLUMNS` rows -- the "no drift" baseline a test
    starts from before injecting an extra, unmapped column."""
    rows = [[name, "varchar", "", None] for name in spec.columns.values() if name is not None]
    rows += [[name, "varchar", "", None] for name in spec.passthrough]
    return rows


class TestCheckTableDrift:
    def test_no_drift_when_live_columns_are_all_known(self):
        from alice_ingest.views import TABLE_SPECS, check_table_drift

        spec = TABLE_SPECS["mon_jdls_parsed"]
        client = FakeShowColumnsClient({"mon_jdls_parsed": _known_live_rows(spec)})

        result = check_table_drift(client, "mon_jdls_parsed", spec)

        assert result.table == "mon_jdls_parsed"
        assert result.unmapped_jdl_columns == ()
        assert result.other_unmapped_columns == ()
        assert result.has_jdl_drift is False

    def test_unmapped_jdl_column_is_flagged_with_its_live_type(self):
        from alice_ingest.views import TABLE_SPECS, check_table_drift

        spec = TABLE_SPECS["mon_jdls_parsed"]
        rows = _known_live_rows(spec) + [["jdl__new_field", "varchar", "", None]]
        client = FakeShowColumnsClient({"mon_jdls_parsed": rows})

        result = check_table_drift(client, "mon_jdls_parsed", spec)

        assert result.unmapped_jdl_columns == (("jdl__new_field", "varchar"),)
        assert result.has_jdl_drift is True

    def test_unmapped_jdl_column_type_is_captured_even_when_non_varchar(self):
        # Brief: "a future non-varchar arrival must be visible."
        from alice_ingest.views import TABLE_SPECS, check_table_drift

        spec = TABLE_SPECS["mon_jdls_parsed"]
        rows = _known_live_rows(spec) + [["jdl__future_numeric", "bigint", "", None]]
        client = FakeShowColumnsClient({"mon_jdls_parsed": rows})

        result = check_table_drift(client, "mon_jdls_parsed", spec)

        assert result.unmapped_jdl_columns == (("jdl__future_numeric", "bigint"),)

    def test_non_jdl_unmapped_column_is_informational_only(self):
        from alice_ingest.views import TABLE_SPECS, check_table_drift

        spec = TABLE_SPECS["mon_jdls_parsed"]
        rows = _known_live_rows(spec) + [["some_new_dlt_helper", "varchar", "", None]]
        client = FakeShowColumnsClient({"mon_jdls_parsed": rows})

        result = check_table_drift(client, "mon_jdls_parsed", spec)

        assert result.unmapped_jdl_columns == ()
        assert result.other_unmapped_columns == (("some_new_dlt_helper", "varchar"),)
        assert result.has_jdl_drift is False

    def test_queries_the_source_alice_table_not_the_contract_view(self):
        from alice_ingest.views import TABLE_SPECS, check_table_drift

        spec = TABLE_SPECS["job_info"]
        client = FakeShowColumnsClient({"job_info": _known_live_rows(spec)})

        check_table_drift(client, "job_info", spec)

        assert client.statements == ["SHOW COLUMNS FROM lake.alice.job_info"]


class TestCheckDrift:
    def test_one_result_per_table_in_table_specs_order(self):
        from alice_ingest.views import TABLE_SPECS, check_drift

        client = FakeShowColumnsClient({})
        results = check_drift(client)

        assert [r.table for r in results] == list(TABLE_SPECS)

    def test_no_drift_across_all_three_tables_when_fixture_matches_mapping(self):
        # The real invariant this task's brief expects TODAY on kind: 12 of
        # 66 jdl__* fields live, all 12 mapped -- zero drift anywhere.
        from alice_ingest.views import TABLE_SPECS, check_drift

        columns_by_table = {table: _known_live_rows(spec) for table, spec in TABLE_SPECS.items()}
        client = FakeShowColumnsClient(columns_by_table)

        results = check_drift(client)

        assert all(not r.has_jdl_drift for r in results)
        assert all(not r.other_unmapped_columns for r in results)


class TestRunDriftReporting:
    """`run()`-level integration-shaped tests (brief's four required cases):
    no drift -> no warning, exit 0; one unmapped jdl__ column -> warning with
    name:type, exit 0; same with --strict -> exit 2; non-jdl__ unmapped live
    column -> informational note only, never strict-fails."""

    @staticmethod
    def _client_factory(columns_by_table: dict[str, list[list]]):
        class RecordingClient:
            def __init__(self, base_uri, user="alice-ingest", timeout=30.0):
                self.base_uri = base_uri

            def run(self, sql, poll_interval=1.0):
                for table, rows in columns_by_table.items():
                    if sql == f"SHOW COLUMNS FROM lake.alice.{table}":
                        return rows
                return []

        return RecordingClient

    def _no_drift_columns(self):
        from alice_ingest.views import TABLE_SPECS

        return {table: _known_live_rows(spec) for table, spec in TABLE_SPECS.items()}

    def test_no_drift_prints_no_warning_and_exits_zero(self, monkeypatch, capsys):
        from alice_ingest import views as views_module

        monkeypatch.setattr(
            views_module, "TrinoClient", self._client_factory(self._no_drift_columns())
        )

        exit_code = views_module.run({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0
        assert "WARNING" not in capsys.readouterr().err

    def test_one_unmapped_jdl_column_warns_but_exits_zero_without_strict(self, monkeypatch, capsys):
        from alice_ingest import views as views_module

        columns_by_table = self._no_drift_columns()
        columns_by_table["mon_jdls_parsed"] = columns_by_table["mon_jdls_parsed"] + [
            ["jdl__new_field", "varchar", "", None]
        ]
        monkeypatch.setattr(views_module, "TrinoClient", self._client_factory(columns_by_table))

        exit_code = views_module.run({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0
        err = capsys.readouterr().err
        assert "WARNING" in err
        assert "jdl__new_field:varchar" in err
        assert "contract regeneration" in err

    def test_unmapped_jdl_column_with_strict_exits_two(self, monkeypatch, capsys):
        from alice_ingest import views as views_module

        columns_by_table = self._no_drift_columns()
        columns_by_table["mon_jdls_parsed"] = columns_by_table["mon_jdls_parsed"] + [
            ["jdl__new_field", "varchar", "", None]
        ]
        monkeypatch.setattr(views_module, "TrinoClient", self._client_factory(columns_by_table))

        exit_code = views_module.run({"TRINO_URI": "http://trino.trino.svc:8080"}, strict=True)

        assert exit_code == 2
        assert "jdl__new_field:varchar" in capsys.readouterr().err

    def test_non_jdl_unmapped_column_never_strict_fails(self, monkeypatch, capsys):
        from alice_ingest import views as views_module

        columns_by_table = self._no_drift_columns()
        columns_by_table["mon_jdls_parsed"] = columns_by_table["mon_jdls_parsed"] + [
            ["some_new_dlt_helper", "varchar", "", None]
        ]
        monkeypatch.setattr(views_module, "TrinoClient", self._client_factory(columns_by_table))

        exit_code = views_module.run({"TRINO_URI": "http://trino.trino.svc:8080"}, strict=True)

        assert exit_code == 0
        err = capsys.readouterr().err
        assert "WARNING" not in err
        assert "INFO" in err
        assert "some_new_dlt_helper:varchar" in err

    def test_strict_defaults_to_false(self, monkeypatch, capsys):
        from alice_ingest import views as views_module

        columns_by_table = self._no_drift_columns()
        columns_by_table["mon_jdls_parsed"] = columns_by_table["mon_jdls_parsed"] + [
            ["jdl__new_field", "varchar", "", None]
        ]
        monkeypatch.setattr(views_module, "TrinoClient", self._client_factory(columns_by_table))

        exit_code = views_module.run({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0


class TestSubtractiveDrift:
    """The 2026-07-13 production-data dress rehearsal crashed apply-views
    with a raw Trino COLUMN_NOT_FOUND traceback: the mapping referenced
    fixture-era `jdl__*` columns that did not exist on the freshly-reset,
    production-fed table (research/2026-07-13_production-data-dress-
    rehearsal.md). Drift detection previously only looked one way (live
    columns the mapping doesn't know); a mapped or passthrough column
    MISSING from the live schema is equally actionable drift -- and worse,
    it makes the view DDL un-appliable, so it must be caught BEFORE any DDL
    runs and reported like drift, not as a traceback."""

    def test_mapped_column_missing_live_is_reported(self):
        from alice_ingest.views import TABLE_SPECS, check_table_drift

        spec = TABLE_SPECS["mon_jdls_parsed"]
        rows = [r for r in _known_live_rows(spec) if r[0] != "jdl__cpu_cores"]
        client = FakeShowColumnsClient({"mon_jdls_parsed": rows})

        result = check_table_drift(client, "mon_jdls_parsed", spec)

        assert "jdl__cpu_cores" in result.missing_mapped_columns
        assert result.has_missing_mapped is True

    def test_passthrough_column_missing_live_is_reported(self):
        from alice_ingest.views import TABLE_SPECS, check_table_drift

        spec = TABLE_SPECS["mon_jdls_parsed"]
        rows = [r for r in _known_live_rows(spec) if r[0] != "full_jdl_raw"]
        client = FakeShowColumnsClient({"mon_jdls_parsed": rows})

        result = check_table_drift(client, "mon_jdls_parsed", spec)

        assert "full_jdl_raw" in result.missing_mapped_columns

    def test_no_missing_when_live_matches_mapping(self):
        from alice_ingest.views import TABLE_SPECS, check_table_drift

        spec = TABLE_SPECS["mon_jdls_parsed"]
        client = FakeShowColumnsClient({"mon_jdls_parsed": _known_live_rows(spec)})

        result = check_table_drift(client, "mon_jdls_parsed", spec)

        assert result.missing_mapped_columns == ()
        assert result.has_missing_mapped is False

    def test_missing_and_unmapped_can_coexist(self):
        from alice_ingest.views import TABLE_SPECS, check_table_drift

        spec = TABLE_SPECS["mon_jdls_parsed"]
        rows = [r for r in _known_live_rows(spec) if r[0] != "jdl__cpu_cores"]
        rows.append(["jdl__brand_new", "varchar", "", None])
        client = FakeShowColumnsClient({"mon_jdls_parsed": rows})

        result = check_table_drift(client, "mon_jdls_parsed", spec)

        assert "jdl__cpu_cores" in result.missing_mapped_columns
        assert result.unmapped_jdl_columns == (("jdl__brand_new", "varchar"),)


class TestRunSubtractiveDrift:
    """run()-level: missing-mapped drift must abort BEFORE any DDL (the DDL
    cannot succeed -- Trino would COLUMN_NOT_FOUND), print an actionable
    WARNING naming the columns, and exit 2 in BOTH modes (non-strict exit 0
    would misreport 'views applied' when nothing was applied)."""

    @staticmethod
    def _recording_client_factory(columns_by_table: dict[str, list[list]]):
        statements: list[str] = []

        class RecordingClient:
            recorded = statements

            def __init__(self, base_uri, user="alice-ingest", timeout=30.0):
                self.base_uri = base_uri

            def run(self, sql, poll_interval=1.0):
                statements.append(sql)
                for table, rows in columns_by_table.items():
                    if sql == f"SHOW COLUMNS FROM lake.alice.{table}":
                        return rows
                return []

        return RecordingClient

    def _columns_with_missing_mapped(self):
        from alice_ingest.views import TABLE_SPECS

        columns = {table: _known_live_rows(spec) for table, spec in TABLE_SPECS.items()}
        columns["mon_jdls_parsed"] = [
            r for r in columns["mon_jdls_parsed"] if r[0] != "jdl__cpu_cores"
        ]
        return columns

    def test_missing_mapped_aborts_before_any_ddl_and_exits_two(self, monkeypatch, capsys):
        from alice_ingest import views as views_module

        factory = self._recording_client_factory(self._columns_with_missing_mapped())
        monkeypatch.setattr(views_module, "TrinoClient", factory)

        exit_code = views_module.run({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 2
        err = capsys.readouterr().err
        assert "WARNING" in err
        assert "jdl__cpu_cores" in err
        assert "missing" in err
        assert "contract regeneration" in err
        assert not any(
            s.startswith("CREATE") for s in factory.recorded
        ), f"DDL must not run against a schema it cannot apply to: {factory.recorded}"

    def test_missing_mapped_with_strict_also_exits_two(self, monkeypatch, capsys):
        from alice_ingest import views as views_module

        factory = self._recording_client_factory(self._columns_with_missing_mapped())
        monkeypatch.setattr(views_module, "TrinoClient", factory)

        exit_code = views_module.run({"TRINO_URI": "http://trino.trino.svc:8080"}, strict=True)

        assert exit_code == 2
        assert "jdl__cpu_cores" in capsys.readouterr().err

    def test_no_missing_still_applies_views_and_exits_zero(self, monkeypatch, capsys):
        from alice_ingest import views as views_module
        from alice_ingest.views import TABLE_SPECS

        columns = {table: _known_live_rows(spec) for table, spec in TABLE_SPECS.items()}
        factory = self._recording_client_factory(columns)
        monkeypatch.setattr(views_module, "TrinoClient", factory)

        exit_code = views_module.run({"TRINO_URI": "http://trino.trino.svc:8080"})

        assert exit_code == 0
        assert any(s.startswith("CREATE OR REPLACE VIEW") for s in factory.recorded)
