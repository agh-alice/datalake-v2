"""Unit tests for the pure JDL-parsing transform (alice_ingest.jdl.parse_jdl).

TDD: written before alice_ingest/jdl.py exists (Plan 2 Task 2, Step 4/2).
`parse_jdl` must never drop a row -- unparseable JDLs are preserved raw
(`full_jdl_raw`) and flagged (`jdl_parse_ok=False`) rather than raised/skipped,
because this function is wired in via dlt's `add_map` on the sqlalchemy
backend (research/2026-07-12_dlt-iceberg-lakekeeper-api-verification.md) and
a raised exception there would abort the whole extraction chunk.
"""

import copy
import json
from pathlib import Path

import pytest

from alice_ingest.jdl import parse_jdl

FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "jdl_samples.json").read_text()
)


def _by_name(name: str) -> dict:
    for case in FIXTURES:
        if case["name"] == name:
            return case
    raise KeyError(name)


class TestParseJdlExplicitCases:
    """One test per case named in the brief (Step 4), independent of the fixture file."""

    def test_valid_jdl_dict_sets_jdl_and_flag_and_removes_full_jdl(self):
        record = {
            "job_id": 42,
            "full_jdl": '{"TTL": "3600", "LPMPassName": "pass1"}',
        }

        result = parse_jdl(record)

        assert result is record  # mutated in place, per the target implementation
        assert result["jdl_parse_ok"] is True
        assert result["jdl"] == {"TTL": "3600", "LPMPassName": "pass1"}
        assert "full_jdl" not in result
        assert "full_jdl_raw" not in result

    def test_garbage_string_preserved_raw_and_flagged_false(self):
        raw = '{\\"TTL\\": \\"3600\\", not even close to valid json'
        record = {"job_id": 43, "full_jdl": raw}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is False
        assert result["full_jdl_raw"] == raw
        assert "jdl" not in result
        assert "full_jdl" not in result  # popped before the parse attempt

    def test_none_jdl_is_flagged_without_raw_preservation(self):
        record = {"job_id": 44, "full_jdl": None}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is False
        assert "jdl" not in result
        # None carries nothing worth preserving raw -- matches the target
        # implementation, which only reaches the try/except for non-None input.
        assert "full_jdl_raw" not in result

    def test_missing_full_jdl_key_is_flagged_same_as_none(self):
        record = {"job_id": 45}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is False
        assert "jdl" not in result
        assert "full_jdl_raw" not in result

    def test_json_array_root_treated_as_parse_failure(self):
        record = {"job_id": 46, "full_jdl": '["TTL", "3600"]'}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is False
        assert result["full_jdl_raw"] == '["TTL", "3600"]'
        assert "jdl" not in result

    def test_json_scalar_root_treated_as_parse_failure(self):
        record = {"job_id": 47, "full_jdl": "42"}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is False
        assert result["full_jdl_raw"] == "42"
        assert "jdl" not in result

    def test_never_raises_on_any_explicit_case(self):
        for full_jdl in (
            '{"a": 1}',
            "not json at all {{{",
            None,
            "[]",
            "null",
            "",
        ):
            parse_jdl({"job_id": 1, "full_jdl": full_jdl})  # must not raise


class TestParseJdlFixtureDriven:
    """Real-shaped samples incl. LPMPassName/LPMPASSNAME casing variants
    (consumer contract: research/2026-07-12_ml-consumer-data-contract.md).
    """

    @pytest.mark.parametrize("case", FIXTURES, ids=[c["name"] for c in FIXTURES])
    def test_fixture_case(self, case):
        record = copy.deepcopy(case["record"])
        expect = case["expect"]

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is expect["jdl_parse_ok"]
        assert ("jdl" in result) is expect["has_jdl"]
        assert ("full_jdl_raw" in result) is expect["has_full_jdl_raw"]
        assert "full_jdl" not in result

        if expect["has_jdl"]:
            lpm_key = expect["jdl_lpm_key"]
            assert result["jdl"][lpm_key] == expect["jdl_lpm_value"]

    def test_fixture_has_both_lpm_casings(self):
        lpm_keys = set()
        for case in FIXTURES:
            raw = case["record"].get("full_jdl")
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                lpm_keys.update(k for k in parsed if k.upper() == "LPMPASSNAME")
        assert "LPMPassName" in lpm_keys
        assert "LPMPASSNAME" in lpm_keys

    def test_fixture_has_at_least_two_deliberately_corrupt_samples(self):
        corrupt = [c for c in FIXTURES if c["expect"]["jdl_parse_ok"] is False]
        assert len(corrupt) >= 2

    def test_fixture_has_at_least_four_samples(self):
        assert len(FIXTURES) >= 4
