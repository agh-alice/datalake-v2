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


class TestLpmCasingMergeAtIngestion:
    """LPMPassName/LPMPASSNAME casing must be coalesced into the canonical
    `LPMPassName` key at ingestion (design spec section 4, deliverables/
    2026-07-12-datalake-v2-design.md: "Fixed at ingestion rather than in the
    consumer: ... LPMPassName/LPMPASSNAME casing"). gen-1 merged these; the
    ML consumer's dtypes contract (research/2026-07-12_ml-consumer-data-
    contract.md) expects a single field, not a case-split pair.
    """

    def test_only_uppercase_variant_present_is_coalesced_to_canonical_key(self):
        record = {
            "job_id": 2001,
            "full_jdl": '{"TTL": "3600", "LPMPASSNAME": "passU"}',
        }

        result = parse_jdl(record)

        assert result["jdl"]["LPMPassName"] == "passU"
        assert "LPMPASSNAME" not in result["jdl"]

    def test_only_canonical_variant_present_is_unchanged(self):
        record = {
            "job_id": 2002,
            "full_jdl": '{"TTL": "3600", "LPMPassName": "passC"}',
        }

        result = parse_jdl(record)

        assert result["jdl"]["LPMPassName"] == "passC"
        assert "LPMPASSNAME" not in result["jdl"]

    def test_both_present_null_canonical_nonnull_variant_nonnull_wins(self):
        record = {
            "job_id": 2003,
            "full_jdl": '{"TTL": "3600", "LPMPassName": null, "LPMPASSNAME": "passN"}',
        }

        result = parse_jdl(record)

        assert result["jdl"]["LPMPassName"] == "passN"
        assert "LPMPASSNAME" not in result["jdl"]

    def test_both_present_null_variant_nonnull_canonical_nonnull_wins(self):
        record = {
            "job_id": 2004,
            "full_jdl": '{"TTL": "3600", "LPMPassName": "passC2", "LPMPASSNAME": null}',
        }

        result = parse_jdl(record)

        assert result["jdl"]["LPMPassName"] == "passC2"
        assert "LPMPASSNAME" not in result["jdl"]


class TestParseJdlEscapedProductionEncoding:
    """Production `mon_jdls.full_jdl` values are stored as ESCAPED JSON text
    (the body of a JSON string literal: literal `\\n`, `\\"`, `\\/` two-char
    sequences, no real newlines) -- discovered live in the 2026-07-13
    production-data dress rehearsal, where 146,896/146,896 real JDLs failed
    `json.loads` and every row landed in `full_jdl_raw` (research/2026-07-13_
    production-data-dress-rehearsal.md). Gen-1's own ETL confirms the
    encoding is production-real: `postgres_dump.py` applies an
    `unescape_json_udf` before schema inference (agh-alice/datalake,
    src/spark/postgres_dump.py). `parse_jdl` must therefore fall back to
    JSON-string-body decoding when direct `json.loads` fails, and only then
    give up into the preserved-raw path.
    """

    @staticmethod
    def _escape(jdl_dict: dict) -> str:
        """Render a dict exactly the way production stores it: pretty JSON,
        then re-serialized as a JSON string literal, outer quotes stripped --
        yields the observed `{\\n  \\"User\\": ...` shape."""
        text = json.dumps(jdl_dict, indent=2)
        return json.dumps(text)[1:-1]

    def test_escaped_valid_jdl_parses_with_flag_true(self):
        jdl = {"User": "aliprod", "TTL": 72000, "LPMPassName": "apass4"}
        raw = self._escape(jdl)
        assert raw.startswith('{\\n')  # the production shape, not clean JSON
        record = {"job_id": 3001, "full_jdl": raw}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is True
        # Values arrive canonicalized to strings (TestParseJdlValueCanonicalization).
        assert result["jdl"] == {"User": "aliprod", "TTL": "72000", "LPMPassName": "apass4"}
        assert "full_jdl" not in result
        assert "full_jdl_raw" not in result

    def test_escaped_legacy_lpm_casing_is_merged_after_decoding(self):
        # 13,243 of the rehearsal window's 146,896 real JDLs carry the
        # legacy ALL-CAPS spelling INSIDE the escaped encoding -- the LPM
        # merge must still apply after the fallback decode.
        raw = self._escape({"TTL": 72000, "LPMPASSNAME": "apass1"})
        record = {"job_id": 3002, "full_jdl": raw}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is True
        assert result["jdl"]["LPMPassName"] == "apass1"
        assert "LPMPASSNAME" not in result["jdl"]

    def test_escaped_solidus_sequence_decodes_to_plain_slash(self):
        # Production JDLs escape forward slashes JSON-style (`QC\/(*.log`) --
        # seen verbatim in the rehearsal sample. json's string-body decode
        # handles `\/` natively; unicode-escape (gen-1's approach) does not.
        raw = '{\\n  \\"Output\\": \\"qc_log_archive.zip:QC\\/*.log@disk=1\\"\\n}'
        record = {"job_id": 3003, "full_jdl": raw}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is True
        assert result["jdl"]["Output"] == "qc_log_archive.zip:QC/*.log@disk=1"

    def test_escaped_garbage_preserves_the_original_raw_not_the_decoded_text(self):
        # The fallback's intermediate decode may SUCCEED while the decoded
        # text is still not valid JSON -- the preserved raw must be the
        # ORIGINAL landing value (audit trail), never the half-decoded form.
        raw = '{\\n  \\"TTL\\": \\"3600\\", this is not valid json'
        record = {"job_id": 3004, "full_jdl": raw}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is False
        assert result["full_jdl_raw"] == raw
        assert "jdl" not in result

    def test_escaped_scalar_root_is_still_a_parse_failure(self):
        # "just text" escapes to itself; the wrapped decode yields a str,
        # not a dict -- same root-type rule as the direct path.
        record = {"job_id": 3005, "full_jdl": "just an escaped\\nstring"}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is False
        assert result["full_jdl_raw"] == "just an escaped\\nstring"

    def test_clean_json_never_takes_the_fallback_path(self):
        # Direct parse short-circuits: a clean-JSON value whose CONTENT
        # happens to look escape-ish must not be double-decoded.
        raw = '{"Comment": "literal backslash-n here: \\\\n stays two chars"}'
        record = {"job_id": 3006, "full_jdl": raw}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is True
        assert result["jdl"]["Comment"] == "literal backslash-n here: \\n stays two chars"


class TestParseJdlValueCanonicalization:
    """Real production JDLs mix JSON types for the SAME field across rows
    (dress-rehearsal run 2: `WorkDirectorySize` is `["50000MB"]` in most
    rows and a plain scalar in others -- dlt then demands a `__v_text`
    variant column, which the deliberate `data_type: freeze` schema
    contract rejects TERMINALLY, research/2026-07-13_production-data-
    dress-rehearsal.md). The fix is canonicalization at parse time: every
    top-level JDL value is served as a string -- str values pass through
    unchanged, None stays None (SQL NULL), and everything else (numbers,
    bools, lists, dicts) becomes its compact JSON text. Every `jdl__*`
    column is therefore varchar BY CONSTRUCTION: variant columns become
    impossible, the freeze contract stays as the guardrail it was meant to
    be, and the evolved column set depends only on the field-NAME
    vocabulary, never on which row's type showed up first (deterministic
    schema across backfill orderings -- the Plan-4 cutover property). The
    ML consumer's dtypes contract types every field as object/float64 and
    its data_loader owns the casting (research/2026-07-12_ml-consumer-
    data-contract.md), so string serving is contract-compatible."""

    def test_string_values_pass_through_unchanged(self):
        record = {"job_id": 1, "full_jdl": '{"User": "aliprod", "MemorySize": "64GB"}'}

        result = parse_jdl(record)

        assert result["jdl"] == {"User": "aliprod", "MemorySize": "64GB"}

    def test_numeric_and_bool_values_become_json_text(self):
        record = {"job_id": 2, "full_jdl": '{"TTL": 72000, "Price": 2.0, "DirectAccess": true}'}

        result = parse_jdl(record)

        assert result["jdl"]["TTL"] == "72000"
        assert result["jdl"]["Price"] == "2.0"
        assert result["jdl"]["DirectAccess"] == "true"

    def test_list_values_become_compact_json_text(self):
        record = {
            "job_id": 3,
            "full_jdl": '{"WorkDirectorySize": ["50000MB"], "Packages": ["A::v1", "B::v2"]}',
        }

        result = parse_jdl(record)

        assert result["jdl"]["WorkDirectorySize"] == '["50000MB"]'
        assert result["jdl"]["Packages"] == '["A::v1","B::v2"]'

    def test_dict_values_become_compact_json_text(self):
        record = {"job_id": 4, "full_jdl": '{"LPMMetaData": {"Comment": "pp 13.6", "Year": 2026}}'}

        result = parse_jdl(record)

        assert result["jdl"]["LPMMetaData"] == '{"Comment":"pp 13.6","Year":2026}'

    def test_null_values_stay_none(self):
        record = {"job_id": 5, "full_jdl": '{"PWG": null, "User": "aliprod"}'}

        result = parse_jdl(record)

        assert result["jdl"]["PWG"] is None
        assert result["jdl"]["User"] == "aliprod"

    def test_mixed_type_field_across_rows_yields_one_string_type(self):
        # THE run-2 failure case: same field, list in one row, scalar in the
        # next -- both must land as strings so dlt never needs a variant.
        r1 = parse_jdl({"job_id": 6, "full_jdl": '{"WorkDirectorySize": ["50000MB"]}'})
        r2 = parse_jdl({"job_id": 7, "full_jdl": '{"WorkDirectorySize": 5000}'})

        assert isinstance(r1["jdl"]["WorkDirectorySize"], str)
        assert isinstance(r2["jdl"]["WorkDirectorySize"], str)

    def test_lpm_merge_happens_before_canonicalization(self):
        # The merged canonical value must be canonicalized like any other
        # (a numeric LPMPASSNAME still ends up a string under the canonical
        # key).
        record = {"job_id": 8, "full_jdl": '{"LPMPASSNAME": 4}'}

        result = parse_jdl(record)

        assert result["jdl"]["LPMPassName"] == "4"
        assert "LPMPASSNAME" not in result["jdl"]

    def test_canonicalization_applies_to_escaped_encoding_too(self):
        raw = json.dumps(json.dumps({"TTL": 72000, "JobTag": ["comment:x"]}, indent=2))[1:-1]
        record = {"job_id": 9, "full_jdl": raw}

        result = parse_jdl(record)

        assert result["jdl_parse_ok"] is True
        assert result["jdl"]["TTL"] == "72000"
        assert result["jdl"]["JobTag"] == '["comment:x"]'

    def test_non_ascii_content_stays_readable_not_escaped(self):
        record = {"job_id": 10, "full_jdl": '{"Comment": ["süß"]}'}

        result = parse_jdl(record)

        assert result["jdl"]["Comment"] == '["süß"]'
