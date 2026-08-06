"""Unit tests for `hack/jdl_fixture_fields.py`'s three pure functions
(`derive_keys`, `_chunked`, `render_sql_fragment`) plus the seam this
module exists to protect: `HAND_AUTHORED` must exactly mirror the field
list `hack/seed-fixture.sh` hand-authors in its own `jsonb_build_object`
literal (module docstring, "Field selection"). A drift here is the
expensive kind of bug this module's own docstring warns about: a field
silently drops out of the fixture and later surfaces as an `apply-views`
subtractive-drift abort that looks like it needs a contract regeneration,
not a one-line fixture-field sync.

Location: `hack/tests/`, mirroring `tools/tests/test_extract_training_data.py`
-- the existing precedent in this repo for a standalone script (not a
package under `ingest/`) with pure-logic unit tests, stdlib `unittest`
only (no pytest dependency needed), same `sys.path.insert` pattern to
import the module under test directly by file location.

Deliberately imports ONLY `jdl_fixture_fields` and, transitively,
`alice_ingest.contract_columns` (a pure data module -- no dlt, no cluster,
no I/O at import time; see that module's own docstring: "does not call
Trino or dlt at import time"). Does NOT import the rest of `alice_ingest`
(pipeline.py etc, which pulls in dlt) -- keeps this file runnable in a
bare Python interpreter with no venv, matching
`hack/jdl_fixture_fields.py`'s own "ephemeral venv, no cluster needed"
verification note and this repo's CI runner (no ingest deps installed for
`hack/lint.sh`).

Run with: python3 -m unittest discover -s hack/tests
(also picked up by `hack/lint.sh`'s own `hack/ unit tests` step, so it
runs in CI without a cluster, on every push/PR that runs the lint gate.)
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "hack"))
sys.path.insert(0, str(REPO_ROOT / "ingest" / "src"))

import jdl_fixture_fields as jff  # noqa: E402
from alice_ingest.contract_columns import (  # noqa: E402
    MON_JDLS_PARSED_COLUMNS,
    MON_JDLS_PARSED_PASSTHROUGH,
)

SEED_FIXTURE_SH = REPO_ROOT / "hack" / "seed-fixture.sh"

# The two fields contract_columns.py maps to `dlt_name=None` (module
# docstring: "production truly never populates them") -- must never show
# up as auto-derived, and must not silently become hand-authored either.
NONE_MAPPED_FIELDS = {"HardBins", "MasterResubmitThreshold"}

# MON_JDLS_PARSED_PASSTHROUGH entries that are NOT jdl__-prefixed JDL
# fields -- landing-table columns / parser bookkeeping / dlt metadata
# (contract_columns.py's own comment above that tuple). Must never appear
# in derive_keys()'s output.
NON_JDL_PASSTHROUGH_BOOKKEEPING = {
    "lpmjobtypeid",
    "full_jdl",
    "jdl_parse_ok",
    "full_jdl_raw",
    "_dlt_load_id",
    "_dlt_id",
}


def _parse_seed_fixture_hand_authored_keys() -> tuple[set[str], str, str]:
    """Parses `hack/seed-fixture.sh`'s own `jsonb_build_object(...)` literal
    (the block between the `ELSE (jsonb_build_object(` marker and its
    `) || $EXTRA_JDL_FIELDS)::text` close) and returns the set of hand-
    authored JDL field names it actually sets, plus the two literal
    casings extracted from its `CASE WHEN ... THEN 'X' ELSE 'Y' END`
    dynamic-key pair (the `LPMPassName`/`LPMPASSNAME` split-casing field).

    Deliberately does NOT hardcode the field list -- reading it out of the
    script is the entire point of this test (a test that duplicates the
    same hardcoded list on both sides guards nothing). Classifies every
    non-blank line inside the block and raises if any line doesn't match
    one of the three expected shapes, so a reformat that moves a key off
    line-start fails loudly (a wrong subset that happens to still match a
    stale HAND_AUTHORED) rather than silently under-parsing.
    """
    text = SEED_FIXTURE_SH.read_text()
    start_marker = "ELSE (jsonb_build_object("
    end_marker = ") || $EXTRA_JDL_FIELDS)::text"
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    block = text[start + len(start_marker) : end]

    key_line_re = re.compile(r"^'([A-Za-z0-9_]+)',(.*)$")
    case_line_re = re.compile(
        r"^CASE WHEN .*? THEN '([A-Za-z0-9_]+)' ELSE '([A-Za-z0-9_]+)' END,?$"
    )
    # A value expression that legitimately continues past its key's own
    # line (e.g. `(ARRAY[...])[1 + gs % 4],` or a bare `),` closing an
    # earlier multi-line call) -- classified as "not a key line" but still
    # expected content, not a parse failure.
    continuation_re = re.compile(r"^[^']|^\)")

    keys: set[str] = set()
    then_key = else_key = None
    lines = [line.strip() for line in block.splitlines()]
    lines = [line for line in lines if line]
    i = 0
    while i < len(lines):
        line = lines[i]
        m = key_line_re.match(line)
        if m:
            keys.add(m.group(1))
            i += 1
            continue
        m = case_line_re.match(line)
        if m:
            then_key, else_key = m.group(1), m.group(2)
            i += 1
            # The CASE/END line is the KEY half of a two-line pair (this
            # jsonb_build_object call formats each pair "key,\n  value,",
            # and the CASE expression is long enough to need its own
            # line) -- the immediately following line is that key's
            # VALUE, consumed here unconditionally rather than
            # reclassified, since a value expression can itself start
            # with a quoted string literal (e.g. `'pass' || ...`) and
            # would otherwise be indistinguishable from a new key line.
            if i >= len(lines):
                raise AssertionError(
                    "hack/seed-fixture.sh jsonb_build_object block: CASE/END "
                    "dynamic-key line has no following value line"
                )
            i += 1
            continue
        if continuation_re.match(line):
            i += 1
            continue
        raise AssertionError(
            f"hack/seed-fixture.sh jsonb_build_object block: unclassified line "
            f"(neither a 'key', value pair, the CASE/END dynamic-key line, nor a "
            f"value continuation) -- update the parser in "
            f"hack/tests/test_jdl_fixture_fields.py: {line!r}"
        )

    if then_key is None or else_key is None:
        raise AssertionError(
            "hack/seed-fixture.sh jsonb_build_object block: no CASE WHEN ... THEN "
            "'X' ELSE 'Y' END dynamic-key line found (expected the LPMPassName/"
            "LPMPASSNAME split-casing pair)"
        )
    # The THEN branch is the canonical key name (jdl.py's LPM-casing
    # coalesce target, design spec section 4) -- that's the field name
    # HAND_AUTHORED must list, not the ELSE (defect) casing.
    keys.add(then_key)
    return keys, then_key, else_key


class TestHandAuthoredMatchesSeedFixture(unittest.TestCase):
    def test_hand_authored_exactly_matches_seed_fixture_literal(self):
        parsed_keys, _then_key, _else_key = _parse_seed_fixture_hand_authored_keys()
        self.assertEqual(
            jff.HAND_AUTHORED,
            frozenset(parsed_keys),
            "hack/jdl_fixture_fields.py's HAND_AUTHORED has drifted from the actual "
            "hand-authored jsonb_build_object literal in hack/seed-fixture.sh -- "
            "keep the two in sync in the same commit (module docstring).",
        )

    def test_dynamic_case_key_is_the_canonical_lpm_casing_pair(self):
        # Self-consistency: the CASE expression's own two literal casings
        # are the documented quality defect, and its THEN branch (the
        # canonical spelling) is the one folded into HAND_AUTHORED.
        _keys, then_key, else_key = _parse_seed_fixture_hand_authored_keys()
        self.assertEqual(then_key, "LPMPassName")
        self.assertEqual(else_key, "LPMPASSNAME")
        self.assertIn("LPMPassName", jff.HAND_AUTHORED)

    def test_hand_authored_disjoint_from_derived_keys(self):
        # The module docstring's whole reason for HAND_AUTHORED to exist:
        # the two halves must never both claim the same key.
        self.assertEqual(jff.HAND_AUTHORED & set(jff.derive_keys()), set())


class TestDeriveKeys(unittest.TestCase):
    def setUp(self):
        self.keys = jff.derive_keys()

    def test_returns_sorted_list(self):
        self.assertEqual(self.keys, sorted(self.keys))

    def test_no_duplicates(self):
        self.assertEqual(len(self.keys), len(set(self.keys)))

    def test_excludes_job_id(self):
        self.assertNotIn("job_id", self.keys)

    def test_excludes_hand_authored_fields(self):
        self.assertEqual(set(self.keys) & jff.HAND_AUTHORED, set())

    def test_excludes_none_mapped_fields(self):
        # HardBins/MasterResubmitThreshold: deliberately excluded
        # (docstring "Field selection", third bullet) -- production never
        # populates them; seeding them would fabricate additive drift.
        self.assertEqual(set(self.keys) & NONE_MAPPED_FIELDS, set())

    def test_excludes_passthrough_bookkeeping_fields(self):
        # lpmjobtypeid/full_jdl/jdl_parse_ok/full_jdl_raw/_dlt_load_id/
        # _dlt_id are landing columns / parser bookkeeping / dlt metadata,
        # not JDL keys (docstring, second bullet) -- must never leak into
        # the derived JDL field set.
        self.assertEqual(set(self.keys) & NON_JDL_PASSTHROUGH_BOOKKEEPING, set())

    def test_every_mapped_key_has_a_non_none_dlt_name(self):
        mapped_in_contract = set(MON_JDLS_PARSED_COLUMNS) - jff.HAND_AUTHORED - {"job_id"}
        derived_mapped = set(self.keys) & set(MON_JDLS_PARSED_COLUMNS)
        self.assertEqual(derived_mapped, mapped_in_contract - NONE_MAPPED_FIELDS)
        for key in derived_mapped:
            self.assertIsNotNone(MON_JDLS_PARSED_COLUMNS[key])

    def test_every_passthrough_key_strips_the_jdl_prefix(self):
        expected_passthrough = {
            name[len("jdl__") :]
            for name in MON_JDLS_PARSED_PASSTHROUGH
            if name.startswith("jdl__")
        }
        derived_passthrough = set(self.keys) - set(MON_JDLS_PARSED_COLUMNS)
        self.assertEqual(derived_passthrough, expected_passthrough)

    def test_total_field_count_matches_contract_plus_hand_authored(self):
        # Sanity total: every dlt_name-having mapped field, minus job_id
        # and minus whatever seed-fixture.sh hand-authors, plus every
        # jdl__-prefixed passthrough field, should equal derive_keys()'s
        # output length exactly (no field lost or doubled).
        mapped_with_dlt_name = {
            k
            for k, v in MON_JDLS_PARSED_COLUMNS.items()
            if v is not None and k != "job_id" and k not in jff.HAND_AUTHORED
        }
        jdl_passthrough = {n for n in MON_JDLS_PARSED_PASSTHROUGH if n.startswith("jdl__")}
        self.assertEqual(len(self.keys), len(mapped_with_dlt_name | jdl_passthrough))
        # And the grand total (auto-derived + hand-authored) should match
        # the fixture's own documented total (seed-fixture.sh's "102 total
        # top-level JDL keys" comment, main()'s own stderr summary line).
        self.assertEqual(len(self.keys) + len(jff.HAND_AUTHORED), 102)

    def test_every_key_is_a_safe_bare_identifier(self):
        # Every derived key gets single-quoted straight into SQL
        # (render_sql_fragment) -- a key containing a quote or other
        # SQL-special character would be a real injection/breakage risk,
        # not just a cosmetic one.
        for key in self.keys:
            self.assertRegex(key, r"^[A-Za-z0-9_]+$", f"unsafe SQL key: {key!r}")


class TestChunked(unittest.TestCase):
    def test_empty_input(self):
        self.assertEqual(jff._chunked([], 40), [])

    def test_smaller_than_chunk_size_is_one_chunk(self):
        self.assertEqual(jff._chunked(["a", "b"], 40), [["a", "b"]])

    def test_exact_multiple_splits_evenly(self):
        keys = [str(i) for i in range(80)]
        chunks = jff._chunked(keys, 40)
        self.assertEqual(len(chunks), 2)
        self.assertEqual([len(c) for c in chunks], [40, 40])

    def test_remainder_goes_in_a_final_partial_chunk(self):
        keys = [str(i) for i in range(90)]
        chunks = jff._chunked(keys, 40)
        self.assertEqual([len(c) for c in chunks], [40, 40, 10])

    def test_chunks_concatenate_back_to_the_original_order(self):
        keys = [str(i) for i in range(97)]
        chunks = jff._chunked(keys, 40)
        self.assertEqual([k for chunk in chunks for k in chunk], keys)

    def test_module_chunk_size_respects_the_postgres_100_arg_boundary(self):
        # Postgres FUNC_MAX_ARGS hard-caps a function call at 100
        # arguments; jsonb_build_object takes 2 args/field, so a single
        # call must never carry more than 50 fields. The module picks 40
        # for headroom (own comment) -- assert that choice, and every
        # chunk it actually produces, stays under the real 50-field cap,
        # not just under the module's own chosen constant (a regression
        # test that only checked against `_MAX_FIELDS_PER_CALL` itself
        # would pass even if that constant were bumped past 50 by
        # mistake).
        self.assertLessEqual(jff._MAX_FIELDS_PER_CALL, 50)
        big_input = [str(i) for i in range(200)]
        for chunk in jff._chunked(big_input, jff._MAX_FIELDS_PER_CALL):
            self.assertLessEqual(len(chunk) * 2, 100)

    def test_reproduces_the_live_90_field_boundary_this_fix_hit(self):
        # The module docstring cites a concrete, live-verified failure: a
        # flat 90-field call hit Postgres's 100-arg cap. Assert chunking
        # 90 fields at the module's real constant never produces a chunk
        # exceeding the 50-field/100-arg limit.
        keys = [str(i) for i in range(90)]
        for chunk in jff._chunked(keys, jff._MAX_FIELDS_PER_CALL):
            self.assertLessEqual(len(chunk) * 2, 100)


class TestRenderSqlFragment(unittest.TestCase):
    def test_empty_keys_render_empty_jsonb(self):
        self.assertEqual(jff.render_sql_fragment([]), "'{}'::jsonb")

    def test_single_call_for_small_key_list(self):
        fragment = jff.render_sql_fragment(["Foo", "Bar"])
        self.assertEqual(fragment.count("jsonb_build_object("), 1)
        self.assertIn("'Foo', 'Foo_' || gs::text", fragment)
        self.assertIn("'Bar', 'Bar_' || gs::text", fragment)

    def test_every_key_present_exactly_once(self):
        keys = [f"field_{i}" for i in range(45)]
        fragment = jff.render_sql_fragment(keys)
        for key in keys:
            self.assertEqual(
                fragment.count(f"'{key}', '{key}_' || gs::text"),
                1,
                f"key {key!r} missing or duplicated in the rendered fragment",
            )

    def test_multiple_calls_concatenated_with_double_pipe(self):
        keys = [f"field_{i}" for i in range(45)]  # > _MAX_FIELDS_PER_CALL (40)
        fragment = jff.render_sql_fragment(keys)
        call_count = fragment.count("jsonb_build_object(")
        self.assertGreaterEqual(call_count, 2)
        # N calls need exactly N-1 `||` concatenation operators.
        self.assertEqual(fragment.count(" ||\n    "), call_count - 1)

    def test_no_call_exceeds_the_postgres_arg_boundary(self):
        keys = [f"field_{i}" for i in range(97)]
        fragment = jff.render_sql_fragment(keys)
        for call_body in fragment.split("jsonb_build_object(")[1:]:
            # Each call's own field count == number of ", '<key>_'" value
            # markers before the next "jsonb_build_object(" / end of string.
            field_count = call_body.count("_' || gs::text")
            self.assertLessEqual(field_count * 2, 100)

    def test_well_typed_result_is_valid_as_a_jsonb_expression_join(self):
        # Both branches must be safely `||`-able onto another jsonb
        # expression in seed-fixture.sh's heredoc -- the empty case must
        # itself be typed jsonb, not a bare string literal.
        self.assertTrue(jff.render_sql_fragment([]).endswith("::jsonb"))
        self.assertTrue(jff.render_sql_fragment(["X"]).strip().endswith(")"))


class TestModuleIsStandaloneRunnable(unittest.TestCase):
    def test_derive_keys_and_render_sql_fragment_compose_without_error(self):
        # End-to-end smoke test of the same call sequence main() performs,
        # exercised directly (no subprocess) so it stays fast and needs no
        # cluster: proves the whole module -- including its
        # contract_columns.py import -- is import-and-run safe in a bare
        # interpreter, matching this file's own module docstring claim.
        keys = jff.derive_keys()
        self.assertGreater(len(keys), 0)
        fragment = jff.render_sql_fragment(keys)
        self.assertIn("jsonb_build_object(", fragment)


if __name__ == "__main__":
    unittest.main()
