#!/usr/bin/env python3
"""Derives the JDL top-level key set hack/seed-fixture.sh must seed, straight
from ingest/src/alice_ingest/contract_columns.py -- Plan 5 Task 4 fixture
enrichment (`apply-views` was aborting: kind's synthetic fixture produced
only ~12 jdl__* columns while the mapping, regenerated against real
production JDLs, references 65). Prints ONE SQL fragment (a `jsonb_build_
object(...)` call) to stdout, meant to be `||`-concatenated onto the
hand-authored JDL object in seed-fixture.sh's heredoc.

Why generate rather than hand-list: contract_columns.py is a STATIC
snapshot that gets REGENERATED against real production data periodically
(that module's own docstring, "How to regenerate this mapping"). Hand-
copying its field list here would drift the next time contract_columns.py
changes underneath it -- reading the module's dicts directly at seed time
means the fixture's field coverage is only ever stale by one `seed-
fixture.sh` rerun, never by a forgotten manual edit. Run this file directly
(no args) to see the current derived count on stderr.

Field selection
--------------------------------------------------------------------------
- Every `MON_JDLS_PARSED_COLUMNS` contract field with a live `jdl__*`
  counterpart (`dlt_name is not None`) -- excluding `job_id` (not a JDL
  field, already a first-class landing column) and excluding whatever
  seed-fixture.sh already hand-authors in its own `jsonb_build_object`
  literal (`HAND_AUTHORED` below, kept in sync by hand since it mirrors
  literal SQL, not a data structure this script can introspect) so the two
  halves never both claim the same key. `LPMPassName` is hand-authored
  specifically because it needs the alternating LPMPassName/LPMPASSNAME
  split-casing CASE expression (the production quality defect jdl.py's
  `_merge_lpm_casing` coalesces), not a flat value.
- Every `jdl__`-prefixed entry in `MON_JDLS_PARSED_PASSTHROUGH` (real
  production fields with no contract mapping), each with its `jdl__`
  prefix stripped back to its JDL source-key spelling. The tuple's other
  entries (`lpmjobtypeid`, `full_jdl`, `jdl_parse_ok`, `full_jdl_raw`,
  `_dlt_load_id`, `_dlt_id`) are NOT JDL fields -- they are landing-table
  columns / parser bookkeeping / dlt metadata (contract_columns.py's own
  comment above `MON_JDLS_PARSED_PASSTHROUGH`) and must not be seeded as
  JDL keys.
- `HardBins`/`MasterResubmitThreshold` (the two `None`-mapped fields) are
  deliberately EXCLUDED: production truly never populates them (module
  docstring), and seeding them here would fabricate a live `jdl__*` column
  with no contract mapping -- additive drift that `apply-views --strict`
  would then (correctly) flag as needing regeneration, for a gap this
  script itself invented rather than one production actually has.

Source-key spelling and dlt's actual normalizer
--------------------------------------------------------------------------
Not hand-simulated, consistent with contract_columns.py's own verification
method (that module's docstring: "verified by running the actual pinned
dependency... NOT hand-simulated").

- Mapped fields use the contract's own field spelling verbatim (e.g.
  `CPUCores`). Verified 2026-08-06 against the pinned `dlt==1.28.2`
  `dlt.common.normalizers.naming.snake_case.NamingConvention` (ephemeral
  venv, no cluster needed): every one of the 64 non-null
  `MON_JDLS_PARSED_COLUMNS` keys normalizes to EXACTLY its mapped
  `jdl__*` value, zero mismatches.
- Passthrough fields have no contract spelling to fall back on; using
  their `jdl__`-stripped name AS-IS relies on the normalizer being
  IDEMPOTENT on an already-lowercase-snake_case string (no case boundary
  or irregular separator left to fix). Verified the same way, same day,
  for all 38 of them: zero mismatches (each one round-trips through
  `normalize_identifier()` unchanged, including digit-adjacent names like
  `o2_dpg_async_reco_tag` and `run3_chunk`).

Values: every generated field is a short per-row-varying string
(`'<key>_' || gs::text`), matching the existing fixture's "values as
strings" property (seed-fixture.sh's module comment: production JDLs mix
JSON types per field, and the pipeline canonicalizes every JDL value to a
string before dlt ever sees it -- jdl.py's `_canonicalize_values`). Only
the KEY's presence needs to be real; the exact string content is not
semantically load-bearing for the column-existence gate this fixes.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "ingest" / "src"))

from alice_ingest.contract_columns import (  # noqa: E402
    MON_JDLS_PARSED_COLUMNS,
    MON_JDLS_PARSED_PASSTHROUGH,
)

# Kept in sync BY HAND with seed-fixture.sh's own jsonb_build_object literal
# (see module docstring, "Field selection"). If seed-fixture.sh's hand-
# authored field list ever changes, update this set in the same commit --
# a stale entry here only causes a harmless silent underlap (the field
# stops being auto-generated but is presumably still hand-authored), a
# stale ABSENCE would cause a duplicate-key jsonb_build_object argument
# error in Postgres, which fails loudly at seed time.
HAND_AUTHORED: frozenset[str] = frozenset(
    {
        "TTL",
        "CPUCores",
        "CPULimit",
        "Executable",
        "JobTag",
        "User",
        "PWG",
        "Packages",
        "CollisionSystem",
        "Requirements",
        "MemorySize",
        "LPMPassName",
    }
)


def derive_keys() -> list[str]:
    """Sorted (stable, reviewable diff across regenerations) union of the
    two field-selection rules in the module docstring."""
    mapped = [
        key
        for key, dlt_name in MON_JDLS_PARSED_COLUMNS.items()
        if dlt_name is not None and key != "job_id" and key not in HAND_AUTHORED
    ]
    passthrough = [
        name[len("jdl__") :] for name in MON_JDLS_PARSED_PASSTHROUGH if name.startswith("jdl__")
    ]
    return sorted(mapped) + sorted(passthrough)


# Postgres hard-caps function calls at 100 arguments (FUNC_MAX_ARGS) --
# `jsonb_build_object(k1, v1, k2, v2, ...)` takes 2 arguments per field, so a
# single call tops out at 50 fields. Chunking at 40 leaves headroom for the
# mapping to keep growing (it has grown before -- contract_columns.py's own
# "How to regenerate" procedure) without this script silently reproducing
# the exact "cannot pass more than 100 arguments to a function" error that
# a flat 90-field call hit during this fix (verified live against a
# throwaway postgres:16 container, 2026-08-06).
_MAX_FIELDS_PER_CALL = 40


def _chunked(keys: list[str], size: int) -> list[list[str]]:
    return [keys[i : i + size] for i in range(0, len(keys), size)]


def render_sql_fragment(keys: list[str]) -> str:
    """One or more `jsonb_build_object(...)` calls, `||`-concatenated, each
    referencing the caller's own `gs` column (this is spliced as literal SQL
    text into a `SELECT ... FROM generate_series(1, 1000) gs` query, per
    seed-fixture.sh). `'{}'::jsonb` if somehow empty, keeping the `||` in
    seed-fixture.sh well-typed either way."""
    if not keys:
        return "'{}'::jsonb"
    calls = []
    for chunk in _chunked(keys, _MAX_FIELDS_PER_CALL):
        pairs = ",\n      ".join(f"'{key}', '{key}_' || gs::text" for key in chunk)
        calls.append(f"jsonb_build_object(\n      {pairs}\n    )")
    return " ||\n    ".join(calls)


def main() -> int:
    keys = derive_keys()
    print(
        f"jdl_fixture_fields.py: {len(keys)} auto-derived JDL fields "
        f"(+ {len(HAND_AUTHORED)} hand-authored in seed-fixture.sh = "
        f"{len(keys) + len(HAND_AUTHORED)} total)",
        file=sys.stderr,
    )
    print(render_sql_fragment(keys))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
