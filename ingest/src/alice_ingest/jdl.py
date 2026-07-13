import json

_LPM_CANONICAL_KEY = "LPMPassName"


def _merge_lpm_casing(jdl: dict) -> None:
    """Coalesce case-variant duplicate keys of LPMPassName into the
    canonical key `LPMPassName`, in place.

    Design spec section 4 (deliverables/2026-07-12-datalake-v2-design.md,
    "Served data contract"): "Fixed at ingestion rather than in the
    consumer: ... LPMPassName/LPMPASSNAME casing." gen-1 merged the two
    casings; the ML consumer's dtypes contract (research/2026-07-12_ml-
    consumer-data-contract.md) expects one field, not a case-split pair.

    If multiple casing variants are present, or the sole variant present is
    not already the canonical spelling, the first non-null value among the
    variants wins (falling back to null only if every variant is null); all
    non-canonical variant keys are removed.
    """
    variant_keys = [k for k in jdl if k.casefold() == _LPM_CANONICAL_KEY.casefold()]
    if variant_keys == [_LPM_CANONICAL_KEY]:
        return  # already canonical, nothing else to coalesce
    if not variant_keys:
        return
    values = [jdl[k] for k in variant_keys]
    value = next((v for v in values if v is not None), values[0])
    for key in variant_keys:
        del jdl[key]
    jdl[_LPM_CANONICAL_KEY] = value


def _decode_jdl_object(raw: str) -> dict:
    """Decode `full_jdl` text to its JDL dict, or raise ValueError.

    Two encodings exist in the wild:

    1. Plain JSON (the kind fixture's shape): `json.loads` directly.

    2. ESCAPED JSON -- the body of a JSON string literal: literal `\\n`,
       `\\"`, `\\/` two-character sequences, no real newlines. This is how
       REAL production `mon_jdls.full_jdl` values are stored: the
       2026-07-13 production-data dress rehearsal found 146,896/146,896
       real JDLs in this encoding (`jdl_parse_ok` was False for the ENTIRE
       sample -- research/2026-07-13_production-data-dress-rehearsal.md),
       and gen-1's own ETL corroborates it (agh-alice/datalake
       src/spark/postgres_dump.py applies an `unescape_json_udf` before
       schema inference, with a "may be double-escaped" fallback). The
       fixture simply never modeled the escaping, which is why this went
       unseen until real data hit the pipeline.

       Decode: re-wrap in quotes and let json itself undo the string-body
       escaping (`json.loads('"' + raw + '"')`), then parse the decoded
       text as JSON. This is the exact inverse of the serializer that
       produced the value (handles `\\/` correctly, which gen-1's
       unicode-escape approach does not) and fails loudly on anything
       that isn't a well-formed escaped JSON string body -- no heuristic
       sniffing. A value that decodes but still isn't a JSON object is a
       parse failure like any other.
    """
    try:
        parsed = json.loads(raw)
    except (ValueError, json.JSONDecodeError):
        # Fallback: production's escaped-JSON encoding (see docstring).
        text = json.loads(f'"{raw}"')  # may itself raise -> caller's except
        parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("JDL root is not an object")
    return parsed


def parse_jdl(record: dict) -> dict:
    """Parse the JDL JSON text column into a dict for dlt's normalizer.

    Requires the sqlalchemy backend (row dicts): arrow backends yield
    pyarrow.Table items and silently skip per-row maps (verified 2026-07-12).
    Unparseable JDLs are preserved raw + flagged, never dropped. Handles
    both plain-JSON and production's escaped-JSON encodings (see
    `_decode_jdl_object`); on failure, `full_jdl_raw` always preserves the
    ORIGINAL landing value, never a half-decoded intermediate.
    """
    raw = record.pop("full_jdl", None)
    if raw is None:
        record["jdl_parse_ok"] = False
        return record
    try:
        parsed = _decode_jdl_object(raw)
        _merge_lpm_casing(parsed)
        record["jdl"] = parsed
        record["jdl_parse_ok"] = True
    except (ValueError, json.JSONDecodeError):
        record["full_jdl_raw"] = raw
        record["jdl_parse_ok"] = False
    return record
