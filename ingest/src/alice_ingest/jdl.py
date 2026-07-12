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


def parse_jdl(record: dict) -> dict:
    """Parse the JDL JSON text column into a dict for dlt's normalizer.

    Requires the sqlalchemy backend (row dicts): arrow backends yield
    pyarrow.Table items and silently skip per-row maps (verified 2026-07-12).
    Unparseable JDLs are preserved raw + flagged, never dropped.
    """
    raw = record.pop("full_jdl", None)
    if raw is None:
        record["jdl_parse_ok"] = False
        return record
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("JDL root is not an object")
        _merge_lpm_casing(parsed)
        record["jdl"] = parsed
        record["jdl_parse_ok"] = True
    except (ValueError, json.JSONDecodeError):
        record["full_jdl_raw"] = raw
        record["jdl_parse_ok"] = False
    return record
