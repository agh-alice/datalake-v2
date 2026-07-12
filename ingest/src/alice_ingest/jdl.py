import json


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
        record["jdl"] = parsed
        record["jdl_parse_ok"] = True
    except (ValueError, json.JSONDecodeError):
        record["full_jdl_raw"] = raw
        record["jdl_parse_ok"] = False
    return record
