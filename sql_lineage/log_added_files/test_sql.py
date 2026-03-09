"""
sql_lineage_extractors — Public API module
(Extended with JOIN expression lineage — backward-compatible)

Enhancements:
- JOIN expression lineage (ON columns, equality pairs, join type)
- CASE / WHERE / GROUP BY / HAVING lineage
- Derived expression column expansion
- Remarks taxonomy as list[str], Table Alias Name preserved
- UNION query support
- Structured logging and exception handling throughout
- ZERO breaking changes to existing behavior

Public API:
- parse_metadata_and_extract_lineage(metadata_json_str, ...)
- extract_lineage_rows(sql, regulation, metadatakey, view_name)
- decode_base64_sql_from_metadata(metadata_json_str, sql_key=...)
- deduplicate_records(records)
"""
from typing import Dict, List

from lineage import extract_lineage_rows
from utils import decode_base64_sql_from_metadata
from deduplication import deduplicate_records
from constants import REMARKS, OUTPUT_KEYS
from logger import get_logger

log = get_logger("test_sql")


def parse_metadata_and_extract_lineage(
    metadata_json_str: str,
    regulation: str = "",
    metadatakey: str = "",
    view_name: str = "",
    sql_key: str = "sql_query",
) -> List[Dict]:
    """
    Decode SQL from JSON metadata (base64), extract lineage, and deduplicate.

    Args:
        metadata_json_str: JSON string containing base64-encoded SQL.
        regulation:        Regulation tag to attach to every lineage row.
        metadatakey:       Metadata key to attach to every lineage row.
        view_name:         View/object name to attach to every lineage row.
        sql_key:           Key in the metadata JSON that holds the base64 SQL.

    Returns:
        Deduplicated list of lineage row dicts.

    Raises:
        ValueError: if metadata_json_str is empty or contains invalid JSON.
    """
    log.info(
       f"""parse_metadata_and_extract_lineage called 
        sql_key={sql_key},
        regulation={regulation},
        metadatakey={metadatakey},
        view_name={view_name}"""
    )

    try:
        sql = decode_base64_sql_from_metadata(metadata_json_str, sql_key)
    except ValueError as exc:
        log.error("Failed to decode SQL from metadata", exc=exc)
        raise

    if not sql:
        log.warning(
            "Decoded SQL is empty; returning empty lineage",
            sql_key=sql_key,
            view_name=view_name,
        )
        return []

    log.debug("SQL decoded successfully", sql_len=len(sql))

    try:
        results = extract_lineage_rows(sql, regulation, metadatakey, view_name)
    except Exception as exc:
        log.error(
            "extract_lineage_rows raised unexpectedly",
            exc=exc,
            view_name=view_name,
        )
        raise

    log.debug("Lineage rows extracted", raw_count=len(results))

    try:
        deduped = deduplicate_records(results)
    except Exception as exc:
        log.error("deduplicate_records raised unexpectedly", exc=exc)
        raise

    log.info(
        "parse_metadata_and_extract_lineage complete",
        raw_count=len(results),
        deduped_count=len(deduped),
        view_name=view_name,
    )
    return deduped


__all__ = [
    "parse_metadata_and_extract_lineage",
    "extract_lineage_rows",
    "decode_base64_sql_from_metadata",
    "deduplicate_records",
    "REMARKS",
    "OUTPUT_KEYS",
]
