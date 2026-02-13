"""
sql_lineage_extractors (modular)
(Extended with JOIN expression lineage — backward-compatible)

Enhancements:
- JOIN expression lineage (ON columns, equality pairs, join type)
- CASE / WHERE / GROUP BY / HAVING lineage
- Derived expression column expansion
- Remarks taxonomy as list[str], Table Alias Name preserved
- ZERO breaking changes to existing behavior

Public API:
- parse_metadata_and_extract_lineage(metadata_json_str, ...)
- extract_lineage_rows(sql, regulation, metadatakey, view_name)
- decode_base64_sql_from_metadata(metadata_json_str, sql_key=...)
- deduplicate_records(records)
"""
from typing import Dict, List

# Re-export public API and constants for backward compatibility
from lineage import extract_lineage_rows
from utils import decode_base64_sql_from_metadata
from deduplication import deduplicate_records
from constants import REMARKS, OUTPUT_KEYS


def parse_metadata_and_extract_lineage(
    metadata_json_str: str,
    regulation: str = "",
    metadatakey: str = "",
    view_name: str = "",
    sql_key: str = "sql_query",
) -> List[Dict]:
    """Decode SQL from JSON metadata (base64), extract lineage, and deduplicate."""
    sql = decode_base64_sql_from_metadata(metadata_json_str, sql_key)
    results = extract_lineage_rows(sql, regulation, metadatakey, view_name)
    return deduplicate_records(results)


__all__ = [
    "parse_metadata_and_extract_lineage",
    "extract_lineage_rows",
    "decode_base64_sql_from_metadata",
    "deduplicate_records",
    "REMARKS",
    "OUTPUT_KEYS",
]
