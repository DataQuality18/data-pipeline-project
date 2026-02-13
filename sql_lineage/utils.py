"""
Utility functions for SQL lineage extraction.
"""
import base64
import json
from typing import List, Optional

from sqlglot import exp


def decode_base64_sql_from_metadata(metadata_json_str: str, sql_key: str = "sql_query") -> str:
    """Decode base64-encoded SQL from a JSON metadata string; falls back to raw value if decode fails."""
    meta = json.loads(metadata_json_str)
    b64 = meta.get(sql_key, "")
    try:
        return base64.b64decode(b64).decode("utf-8")
    except Exception:
        return b64


def ensure_list(val) -> List[str]:
    """Return val as a list of strings; None -> [], single value -> [str(val)]."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [str(val)]


def safe_name(obj) -> Optional[str]:
    """Extract a string name from an AST node or string; returns None for None."""
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    return getattr(obj, "name", None) or str(obj)


def extract_columns_from_expression(expr):
    """Return all Column nodes found under the given expression (recursive)."""
    if not isinstance(expr, exp.Expression):
        return []
    return list(expr.find_all(exp.Column))
