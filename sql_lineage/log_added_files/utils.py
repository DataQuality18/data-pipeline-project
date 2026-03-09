"""
Utility functions for SQL lineage extraction.
"""
import base64
import json
from typing import List, Optional

from sqlglot import exp

from app.logging_config import get_logger

logger = get_logger(__name__)


def decode_base64_sql_from_metadata(metadata_json_str: str, sql_key: str = "sql_query") -> str:
    """
    Decode base64-encoded SQL from a JSON metadata string.
    Falls back to returning the raw value if base64 decoding fails.

    Raises:
        ValueError: if metadata_json_str is empty or not valid JSON.
    """
    if not metadata_json_str or not metadata_json_str.strip():
        logger.error("Empty metadata string passed to decode_base64_sql_from_metadata")
        raise ValueError("metadata_json_str must not be empty")

    logger.debug("Decoding metadata JSON", sql_key=sql_key, input_len=len(metadata_json_str))

    try:
        meta = json.loads(metadata_json_str)
    except json.JSONDecodeError as exc:
        logger.error(
            "Failed to parse metadata JSON",
            exc=exc,
            preview=metadata_json_str[:200],
        )
        raise ValueError(f"Invalid JSON in metadata: {exc}") from exc

    b64 = meta.get(sql_key, "")
    if not b64:
        logger.warning(
            "SQL key not found or empty in metadata",
            sql_key=sql_key,
            available_keys=list(meta.keys()),
        )
        return ""

    try:
        decoded = base64.b64decode(b64).decode("utf-8")
        logger.debug("Base64 SQL decoded successfully", decoded_len=len(decoded))
        return decoded
    except Exception as exc:
        logger.warning(
            "Base64 decode failed; returning raw value",
            exc=exc,
            sql_key=sql_key,
            raw_preview=str(b64)[:80],
        )
        return b64


def ensure_list(val) -> List[str]:
    """Return val as a list of strings; None -> [], single value -> [str(val)]."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [str(val)]


def safe_name(obj) -> Optional[str]:
    """
    Extract a string name from an AST node or string.
    Returns None if obj is None.
    Never raises — any unexpected node type falls back to str().
    """
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    try:
        return getattr(obj, "name", None) or str(obj)
    except Exception as exc:
        logger.warning("safe_name: unexpected error extracting name", exc=exc, obj_type=type(obj).__name__)
        return None


def extract_columns_from_expression(expr) -> List[exp.Column]:
    """
    Return all Column nodes found recursively under the given expression.
    Returns an empty list if expr is not a valid sqlglot Expression.
    """
    if not isinstance(expr, exp.Expression):
        if expr is not None:
            logger.debug(
                "extract_columns_from_expression: non-Expression input ignored",
                input_type=type(expr).__name__,
            )
        return []
    try:
        return list(expr.find_all(exp.Column))
    except Exception as exc:
        logger.error(
            "extract_columns_from_expression: error traversing expression",
            exc=exc,
            expr_type=type(expr).__name__,
        )
        return []
