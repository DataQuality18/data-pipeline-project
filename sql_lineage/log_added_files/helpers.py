"""
Select-list and context helpers for SQL lineage (v2).
"""
from typing import Optional, Tuple

from sqlglot import exp

from app.logging_config import get_logger

logger = get_logger(__name__)


def extract_select_list(select_exp) -> list:
    """
    Return list of (sql_text, alias, node) for each projection in the SELECT list.

    Args:
        select_exp: a sqlglot exp.Select node.

    Returns:
        List of (str, Optional[str], exp.Expression) tuples.
        Returns [] if select_exp has no expressions.
    """
    if not isinstance(select_exp, exp.Select):
        logger.warning(
            f"""extract_select_list: expected exp.Select
            received_type={type(select_exp).__name__}"""
        )
        return []

    projections = []
    for proj in select_exp.expressions:
        try:
            alias = None
            node = proj
            if isinstance(proj, exp.Alias):
                alias = proj.alias_or_name
                node = proj.this
            projections.append((str(node), alias, node))
        except Exception as exc:
            logger.error(
                f"""extract_select_list: error processing projection; skipping",
                exc={exc},
                proj_type={type(proj).__name__}"""
            )

    logger.debug(f"Extracted SELECT projections  projection_count={len(projections)}")
    return projections


def _nearest_subquery_alias(select_node: exp.Select) -> Optional[str]:
    """
    Find the alias of the nearest enclosing Subquery for a given Select, if any.
    Useful for tagging outer/inner layers (e.g., TSR_TS_DATA).

    Returns None if the Select is not inside a Subquery.
    Never raises.
    """
    if not isinstance(select_node, exp.Select):
        logger.warning(
            f"""_nearest_subquery_alias: expected exp.Select",
            received_type={type(select_node).__name__}"""
        )
        return None

    try:
        parent_subq = select_node.parent
        while parent_subq and not isinstance(parent_subq, exp.Subquery):
            parent_subq = parent_subq.parent

        if isinstance(parent_subq, exp.Subquery):
            alias = parent_subq.alias_or_name if parent_subq.args.get("alias") else None
            logger.debug(f"Nearest subquery alias found alias={alias}")
            return alias
    except Exception as exc:
        logger.error(
            f"""_nearest_subquery_alias: unexpected error traversing parents 
            exc={exc}"""
        )
    return None


def get_outer_derived_table(select: exp.Select) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (table_name, table_alias) for:
        SELECT ... FROM ( SELECT ... ) TSR_TS_DATA

    Returns (None, None) if the FROM clause is not a Subquery.
    Never raises.
    """
    if not isinstance(select, exp.Select):
        logger.warning(
           f"""get_outer_derived_table: expected exp.Select",
            received_type={type(select).__name__}"""
        )
        return None, None

    try:
        from_node = select.args.get("from")
        if not from_node:
            return None, None

        subq = from_node.this
        if isinstance(subq, exp.Subquery):
            alias = subq.alias_or_name
            logger.debug(f"Outer derived table detected subquery_alias={alias}")
            return alias, None
    except Exception as exc:
        logger.error(
            f"""get_outer_derived_table: error inspecting FROM clause",
            exc={exc}"""
        )
    return None, None


def is_function_only_expression(expr: exp.Expression) -> bool:
    """
    Return True if expr is a function/anonymous call with no column references.
    Returns False on any error.
    """
    try:
        result = (
            isinstance(expr, (exp.Func, exp.Anonymous))
            and not list(expr.find_all(exp.Column))
        )
        return result
    except Exception as exc:
        logger.error(
            f"""is_function_only_expression: error inspecting expression 
            exc={exc},
            expr_type={type(expr).__name__}"""
        )
        return False
