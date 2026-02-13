"""
Select-list and context helpers for SQL lineage (v2).
"""
from typing import Optional, Tuple

from sqlglot import exp


def extract_select_list(select_exp):
    """Return list of (sql_text, alias, node) for each projection in the SELECT list."""
    projections = []
    for proj in select_exp.expressions:
        alias = None
        node = proj
        if isinstance(proj, exp.Alias):
            alias = proj.alias_or_name
            node = proj.this
        projections.append((str(node), alias, node))
    return projections


def _nearest_subquery_alias(select_node: exp.Select) -> Optional[str]:
    """
    Finds the alias of the nearest enclosing Subquery for a given Select, if any.
    Useful for tagging outer/inner layers (e.g., TSR_TS_DATA).
    """
    parent_subq = select_node.parent
    while parent_subq and not isinstance(parent_subq, exp.Subquery):
        parent_subq = parent_subq.parent
    if isinstance(parent_subq, exp.Subquery):
        return parent_subq.alias_or_name if parent_subq.args.get("alias") else None
    return None


def get_outer_derived_table(select: exp.Select) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (table_name, table_alias) for:
      SELECT ... FROM ( SELECT ... ) TSR_TS_DATA
    """
    from_node = select.args.get("from")
    if not from_node:
        return None, None

    subq = from_node.this
    if isinstance(subq, exp.Subquery):
        return subq.alias_or_name, None  # table name, alias
    return None, None


def is_function_only_expression(expr: exp.Expression) -> bool:
    """True if expr is a function (or anonymous) and contains no column references."""
    return (
        isinstance(expr, (exp.Func, exp.Anonymous))
        and not list(expr.find_all(exp.Column))
    )
