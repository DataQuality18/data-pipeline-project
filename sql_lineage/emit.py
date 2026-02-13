"""
Lineage row emission: dataset lineage, column lineage, and STAR resolution.
"""
from typing import Dict, List, Optional

from sqlglot import exp

from constants import REMARKS
from utils import ensure_list, safe_name
from scope import (
    _pick_base_table_from_subquery,
    _resolve_source,
    _attach_enclosing_alias_if_missing,
)


def emit_dataset_lineage(ast, regulation, metadatakey, view_name):
    """
    Emit lineage for outermost SELECT * FROM (subquery) alias patterns.
    Uses SQLGlot AST: select.args['from_'].
    """
    rows = []

    for select in ast.find_all(exp.Select):
        # Ascend to find if this Select is the outermost (no parent Select)
        parent = select.parent
        while parent:
            if isinstance(parent, exp.Select):
                break
            parent = parent.parent
        else:
            # This is the outermost Select
            from_node = select.args.get("from_")
            if not from_node:
                continue

            subq = from_node.this
            # FROM (subquery) alias and SELECT *
            if isinstance(subq, exp.Subquery) and subq.alias_or_name:
                if any(isinstance(e, exp.Star) for e in select.expressions):
                    db, base_table = _pick_base_table_from_subquery(subq)
                    if not base_table:
                        base_table = subq.alias_or_name
                        table_alias = ""
                    else:
                        table_alias = subq.alias_or_name
                    rows.append({
                        "Database Name": db or "",
                        "Table Name": base_table,
                        "Table Alias Name": table_alias,
                        "Column Name": "*",
                        "Alias Name": "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": [
                            REMARKS["ALL_COLUMNS"],
                            REMARKS["DERIVED_TABLE"]
                        ],
                    })
    return rows


def _emit_column_lineage(
    results,
    qualifier: Optional[str],
    column_name: str,
    from_scope: dict,
    regulation: str,
    metadatakey: str,
    view_name: str,
    remark_list,
    fallback_alias: Optional[str] = None,
    explicit_table_name: Optional[str] = None,
    explicit_table_alias: Optional[str] = None,
    local_scope: Optional[dict] = None,
):
    """
    Append one normalized lineage row. Resolution order: explicit_table_*,
    then qualifier, then fallback_alias, then single local_scope source, then sole from_scope.
    """
    db = ""
    table = explicit_table_name or ""
    table_alias = explicit_table_alias or ""
    effective_qualifier = qualifier or fallback_alias

    if not table or not table_alias:
        if effective_qualifier and effective_qualifier in from_scope:
            _, db, table, table_alias = from_scope[effective_qualifier]
        elif len(from_scope) == 1:
            _, db, table, table_alias = next(iter(from_scope.values()))
        # Single source in this SELECT (e.g. FROM cte or one table): resolve via that key
        elif local_scope and len(local_scope) == 1:
            key = next(iter(local_scope.keys()))
            if key in from_scope:
                _, db, table, table_alias = from_scope[key]
    if not table and qualifier in from_scope:
        _, db, table, table_alias = from_scope[qualifier]

    # Invalid or derived: set table/alias and tag remarks
    if not table:
        if effective_qualifier and effective_qualifier not in from_scope:
            table = effective_qualifier
            table_alias = ""
            effective_qualifier = ""
            if REMARKS["INVALID_TABLE_ALIAS"] not in remark_list:
                remark_list.append(REMARKS["INVALID_TABLE_ALIAS"])
        else:
            table = effective_qualifier or ""
            table_alias = ""
            effective_qualifier = ""
            if REMARKS["DERIVED_TABLE"] not in remark_list:
                remark_list.append(REMARKS["DERIVED_TABLE"])

    results.append({
        "Database Name": db or "",
        "Table Name": table or "",
        "Table Alias Name": table_alias or (effective_qualifier or ""),
        "Column Name": column_name,
        "Alias Name": "",
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": ensure_list(remark_list),
    })


def resolve_star(star_node, local_scope, global_scope, enclosing_alias, regulation, metadatakey, view_name):
    """Resolve SELECT * (or tbl.*) to one lineage row with Column Name '*'."""
    qualifier = ""

    if hasattr(star_node, "table") and star_node.table:
        qualifier = star_node.table
    else:
        this_arg = getattr(star_node, "args", {}).get("this")
        if this_arg is not None:
            qualifier = safe_name(this_arg) or ""

    db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)
    db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias, global_scope)

    return [{
        "Database Name": db,
        "Table Name": table,
        "Table Alias Name": table_alias,
        "Column Name": "*",
        "Alias Name": "",
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": [REMARKS["ALL_COLUMNS"]],
    }]
