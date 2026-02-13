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
    Handles:
      SELECT * FROM ( SELECT ... ) TSR_TS_DATA

    Uses SQLGlot AST correctly:
      select.args['from']
    """
    rows = []

    for select in ast.find_all(exp.Select):
        # check outermost SELECT (no parent Select above it)
        parent = select.parent
        while parent:
            if isinstance(parent, exp.Select):
                break
            parent = parent.parent
        else:
            # outermost
            from_node = select.args.get("from")
            if not from_node:
                continue

            subq = from_node.this
            if isinstance(subq, exp.Subquery) and subq.alias_or_name:
                # ensure SELECT *
                if any(isinstance(e, exp.Star) for e in select.expressions):
                    db, base_table = _pick_base_table_from_subquery(subq)

                    if not base_table:
                        base_table = subq.alias_or_name
                        table_alias = ""
                    else:
                        table_alias = subq.alias_or_name

                    rows.append({
                        "databaseName": db or "",
                        "tableName": base_table or "",
                        "tableAliasName": table_alias,
                        "columnName": "*",
                        "aliasName": "",
                        "regulation": regulation,
                        "metadatakey": metadatakey,
                        "viewName": view_name,
                        "remarks": [
                            REMARKS["ALL_COLUMNS"],
                            REMARKS["DERIVED_TABLE"],
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
    Emits a normalized lineage row.

    Resolution order:
      1) explicit_table_* overrides (if provided)
      2) qualifier (if present and resolvable)
      3) fallback_alias (e.g., JOIN alias) if provided
      4) single local_scope source (if provided)
      5) sole entry in from_scope
    """
    db = ""
    table = explicit_table_name or ""
    table_alias = explicit_table_alias or ""
    effective_qualifier = qualifier or fallback_alias

    # Resolve from scope if explicit not given
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

    # If still no table, handle invalid / derived
    if not table:
        if effective_qualifier and effective_qualifier not in from_scope:
            # INVALID ALIAS -> do not invent table; capture alias
            table = ""
            table_alias = effective_qualifier
            if REMARKS["INVALID_TABLE_ALIAS"] not in remark_list:
                remark_list.append(REMARKS["INVALID_TABLE_ALIAS"])
        else:
            # TRUE DERIVED (subquery alias)
            table = effective_qualifier or ""
            table_alias = ""
            if REMARKS["DERIVED_TABLE"] not in remark_list:
                remark_list.append(REMARKS["DERIVED_TABLE"])

    results.append({
        "databaseName": str(db or "").lower(),
        "tableName": str(table or "").lower(),
        "tableAliasName": str(table_alias or (effective_qualifier or "")).lower(),
        "columnName": str(column_name or "").lower(),
        "aliasName": "",
        "regulation": regulation,
        "metadatakey": metadatakey,
        "viewName": view_name,
        "remarks": ensure_list(remark_list),
    })


def resolve_star(
    star_node,
    local_scope,
    global_scope,
    enclosing_alias,
    regulation,
    metadatakey,
    view_name,
):
    """Resolve SELECT * (or tbl.*) to one lineage row with Column Name '*'."""
    qualifier = ""
    if hasattr(star_node, "table") and star_node.table:
        qualifier = star_node.table
    else:
        this_arg = getattr(star_node, "args", {}).get("this")
        if this_arg is not None:
            qualifier = safe_name(this_arg) or ""

    db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)
    db, table, table_alias = _attach_enclosing_alias_if_missing(
        db, table, table_alias, enclosing_alias, global_scope
    )

    return [{
        "databaseName": db,
        "tableName": table,
        "tableAliasName": table_alias,
        "columnName": "*",
        "aliasName": "",
        "regulation": regulation,
        "metadatakey": metadatakey,
        "viewName": view_name,
        "remarks": [REMARKS["ALL_COLUMNS"]],
    }]
