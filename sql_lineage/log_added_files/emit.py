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
from app.logging_config import get_logger

logger = get_logger(__name__)


def emit_dataset_lineage(ast, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    """
    Handle dataset-level lineage for patterns like:
        SELECT * FROM ( SELECT ... ) TSR_TS_DATA

    Returns a (possibly empty) list of lineage row dicts.
    Never raises.
    """
    if ast is None:
        logger.warning("emit_dataset_lineage: received None AST; returning empty")
        return []

    rows: List[Dict] = []

    try:
        for select in ast.find_all(exp.Select):
            try:
                # Only process outermost SELECT (no parent Select above it)
                parent = select.parent
                while parent:
                    if isinstance(parent, exp.Select):
                        break
                    parent = parent.parent
                else:
                    # outermost SELECT
                    from_node = select.args.get("from")
                    if not from_node:
                        continue

                    subq = from_node.this
                    if isinstance(subq, exp.Subquery) and subq.alias_or_name:
                        if any(isinstance(e, exp.Star) for e in select.expressions):
                            db, base_table = _pick_base_table_from_subquery(subq)

                            if not base_table:
                                base_table = subq.alias_or_name
                                table_alias = ""
                            else:
                                table_alias = subq.alias_or_name

                            logger.debug(
                                "Dataset lineage row emitted",
                                db=db,
                                base_table=base_table,
                                table_alias=table_alias,
                                view_name=view_name,
                            )
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
            except Exception as exc:
                logger.error(
                    "emit_dataset_lineage: error processing a Select node; skipping",
                    exc=exc,
                )
    except Exception as exc:
        logger.error("emit_dataset_lineage: error traversing AST", exc=exc)

    logger.debug("Dataset lineage emission complete", row_count=len(rows))
    return rows


def _emit_column_lineage(
    results: list,
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
) -> None:
    """
    Emit a single normalized lineage row into results.

    Resolution order:
      1) explicit_table_* overrides (if provided)
      2) qualifier (if present and resolvable in from_scope)
      3) fallback_alias (e.g., JOIN alias) if provided
      4) single local_scope source (if provided)
      5) sole entry in from_scope

    Never raises; appends nothing if a critical error occurs.
    """
    if not column_name:
        logger.warning(
            "_emit_column_lineage: empty column_name received; skipping emit",
            qualifier=qualifier,
            view_name=view_name,
        )
        return

    try:
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
            elif local_scope and len(local_scope) == 1:
                key = next(iter(local_scope.keys()))
                if key in from_scope:
                    _, db, table, table_alias = from_scope[key]

        if not table and qualifier in from_scope:
            _, db, table, table_alias = from_scope[qualifier]

        # Handle invalid alias or derived table
        if not table:
            if effective_qualifier and effective_qualifier not in from_scope:
                # Unknown alias — mark as invalid; do NOT invent table
                table = ""
                table_alias = effective_qualifier
                if REMARKS["INVALID_TABLE_ALIAS"] not in remark_list:
                    remark_list.append(REMARKS["INVALID_TABLE_ALIAS"])
                logger.debug(
                    "Invalid table alias detected",
                    qualifier=effective_qualifier,
                    column=column_name,
                )
            else:
                # True derived source (subquery alias)
                table = effective_qualifier or ""
                table_alias = ""
                if REMARKS["DERIVED_TABLE"] not in remark_list:
                    remark_list.append(REMARKS["DERIVED_TABLE"])
                logger.debug(
                    "Derived table source used",
                    qualifier=effective_qualifier,
                    column=column_name,
                )

        row = {
            "databaseName": str(db or "").lower(),
            "tableName": str(table or "").lower(),
            "tableAliasName": str(table_alias or (effective_qualifier or "")).lower(),
            "columnName": str(column_name or "").lower(),
            "aliasName": "",
            "regulation": regulation,
            "metadatakey": metadatakey,
            "viewName": view_name,
            "remarks": ensure_list(remark_list),
        }
        results.append(row)
        logger.debug(
            "Column lineage row emitted",
            table=row["tableName"],
            column=row["columnName"],
            remarks=row["remarks"],
        )

    except Exception as exc:
        logger.error(
            "_emit_column_lineage: unexpected error; skipping row",
            exc=exc,
            column_name=column_name,
            qualifier=qualifier,
            view_name=view_name,
        )


def resolve_star(
    star_node,
    local_scope: dict,
    global_scope: dict,
    enclosing_alias: Optional[str],
    regulation: str,
    metadatakey: str,
    view_name: str,
) -> List[Dict]:
    """
    Resolve SELECT * (or tbl.*) to one lineage row with columnName='*'.
    Never raises; returns [] on error.
    """
    try:
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

        logger.debug(
            "STAR resolved",
            qualifier=qualifier,
            table=table,
            db=db,
            enclosing_alias=enclosing_alias,
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

    except Exception as exc:
        logger.error(
            "resolve_star: unexpected error",
            exc=exc,
            view_name=view_name,
        )
        return []
