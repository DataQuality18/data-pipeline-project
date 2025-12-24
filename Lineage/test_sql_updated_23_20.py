"""
SQL Lineage extractor (sqlglot-based)

Key enhancements added (without breaking existing behavior):
1) Proper STAR (*) handling:
   - Supports unqualified * and qualified t.* stars.
   - If FROM is a derived table (subquery with alias) at the OUTER query level,
     we emit the derived table name as "Table Name" and mark remark "derived_table".
   - If * appears inside JOIN subqueries, we emit underlying physical table name,
     but "Table Alias Name" becomes the enclosing subquery alias (CURRENT_RECORD / PREVIOUS_RECORD).

2) Better scope building:
   - Builds FROM scope ONLY from the current SELECT's immediate FROM + JOIN sources.
     (Avoids polluting outer scopes with tables from inner subqueries.)

3) Derived expressions mapping:
   - If a derived expression contains no column references (e.g., DATE_FORMAT(CURRENT_TIMESTAMP())),
     map it to the PRIMARY base table of that SELECT (first table in FROM, typically TSR),
     not to a random joined source.

This file is intended to be dropped into your repo in place of the previous test_sql.py/test_sql_latestt.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Iterable, Set

import json
import base64
import sqlglot
from sqlglot import exp


# -------------------------
# Remarks (keep as list)
# -------------------------
REMARKS = {
    "DATABASE_NOT_SPECIFIED": "database_not_specified_in_query",
    "COLUMN_SELECTED_WITH_DB": "column_selected_with_db_in_query",
    "TABLE_AMBIGUOUS": "table_ambiguous",
    "ALL_COLUMNS": "all_columns_selected",
    "DERIVED_EXPRESSION": "derived_expression",
    "CASE_EXPRESSION": "case_expression",
    "WHERE_CLAUSE_COLUMN": "where_clause_column",
    "DERIVED_TABLE": "derived_table",
}


# -------------------------
# Helpers
# -------------------------
def safe_name(node: Any) -> str:
    """
    Best-effort conversion of sqlglot nodes -> string.
    """
    if node is None:
        return ""
    try:
        # For Identifier, TableAlias, etc.
        if hasattr(node, "name") and isinstance(getattr(node, "name"), str):
            return node.name
    except Exception:
        pass
    try:
        # If node is already a string
        if isinstance(node, str):
            return node
        return str(node).strip()
    except Exception:
        return ""


def _get_alias_name(node: Any) -> str:
    """
    Extract alias name for Table/Subquery/Alias expressions.
    """
    if node is None:
        return ""
    # sqlglot: .alias is often a TableAlias node; .alias_or_name returns string alias if exists
    try:
        a = getattr(node, "alias_or_name", None)
        if isinstance(a, str):
            # alias_or_name returns table name when alias is missing; we don't want that.
            # So only accept if node actually has an alias arg.
            if isinstance(getattr(node, "args", None), dict) and node.args.get("alias"):
                return a
    except Exception:
        pass
    # fallback: read args["alias"]
    try:
        alias_node = node.args.get("alias") if isinstance(getattr(node, "args", None), dict) else None
        if alias_node is None:
            return ""
        # alias_node can be TableAlias, Identifier, etc.
        if hasattr(alias_node, "this"):
            return safe_name(alias_node.this)
        return safe_name(alias_node)
    except Exception:
        return ""


def _iter_immediate_sources(select_node: exp.Select) -> Iterable[Tuple[str, Any, str]]:
    """
    Yields (role, source_node, alias_hint) for the current SELECT.
    role: "from" or "join"
    """
    frm = select_node.args.get("from")
    if frm is not None and getattr(frm, "this", None) is not None:
        src = frm.this
        yield ("from", src, _get_alias_name(src))

    joins = select_node.args.get("joins") or []
    for j in joins:
        src = getattr(j, "this", None)
        if src is None:
            continue
        yield ("join", src, _get_alias_name(src))


def _first_table_in_select(select_node: exp.Select) -> Tuple[str, str, str]:
    """
    Return (db, table, alias) for the FIRST base table in FROM (not JOIN),
    best-effort. Used to map derived expressions with no columns.
    """
    frm = select_node.args.get("from")
    if frm is None or frm.this is None:
        return ("", "", "")
    src = frm.this

    # FROM table
    if isinstance(src, exp.Table):
        db = safe_name(src.db)
        tbl = safe_name(src.this)
        alias = _get_alias_name(src)
        return (db, tbl, alias)

    # FROM (subquery) alias
    if isinstance(src, exp.Subquery):
        alias = _get_alias_name(src)
        # find first physical table inside
        inner_select = src.this if isinstance(src.this, exp.Expression) else None
        if inner_select is not None:
            for t in inner_select.find_all(exp.Table):
                db = safe_name(t.db)
                tbl = safe_name(t.this)
                return (db, tbl, alias)
        return ("", "", alias)

    return ("", "", "")


@dataclass(frozen=True)
class ScopeEntry:
    db: str
    table: str
    table_alias: str
    is_derived: bool = False  # derived table in outer FROM


def build_from_scope_map(select_node: exp.Select) -> Dict[str, ScopeEntry]:
    """
    Build a scope map for ONLY this SELECT's immediate sources.

    Key: qualifier (alias if exists else table name)
    Value: ScopeEntry(db, table, table_alias, is_derived)
    """
    scope: Dict[str, ScopeEntry] = {}

    for role, src, _alias_hint in _iter_immediate_sources(select_node):
        # -------- Table --------
        if isinstance(src, exp.Table):
            db = safe_name(src.db)
            tbl = safe_name(src.this)
            alias = _get_alias_name(src)
            key = alias or tbl
            if key and key not in scope:
                scope[key] = ScopeEntry(db=db, table=tbl, table_alias=alias, is_derived=False)
            continue

        # -------- Subquery --------
        if isinstance(src, exp.Subquery):
            subq_alias = _get_alias_name(src)
            if not subq_alias:
                # no alias -> can't reference from outside
                continue

            # If subquery is the main FROM source for this SELECT => treat as DERIVED TABLE
            # If subquery is inside JOIN => treat as JOIN alias mapping to physical base table
            if role == "from":
                # derived table output expects Table Name as derived alias
                scope[subq_alias] = ScopeEntry(db="", table=subq_alias, table_alias=subq_alias, is_derived=True)
            else:
                # join subquery: map alias -> underlying physical base table
                base_db, base_tbl = "", ""
                try:
                    inner = src.this
                    if inner is not None:
                        # Prefer first table in its FROM
                        if isinstance(inner, exp.Select):
                            base_db, base_tbl, _ = _first_table_in_select(inner)
                        else:
                            # fallback: first table anywhere
                            for t in inner.find_all(exp.Table):
                                base_db = safe_name(t.db)
                                base_tbl = safe_name(t.this)
                                break
                except Exception:
                    pass
                scope[subq_alias] = ScopeEntry(db=base_db, table=base_tbl, table_alias=subq_alias, is_derived=False)

            continue

        # -------- Alias wrapper (rare) --------
        if isinstance(src, exp.Alias):
            alias_name = _get_alias_name(src)
            inner = src.this
            if not alias_name or inner is None:
                continue
            if isinstance(inner, exp.Table):
                db = safe_name(inner.db)
                tbl = safe_name(inner.this)
                scope[alias_name] = ScopeEntry(db=db, table=tbl, table_alias=alias_name, is_derived=False)
            elif isinstance(inner, exp.Subquery):
                # same treatment as Subquery
                if role == "from":
                    scope[alias_name] = ScopeEntry(db="", table=alias_name, table_alias=alias_name, is_derived=True)
                else:
                    base_db, base_tbl = "", ""
                    try:
                        inner_sel = inner.this
                        if isinstance(inner_sel, exp.Select):
                            base_db, base_tbl, _ = _first_table_in_select(inner_sel)
                        else:
                            for t in inner_sel.find_all(exp.Table):
                                base_db = safe_name(t.db)
                                base_tbl = safe_name(t.this)
                                break
                    except Exception:
                        pass
                    scope[alias_name] = ScopeEntry(db=base_db, table=base_tbl, table_alias=alias_name, is_derived=False)

    return scope


def _star_qualifier(star_node: exp.Expression) -> str:
    """
    Extract qualifier for star.
    Handles both:
      - exp.Star (possibly qualified)
      - exp.Column where name == "*" (qualified star)
    """
    # exp.Column(table="t", this="*") case
    try:
        if isinstance(star_node, exp.Column) and safe_name(star_node.name) == "*":
            return safe_name(star_node.table)
    except Exception:
        pass

    # exp.Star case
    try:
        # some sqlglot versions store in args
        q = safe_name(getattr(star_node, "table", None))
        if q:
            return q
    except Exception:
        pass

    try:
        # args might have "this" or "table"
        if isinstance(getattr(star_node, "args", None), dict):
            for k in ("table", "this"):
                q = safe_name(star_node.args.get(k))
                if q and q != "*":
                    return q
    except Exception:
        pass

    return ""


def extract_columns_from_expression(expr_node: exp.Expression) -> List[exp.Column]:
    """
    Return unique column nodes referenced inside an expression.
    """
    cols: List[exp.Column] = []
    seen: Set[str] = set()
    for c in expr_node.find_all(exp.Column):
        try:
            name = safe_name(c.name)
            if name == "*":
                continue
            key = f"{safe_name(c.table)}::{name}"
            if key not in seen:
                seen.add(key)
                cols.append(c)
        except Exception:
            continue
    return cols


def _make_row(
    db: str,
    table: str,
    table_alias: str,
    col: str,
    alias: str,
    regulation: str,
    metadatakey: str,
    view_name: str,
    remarks: List[str],
) -> Dict[str, Any]:
    return {
        "Database Name": db or "",
        "Table Name": table or "",
        "Table Alias Name": table_alias or "",
        "Column Name": col or "",
        "Alias Name": alias or "",
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": remarks or [],
    }


def _resolve_scope_entry(
    col_qualifier: str,
    from_scope: Dict[str, ScopeEntry],
    primary_key: str,
) -> Tuple[str, str, str, List[str]]:
    """
    Resolve qualifier -> (db, table, table_alias, remarks)
    """
    remarks: List[str] = []
    entry: Optional[ScopeEntry] = None

    if col_qualifier and col_qualifier in from_scope:
        entry = from_scope[col_qualifier]
    elif len(from_scope) == 1:
        entry = next(iter(from_scope.values()))
    elif primary_key and primary_key in from_scope:
        # only use primary_key for derived expr fallback, not for normal columns
        entry = None
    else:
        entry = None

    if entry is None:
        remarks.append(REMARKS["TABLE_AMBIGUOUS"])
        return ("", "", "", remarks)

    if entry.is_derived:
        remarks.append(REMARKS["DERIVED_TABLE"])

    if entry.db:
        remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"])
    else:
        remarks.append(REMARKS["DATABASE_NOT_SPECIFIED"])

    return (entry.db, entry.table, entry.table_alias or col_qualifier, remarks)


def resolve_star(
    select_node: exp.Select,
    star_node: exp.Expression,
    from_scope: Dict[str, ScopeEntry],
    regulation: str,
    metadatakey: str,
    view_name: str,
    parent_subquery_alias: str = "",
) -> List[Dict[str, Any]]:
    """
    Resolve STAR (*) selection with correct Table/alias output.
    """
    qualifier = _star_qualifier(star_node)

    # Determine primary base table for derived expr fallback
    primary_db, primary_tbl, primary_alias = _first_table_in_select(select_node)
    primary_key = primary_alias or primary_tbl

    db, table, table_alias, remarks = _resolve_scope_entry(qualifier, from_scope, primary_key)

    # If unqualified star and FROM is a single derived table => keep derived table name
    # Already handled by scope entry (is_derived => table==alias, alias==alias)

    # If we're inside a JOIN subquery, and inner physical table has no alias,
    # propagate enclosing alias (CURRENT_RECORD / PREVIOUS_RECORD)
    if parent_subquery_alias and not table_alias and table:
        table_alias = parent_subquery_alias

    remarks = [REMARKS["ALL_COLUMNS"]] + remarks

    return [_make_row(
        db=db,
        table=table,
        table_alias=table_alias,
        col="*",
        alias="",
        regulation=regulation,
        metadatakey=metadatakey,
        view_name=view_name,
        remarks=remarks,
    )]


def resolve_column(
    select_node: exp.Select,
    col_node: exp.Column,
    from_scope: Dict[str, ScopeEntry],
    regulation: str,
    metadatakey: str,
    view_name: str,
    col_alias: str = "",
    extra_remarks: Optional[List[str]] = None,
    parent_subquery_alias: str = "",
) -> Dict[str, Any]:
    qualifier = safe_name(col_node.table)
    col_name = safe_name(col_node.name)

    # Primary base table for derived expressions without columns
    primary_db, primary_tbl, primary_alias = _first_table_in_select(select_node)
    primary_key = primary_alias or primary_tbl

    # For plain columns: if multiple scope entries and no qualifier, do NOT force to primary.
    # Keep ambiguous.
    if not qualifier and len(from_scope) > 1:
        db, table, table_alias, remarks = ("", "", "", [REMARKS["TABLE_AMBIGUOUS"]])
    else:
        db, table, table_alias, remarks = _resolve_scope_entry(qualifier, from_scope, primary_key)

    if parent_subquery_alias and table and not table_alias:
        table_alias = parent_subquery_alias

    if extra_remarks:
        remarks = extra_remarks + remarks

    return _make_row(
        db=db,
        table=table,
        table_alias=table_alias or qualifier,
        col=col_name,
        alias=col_alias or "",
        regulation=regulation,
        metadatakey=metadatakey,
        view_name=view_name,
        remarks=remarks,
    )


def resolve_expression_projection(
    select_node: exp.Select,
    expr_node: exp.Expression,
    out_alias: str,
    from_scope: Dict[str, ScopeEntry],
    regulation: str,
    metadatakey: str,
    view_name: str,
    parent_subquery_alias: str = "",
) -> List[Dict[str, Any]]:
    """
    Handle derived expressions, CASE, etc.
    """
    extra = [REMARKS["DERIVED_EXPRESSION"]]
    if isinstance(expr_node, exp.Case):
        extra.append(REMARKS["CASE_EXPRESSION"])

    cols = extract_columns_from_expression(expr_node)
    if cols:
        rows = []
        for c in cols:
            rows.append(resolve_column(
                select_node=select_node,
                col_node=c,
                from_scope=from_scope,
                regulation=regulation,
                metadatakey=metadatakey,
                view_name=view_name,
                col_alias=out_alias,
                extra_remarks=extra,
                parent_subquery_alias=parent_subquery_alias,
            ))
        return rows

    # No columns referenced => map to PRIMARY base table of the SELECT (first FROM table)
    primary_db, primary_tbl, primary_alias = _first_table_in_select(select_node)
    primary_key = primary_alias or primary_tbl
    if primary_key and primary_key in from_scope:
        entry = from_scope[primary_key]
        db, table, table_alias = entry.db, entry.table, entry.table_alias
        remarks = extra[:]
        if entry.is_derived:
            remarks.append(REMARKS["DERIVED_TABLE"])
        remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"])
        if parent_subquery_alias and table and not table_alias:
            table_alias = parent_subquery_alias
        return [_make_row(db, table, table_alias or primary_key, col=out_alias or "", alias=out_alias or "", regulation=regulation, metadatakey=metadatakey, view_name=view_name, remarks=remarks)]

    # Fallback: can't map
    return [_make_row("", "", parent_subquery_alias or "", col=out_alias or "", alias=out_alias or "", regulation=regulation, metadatakey=metadatakey, view_name=view_name, remarks=extra + [REMARKS["TABLE_AMBIGUOUS"]])]


def _get_enclosing_subquery_alias(node: exp.Expression) -> str:
    """
    For a SELECT inside a Subquery, return Subquery alias (CURRENT_RECORD / PREVIOUS_RECORD / TSR_TS_DATA).
    """
    try:
        p = node.parent
        while p is not None:
            if isinstance(p, exp.Subquery):
                return _get_alias_name(p)
            p = p.parent
    except Exception:
        pass
    return ""


def extract_lineage_for_select(
    select_node: exp.Select,
    regulation: str,
    metadatakey: str,
    view_name: str,
) -> List[Dict[str, Any]]:
    """
    Produce lineage rows for one SELECT node (select list + where clause columns).
    """
    parent_alias = _get_enclosing_subquery_alias(select_node)

    from_scope = build_from_scope_map(select_node)

    results: List[Dict[str, Any]] = []

    # ---- SELECT list ----
    for proj in select_node.expressions or []:
        out_alias = ""
        node = proj
        if isinstance(proj, exp.Alias):
            out_alias = safe_name(proj.alias_or_name)
            node = proj.this

        # STAR
        if isinstance(node, exp.Star) or (isinstance(node, exp.Column) and safe_name(node.name) == "*"):
            results.extend(resolve_star(
                select_node=select_node,
                star_node=node,
                from_scope=from_scope,
                regulation=regulation,
                metadatakey=metadatakey,
                view_name=view_name,
                parent_subquery_alias=parent_alias,
            ))
            continue

        # Direct Column
        if isinstance(node, exp.Column):
            results.append(resolve_column(
                select_node=select_node,
                col_node=node,
                from_scope=from_scope,
                regulation=regulation,
                metadatakey=metadatakey,
                view_name=view_name,
                col_alias=out_alias,
                extra_remarks=[],
                parent_subquery_alias=parent_alias,
            ))
            continue

        # Derived expressions / CASE / functions
        results.extend(resolve_expression_projection(
            select_node=select_node,
            expr_node=node,
            out_alias=out_alias or safe_name(node),
            from_scope=from_scope,
            regulation=regulation,
            metadatakey=metadatakey,
            view_name=view_name,
            parent_subquery_alias=parent_alias,
        ))

    # ---- WHERE clause columns ----
    where_expr = select_node.args.get("where")
    if where_expr is not None:
        for c in where_expr.find_all(exp.Column):
            col_name = safe_name(c.name)
            if col_name == "*":
                continue
            results.append(resolve_column(
                select_node=select_node,
                col_node=c,
                from_scope=from_scope,
                regulation=regulation,
                metadatakey=metadatakey,
                view_name=view_name,
                col_alias="",
                extra_remarks=[REMARKS["WHERE_CLAUSE_COLUMN"]],
                parent_subquery_alias=parent_alias,
            ))

    return results


def extract_lineage_rows(
    sql: str,
    regulation: str,
    metadatakey: str,
    view_name: str,
    dialect: str = "sparksql",
) -> List[Dict[str, Any]]:
    """
    Parse SQL and extract lineage rows across all SELECTs (outer + subqueries).
    """
    ast = sqlglot.parse_one(sql, read=dialect)
    results: List[Dict[str, Any]] = []

    for sel in ast.find_all(exp.Select):
        results.extend(extract_lineage_for_select(sel, regulation, metadatakey, view_name))

    return results


# -------------------------
# Convenience wrapper used by API layer (optional)
# -------------------------

# -------------------------
# Metadata JSON helpers (kept for compatibility with your current API layer)
# -------------------------
def decode_base64_sql_from_metadata(metadata_json_str: str, sql_key: str = "sql_query") -> str:
    meta = json.loads(metadata_json_str) if metadata_json_str else {}
    b64 = meta.get(sql_key, "") if isinstance(meta, dict) else ""
    try:
        return base64.b64decode(b64).decode("utf-8")
    except Exception:
        return b64


def parse_metadata_and_extract_lineage(
    metadata_json_str: str,
    regulation: str = "",
    metadatakey: str = "",
    view_name: str = "",
    sql_key: str = "sql_query",
) -> List[Dict[str, Any]]:
    sql = decode_base64_sql_from_metadata(metadata_json_str, sql_key)
    return extract_lineage_rows(sql, regulation, metadatakey, view_name)

def parse_sql_lineage(
    sql: str,
    regulation: str,
    metadatakey: str,
    view_name: str,
    dialect: str = "sparksql",
) -> Dict[str, Any]:
    rows = extract_lineage_rows(sql, regulation, metadatakey, view_name, dialect=dialect)
    return {
        "success": True,
        "message": f"Successfully extracted lineage for 1 SQL queries",
        "total_records": len(rows),
        "lineage_data": rows,
    }