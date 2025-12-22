"""
sql_lineage_extractor.py
(Final backward-compatible enhanced version)

Enhancements Added:
- CASE expression lineage
- WHERE / GROUP BY / HAVING lineage
- Derived expression column expansion
- Remarks taxonomy as list[str]
- Table Alias Name preserved
- ZERO breaking changes to existing behavior

NEW ADDITIVE FIX (no old behavior removed):
- Use per-SELECT local from-scope to resolve:
  - SELECT * inside subqueries (TS_FACT)
  - WHERE ROW_NUM inside subqueries
  - CASE/derived columns when scope is local
"""

import base64
import json
from typing import List, Dict, Tuple, Optional
import sqlglot
from sqlglot import exp

# -------------------------
# Constants
# -------------------------

REMARKS = {
    "ALL_COLUMNS": "all_columns_selected",
    "COLUMN_SELECTED": "column_selected",
    "COLUMN_SELECTED_WITH_DB": "column_selected_with_database",
    "COLUMN_SELECTED_NO_DB": "column_selected_database_not_specified",
    "TABLE_AMBIGUOUS": "table_name_ambiguous",
    "DATABASE_NOT_SPECIFIED": "database_not_specified_in_query",
    "INNER_ALIAS": "Inner Query Alias Layer",
    "SUBQUERY_LAYER": "Subquery Layer",
    "DERIVED_EXPR": "derived_expression",

    # NEW (Non-breaking)
    "CASE_EXPR": "case_expression",
    "WHERE_COLUMN": "where_clause_column",
    "GROUP_BY_COLUMN": "group_by_column",
    "HAVING_COLUMN": "having_clause_column",
}

OUTPUT_KEYS = [
    "Database Name",
    "Table Name",
    "Table Alias Name",
    "Column Name",
    "Alias Name",
    "Regulation",
    "Metadatakey",
    "View Name",
    "Remarks",
]

# -------------------------
# Utilities
# -------------------------

def decode_base64_sql_from_metadata(metadata_json_str: str, sql_key: str = "sql_query") -> str:
    meta = json.loads(metadata_json_str)
    b64 = meta.get(sql_key, "")
    try:
        return base64.b64decode(b64).decode("utf-8")
    except Exception:
        return b64


def ensure_list(val) -> List[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [str(val)]


def safe_name(obj) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    return getattr(obj, "name", None) or str(obj)


def extract_columns_from_expression(expr):
    if not isinstance(expr, exp.Expression):
        return []
    return list(expr.find_all(exp.Column))


# -------------------------
# FROM-scope mapping
# -------------------------

def _pick_base_table_from_subquery(subq: exp.Subquery) -> Tuple[str, str]:
    """
    Try to resolve the "real" base table behind a JOIN subquery like:

        (SELECT * FROM some_db.some_table WHERE row_num = 1) CURRENT_RECORD

    Returns: (db, table_name)
    If multiple tables exist, returns ("", "__SUBQUERY__").
    """
    tables = list(subq.find_all(exp.Table))

    uniq = []
    seen = set()
    for t in tables:
        t_name = safe_name(t.this) or ""
        t_db = safe_name(t.db) or ""
        key = (t_db, t_name)
        if t_name and key not in seen:
            seen.add(key)
            uniq.append(key)

    if len(uniq) == 1:
        return uniq[0][0], uniq[0][1]

    return "", "__SUBQUERY__"


def build_from_scope_map(ast_root) -> Dict[str, Tuple]:
    """
    alias_key -> (node, db, table_name, table_alias)

    Includes:
    - Base tables
    - Table aliases
    - JOIN subquery aliases (CURRENT_RECORD / PREVIOUS_RECORD)
    """
    from_map = {}

    # 1) All Tables
    for tbl in ast_root.find_all(exp.Table):
        table_name = safe_name(tbl.this)
        db = safe_name(tbl.db)
        table_alias = tbl.alias_or_name if tbl.alias else None
        key = table_alias or table_name
        if key and key not in from_map:
            from_map[key] = (tbl, db, table_name, table_alias)

    # 2) exp.Alias
    for alias in ast_root.find_all(exp.Alias):
        key = alias.alias_or_name
        if not key:
            continue

        if isinstance(alias.this, exp.Table):
            tbl = alias.this
            table_name = safe_name(tbl.this)
            db = safe_name(tbl.db)
            from_map[key] = (alias, db, table_name, key)

        elif isinstance(alias.this, exp.Subquery):
            from_map[key] = (alias, None, "__SUBQUERY__", key)

    # 3) JOIN Subquery alias mapping (CURRENT_RECORD / PREVIOUS_RECORD)
    for subq in ast_root.find_all(exp.Subquery):
        subq_alias = subq.alias_or_name if subq.args.get("alias") else None
        if not subq_alias:
            continue

        db, base_table = _pick_base_table_from_subquery(subq)
        from_map[subq_alias] = (subq, db, base_table, subq_alias)

    return from_map


# -------------------------
# NEW (ADDITIVE): resolver that prefers local scope but preserves old global behavior
# -------------------------

def _resolve_source(
    qualifier: str,
    local_scope: Dict[str, Tuple],
    global_scope: Dict[str, Tuple],
) -> Tuple[str, str, str, str]:
    """
    Returns: (db, table, table_alias, resolution_mode)

    resolution_mode is just for internal clarity; you can ignore it.
    """
    # 1) If qualifier exists, prefer local scope
    if qualifier:
        if qualifier in local_scope:
            _, db, table, table_alias = local_scope[qualifier]
            return db or "", table or "", table_alias or "", "QUAL_LOCAL"
        if qualifier in global_scope:
            _, db, table, table_alias = global_scope[qualifier]
            return db or "", table or "", table_alias or "", "QUAL_GLOBAL"
        return "", "", "", "QUAL_NOT_FOUND"

    # 2) If no qualifier, try single-table local scope
    if len(local_scope) == 1:
        _, db, table, table_alias = next(iter(local_scope.values()))
        return db or "", table or "", table_alias or "", "UNQUAL_LOCAL_SINGLE"

    # 3) Fall back to old behavior: single-table global scope
    if len(global_scope) == 1:
        _, db, table, table_alias = next(iter(global_scope.values()))
        return db or "", table or "", table_alias or "", "UNQUAL_GLOBAL_SINGLE"

    return "", "", "", "AMBIGUOUS"


# -------------------------
# Select list helper
# -------------------------

def extract_select_list(select_exp):
    projections = []
    for proj in select_exp.expressions:
        alias = None
        node = proj
        if isinstance(proj, exp.Alias):
            alias = proj.alias_or_name
            node = proj.this
        projections.append((str(node), alias, node))
    return projections


# -------------------------
# STAR handling (UPDATED additively)
# -------------------------

def resolve_star(select_node, star_node, local_scope, global_scope, regulation, metadatakey, view_name):
    """
    OLD behavior: returned blanks.
    NEW additive behavior: fill table/db/alias when determinable.
    This fixes:
      - SELECT * inside CURRENT_RECORD / PREVIOUS_RECORD subqueries (TS_FACT)
      - TSR.* also becomes correctly attributable (if star has qualifier)
    """
    # If STAR is qualified like TSR.*
    qualifier = ""
    try:
        qualifier = getattr(star_node, "table", "") or ""
    except Exception:
        qualifier = ""

    db, table, table_alias, _ = _resolve_source(qualifier, local_scope, global_scope)

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


# -------------------------
# Core extraction
# -------------------------

def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    ast = sqlglot.parse_one(sql)
    results = []

    # OLD behavior kept: global scope
    global_scope = build_from_scope_map(ast)

    selects = list(ast.find_all(exp.Select))

    for select in selects:
        # ✅ NEW additive: local scope for THIS select only
        local_scope = build_from_scope_map(select)

        for col_text, col_alias, col_node in extract_select_list(select):

            # -------------------------
            # STAR
            # -------------------------
            if isinstance(col_node, exp.Star):
                results.extend(
                    resolve_star(select, col_node, local_scope, global_scope, regulation, metadatakey, view_name)
                )
                continue

            # -------------------------
            # DIRECT COLUMN
            # -------------------------
            if isinstance(col_node, exp.Column):
                qualifier = col_node.table or ""
                column_name = col_node.name
                remarks = []

                db, table, table_alias, mode = _resolve_source(qualifier, local_scope, global_scope)

                # Preserve OLD remarks logic as much as possible
                if qualifier and (qualifier in local_scope or qualifier in global_scope):
                    remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"])
                elif not qualifier and (len(local_scope) == 1 or len(global_scope) == 1):
                    remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"])
                else:
                    remarks.append(REMARKS["TABLE_AMBIGUOUS"])

                results.append({
                    "Database Name": db,
                    "Table Name": table,
                    "Table Alias Name": table_alias,
                    "Column Name": column_name,
                    "Alias Name": col_alias or "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": remarks,
                })
                continue

            # -------------------------
            # DERIVED / CASE EXPRESSIONS
            # -------------------------
            derived_columns = extract_columns_from_expression(col_node)

            if derived_columns:
                for dcol in derived_columns:
                    qualifier = dcol.table or ""
                    column_name = dcol.name

                    db, table, table_alias, _ = _resolve_source(qualifier, local_scope, global_scope)

                    remarks = [REMARKS["DERIVED_EXPR"]]
                    if isinstance(col_node, exp.Case):
                        remarks.append(REMARKS["CASE_EXPR"])

                    results.append({
                        "Database Name": db,
                        "Table Name": table,
                        "Table Alias Name": table_alias,
                        "Column Name": column_name,
                        "Alias Name": col_alias or "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": remarks,
                    })
            else:
                # fallback (original behavior)
                results.append({
                    "Database Name": "",
                    "Table Name": "",
                    "Table Alias Name": "",
                    "Column Name": col_alias or col_text,
                    "Alias Name": col_alias or "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": [REMARKS["DERIVED_EXPR"]],
                })

        # -------------------------
        # WHERE / GROUP BY / HAVING lineage
        # -------------------------

        def process_clause(expr, remark_key):
            if not expr:
                return
            for c in extract_columns_from_expression(expr):
                qualifier = c.table or ""   # often empty inside subquery WHERE (ROW_NUM = 1)
                column_name = c.name

                db, table, table_alias, _ = _resolve_source(qualifier, local_scope, global_scope)

                results.append({
                    "Database Name": db,
                    "Table Name": table,
                    "Table Alias Name": table_alias,
                    "Column Name": column_name,
                    "Alias Name": "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": [remark_key],
                })

        process_clause(select.args.get("where"), REMARKS["WHERE_COLUMN"])

        if select.args.get("group"):
            for g in select.args["group"].expressions:
                process_clause(g, REMARKS["GROUP_BY_COLUMN"])

        process_clause(select.args.get("having"), REMARKS["HAVING_COLUMN"])

    # -------------------------
    # Final normalization (UNCHANGED)
    # -------------------------

    normalized = []
    for r in results:
        row = {}
        for k in OUTPUT_KEYS:
            if k == "Remarks":
                row[k] = ensure_list(r.get(k))
            else:
                row[k] = str(r.get(k, "")) if r.get(k) is not None else ""
        normalized.append(row)

    return normalized


# -------------------------
# Public API (UNCHANGED)
# -------------------------

def parse_metadata_and_extract_lineage(
    metadata_json_str: str,
    regulation: str = "",
    metadatakey: str = "",
    view_name: str = "",
    sql_key: str = "sql_query",
) -> List[Dict]:
    sql = decode_base64_sql_from_metadata(metadata_json_str, sql_key)
    return extract_lineage_rows(sql, regulation, metadatakey, view_name)
