"""
sql_lineage_extractor.py
(Backward-compatible enhanced version)

Fixes added in this version (NO removal of old behavior):
1) Correct scoping for derived tables (FROM (SELECT ...) ALIAS):
   - Outer SELECT * now resolves to derived-table alias (e.g., TSR_TS_DATA)
   - Unqualified columns in outer expressions (e.g., SUBMISSION_TYPE, KEYS_SRC_SYS)
     resolve to derived-table alias when it is the only FROM source

2) Derived expressions with NO input columns (CURRENT_TIMESTAMP, literals, etc.):
   - If the SELECT is inside a subquery with an alias, we attach that alias
     to the output (Table Alias Name), so DATA_SOURCE_DATE / INITIAL_EXCEPTION_TS
     are not blank anymore.

3) JOIN subqueries (CURRENT_RECORD / PREVIOUS_RECORD):
   - Still supported: CURRENT_RECORD.ID resolves to base table
   - Added: WHERE ROW_NUM = 1 (unqualified) resolves to base table,
     and also gets the enclosing subquery alias as Table Alias Name when needed.
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
# FROM-scope mapping (GLOBAL - OLD behavior)
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
    GLOBAL scope map (existing behavior):
      alias_key -> (node, db, table_name, table_alias)

    NOTE:
    - This scans the whole AST and therefore includes nested sources too.
    - We keep this for backward compatibility.
    """
    from_map = {}

    # 1) Tables
    for tbl in ast_root.find_all(exp.Table):
        table_name = safe_name(tbl.this)
        db = safe_name(tbl.db)
        table_alias = tbl.alias_or_name if tbl.alias else None
        key = table_alias or table_name
        if key and key not in from_map:
            from_map[key] = (tbl, db, table_name, table_alias)

    # 2) Alias nodes
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

    # 3) Subquery alias mapping (CURRENT_RECORD / PREVIOUS_RECORD / TSR_TS_DATA etc.)
    for subq in ast_root.find_all(exp.Subquery):
        subq_alias = subq.alias_or_name if subq.args.get("alias") else None
        if not subq_alias:
            continue
        db, base_table = _pick_base_table_from_subquery(subq)
        from_map[subq_alias] = (subq, db, base_table, subq_alias)

    return from_map


# -------------------------
# NEW: SELECT-LOCAL scope map (THIS is the missing piece)
# -------------------------

def build_select_scope_map(select_exp: exp.Select) -> Dict[str, Tuple]:
    """
    LOCAL scope map for exactly one SELECT (does NOT traverse into nested subqueries).

    This fixes:
    - Outer SELECT * FROM (SELECT ...) TSR_TS_DATA  -> '*' resolves to TSR_TS_DATA
    - Unqualified columns inside outer CASE (SUBMISSION_TYPE, KEYS_SRC_SYS) -> TSR_TS_DATA
    """
    from_map: Dict[str, Tuple] = {}

    def _add_source(src):
        # FROM/JOIN source can be Table / Subquery / Alias-wrapped
        if src is None:
            return

        # Sometimes sources arrive wrapped in exp.Alias
        if isinstance(src, exp.Alias):
            # alias.this could be Table or Subquery
            key = src.alias_or_name
            if isinstance(src.this, exp.Table):
                tbl = src.this
                table_name = safe_name(tbl.this)
                db = safe_name(tbl.db)
                from_map[key] = (src, db, table_name, key)
                return
            if isinstance(src.this, exp.Subquery):
                db, base_table = _pick_base_table_from_subquery(src.this)
                from_map[key] = (src.this, db, base_table, key)
                return

        # Direct table
        if isinstance(src, exp.Table):
            table_name = safe_name(src.this)
            db = safe_name(src.db)
            table_alias = src.alias_or_name if src.alias else None
            key = table_alias or table_name
            if key:
                from_map[key] = (src, db, table_name, table_alias)
            return

        # Direct subquery in FROM/JOIN
        if isinstance(src, exp.Subquery):
            subq_alias = src.alias_or_name if src.args.get("alias") else None
            if subq_alias:
                db, base_table = _pick_base_table_from_subquery(src)
                from_map[subq_alias] = (src, db, base_table, subq_alias)
            return

    # FROM sources
    from_clause = select_exp.args.get("from")
    if from_clause and hasattr(from_clause, "expressions"):
        for src in from_clause.expressions:
            _add_source(src)

    # JOIN sources
    for j in (select_exp.args.get("joins") or []):
        # sqlglot join target is usually stored in j.this
        _add_source(getattr(j, "this", None))

    return from_map


def get_enclosing_subquery_alias(select_exp: exp.Select) -> str:
    """
    If this SELECT is inside a Subquery with an alias, return that alias.
    Example:
        FROM (SELECT ... ) TSR_TS_DATA
    Inner SELECT's parent is exp.Subquery -> alias TSR_TS_DATA
    """
    parent = getattr(select_exp, "parent", None)
    if isinstance(parent, exp.Subquery) and parent.args.get("alias"):
        return parent.alias_or_name or ""
    return ""


# -------------------------
# Resolver helpers
# -------------------------

def _resolve_source(
    qualifier: str,
    local_scope: Dict[str, Tuple],
    global_scope: Dict[str, Tuple],
) -> Tuple[str, str, str]:
    """
    Resolve (db, table_name, table_alias) using:
      1) qualifier in local
      2) qualifier in global
      3) unqualified + local has single source
      4) unqualified + global has single source
      5) else -> blank (ambiguous)
    """
    if qualifier:
        if qualifier in local_scope:
            _, db, table, table_alias = local_scope[qualifier]
            return db or "", table or "", table_alias or ""
        if qualifier in global_scope:
            _, db, table, table_alias = global_scope[qualifier]
            return db or "", table or "", table_alias or ""
        return "", "", ""

    # unqualified
    if len(local_scope) == 1:
        _, db, table, table_alias = next(iter(local_scope.values()))
        return db or "", table or "", table_alias or ""

    if len(global_scope) == 1:
        _, db, table, table_alias = next(iter(global_scope.values()))
        return db or "", table or "", table_alias or ""

    return "", "", ""


def _attach_enclosing_alias_if_missing(db: str, table: str, table_alias: str, enclosing_alias: str) -> Tuple[str, str, str]:
    """
    If we couldn't resolve a table alias (or table), but we ARE inside a subquery alias,
    attach that alias so outputs are not blank for derived columns.
    """
    if enclosing_alias and not table_alias:
        table_alias = enclosing_alias
    if enclosing_alias and not table and not db:
        # This is an output column of the derived table, not a physical table column.
        # Use a stable marker instead of leaving blank.
        table = "__DERIVED__"
    return db, table, table_alias


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
# STAR handling (UPDATED)
# -------------------------

def resolve_star(star_node, local_scope, global_scope, enclosing_alias, regulation, metadatakey, view_name):
    """
    Fix:
    - '*' in outer SELECT should resolve to TSR_TS_DATA (derived table alias)
    - 'TSR.*' should resolve to TSR
    - 'SELECT * FROM TS_FACT' inside CURRENT_RECORD should resolve to TS_FACT + CURRENT_RECORD alias
    """
    qualifier = ""

    # Best-effort extraction of qualifier for qualified star (e.g., TSR.*)
    if hasattr(star_node, "table") and star_node.table:
        qualifier = star_node.table
    else:
        # sqlglot sometimes stores qualifier in args['this']
        this_arg = getattr(star_node, "args", {}).get("this")
        if this_arg is not None:
            qualifier = safe_name(this_arg) or ""

    db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)

    # If it's unqualified star and we're inside a derived subquery alias, attach it
    db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias)

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
    results: List[Dict] = []

    # Global map kept (old behavior)
    global_scope = build_from_scope_map(ast)
    selects = list(ast.find_all(exp.Select))

    for select in selects:
        # NEW: local scope for this SELECT
        local_scope = build_select_scope_map(select)
        enclosing_alias = get_enclosing_subquery_alias(select)

        for col_text, col_alias, col_node in extract_select_list(select):

            # -------------------------
            # STAR
            # -------------------------
            if isinstance(col_node, exp.Star):
                results.extend(
                    resolve_star(col_node, local_scope, global_scope, enclosing_alias, regulation, metadatakey, view_name)
                )
                continue

            # -------------------------
            # DIRECT COLUMN
            # -------------------------
            if isinstance(col_node, exp.Column):
                qualifier = col_node.table or ""
                column_name = col_node.name
                remarks: List[str] = []

                db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)

                # If inside derived table alias and alias is missing, attach it
                db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias)

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

                    db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)
                    db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias)

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
                # Derived expression with no input columns (CURRENT_TIMESTAMP, literals, etc.)
                db, table, table_alias = "", "", ""
                db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias)

                results.append({
                    "Database Name": db,
                    "Table Name": table,
                    "Table Alias Name": table_alias,
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
                qualifier = c.table or ""   # often empty in subquery WHERE ROW_NUM = 1
                column_name = c.name

                db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)
                db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias)

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
    # Final normalization
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
# Public API
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
