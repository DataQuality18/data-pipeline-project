"""
sql_lineage_extractor.py
(Backward-compatible enhanced version)

NEW FIX (ADD ONLY; old behavior preserved):
- For derived expressions that have NO input columns (e.g., DATE_FORMAT(CURRENT_TIMESTAMP,...)):
  Instead of attributing them to the derived-table alias (TSR_TS_DATA),
  we attribute them to the *primary physical source table in that SELECT*.

Why:
- In your inner SELECT (the one building TSR_TS_DATA), DATE_FORMAT(CURRENT_TIMESTAMP,...) is computed
  while selecting from APP_REGHUB_RHOO_OTC_TSR_ESMA_DATA TSR, so you want Table Alias Name = TSR.
- For CURRENT_RECORD / PREVIOUS_RECORD subqueries, the primary physical table is the TS_FACT table,
  so ROW_NUM / SELECT * resolves to TS_FACT + alias CURRENT_RECORD/PREVIOUS_RECORD.

What remains unchanged:
- Outer SELECT * FROM ( ... ) TSR_TS_DATA still maps '*' to TSR_TS_DATA (derived table output).
- Column resolution with qualifiers continues to work as before.
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
    "DERIVED_EXPR": "derived_expression",
    "CASE_EXPR": "case_expression",
    "WHERE_COLUMN": "where_clause_column",
    "GROUP_BY_COLUMN": "group_by_column",
    "HAVING_COLUMN": "having_clause_column",
    "DERIVED_TABLE": "derived_table",
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
# FROM-scope mapping (GLOBAL - kept)
# -------------------------

def _pick_base_table_from_subquery(subq: exp.Subquery) -> Tuple[str, str]:
    """
    For subquery like: (SELECT * FROM db.table WHERE ...) ALIAS
    If exactly one base table exists, return it; else return ("", "__SUBQUERY__").
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
    GLOBAL scope map:
      alias_key -> (node, db, table_name, table_alias)
    """
    from_map: Dict[str, Tuple] = {}

    # Tables
    for tbl in ast_root.find_all(exp.Table):
        table_name = safe_name(tbl.this)
        db = safe_name(tbl.db)
        table_alias = tbl.alias_or_name if tbl.alias else None
        key = table_alias or table_name
        if key and key not in from_map:
            from_map[key] = (tbl, db, table_name, table_alias)

    # Aliases
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

    # Subquery aliases (CURRENT_RECORD / PREVIOUS_RECORD / TSR_TS_DATA)
    for subq in ast_root.find_all(exp.Subquery):
        subq_alias = subq.alias_or_name if subq.args.get("alias") else None
        if not subq_alias:
            continue
        db, base_table = _pick_base_table_from_subquery(subq)
        from_map[subq_alias] = (subq, db, base_table, subq_alias)

    return from_map


# -------------------------
# SELECT-LOCAL scope map (no traversal into nested subqueries)
# -------------------------

def build_select_scope_map(select_exp: exp.Select) -> Dict[str, Tuple]:
    """
    LOCAL scope for one SELECT:
    includes only the FROM + JOIN sources of *this* SELECT.
    """
    from_map: Dict[str, Tuple] = {}

    def _add_source(src, *, context: str):
        """Add a FROM/JOIN source into local scope map.
    
        context:
          - "from": main FROM-clause source (derived table should be represented by its alias)
          - "join": JOIN sources (subqueries should map to underlying/base table when possible)
        """
        if not src:
            return
    
        if isinstance(src, exp.Table):
            alias_key = safe_name(src.alias_or_name or src.name)
            from_map[alias_key] = (
                src,
                safe_name(src.db),
                safe_name(src.this),
                safe_name(src.alias_or_name) if src.alias else "",
            )
            return
    
        # Subquery: (SELECT ...) alias
        if isinstance(src, exp.Subquery):
            subq_alias = safe_name(src.alias_or_name) if src.args.get("alias") else None
            if not subq_alias:
                return
    
            if context == "from":
                # Outer query selecting FROM a derived table: treat derived alias as the table name
                from_map[subq_alias] = (src, "", subq_alias, subq_alias)
            else:
                # JOIN subquery: map alias to underlying/base table so qualifier columns resolve
                db, base_table = _pick_base_table_from_subquery(src)
                from_map[subq_alias] = (src, db, base_table, subq_alias)
            return
    from_clause = select_exp.args.get("from")
    if from_clause:
        sources = []
        if hasattr(from_clause, "expressions") and from_clause.expressions:
            sources = list(from_clause.expressions)
        elif getattr(from_clause, "this", None) is not None:
            sources = [from_clause.this]
        for src in sources:
            _add_source(src, context="from")

    # JOINs
    for j in (select_exp.args.get("joins") or []):
        _add_source(getattr(j, "this", None), context="join")

    return from_map


def get_enclosing_subquery_alias(select_exp: exp.Select) -> str:
    parent = getattr(select_exp, "parent", None)
    if isinstance(parent, exp.Subquery) and parent.args.get("alias"):
        return parent.alias_or_name or ""
    return ""


# -------------------------
# Resolver helpers
# -------------------------

def _resolve_source(qualifier: str, local_scope: Dict[str, Tuple], global_scope: Dict[str, Tuple]) -> Tuple[str, str, str]:
    """
    Resolve (db, table, table_alias) using:
      - qualifier in local, then global
      - if unqualified and local has exactly one source => that
      - else if unqualified and global has exactly one source => that
    """
    if qualifier:
        if qualifier in local_scope:
            _, db, table, table_alias = local_scope[qualifier]
            return db or "", table or "", table_alias or ""
        if qualifier in global_scope:
            _, db, table, table_alias = global_scope[qualifier]
            return db or "", table or "", table_alias or ""
        return "", "", ""

    if len(local_scope) == 1:
        _, db, table, table_alias = next(iter(local_scope.values()))
        return db or "", table or "", table_alias or ""

    if len(global_scope) == 1:
        _, db, table, table_alias = next(iter(global_scope.values()))
        return db or "", table or "", table_alias or ""

    return "", "", ""


def _pick_primary_physical_source(local_scope: Dict[str, Tuple]) -> Tuple[str, str, str]:
    """
    Pick the "primary physical table" for a SELECT:
    - Prefer alias 'TSR' if present (your requirement).
    - Else pick the first entry that is not __DERIVED__/__SUBQUERY__.
    - Else return ("", "", "").
    """
    if "TSR" in local_scope:
        _, db, table, table_alias = local_scope["TSR"]
        return db or "", table or "", table_alias or "TSR"

    for _, (_node, db, table, table_alias) in local_scope.items():
        if table and table not in ("__DERIVED__", "__SUBQUERY__"):
            return db or "", table or "", table_alias or ""

    return "", "", ""


def _attach_context_for_no_input_expr(
    db: str,
    table: str,
    table_alias: str,
    local_scope: Dict[str, Tuple],
    enclosing_alias: str,
    global_scope: Dict[str, Tuple],
) -> Tuple[str, str, str]:
    """
    For expressions like DATE_FORMAT(CURRENT_TIMESTAMP,...):
    - Attach to the primary physical source of the SELECT if available (e.g., TSR).
    - If not available and we are inside CURRENT_RECORD/PREVIOUS_RECORD:
        use the base physical table for that subquery alias (TS_FACT) and keep alias.
    - Else if nothing else works: attach to enclosing derived alias (TSR_TS_DATA) and set __DERIVED__.
    """
    # 1) Prefer primary physical source within this SELECT (TSR)
    p_db, p_table, p_alias = _pick_primary_physical_source(local_scope)
    if p_table:
        return p_db, p_table, p_alias

    # 2) If inside CURRENT_RECORD/PREVIOUS_RECORD and global scope knows its base (TS_FACT)
    if enclosing_alias and enclosing_alias in global_scope:
        _, db2, base_table, _ = global_scope[enclosing_alias]
        if base_table and base_table not in ("__SUBQUERY__", "__DERIVED__"):
            return (db2 or ""), base_table, enclosing_alias

    # 3) Otherwise attach to enclosing alias as derived output
    if enclosing_alias:
        return db, "__DERIVED__", enclosing_alias

    return db, table, table_alias


def _attach_enclosing_alias_if_missing(
    db: str,
    table: str,
    table_alias: str,
    enclosing_alias: str,
    global_scope: Dict[str, Tuple],
) -> Tuple[str, str, str]:
    """
    Generic attachment used for STAR / WHERE / normal columns when qualifier missing.

    If we're inside CURRENT_RECORD/PREVIOUS_RECORD and missing table:
      -> use TS_FACT table name via global map and keep alias.
    If we're inside TSR_TS_DATA and missing table:
      -> keep __DERIVED__ and attach alias.
    """
    if enclosing_alias and not table_alias:
        table_alias = enclosing_alias

    if enclosing_alias and not table:
        if enclosing_alias in global_scope:
            _, db2, base_table, _ = global_scope[enclosing_alias]
            if base_table and base_table not in ("__SUBQUERY__", "__DERIVED__"):
                if not db:
                    db = db2 or ""
                table = base_table
            else:
                table = "__DERIVED__"
        else:
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
# STAR handling
# -------------------------

def resolve_star(star_node, local_scope, global_scope, enclosing_alias, regulation, metadatakey, view_name):
    qualifier = ""

    if hasattr(star_node, "table") and star_node.table:
        qualifier = star_node.table
    else:
        this_arg = getattr(star_node, "args", {}).get("this")
        if this_arg is not None:
            qualifier = safe_name(this_arg) or ""

    db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)
    db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias, global_scope)
    remarks = [REMARKS["ALL_COLUMNS"]]
    if not qualifier and len(local_scope) == 1:
        only_node, _, _, _ = next(iter(local_scope.values()))
        if isinstance(only_node, exp.Subquery) and table == table_alias and table:
            remarks.append(REMARKS["DERIVED_TABLE"])


    return [{
        "Database Name": db,
        "Table Name": table,
        "Table Alias Name": table_alias,
        "Column Name": "*",
        "Alias Name": "",
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": remarks,
    }]


# -------------------------
# Core extraction
# -------------------------

def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    ast = sqlglot.parse_one(sql)
    results: List[Dict] = []

    global_scope = build_from_scope_map(ast)
    selects = list(ast.find_all(exp.Select))

    for select in selects:
        local_scope = build_select_scope_map(select)
        enclosing_alias = get_enclosing_subquery_alias(select)

        for col_text, col_alias, col_node in extract_select_list(select):

            # STAR
            if isinstance(col_node, exp.Star):
                results.extend(
                    resolve_star(col_node, local_scope, global_scope, enclosing_alias, regulation, metadatakey, view_name)
                )
                continue

            # DIRECT COLUMN
            if isinstance(col_node, exp.Column):
                qualifier = col_node.table or ""
                column_name = col_node.name
                remarks: List[str] = []

                db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)
                db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias, global_scope)
                # Tag derived-table sources (SELECT ... FROM (subquery) ALIAS)
                if (
                    (qualifier and qualifier in local_scope and isinstance(local_scope[qualifier][0], exp.Subquery) and table == table_alias and table)
                    or (not qualifier and len(local_scope) == 1 and isinstance(next(iter(local_scope.values()))[0], exp.Subquery) and table == table_alias and table)
                ):
                    remarks.append(REMARKS["DERIVED_TABLE"])


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

            # DERIVED / CASE
            derived_columns = extract_columns_from_expression(col_node)

            if derived_columns:
                for dcol in derived_columns:
                    qualifier = dcol.table or ""
                    column_name = dcol.name

                    db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)
                    db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias, global_scope)
                    # Tag derived-table sources (SELECT ... FROM (subquery) ALIAS)
                    if (
                        (dqual and dqual in local_scope and isinstance(local_scope[dqual][0], exp.Subquery) and table == table_alias and table)
                        or (not dqual and len(local_scope) == 1 and isinstance(next(iter(local_scope.values()))[0], exp.Subquery) and table == table_alias and table)
                    ):
                        remarks.append(REMARKS["DERIVED_TABLE"])


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
                # Derived expression with no input columns (CURRENT_TIMESTAMP, literals)
                db, table, table_alias = "", "", ""
                db, table, table_alias = _attach_context_for_no_input_expr(
                    db, table, table_alias,
                    local_scope=local_scope,
                    enclosing_alias=enclosing_alias,
                    global_scope=global_scope,
                )

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

        # WHERE / GROUP BY / HAVING
        def process_clause(expr, remark_key):
            if not expr:
                return
            for c in extract_columns_from_expression(expr):
                qualifier = c.table or ""
                column_name = c.name

                db, table, table_alias = _resolve_source(qualifier, local_scope, global_scope)
                db, table, table_alias = _attach_enclosing_alias_if_missing(db, table, table_alias, enclosing_alias, global_scope)

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

    # Normalize
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