"""
sql_lineage_extractor.py
- SELECT / STAR lineage
- Derived & CASE expressions
- WHERE / GROUP BY / HAVING lineage
- JOIN lineage with:
  - join type tagging
  - left_key / right_key roles
  - USING clause support
- Qualified column resolution (DB.COLUMN) ✅ FIX
- Deduplication rules

"""

import base64
import json
from typing import List, Dict, Tuple, Optional, Set
import sqlglot
from sqlglot import exp

# -------------------------
# Constants
# -------------------------

REMARKS = {
    "ALL_COLUMNS": "all_columns_selected",
    "COLUMN_SELECTED_WITH_DB": "column_selected_with_database",
    "DATABASE_NOT_SPECIFIED": "database_not_specified_in_query",
    "TABLE_AMBIGUOUS": "table_name_ambiguous",
    "DERIVED_EXPR": "derived_expression",
    "CASE_EXPR": "case_expression",
    "WHERE_COLUMN": "where_clause_column",
    "GROUP_BY_COLUMN": "group_by_column",
    "HAVING_COLUMN": "having_clause_column",
    # JOIN
    "JOIN_COLUMN": "join_condition_column",
    "LEFT_KEY": "left_key",
    "RIGHT_KEY": "right_key",
    # JOIN TYPES
    "INNER_JOIN": "inner_join",
    "LEFT_JOIN": "left_join",
    "RIGHT_JOIN": "right_join",
    "FULL_JOIN": "full_join",
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
# FROM scope
# -------------------------

def build_from_scope_map(ast_root) -> Dict[str, Tuple]:
    """
    alias_key -> (node, db, table_name, table_alias)
    """
    from_map = {}

    for tbl in ast_root.find_all(exp.Table):
        table = safe_name(tbl.this)
        db = safe_name(tbl.db)
        alias = tbl.alias_or_name if tbl.alias else None
        from_map[alias or table] = (tbl, db, table, alias)

    for alias in ast_root.find_all(exp.Alias):
        key = alias.alias_or_name
        if isinstance(alias.this, exp.Table):
            tbl = alias.this
            from_map[key] = (alias, safe_name(tbl.db), safe_name(tbl.this), key)
        elif isinstance(alias.this, exp.Subquery):
            from_map[key] = (alias, None, "__SUBQUERY__", key)

    return from_map


# -------------------------
# Qualified column resolver
# -------------------------

def resolve_qualified_column(col: exp.Column, from_scope):
    """
    Resolves DB.COLUMN when no table alias is used.
    """
    if not col.table:
        return None

    qualifier = col.table
    column = col.name

    for _, db, table, alias in from_scope.values():
        if db == qualifier:
            return db, table, alias, column

    return None


# -------------------------
# Deduplication helper
# -------------------------

def row_signature(row: Dict) -> Tuple:
    return tuple((k, tuple(row[k]) if isinstance(row[k], list) else row[k]) for k in OUTPUT_KEYS)


# -------------------------
# Core extraction
# -------------------------

def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    ast = sqlglot.parse_one(sql)
    results: List[Dict] = []
    seen: Set[Tuple] = set()

    from_scope = build_from_scope_map(ast)

    # -------------------------
    # SELECT / WHERE / GROUP / HAVING
    # -------------------------

    for select in ast.find_all(exp.Select):

        for proj in select.expressions:
            alias = proj.alias_or_name if isinstance(proj, exp.Alias) else ""
            node = proj.this if isinstance(proj, exp.Alias) else proj

            # STAR
            if isinstance(node, exp.Star):
                row = {
                    "Database Name": "",
                    "Table Name": "",
                    "Table Alias Name": "",
                    "Column Name": "*",
                    "Alias Name": "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": [REMARKS["ALL_COLUMNS"]],
                }
                sig = row_signature(row)
                if sig not in seen:
                    seen.add(sig)
                    results.append(row)
                continue

            # Column resolution
            columns = extract_columns_from_expression(node)
            if not columns:
                continue

            for col in columns:
                db = table = table_alias = ""

                # Alias-qualified
                if col.table and col.table in from_scope:
                    _, db, table, table_alias = from_scope[col.table]

                # Qualified DB.COLUMN
                elif resolve_qualified_column(col, from_scope):
                    db, table, table_alias, _ = resolve_qualified_column(col, from_scope)

                # Single table fallback
                elif len(from_scope) == 1:
                    _, db, table, table_alias = next(iter(from_scope.values()))

                remarks = [REMARKS["DERIVED_EXPR"]]
                if isinstance(node, exp.Case):
                    remarks.append(REMARKS["CASE_EXPR"])

                if db:
                    remarks = [REMARKS["COLUMN_SELECTED_WITH_DB"]]

                row = {
                    "Database Name": db or "",
                    "Table Name": table or "",
                    "Table Alias Name": table_alias or "",
                    "Column Name": col.name,
                    "Alias Name": alias or "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": remarks,
                }

                sig = row_signature(row)
                if sig not in seen:
                    seen.add(sig)
                    results.append(row)

        def process_clause(expr, remark):
            if not expr:
                return
            for col in extract_columns_from_expression(expr):
                db = table = table_alias = ""

                if col.table and col.table in from_scope:
                    _, db, table, table_alias = from_scope[col.table]
                elif len(from_scope) == 1:
                    _, db, table, table_alias = next(iter(from_scope.values()))

                row = {
                    "Database Name": db or "",
                    "Table Name": table or "",
                    "Table Alias Name": table_alias or "",
                    "Column Name": col.name,
                    "Alias Name": "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": [remark],
                }

                sig = row_signature(row)
                if sig not in seen:
                    seen.add(sig)
                    results.append(row)

        process_clause(select.args.get("where"), REMARKS["WHERE_COLUMN"])
        if select.args.get("group"):
            for g in select.args["group"].expressions:
                process_clause(g, REMARKS["GROUP_BY_COLUMN"])
        process_clause(select.args.get("having"), REMARKS["HAVING_COLUMN"])

    # -------------------------
    # JOIN processing
    # -------------------------

    for join in ast.find_all(exp.Join):
        kind = (join.args.get("kind") or "").upper()

        join_type = (
            REMARKS["LEFT_JOIN"] if "LEFT" in kind else
            REMARKS["RIGHT_JOIN"] if "RIGHT" in kind else
            REMARKS["FULL_JOIN"] if "FULL" in kind else
            REMARKS["INNER_JOIN"]
        )

        # USING clause
        if join.args.get("using"):
            for col in join.args["using"].expressions:
                for _, db, table, alias in from_scope.values():
                    row = {
                        "Database Name": db or "",
                        "Table Name": table or "",
                        "Table Alias Name": alias or "",
                        "Column Name": col.name,
                        "Alias Name": "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": [REMARKS["JOIN_COLUMN"], join_type],
                    }
                    sig = row_signature(row)
                    if sig not in seen:
                        seen.add(sig)
                        results.append(row)
            continue

        # ON clause
        on_expr = join.args.get("on")
        if not on_expr:
            continue

        for eq in on_expr.find_all(exp.EQ):
            for cols, role in [
                (extract_columns_from_expression(eq.left), REMARKS["LEFT_KEY"]),
                (extract_columns_from_expression(eq.right), REMARKS["RIGHT_KEY"]),
            ]:
                for col in cols:
                    if col.table not in from_scope:
                        continue
                    _, db, table, alias = from_scope[col.table]

                    row = {
                        "Database Name": db or "",
                        "Table Name": table or "",
                        "Table Alias Name": alias or "",
                        "Column Name": col.name,
                        "Alias Name": "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": [
                            REMARKS["JOIN_COLUMN"],
                            join_type,
                            role,
                        ],
                    }

                    sig = row_signature(row)
                    if sig not in seen:
                        seen.add(sig)
                        results.append(row)

    # -------------------------
    # Final normalization
    # -------------------------

    final = []
    for r in results:
        row = {}
        for k in OUTPUT_KEYS:
            row[k] = ensure_list(r[k]) if k == "Remarks" else str(r.get(k, ""))
        final.append(row)

    return final


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
