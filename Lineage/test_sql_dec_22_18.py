"""
sql_lineage_extractor_final.py


Output keys EXACTLY:
{
  "Database Name",
  "Table Name",
  "Table Alias Name",
  "Column Name",
  "Alias Name",
  "Regulation",
  "Metadatakey",
  "View Name",
  "Remarks"
}
"""

import base64
import json
from typing import List, Dict, Tuple, Optional, Set
import sqlglot
from sqlglot import exp

# ======================================================
# CONSTANTS
# ======================================================

REMARKS = {
    "ALL_COLUMNS": "all_columns_selected",
    "DERIVED": "derived_expression",
    "CASE": "case_expression",
    "WHERE": "where_clause_column",
    "GROUP_BY": "group_by_column",
    "HAVING": "having_clause_column",

    "JOIN": "join_condition_column",
    "LEFT_KEY": "left_key",
    "RIGHT_KEY": "right_key",
    "INNER_JOIN": "inner_join",
    "LEFT_JOIN": "left_join",

    "DB_NOT_SPECIFIED": "database_not_specified_in_query",
    "TABLE_AMBIGUOUS": "table_name_ambiguous",
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

# ======================================================
# UTILITIES
# ======================================================

def decode_sql(metadata_json_str: str, sql_key: str = "sql_query") -> str:
    meta = json.loads(metadata_json_str)
    raw = meta.get(sql_key, "")
    try:
        return base64.b64decode(raw).decode("utf-8")
    except Exception:
        return raw


def safe_name(obj) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    return getattr(obj, "name", None) or str(obj)


def ensure_list(val):
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [val]


def extract_columns(expr):
    if not isinstance(expr, exp.Expression):
        return []
    return list(expr.find_all(exp.Column))


def row_signature(row: Dict) -> Tuple:
    return (
        row["Database Name"],
        row["Table Name"],
        row["Table Alias Name"],
        row["Column Name"],
        row["Alias Name"],
        tuple(sorted(row["Remarks"])),
    )

# ======================================================
# FROM / JOIN / SUBQUERY SCOPE BUILDER (MERGED)
# ======================================================

def _pick_base_table_from_subquery(subq: exp.Subquery) -> Tuple[str, str]:
    tables = list(subq.find_all(exp.Table))
    uniq = set()

    for t in tables:
        db = safe_name(t.db) or ""
        name = safe_name(t.this) or ""
        if name:
            uniq.add((db, name))

    if len(uniq) == 1:
        return list(uniq)[0]

    return "", "__SUBQUERY__"


def build_from_scope(ast) -> Dict[str, Tuple]:
    """
    alias -> (db, table, alias)
    """
    scope = {}

    # Tables
    for t in ast.find_all(exp.Table):
        alias = t.alias_or_name
        scope[alias] = (
            safe_name(t.db) or "",
            safe_name(t.this) or "",
            alias or "",
        )

    # Aliases
    for a in ast.find_all(exp.Alias):
        key = a.alias_or_name
        if isinstance(a.this, exp.Table):
            t = a.this
            scope[key] = (
                safe_name(t.db) or "",
                safe_name(t.this) or "",
                key,
            )

    # JOIN subquery aliases
    for subq in ast.find_all(exp.Subquery):
        alias = subq.alias_or_name
        if not alias:
            continue
        db, table = _pick_base_table_from_subquery(subq)
        scope[alias] = (db or "", table or "", alias)

    return scope

# ======================================================
# CORE EXTRACTION
# ======================================================

def extract_lineage_rows(
    sql: str,
    regulation: str,
    metadatakey: str,
    view_name: str,
) -> List[Dict]:

    ast = sqlglot.parse_one(sql)
    scope = build_from_scope(ast)

    results = []
    seen: Set[Tuple] = set()

    # -------------------------
    # SELECT / DERIVED / CASE
    # -------------------------
    for select in ast.find_all(exp.Select):
        for proj in select.expressions:

            # STAR
            if isinstance(proj, exp.Star):
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

            alias_name = proj.alias_or_name if isinstance(proj, exp.Alias) else ""
            node = proj.this if isinstance(proj, exp.Alias) else proj

            cols = extract_columns(node)

            if cols:
                for c in cols:
                    qualifier = c.table
                    db, table, tbl_alias = scope.get(
                        qualifier,
                        ("", "", qualifier or "")
                    )

                    remarks = [REMARKS["DERIVED"]]
                    if isinstance(node, exp.Case):
                        remarks.append(REMARKS["CASE"])

                    row = {
                        "Database Name": db,
                        "Table Name": table,
                        "Table Alias Name": tbl_alias,
                        "Column Name": c.name,
                        "Alias Name": alias_name,
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": remarks,
                    }

                    sig = row_signature(row)
                    if sig not in seen:
                        seen.add(sig)
                        results.append(row)

        # -------------------------
        # WHERE / GROUP BY / HAVING
        # -------------------------
        def process_clause(expr, remark):
            if not expr:
                return
            for c in extract_columns(expr):
                qualifier = c.table
                db, table, tbl_alias = scope.get(
                    qualifier,
                    ("", "", qualifier or "")
                )

                row = {
                    "Database Name": db,
                    "Table Name": table,
                    "Table Alias Name": tbl_alias,
                    "Column Name": c.name,
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

        process_clause(select.args.get("where"), REMARKS["WHERE"])

        if select.args.get("group"):
            for g in select.args["group"].expressions:
                process_clause(g, REMARKS["GROUP_BY"])

        process_clause(select.args.get("having"), REMARKS["HAVING"])

    # -------------------------
    # JOIN CONDITIONS (LEFT / RIGHT / TYPE)
    # -------------------------
    for join in ast.find_all(exp.Join):
        kind = (join.args.get("kind") or "").upper()
        join_type = REMARKS["LEFT_JOIN"] if "LEFT" in kind else REMARKS["INNER_JOIN"]

        for eq in join.find_all(exp.EQ):
            for side, role in [
                (eq.left, REMARKS["LEFT_KEY"]),
                (eq.right, REMARKS["RIGHT_KEY"]),
            ]:
                for c in extract_columns(side):
                    qualifier = c.table
                    db, table, tbl_alias = scope.get(
                        qualifier,
                        ("", "", qualifier or "")
                    )

                    row = {
                        "Database Name": db,
                        "Table Name": table,
                        "Table Alias Name": tbl_alias,
                        "Column Name": c.name,
                        "Alias Name": "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": [REMARKS["JOIN"], join_type, role],
                    }

                    sig = row_signature(row)
                    if sig not in seen:
                        seen.add(sig)
                        results.append(row)

    return results

# ======================================================
# PUBLIC API (UNCHANGED SIGNATURE)
# ======================================================

def parse_metadata_and_extract_lineage(
    metadata_json_str: str,
    regulation: str = "",
    metadatakey: str = "",
    view_name: str = "",
    sql_key: str = "sql_query",
) -> List[Dict]:

    sql = decode_sql(metadata_json_str, sql_key)
    rows = extract_lineage_rows(sql, regulation, metadatakey, view_name)

    # Final normalization
    normalized = []
    for r in rows:
        row = {}
        for k in OUTPUT_KEYS:
            if k == "Remarks":
                row[k] = ensure_list(r.get(k))
            else:
                row[k] = str(r.get(k, "")) if r.get(k) is not None else ""
        normalized.append(row)

    return normalized
