"""
sql_lineage_extractor.py

Usage:
    rows = parse_metadata_and_extract_lineage(metadata_json_str,
                                              regulation="PCI",
                                              metadatakey="mk1",
                                              view_name="v1",
                                              sql_key="sql_query")

Returns list[dict] with exact keys:
{
  "Database Name": "",
  "Table Name": "",
  "Column Name": "",
  "Alias Name": "",
  "Regulation": "...",
  "Metadatakey": "...",
  "View Name": "...",
  "Remarks": "..."
}
"""

import base64
import json
from typing import List, Dict, Tuple, Optional
import sqlglot
from sqlglot import exp

# -------------------------
# Utilities
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
}

def decode_base64_sql_from_metadata(metadata_json_str: str, sql_key: str = "sql_query") -> str:
    meta = json.loads(metadata_json_str)
    if sql_key not in meta:
        raise KeyError(f"metadata JSON does not contain key '{sql_key}'")
    b64 = meta[sql_key]
    # attempt decode; if fails, assume already raw SQL
    try:
        sql_bytes = base64.b64decode(b64)
        return sql_bytes.decode("utf-8")
    except Exception:
        return b64


def node_alias_or_name(node: exp.Expression) -> Optional[str]:
    """Return alias or name for Table/Alias nodes."""
    if hasattr(node, "alias_or_name") and node.alias_or_name:
        return node.alias_or_name
    # for Table nodes, fallback to .this.name
    if isinstance(node, exp.Table) and getattr(node, "this", None) and getattr(node.this, "name", None):
        return node.this.name
    return None


def extract_select_list(select_exp: exp.Select) -> List[Tuple[str, Optional[str], exp.Expression]]:
    """
    Returns list of tuples for each select projection:
      (pretty_text, alias_or_None, original_expression_node)
    """
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
# FROM-scope mapping (DB-aware)
# -------------------------

def build_from_scope_map(ast_root: exp.Expression) -> Dict[str, Tuple[exp.Expression, Optional[str], str]]:
    """
    Maps alias_or_table_name -> (node, db_name_or_None, actual_table_name_or_marker)
    - node: exp.Table or exp.Alias (if alias wraps subquery or table)
    - db_name_or_None: schema/database if present
    - actual_table_name_or_marker: actual table name or "__SUBQUERY__"
    """
    from_map = {}

    # Direct tables
    for tbl in ast_root.find_all(exp.Table):
        # actual table name identifier
        table_name = tbl.this.name if getattr(tbl, "this", None) and getattr(tbl.this, "name", None) else str(tbl)
        db_name = None
        if getattr(tbl, "db", None):
            db_name = tbl.db.name if getattr(tbl.db, "name", None) else str(tbl.db)
        alias = node_alias_or_name(tbl) or table_name
        from_map[alias] = (tbl, db_name, table_name)

    # Aliases that might wrap tables or subqueries (prefer alias key)
    for alias in ast_root.find_all(exp.Alias):
        # alias.this may be Table or Subquery
        key = alias.alias_or_name
        if not key:
            continue
        if isinstance(alias.this, exp.Table):
            tbl = alias.this
            table_name = tbl.this.name if getattr(tbl, "this", None) and getattr(tbl.this, "name", None) else str(tbl)
            db = tbl.db.name if getattr(tbl, "db", None) and getattr(tbl.db, "name", None) else None
            from_map[key] = (alias, db, table_name)
        elif isinstance(alias.this, exp.Subquery):
            from_map[key] = (alias, None, "__SUBQUERY__")

    return from_map


def build_from_scope_map_for_subquery(subquery_node: exp.Subquery) -> Dict[str, Tuple[exp.Expression, Optional[str], str]]:
    """
    Build a from-scope map limited to the subquery (useful when diving into subqueries).
    """
    try:
        inner = subquery_node.this  # should be a Select or CTE block
    except Exception:
        inner = None
    if inner is None:
        return {}
    return build_from_scope_map(inner)


# -------------------------
# STAR resolution & subquery dive
# -------------------------

def resolve_star(select_node: exp.Select, star_node: exp.Star, from_scope: Dict[str, Tuple[exp.Expression, Optional[str], str]],
                 regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    """
    Expand a star expression and produce rows.
    Rules:
      - Emit all_columns_selected row (Column Name = "*")
      - If star has qualifier that maps to alias->subquery, emit Inner Query Alias Layer + Subquery Layer rows
      - If star has qualifier that maps to a table, emit a star row and mark db if present, else database_not_specified_in_query
      - If star unqualified and FROM has a single table, map to that
    """
    rows = []

    qualifier = getattr(star_node, "this", None)  # qualifier (Identifier) for qualified star like a.*
    qualifier_name = None
    if qualifier:
        # qualifier could be exp.Identifier or exp.Column? convert to string/name
        qualifier_name = getattr(qualifier, "name", None) or str(qualifier)

    # Basic all_columns_selected row
    star_row = {
        "Database Name": "",
        "Table Name": qualifier_name if qualifier_name else "",
        "Column Name": "*",
        "Alias Name": "",
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": "all_columns_selected"
    }

    # If unqualified star and only one table in FROM-scope, attach that table
    if not qualifier_name:
        # try single table in from_scope
        table_keys = [k for k in from_scope.keys()]
        if len(table_keys) == 1:
            k = table_keys[0]
            node, db, actual_table = from_scope[k]
            star_row["Table Name"] = actual_table if actual_table else k
            if db:
                star_row["Database Name"] = db
                star_row["Remarks"] = REMARKS["COLUMN_SELECTED_WITH_DB"]
            else:
                star_row["Remarks"] += f"""; {REMARKS["COLUMN_SELECTED_NO_DB"]}"""
    else:
        # qualifier present: find mapping
        mapped = from_scope.get(qualifier_name)
        if mapped:
            node, db, actual_table = mapped
            # if mapping points to alias of subquery
            if isinstance(node, exp.Alias) and isinstance(node.this, exp.Subquery):
                # Inner Query Alias Layer
                inner_row = {
                    "Database Name": db if db else "",
                    "Table Name": qualifier_name,
                    "Column Name": "* (subquery)",
                    "Alias Name": qualifier_name,
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": REMARKS["INNER_ALIAS"]
                }
                rows.append(star_row)  # original star
                rows.append(inner_row)
                # Dive into subquery to list its select columns
                subq = node.this  # exp.Subquery
                if isinstance(subq, exp.Subquery) and isinstance(subq.this, exp.Select):
                    sub_select = subq.this
                    # build inner from scope for the subquery (to resolve dbs if inner tables have db qualifiers)
                    inner_from_scope = build_from_scope_map_for_subquery(subq)
                    for col_text, col_alias, col_node in extract_select_list(sub_select):
                        # If the column here is a star inside subquery, expand only top-level as marker
                        if isinstance(col_node, exp.Star):
                            sub_inner_row = {
                                "Database Name": "",
                                "Table Name": qualifier_name,
                                "Column Name": "*",
                                "Alias Name": "",
                                "Regulation": regulation,
                                "Metadatakey": metadatakey,
                                "View Name": view_name,
                                "Remarks": REMARKS["SUBQUERY_LAYER"]#"Subquery Layer; all_columns_selected"
                            }
                            # attach db if inner_from_scope has single table with db
                            if not col_node.this and len(inner_from_scope) == 1:
                                k2 = list(inner_from_scope.keys())[0]
                                _, db2, actual2 = inner_from_scope[k2]
                                if db2:
                                    sub_inner_row["Database Name"] = db2
                                else:
                                    sub_inner_row["Remarks"] += f""";{REMARKS["DATABASE_NOT_SPECIFIED"]}"""
                            rows.append(sub_inner_row)
                            continue

                        # Decide column display name
                        col_name = col_alias if col_alias else col_text
                        # If column node is Column with qualifier, try to map to inner table/db
                        col_db = ""
                        if isinstance(col_node, exp.Column):
                            tbl_q = getattr(col_node, "table", None)
                            if tbl_q:
                                mapped_inner = inner_from_scope.get(tbl_q)
                                if mapped_inner:
                                    _, db2, actual2 = mapped_inner
                                    col_db = db2 if db2 else ""
                                else:
                                    # no mapping; couldn't find db -> mark missing
                                    col_db = ""
                        sub_row = {
                            "Database Name": col_db if col_db else "",
                            "Table Name": qualifier_name,
                            "Column Name": col_name,
                            "Alias Name": col_alias if col_alias else "",
                            "Regulation": regulation,
                            "Metadatakey": metadatakey,
                            "View Name": view_name,
                            "Remarks": REMARKS["SUBQUERY_LAYER"]
                        }
                        # if column had a table qualifier but that inner mapping has no db -> remark
                        if isinstance(col_node, exp.Column) and getattr(col_node, "table", None):
                            mapped_inner = inner_from_scope.get(col_node.table)
                            if mapped_inner and mapped_inner[1] is None:
                                sub_row["Remarks"] += f"""; {REMARKS["DATABASE_NOT_SPECIFIED"]}"""
                        rows.append(sub_row)
                return rows  # we added star_row + inner + sub rows already
            else:
                # qualifier maps to a table (or alias that points to table)
                if db:
                    star_row["Database Name"] = db
                else:
                    star_row["Remarks"] += f"""; {REMARKS["DATABASE_NOT_SPECIFIED"]}"""
                # set Table Name to the actual table (not alias)
                star_row["Table Name"] = actual_table if actual_table and actual_table != "__SUBQUERY__" else qualifier_name
        else:
            # qualifier not found in from_scope: keep qualifier as Table Name but mark missing db
            star_row["Table Name"] = qualifier_name
            star_row["Remarks"] += f"""; {REMARKS["DATABASE_NOT_SPECIFIED"]}"""

    rows.insert(0, star_row)
    return rows


# -------------------------
# Core extraction logic
# -------------------------

def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    """
    Parse SQL and extract lineage rows conforming to exact schema.
    """
    try:
        ast = sqlglot.parse_one(sql)
    except Exception as e:
        raise RuntimeError(f"sqlglot failed to parse SQL: {e}")

    results: List[Dict] = []

    # Build a top-level from-scope map
    from_scope = build_from_scope_map(ast)

    # Find SELECT nodes (could be main + sub-selects); we primarily iterate top-level selects
    selects = list(ast.find_all(exp.Select))
    if not selects:
        # fallback: if AST itself is a Select
        if isinstance(ast, exp.Select):
            selects = [ast]

    # We'll process selects in order found (top-level first often)
    for select in selects:
        select_list = extract_select_list(select)
        for col_text, col_alias, col_node in select_list:
            # STAR (qualified or unqualified)
            if isinstance(col_node, exp.Star) or (isinstance(col_node, exp.Identifier) and str(col_node) == "*"):
                star_node = col_node if isinstance(col_node, exp.Star) else exp.Star(this=None)
                star_rows = resolve_star(select, star_node, from_scope, regulation, metadatakey, view_name)
                # ensure remarks strings and keys
                for r in star_rows:
                    # normalize keys and ensure all keys present
                    normalized = {
                        "Database Name": str(r.get("Database Name", "")) if r.get("Database Name", "") is not None else "",
                        "Table Name": str(r.get("Table Name", "")) if r.get("Table Name", "") is not None else "",
                        "Column Name": str(r.get("Column Name", "")) if r.get("Column Name", "") is not None else "",
                        "Alias Name": str(r.get("Alias Name", "")) if r.get("Alias Name", "") is not None else "",
                        "Regulation": str(r.get("Regulation", "")),
                        "Metadatakey": str(r.get("Metadatakey", "")),
                        "View Name": str(r.get("View Name", "")),
                        "Remarks": str(r.get("Remarks", ""))
                    }
                    results.append(normalized)
                continue

            # Normal column/expression projection
            col_display_name = col_alias if col_alias else col_text
            db_name = ""
            table_name = ""
            remarks = REMARKS["COLUMN_SELECTED_WITH_DB"]

            # If it's a Column node we can get qualifier and name
            if isinstance(col_node, exp.Column):
                qualifier = getattr(col_node, "table", None)
                col_base_name = getattr(col_node, "name", None) or col_display_name
                if qualifier:
                    mapped = from_scope.get(qualifier)
                    if mapped:
                        _, db, actual_table = mapped
                        table_name = actual_table if actual_table and actual_table != "__SUBQUERY__" else qualifier
                        if db:
                            db_name = db
                        else:
                            remarks = REMARKS["DATABASE_NOT_SPECIFIED"]
                    else:
                        # qualifier not in from_scope; emit with qualifier as table name (db unknown)
                        table_name = qualifier
                        remarks = REMARKS["DATABASE_NOT_SPECIFIED"]
                else:
                    # Unqualified column: can't determine table -> mark missing db and empty table
                    table_name = ""
                    remarks = REMARKS["TABLE_AMBIGUOUS"]
                row = {
                    "Database Name": db_name,
                    "Table Name": table_name,
                    "Column Name": col_base_name if col_base_name  else "",
                    "Alias Name": col_alias if col_alias else "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": remarks
                }
                results.append(row)
                continue

            # If it's a Subquery expression used as a projection: dive and list subquery columns
            if isinstance(col_node, exp.Subquery):
                sub = col_node
                if isinstance(sub.this, exp.Select):
                    inner_select = sub.this
                    inner_from_scope = build_from_scope_map_for_subquery(sub)
                    for sub_text, sub_alias, sub_node in extract_select_list(inner_select):
                        sub_col_name = sub_alias if sub_alias else sub_text
                        sub_db = ""
                        sub_table = ""
                        sub_remark = ""
                        if isinstance(sub_node, exp.Column):
                            q = getattr(sub_node, "table", None)
                            if q:
                                mapped = inner_from_scope.get(q)
                                if mapped:
                                    _, db2, actual2 = mapped
                                    sub_table = actual2 if actual2 and actual2 != "__SUBQUERY__" else q
                                    if db2:
                                        sub_db = db2
                                    else:
                                        sub_remark = "database_not_specified_in_query"
                                else:
                                    sub_table = q
                                    sub_remark = "database_not_specified_in_query"
                            else:
                                sub_remark = "database_not_specified_in_query"
                        # Add row for each subquery column
                        results.append({
                            "Database Name": sub_db,
                            "Table Name": sub_table,
                            "Column Name": sub_col_name,
                            "Alias Name": sub_alias if sub_alias else "",
                            "Regulation": regulation,
                            "Metadatakey": metadatakey,
                            "View Name": view_name,
                            "Remarks": "Subquery Layer" + (f"; {sub_remark}" if sub_remark else "")
                        })
                else:
                    # scalar expression that isn't a Select; just emit as expression
                    results.append({
                        "Database Name": "",
                        "Table Name": "",
                        "Column Name": col_display_name,
                        "Alias Name": col_alias if col_alias else "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": REMARKS["DERIVED_EXPR"]
                    })
                continue

            # Functions / expressions / literals
            # We don't always know table/db; emit expression with possible alias
            results.append({
                "Database Name": "",
                "Table Name": "",
                "Column Name": col_display_name,
                "Alias Name": col_alias if col_alias else "",
                "Regulation": regulation,
                "Metadatakey": metadatakey,
                "View Name": view_name,
                "Remarks": REMARKS["DERIVED_EXPR"]
            })

    # Final normalization: ensure all fields are present and strings
    normalized_rows = []
    keys = ["Database Name", "Table Name", "Column Name", "Alias Name", "Regulation", "Metadatakey", "View Name", "Remarks"]
    for r in results:
        nr = {k: (str(r.get(k, "")) if r.get(k, "") is not None else "") for k in keys}
        normalized_rows.append(nr)

    return normalized_rows


# -------------------------
# Public API
# -------------------------

def parse_metadata_and_extract_lineage(metadata_json_str: str,
                                       regulation: str = "",
                                       metadatakey: str = "",
                                       view_name: str = "",
                                       sql_key: str = "sql_query") -> List[Dict]:
    sql = decode_base64_sql_from_metadata(metadata_json_str, sql_key=sql_key)
    return extract_lineage_rows(sql, regulation, metadatakey, view_name)
