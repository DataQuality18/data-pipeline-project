# """
# sql_lineage_extractor.py

# Usage:
#     - Call parse_metadata_and_extract_lineage(metadata_json_str, regulation, metadatakey, view_name)
#     - It returns a list of dicts with EXACT field names:
#       {
#         "Database Name": "",
#         "Table Name": "",
#         "Column Name": "",
#         "Alias Name": "",
#         "Regulation": "...",
#         "Metadatakey": "...",
#         "View Name": "...",
#         "Remarks": "..."
#       }
# """

# import base64
# import json
# from typing import List, Dict, Optional, Tuple, Union
# import sqlglot
# from sqlglot import exp

# # -------------------------
# # Helper AST traversal utilities
# # -------------------------

# def decode_base64_sql_from_metadata(metadata_json_str: str, sql_key: str = "sql_query") -> str:
#     """
#     Accepts a JSON string (metadata JSON). The SQL is expected in base64 under metadata[sql_key].
#     Returns decoded SQL string.
#     """
#     meta = json.loads(metadata_json_str)
#     if sql_key not in meta:
#         raise KeyError(f"metadata JSON does not contain key '{sql_key}'")
#     b64 = meta[sql_key]
#     try:
#         sql_bytes = base64.b64decode(b64)
#         return sql_bytes.decode("utf-8")
#     except Exception as e:
#         # if it's already plain text, just return
#         return b64

# def get_table_identifiers(from_exp: exp.Expression) -> List[Tuple[str, Optional[str], Optional[exp.Expression]]]:
#     """
#     Given a FROM expression (or the whole AST), find table references.
#     Returns list of tuples: (table_name, db_name_or_None, source_expression)
#       - source_expression is the expression representing the table (Table, or Subquery)
#     """
#     tables = []

#     # Search for Table/Identifier, and Subquery/alias expressions under FROM
#     for node in from_exp.find_all((exp.Table, exp.Subquery, exp.Values), maxdepth=None):
#         if isinstance(node, exp.Table):
#             # table may have .this -> Identifier or Expression
#             table_id = node.this.name if hasattr(node.this, "name") else str(node)
#             db = None
#             if getattr(node, "db", None):
#                 db = node.db.name if hasattr(node.db, "name") else str(node.db)
#             # alias if present
#             tables.append((table_id, db, node))
#         elif isinstance(node, exp.Subquery):
#             # subquery may have alias on the parent alias node (handled when selecting FROM)
#             tables.append(("__SUBQUERY__", None, node))
#     # Also find Aliases that wrap subqueries or tables
#     for alias in from_exp.find_all(exp.Alias, maxdepth=None):
#         # alias.this is a sub-expression (Table or Subquery)
#         name = alias.alias_or_name
#         child = alias.this
#         if isinstance(child, exp.Table):
#             table_id = child.this.name if hasattr(child.this, "name") else str(child)
#             db = None
#             if getattr(child, "db", None):
#                 db = child.db.name if hasattr(child.db, "name") else str(child.db)
#             tables.append((table_id, db, alias))  # alias used as source so we can get alias name
#         elif isinstance(child, exp.Subquery):
#             tables.append((name, None, alias))  # name is alias mapping to subquery
#     return tables

# def normalize_identifier(ident: Union[exp.Identifier, exp.Column, str]) -> str:
#     if isinstance(ident, exp.Column):
#         parts = []
#         if ident.table:
#             parts.append(ident.table)
#         if ident.name:
#             parts.append(ident.name)
#         return ".".join(parts)
#     if isinstance(ident, exp.Identifier):
#         return ident.name
#     return str(ident)

# def extract_select_list(select_exp: exp.Select) -> List[Tuple[str, Optional[str], exp.Expression]]:
#     """
#     Returns list of tuples for each select projection:
#       (column_expression_string, alias_or_None, original_expression_node)
#     """
#     projections = []
#     for proj in select_exp.expressions:
#         alias = None
#         node = proj
#         if isinstance(proj, exp.Alias):
#             alias = proj.alias_or_name
#             node = proj.this
#         # column node could be Star, Column, Func, Literal, Subquery (scalar subquery), etc.
#         projections.append((str(node), alias, node))
#     return projections

# # -------------------------
# # Core extraction logic
# # -------------------------

# def resolve_star(select_node: exp.Select, star: exp.Star, from_scope_tables: dict) -> List[Dict]:
#     """
#     Given a Star node, find what it refers to and generate rows.
#     from_scope_tables: mapping of alias_or_table_name -> (node, db)
#     Returns list of rows (remarks etc.) for the star expansion.
#     """
#     results = []
#     table_qualifier = getattr(star, "this", None)  # in sqlglot, star.this may represent table qualifier
#     qualifier_name = None
#     if table_qualifier:
#         # may be an Identifier or name
#         qualifier_name = normalize_identifier(table_qualifier)
#     # Build a row indicating all_columns_selected
#     star_row = {
#         "Database Name": "",
#         "Table Name": qualifier_name if qualifier_name else "",
#         "Column Name": "*",
#         "Alias Name": "",
#         "Regulation": "",
#         "Metadatakey": "",
#         "View Name": "",
#         "Remarks": "all_columns_selected"
#     }
#     results.append(star_row)

#     # If the star qualifier is a known alias that maps to a subquery, dive
#     if qualifier_name and qualifier_name in from_scope_tables:
#         source_node, db = from_scope_tables[qualifier_name]
#         # If source_node is alias/subquery, then produce inner alias layer row
#         if isinstance(source_node, exp.Alias) and isinstance(source_node.this, exp.Subquery):
#             # Inner Query Alias Layer row
#             inner_row = {
#                 "Database Name": db if db else "",
#                 "Table Name": qualifier_name,
#                 "Column Name": "* (subquery)",
#                 "Alias Name": qualifier_name,
#                 "Regulation": "",
#                 "Metadatakey": "",
#                 "View Name": "",
#                 "Remarks": "Inner Query Alias Layer"
#             }
#             results.append(inner_row)
#             # Dive into subquery to list its select columns
#             subq = source_node.this  # exp.Subquery
#             if isinstance(subq, exp.Subquery) and isinstance(subq.this, exp.Select):
#                 sub_select = subq.this
#                 for col_text, col_alias, col_node in extract_select_list(sub_select):
#                     # For each column inside the subquery, emit a Subquery Layer row
#                     col_name = col_alias if col_alias else col_text
#                     sub_row = {
#                         "Database Name": "",
#                         "Table Name": qualifier_name,
#                         "Column Name": col_name,
#                         "Alias Name": col_alias if col_alias else "",
#                         "Regulation": "",
#                         "Metadatakey": "",
#                         "View Name": "",
#                         "Remarks": "Subquery Layer"
#                     }
#                     results.append(sub_row)
#     return results

# # def build_from_scope_map(ast_root: exp.Expression) -> dict:
# #     """
# #     Map alias/table names -> (node, db)
# #     Node can be:
# #       - exp.Table
# #       - exp.Alias (wrapping a subquery or table)
# #     """
# #     from_map = {}
# #     # Look into FROM clauses (may be several if multiple subqueries)
# #     for from_clause in ast_root.find_all(exp.From):
# #         for child in from_clause.find_all(exp.Expression):
# #             if isinstance(child, exp.Alias):
# #                 name = child.alias_or_name
# #                 # determine db if child.this is Table
# #                 db = None
# #                 if isinstance(child.this, exp.Table) and getattr(child.this, "db", None):
# #                     db = child.this.db.name if hasattr(child.this.db, "name") else str(child.this.db)
# #                 from_map[name] = (child, db)
# #             elif isinstance(child, exp.Table):
# #                 name = child.this.name if hasattr(child.this, "name") else str(child)
# #                 db = child.db.name if getattr(child, "db", None) and hasattr(child.db, "name") else None
# #                 from_map[name] = (child, db)
# #             elif isinstance(child, exp.Subquery):
# #                 # unnamed subquery: we may not have an alias; ignore unless alias is present in parent
# #                 parent_alias = None
# #                 # try to see if the parent is an Alias (higher level)
# #                 for p in ast_root.walk():
# #                     pass
# #     # Also find Aliases anywhere under FROM that correspond to subqueries/tables
# #     for alias in ast_root.find_all(exp.Alias):
# #         name = alias.alias_or_name
# #         if alias.this and (isinstance(alias.this, exp.Subquery) or isinstance(alias.this, exp.Table)):
# #             db = None
# #             if isinstance(alias.this, exp.Table) and getattr(alias.this, "db", None):
# #                 db = alias.this.db.name if hasattr(alias.this.db, "name") else None
# #             from_map[name] = (alias, db)
# #     return from_map
# def build_from_scope_map(ast_root: exp.Expression) -> dict:
#     """
#     Map alias/table names -> (node, db_name, table_name)
#     node can be exp.Table or exp.Alias.
#     db_name = schema/database name
#     table_name = actual table (not alias)
#     """
#     from_map = {}

#     for tbl in ast_root.find_all(exp.Table):
#         # Extract table name
#         table_name = tbl.this.name if hasattr(tbl.this, "name") else str(tbl.this)

#         # Extract db/schema
#         db_name = None
#         if getattr(tbl, "db", None):
#             if hasattr(tbl.db, "name"):
#                 db_name = tbl.db.name

#         # alias?
#         alias = tbl.alias_or_name

#         key = alias if alias else table_name
#         from_map[key] = (tbl, db_name, table_name)

#     # subquery aliases
#     for alias in ast_root.find_all(exp.Alias):
#         if isinstance(alias.this, exp.Subquery):
#             key = alias.alias_or_name
#             from_map[key] = (alias, None, "__SUBQUERY__")

#     return from_map

# def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
#     """
#     Main entry: parse SQL, extract rows list.
#     """
#     try:
#         ast = sqlglot.parse_one(sql)
#     except Exception as e:
#         raise RuntimeError(f"sqlglot failed to parse SQL: {e}")

#     results = []

#     # Build a from-scope map: alias/table -> (node, db)
#     from_scope = build_from_scope_map(ast)

#     # Find SELECT nodes
#     selects = list(ast.find_all(exp.Select))
#     if not selects:
#         # Maybe it's a CTE or single expression
#         selects = [ast] if isinstance(ast, exp.Select) else []

#     for select in selects:
#         select_list = extract_select_list(select)
#         # For each projection
#         for col_text, col_alias, col_node in select_list:
#             # if this projection is a star
#             if isinstance(col_node, exp.Star) or (isinstance(col_node, exp.Identifier) and str(col_node) == "*"):
#                 # resolve star, possibly dive into subquery alias
#                 star_rows = resolve_star(select, col_node if isinstance(col_node, exp.Star) else exp.Star(this=None), from_scope)
#                 # attach regulation/metadatakey/view_name and ensure exact keys
#                 for r in star_rows:
#                     r["Regulation"] = regulation
#                     r["Metadatakey"] = metadatakey
#                     r["View Name"] = view_name
#                     # If table had no db prefix and table exists in from_scope with db None -> mark database_not_specified_in_query
#                     if r["Table Name"]:
#                         mapped = from_scope.get(r["Table Name"])
#                         if mapped and mapped[1] is None:
#                             r["Remarks"] += "; database_not_specified_in_query"
#                     results.append(r)
#             else:
#                 # Normal column expression (could be Column, Function, Literal, etc.)
#                 col_name = col_alias if col_alias else col_text
#                 # Try to identify table/qualifier for this column if it's a Column node
#                 table_name = ""
#                 db_name = ""
#                 if isinstance(col_node, exp.Column):
#                     if col_node.table:
#                         table_name = col_node.table
#                         mapped = from_scope.get(table_name)
#                         if mapped:
#                             # mapped[1] is db
#                             db_name = mapped[1] if mapped[1] else ""
#                             if mapped[1] is None:
#                                 remarks = "database_not_specified_in_query"
#                             else:
#                                 remarks = ""
#                         else:
#                             remarks = ""
#                     else:
#                         remarks = "database_not_specified_in_query"
#                 else:
#                     # not a column; could be expression/result of subquery, scalar subquery, function
#                     remarks = ""
#                     # If it's a scalar subquery, attempt to extract its select columns
#                     if isinstance(col_node, exp.Subquery):
#                         # dive into subquery select list and emit Subquery Layer rows
#                         sub = col_node
#                         if isinstance(sub.this, exp.Select):
#                             for sub_text, sub_alias, sub_node in extract_select_list(sub.this):
#                                 subcol_name = sub_alias if sub_alias else sub_text
#                                 sub_row = {
#                                     "Database Name": "",
#                                     "Table Name": "",  # scalar subquery may not have immediate table name
#                                     "Column Name": subcol_name,
#                                     "Alias Name": "",
#                                     "Regulation": regulation,
#                                     "Metadatakey": metadatakey,
#                                     "View Name": view_name,
#                                     "Remarks": "Subquery Layer"
#                                 }
#                                 results.append(sub_row)
#                 row = {
#                     "Database Name": db_name if db_name else "",
#                     "Table Name": table_name,
#                     "Column Name": col_name,
#                     "Alias Name": col_alias if col_alias else "",
#                     "Regulation": regulation,
#                     "Metadatakey": metadatakey,
#                     "View Name": view_name,
#                     "Remarks": remarks
#                 }
#                 results.append(row)

#     # Post-process: ensure all rows have the exact keys and values are strings
#     normalized = []
#     keys = ["Database Name", "Table Name", "Column Name", "Alias Name", "Regulation", "Metadatakey", "View Name", "Remarks"]
#     for r in results:
#         nr = {k: (str(r.get(k, "")) if r.get(k, "") is not None else "") for k in keys}
#         normalized.append(nr)
#     return normalized

# # -------------------------
# # Main high-level convenience function used by pipeline
# # -------------------------

# def parse_metadata_and_extract_lineage(metadata_json_str: str,
#                                        regulation: str = "",
#                                        metadatakey: str = "",
#                                        view_name: str = "",
#                                        sql_key: str = "sql_query") -> List[Dict]:
#     """
#     Given metadata JSON (string), decode sql_query (base64), parse, and return lineage rows.
#     """
#     sql = decode_base64_sql_from_metadata(metadata_json_str, sql_key=sql_key)
#     rows = extract_lineage_rows(sql, regulation, metadatakey, view_name)
#     return rows


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
            else:
                star_row["Remarks"] += "; database_not_specified_in_query"
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
                    "Remarks": "Inner Query Alias Layer"
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
                                "Remarks": "Subquery Layer; all_columns_selected"
                            }
                            # attach db if inner_from_scope has single table with db
                            if not col_node.this and len(inner_from_scope) == 1:
                                k2 = list(inner_from_scope.keys())[0]
                                _, db2, actual2 = inner_from_scope[k2]
                                if db2:
                                    sub_inner_row["Database Name"] = db2
                                else:
                                    sub_inner_row["Remarks"] += "; database_not_specified_in_query"
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
                            "Remarks": "Subquery Layer"
                        }
                        # if column had a table qualifier but that inner mapping has no db -> remark
                        if isinstance(col_node, exp.Column) and getattr(col_node, "table", None):
                            mapped_inner = inner_from_scope.get(col_node.table)
                            if mapped_inner and mapped_inner[1] is None:
                                sub_row["Remarks"] += "; database_not_specified_in_query"
                        rows.append(sub_row)
                return rows  # we added star_row + inner + sub rows already
            else:
                # qualifier maps to a table (or alias that points to table)
                if db:
                    star_row["Database Name"] = db
                else:
                    star_row["Remarks"] += "; database_not_specified_in_query"
                # set Table Name to the actual table (not alias)
                star_row["Table Name"] = actual_table if actual_table and actual_table != "__SUBQUERY__" else qualifier_name
        else:
            # qualifier not found in from_scope: keep qualifier as Table Name but mark missing db
            star_row["Table Name"] = qualifier_name
            star_row["Remarks"] += "; database_not_specified_in_query"

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
            remarks = ""

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
                            remarks = "database_not_specified_in_query"
                    else:
                        # qualifier not in from_scope; emit with qualifier as table name (db unknown)
                        table_name = qualifier
                        remarks = "database_not_specified_in_query"
                else:
                    # Unqualified column: can't determine table -> mark missing db and empty table
                    table_name = ""
                    remarks = "database_not_specified_in_query"
                row = {
                    "Database Name": db_name,
                    "Table Name": table_name,
                    "Column Name": col_base_name if col_alias is None else col_alias,
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
                        "Remarks": ""
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
                "Remarks": ""
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


# -------------------------
# Example usage / tests
# -------------------------
if __name__ == "__main__":
    # Example: SELECT * from subq alias which selects columns
    import pprint
    sample_sql = """
    SELECT a.col1, b.col2, s.*
    FROM schema.table_a a
    JOIN (SELECT x AS col_x, y FROM other_table) s ON a.id = s.y
    JOIN table_b b ON a.b_id = b.id
    """
    sql_txt = """

SELECT *
FROM (
    SELECT
        T.TRADE_SK,
        T.DWH_MESSAGE_HASHCODE,
        T.TRADE_EVENT_TIMESTAMP,
        T.PARTY_EXECUTION_TIMESTAMP,
        T.UTI,
        T.UTI_NAMESPACE,
        T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY,
        T.FIRM_ACCOUNT_MNEMONIC,
        T.FIRM_PARTY_GFCID,
        T.COUNTER_PARTY_MNEMONIC,
        T.COUNTER_PARTY_GFCID,
        T.USI,
        T.USI_NAMESPACE,
        T.PRIMARY_ASSET_CLASS,
        T.TRADE_UTI_ID,
        T.FIRM_PARTY_LEI,
        T.ACTUAL_TERMINATION_DATE,
        T.BUSINESS_DATE,
        T.DWH_CREATE_TIMESTAMP,
        T.TRADE_PUBLISHING_SYSTEM_NAME,
        T.TRADE_DATE,
        T.COUNTER_PARTY_LEI,
        T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY AS SB_SUMMARY,
        ROW_NUMBER() OVER (
            PARTITION BY T.TRADE_UT_ID
            ORDER BY CASE
                WHEN NVL(T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY, '') IN ('', 'NULL', 'none', 'NONE')
                    THEN 3
                WHEN NVL(T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY, '') NOT IN ('', 'NULL', 'none', 'NONE')
                    THEN 2
                ELSE 1
            END DESC,
            T.TRADE_EVENT_TIMESTAMP DESC
        ) AS ROWNUMBERBANK_1,
        T.TRADE_CLEARING_STATUS,
        T.CLEARING_HOUSE_ID,
        T.UPI,
        T.CLEARING_TRADE_ID,
        T.DWH_UPDATED_TIME,
        GFOLYNSD_STANDARIZATION.TRADE_FACT_DATA
    FROM
        GFOLYRE_MANAGED.APP_REGHUB_RHOO_TRADE T
        LEFT JOIN GFOLYNSD_STANDARIZATION.TRADE_FACT_DATA_L ON TRADE_UT_ID = T.TRADE_UT_ID
    WHERE
        T.TRADE_STATUS = 'ACTIVE'
        AND T.ACTUAL_TERMINATION_DATE >= TO_TIMESTAMP(
            DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), 'yyyyMMdd'),
            'yyyyMMdd'
        )
        AND T.ACTUAL_TERMINATION_DATE >= TO_TIMESTAMP(
            DATE_FORMAT(
                DATE_SUB(
                    TO_TIMESTAMP(TRADE_EVENT_TIMESTAMP, 'America/New_York'),
                    (CASE WHEN '#DAY_OF_WEEK#' = 'MONDAY'
                         THEN 3
                         WHEN '#DAY_OF_WEEK#' = 'MONDAY'
                         THEN 3 END)
                ),
                'yyyyMMdd'
            ),
            'yyyyMMdd'
        )
        AND T.DWH_BUSINESS_DATE <= CAST(
            DATE_FORMAT(
                DATE_SUB(
                    DATE(T.DWH_EVENT_TIMESTAMP),
                    3
                ),
                'yyyy-MM-dd'
            ) AS TIMESTAMP
        )
        AND T.DWH_UPDATED_TIME >= TO_TIMESTAMP(
            DATE_FORMAT(
                DATE_SUB(TO_TIMESTAMP(SYSTIMESTAMP), 5),
                'yyyy-MM-dd HH:mm:ss.SSS'
            )
        )
        AND T.DWH_UPDATED_TIME < TO_TIMESTAMP(
            DATE_FORMAT(
                DATE_SUB(TO_TIMESTAMP(SYSTIMESTAMP), 6),
                'yyyy-MM-dd HH:mm:ss.SSS'
            )
        )
)
WHERE ROWNUMBERBANK_1 = 1
    OR (
        IF EXISTS (
            SELECT T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY
            WHERE REPLACE(NVL(LATEST_VERSION, 'N'), 'Y', 'N') = 'Y'
        ) THEN LATEST_VERSION
        ELSE EXISTING_VALUE
    )

"""
    metadata = {"sql_query": base64.b64encode(sql_txt.encode()).decode()}
    rows = parse_metadata_and_extract_lineage(json.dumps(metadata), regulation="PCI", metadatakey="mk1", view_name="v1")
    print(json.dumps(rows, indent=2))

    map_query = """SELECT 
    P.UTID,
    P.DB_KEY,
    MAP(
        'PAYMENT_TYPE', PAYMENT_TYPE,
        'PAYMENT_AMOUNT', PAYMENT_AMOUNT,
        'PAYMENT_CURRENCY', PAYMENT_CURRENCY,
        'PAYMENT_DATE', PAYMENT_DATE,
        'FIRM_ACCOUNT_MNEMONIC', FIRM_ACCOUNT_MNEMONIC,
        'COUNTER_PARTY_MNEMONIC', COUNTER_PARTY_MNEMONIC,
        'LEG_TYPE', LEG_TYPE
    ) AS CASHFLOW_MAP
FROM 
    GFG_WORK.RHOO_REF_TRADE_CASHFLOW_DATA P
WHERE 
    P.GEMFIRE_ENV = '##GEMFIRE_CONNECTING_ENV##';
 """
    # metadata = {"sql_query": base64.b64encode(map_query.encode()).decode()}
    # rows = parse_metadata_and_extract_lineage(json.dumps(metadata), regulation="PCI", metadatakey="mk1", view_name="v1")
    # # rows = parse_sql_lineage(
    # #     map_query,
    # #     regulation="SEC",
    # #     metadatakey="KEY123",
    # #     view_name="VW_SAMPLE",
    # #     dialect='spark'
    # # )

    # import json
    # print("=====================this map query ========================================")
    # print(json.dumps(rows, indent=2))