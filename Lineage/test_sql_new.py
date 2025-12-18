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
  "Table Alias": "",
  "Column Alias": "",
  "Regulation": "...",
  "Metadatakey": "...",
  "View Name": "...",
  "Remarks": ["..."]
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


def ensure_remarks_list(val) -> List[str]:
    """Normalize remarks to a list[str]."""
    if val is None:
        return []
    if isinstance(val, list):
        return [str(v) for v in val if v is not None and str(v) != ""]
    if isinstance(val, str):
        v = val.strip()
        return [v] if v else []
    # fallback
    v = str(val).strip()
    return [v] if v else []

def add_remark(remarks: List[str], remark: str) -> List[str]:
    """Append a remark if not already present."""
    if remark and remark not in remarks:
        remarks.append(remark)
    return remarks

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
    Build a normalized list of select projections.
    Each item is: (col_text, col_alias, col_node)
      - col_text: SQL string for the projection node (without the alias wrapper)
      - col_alias: output column alias (AS alias) if present
      - col_node: the underlying expression node (Column / Star / Case / Func / etc.)
    """
    projections: List[Tuple[str, Optional[str], exp.Expression]] = []
    for proj in select_exp.expressions:
        col_alias: Optional[str] = None
        col_node: exp.Expression = proj
        if isinstance(proj, exp.Alias):
            col_alias = proj.alias_or_name
            col_node = proj.this
        # Use sql() for stable representation; str(node) is sometimes less consistent
        col_text = col_node.sql(dialect=None) if hasattr(col_node, "sql") else str(col_node)
        projections.append((col_text, col_alias, col_node))
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



def _resolve_column_against_scope(col: exp.Column,
                                 scope: Dict[str, Tuple[exp.Expression, Optional[str], str]],
                                 regulation: str,
                                 metadatakey: str,
                                 view_name: str,
                                 output_alias: str,
                                 extra_remarks: Optional[List[str]] = None) -> List[Dict]:
    """Resolve a Column node to one or more lineage rows using the provided scope."""
    extra_remarks = extra_remarks or []
    rows: List[Dict] = []
    qualifier = getattr(col, "table", None)
    col_name = getattr(col, "name", None) or (col.sql() if hasattr(col, "sql") else str(col))
    table_alias = ""
    db_name = ""
    table_name = ""
    remarks = ensure_remarks_list(extra_remarks)

    if qualifier:
        table_alias = str(qualifier)
        mapped = scope.get(table_alias)
        if mapped:
            node, db, actual_table = mapped
            # subquery alias: dive and resolve within the subquery
            if actual_table == "__SUBQUERY__" or (isinstance(node, exp.Alias) and isinstance(node.this, exp.Subquery)):
                add_remark(remarks, f"via_subquery:{table_alias}")
                rows.extend(resolve_subquery_column_sources(node, col_name, regulation, metadatakey, view_name, output_alias, remarks))
                return rows
            table_name = actual_table if actual_table else table_alias
            if db:
                db_name = db
            else:
                add_remark(remarks, REMARKS["DATABASE_NOT_SPECIFIED"])
        else:
            # unknown qualifier
            table_name = table_alias
            add_remark(remarks, REMARKS["DATABASE_NOT_SPECIFIED"])
    else:
        # unqualified: if only one table in scope, assign it; else ambiguous
        if len(scope) == 1:
            table_alias = next(iter(scope))
            node, db, actual_table = scope[table_alias]
            if actual_table == "__SUBQUERY__" or (isinstance(node, exp.Alias) and isinstance(node.this, exp.Subquery)):
                add_remark(remarks, f"via_subquery:{table_alias}")
                rows.extend(resolve_subquery_column_sources(node, col_name, regulation, metadatakey, view_name, output_alias, remarks))
                return rows
            table_name = actual_table if actual_table else table_alias
            if db:
                db_name = db
            else:
                add_remark(remarks, REMARKS["DATABASE_NOT_SPECIFIED"])
        else:
            add_remark(remarks, REMARKS["TABLE_AMBIGUOUS"])

    rows.append({
        "Database Name": db_name,
        "Table Name": table_name,
        "Table Alias": table_alias,
        "Column Name": col_name,
        "Column Alias": output_alias,
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": remarks
    })
    return rows


def resolve_subquery_column_sources(subquery_alias_node: exp.Expression,
                                   target_output_col: str,
                                   regulation: str,
                                   metadatakey: str,
                                   view_name: str,
                                   output_alias: str,
                                   remarks: Optional[List[str]] = None,
                                   _depth: int = 0) -> List[Dict]:
    """
    Given a subquery alias node (usually exp.Alias where .this is exp.Subquery),
    find the projection inside the subquery that produces `target_output_col`,
    then resolve its source columns using the subquery's inner FROM scope.
    """
    remarks = ensure_remarks_list(remarks)
    if _depth > 3:
        # prevent runaway recursion
        add_remark(remarks, "subquery_resolution_depth_exceeded")
        return [{
            "Database Name": "",
            "Table Name": "",
            "Table Alias": "",
            "Column Name": target_output_col,
            "Column Alias": output_alias,
            "Regulation": regulation,
            "Metadatakey": metadatakey,
            "View Name": view_name,
            "Remarks": remarks
        }]

    # Normalize input to an exp.Subquery
    subq = None
    if isinstance(subquery_alias_node, exp.Alias) and isinstance(subquery_alias_node.this, exp.Subquery):
        subq = subquery_alias_node.this
    elif isinstance(subquery_alias_node, exp.Subquery):
        subq = subquery_alias_node
    else:
        # unknown shape
        add_remark(remarks, "subquery_alias_node_unexpected")
        return [{
            "Database Name": "",
            "Table Name": "",
            "Table Alias": "",
            "Column Name": target_output_col,
            "Column Alias": output_alias,
            "Regulation": regulation,
            "Metadatakey": metadatakey,
            "View Name": view_name,
            "Remarks": remarks
        }]

    if not isinstance(subq.this, exp.Select):
        add_remark(remarks, "subquery_not_select")
        return [{
            "Database Name": "",
            "Table Name": "",
            "Table Alias": "",
            "Column Name": target_output_col,
            "Column Alias": output_alias,
            "Regulation": regulation,
            "Metadatakey": metadatakey,
            "View Name": view_name,
            "Remarks": remarks
        }]

    inner_select: exp.Select = subq.this
    inner_scope = build_from_scope_map_for_subquery(subq)

    # Find matching projection in the subquery
    match_node: Optional[exp.Expression] = None
    for inner_text, inner_alias, inner_node in extract_select_list(inner_select):
        out_name = inner_alias
        if not out_name:
            if isinstance(inner_node, exp.Column):
                out_name = getattr(inner_node, "name", None)
            else:
                out_name = inner_text
        if out_name == target_output_col:
            match_node = inner_node
            break

    if match_node is None:
        # If we couldn't match, emit a conservative row with subquery as table
        add_remark(remarks, "subquery_output_column_not_found")
        return [{
            "Database Name": "",
            "Table Name": "__SUBQUERY__",
            "Table Alias": "",
            "Column Name": target_output_col,
            "Column Alias": output_alias,
            "Regulation": regulation,
            "Metadatakey": metadatakey,
            "View Name": view_name,
            "Remarks": remarks
        }]

    # If the matched projection is a simple Column, resolve it against inner scope
    if isinstance(match_node, exp.Column):
        return _resolve_column_against_scope(match_node, inner_scope, regulation, metadatakey, view_name, output_alias, remarks)

    # If the matched projection is a derived expression, resolve all its inner columns
    cols = list({c.sql(): c for c in match_node.find_all(exp.Column)}.values())
    if cols:
        out_rows: List[Dict] = []
        for c in cols:
            out_rows.extend(_resolve_column_against_scope(c, inner_scope, regulation, metadatakey, view_name, output_alias, remarks + [REMARKS["DERIVED_EXPR"]]))
        return out_rows

    # no columns inside; fallback
    add_remark(remarks, REMARKS["DERIVED_EXPR"])
    return [{
        "Database Name": "",
        "Table Name": "__SUBQUERY__",
        "Table Alias": "",
        "Column Name": target_output_col,
        "Column Alias": output_alias,
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": remarks
    }]


def resolve_expression_sources(expr_node: exp.Expression,
                               scope: Dict[str, Tuple[exp.Expression, Optional[str], str]],
                               regulation: str,
                               metadatakey: str,
                               view_name: str,
                               output_alias: str,
                               base_remarks: Optional[List[str]] = None) -> List[Dict]:
    """Resolve derived expressions (CASE, functions, math) to input column lineage rows."""
    base_remarks = ensure_remarks_list(base_remarks)
    add_remark(base_remarks, REMARKS["DERIVED_EXPR"])

    # Collect unique Column nodes used in the expression
    cols_map = {}
    for c in expr_node.find_all(exp.Column):
        cols_map[c.sql()] = c
    cols = list(cols_map.values())

    if not cols:
        # no columns inside, emit one row describing derived output only
        return [{
            "Database Name": "",
            "Table Name": "",
            "Table Alias": "",
            "Column Name": expr_node.sql() if hasattr(expr_node, "sql") else str(expr_node),
            "Column Alias": output_alias,
            "Regulation": regulation,
            "Metadatakey": metadatakey,
            "View Name": view_name,
            "Remarks": base_remarks
        }]

    rows: List[Dict] = []
    for c in cols:
        rows.extend(_resolve_column_against_scope(c, scope, regulation, metadatakey, view_name, output_alias, base_remarks))
    return rows

# -------------------------
# STAR resolution & subquery dive
# -------------------------

def resolve_star(select_node: exp.Select,
                 star_node: exp.Star,
                 from_scope: Dict[str, Tuple[exp.Expression, Optional[str], str]],
                 regulation: str,
                 metadatakey: str,
                 view_name: str) -> List[Dict]:
    """
    Expand a star projection (* or T.*) into lineage rows.
    Emits at least one marker row (Column Name = '*') and, when possible,
    additional rows for subquery stars (T.* where T is a subquery alias) that
    list the subquery's projected columns.
    """
    rows: List[Dict] = []

    qualifier = getattr(star_node, "this", None)
    qualifier_name: Optional[str] = None
    if qualifier:
        qualifier_name = getattr(qualifier, "name", None) or str(qualifier)

    remarks = [REMARKS["ALL_COLUMNS"]]
    table_alias = qualifier_name or ""
    db_name = ""
    table_name = qualifier_name or ""

    # Unqualified star: if only one table, map it
    if not qualifier_name and len(from_scope) == 1:
        table_alias = next(iter(from_scope))
        node, db, actual_table = from_scope[table_alias]
        table_name = actual_table if actual_table else table_alias
        if db:
            db_name = db
        else:
            add_remark(remarks, REMARKS["DATABASE_NOT_SPECIFIED"])

    # Qualified star: map qualifier
    if qualifier_name:
        mapped = from_scope.get(qualifier_name)
        if mapped:
            node, db, actual_table = mapped
            # If qualifier is a subquery alias, emit additional rows for its columns
            if isinstance(node, exp.Alias) and isinstance(node.this, exp.Subquery):
                # marker row for the star itself
                rows.append({
                    "Database Name": db_name,
                    "Table Name": qualifier_name,
                    "Table Alias": qualifier_name,
                    "Column Name": "*",
                    "Column Alias": "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": remarks
                })
                # Subquery layer rows
                subq = node.this
                if isinstance(subq.this, exp.Select):
                    inner_select = subq.this
                    inner_scope = build_from_scope_map_for_subquery(subq)
                    for inner_text, inner_alias, inner_node in extract_select_list(inner_select):
                        out_alias = inner_alias if inner_alias else (getattr(inner_node, "name", None) if isinstance(inner_node, exp.Column) else inner_text)
                        # For each projected column, resolve lineage within subquery
                        if isinstance(inner_node, exp.Star):
                            # We cannot expand inner star without metadata; emit marker
                            rows.append({
                                "Database Name": "",
                                "Table Name": qualifier_name,
                                "Table Alias": qualifier_name,
                                "Column Name": "*",
                                "Column Alias": out_alias or "",
                                "Regulation": regulation,
                                "Metadatakey": metadatakey,
                                "View Name": view_name,
                                "Remarks": [REMARKS["SUBQUERY_LAYER"], REMARKS["ALL_COLUMNS"]]
                            })
                        elif isinstance(inner_node, exp.Column):
                            sub_rows = _resolve_column_against_scope(inner_node, inner_scope, regulation, metadatakey, view_name, out_alias or "", [REMARKS["SUBQUERY_LAYER"], f"via_subquery:{qualifier_name}"])
                            rows.extend(sub_rows)
                        else:
                            rows.extend(resolve_expression_sources(inner_node, inner_scope, regulation, metadatakey, view_name, out_alias or "", [REMARKS["SUBQUERY_LAYER"], f"via_subquery:{qualifier_name}"]))
                return rows

            # Otherwise qualifier maps to table
            table_alias = qualifier_name
            table_name = actual_table if actual_table and actual_table != "__SUBQUERY__" else qualifier_name
            if db:
                db_name = db
            else:
                add_remark(remarks, REMARKS["DATABASE_NOT_SPECIFIED"])
        else:
            add_remark(remarks, REMARKS["DATABASE_NOT_SPECIFIED"])

    # Default single marker row
    rows.append({
        "Database Name": db_name,
        "Table Name": table_name if table_name else "",
        "Table Alias": table_alias,
        "Column Name": "*",
        "Column Alias": "",
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": remarks
    })
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
                for r in star_rows:
                    results.append({
                        "Database Name": r.get("Database Name", "") or "",
                        "Table Name": r.get("Table Name", "") or "",
                        "Table Alias": r.get("Table Alias", "") or "",
                        "Column Name": r.get("Column Name", "") or "",
                        "Column Alias": r.get("Column Alias", "") or "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": ensure_remarks_list(r.get("Remarks", [])),
                    })
                continue

            # Output column alias for this projection (what it ends as)
            output_alias = col_alias if col_alias else ""

            # Simple Column projection
            if isinstance(col_node, exp.Column):
                base_rows = _resolve_column_against_scope(
                    col_node,
                    from_scope,
                    regulation,
                    metadatakey,
                    view_name,
                    output_alias,
                    [REMARKS["COLUMN_SELECTED"]],
                )
                results.extend(base_rows)
                continue

            # Subquery projection used directly in SELECT: treat as derived; emit its inner select lineage
            if isinstance(col_node, exp.Subquery) and isinstance(col_node.this, exp.Select):
                inner_select = col_node.this
                inner_scope = build_from_scope_map_for_subquery(col_node)
                for sub_text, sub_alias, sub_node in extract_select_list(inner_select):
                    out_alias = sub_alias if sub_alias else (getattr(sub_node, "name", None) if isinstance(sub_node, exp.Column) else sub_text)
                    if isinstance(sub_node, exp.Column):
                        results.extend(_resolve_column_against_scope(sub_node, inner_scope, regulation, metadatakey, view_name, out_alias or "", [REMARKS["SUBQUERY_LAYER"]]))
                    elif isinstance(sub_node, exp.Star):
                        results.append({
                            "Database Name": "",
                            "Table Name": "__SUBQUERY__",
                            "Table Alias": "",
                            "Column Name": "*",
                            "Column Alias": out_alias or "",
                            "Regulation": regulation,
                            "Metadatakey": metadatakey,
                            "View Name": view_name,
                            "Remarks": [REMARKS["SUBQUERY_LAYER"], REMARKS["ALL_COLUMNS"]],
                        })
                    else:
                        results.extend(resolve_expression_sources(sub_node, inner_scope, regulation, metadatakey, view_name, out_alias or "", [REMARKS["SUBQUERY_LAYER"]]))
                continue

            # Derived expression (CASE, function, arithmetic, etc.)
            # Requirement: Column Alias should be what it ends as (AS alias); Column Name should be the input columns used
            results.extend(
                resolve_expression_sources(
                    col_node,
                    from_scope,
                    regulation,
                    metadatakey,
                    view_name,
                    output_alias,
                    [REMARKS["DERIVED_EXPR"]],
                )
            )

    # Final normalization:
# Final normalization: ensure all fields are present and strings
    normalized_rows = []
    keys = ["Database Name", "Table Name", "Table Alias", "Column Name", "Column Alias", "Regulation", "Metadatakey", "View Name", "Remarks"]
    for r in results:
        nr = {k: (ensure_remarks_list(r.get(k, [])) if k == "Remarks" else (str(r.get(k, "")) if r.get(k, "") is not None else "")) for k in keys}
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
