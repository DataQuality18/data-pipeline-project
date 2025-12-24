
"""
sql_lineage_extractors.py
(Extended with JOIN expression lineage — backward-compatible)

Enhancements Added (this version):
- JOIN expression lineage:
  - Columns participating in ON clauses
  - Equality pairs (a.col = b.col)
  - Join type tagging (LEFT / INNER / RIGHT / FULL)
- Keeps prior enhancements:
  - CASE / WHERE / GROUP BY / HAVING lineage
  - Derived expression column expansion
  - Remarks taxonomy as list[str]
  - Table Alias Name preserved
- ZERO breaking changes to existing behavior
"""
import base64
import json
from typing import List, Dict, Tuple, Optional
import sqlglot
from sqlglot import exp

# -----------------------
# Constants
# -----------------------
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
    # JOIN lineage
    "JOIN_ON_COLUMN": "join_on_clause_column",
    "JOIN_EQ_PAIR": "join_equality_pair",
    "JOIN_TYPE": "join_type",
    # NEW: subquery WHERE within JOIN
    "JOIN_SUBQUERY_WHERE_COLUMN": "join_subquery_where_column",
    "FUNCTION_EXPR": "function_expression",
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

# -----------------------
# Utilities
# -----------------------
def emit_dataset_lineage(ast, regulation, metadatakey, view_name):
    """
    Handles:
      SELECT * FROM ( SELECT ... ) TSR_TS_DATA

    Uses SQLGlot AST correctly:
      select.args['from_']
    """
    rows = []

    for select in ast.find_all(exp.Select):
        # check outermost SELECT
        parent = select.parent
        while parent:
            if isinstance(parent, exp.Select):
                break
            parent = parent.parent
        else:
            # outermost
            from_node = select.args.get("from_")
            if not from_node:
                continue

            subq = from_node.this
            if isinstance(subq, exp.Subquery) and subq.alias_or_name:
                # ensure SELECT *
                if any(isinstance(e, exp.Star) for e in select.expressions):
                    rows.append({
                        "Database Name": "",
                        "Table Name": subq.alias_or_name,   # TSR_TS_DATA
                        "Table Alias Name": "",
                        "Column Name": "*",
                        "Alias Name": "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": [
                            "all_columns_selected",
                            "table name Derived"
                        ],
                    })
    return rows

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

# -----------------------
# FROM-scope mapping
# -----------------------
def _pick_base_table_from_subquery(subq: exp.Subquery) -> Tuple[str, str]:
    """
    Resolve the base table inside a subquery:
      (SELECT * FROM db.tbl WHERE ...) alias
    Returns: (db, table_name)
    If multiple tables exist, returns ("", "__SUBQUERY__").
    """
    # breakpoint()
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

    Maps:
      - Plain tables and their aliases (e.g., TSR)
      - Subquery aliases (e.g., CURRENT_RECORD, PREVIOUS_RECORD, TSR_TS_DATA)
    """
    from_map = {}

    # 1) All tables in the query
    for tbl in ast_root.find_all(exp.Table):
        table_name = safe_name(tbl.this)
        db = safe_name(tbl.db)
        table_alias = tbl.alias_or_name if tbl.alias else None
        key = table_alias or table_name
        if key and key not in from_map:
            from_map[key] = (tbl, db, table_name, table_alias)

    # 2) Aliases
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
            # Record alias; resolve base table separately
            from_map[key] = (alias, None, "__SUBQUERY__", key)

    # 3) Subquery alias mapping (JOIN subqueries and FROM subqueries)
    for subq in ast_root.find_all(exp.Subquery):
        subq_alias = subq.alias_or_name if subq.args.get("alias") else None
        if not subq_alias:
            continue
        db, base_table = _pick_base_table_from_subquery(subq)
        from_map[subq_alias] = (subq, db, base_table, subq_alias)

    return from_map

def is_function_only_expression(expr: exp.Expression) -> bool:
    return (
        isinstance(expr, (exp.Func, exp.Anonymous))
        and not list(expr.find_all(exp.Column))
    )

# -----------------------
# SELECT list helper
# -----------------------
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

# -----------------------
# STAR handling
# -----------------------
def resolve_star(select_node, star_node, from_scope, regulation, metadatakey, view_name):
    return [{
        "Database Name": "",
        "Table Name": "",
        "Table Alias Name": "",
        "Column Name": "*",
        "Alias Name": "",
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": [REMARKS["ALL_COLUMNS"]],
    }]

# -----------------------
# Emit helper (supports explicit overrides)
# -----------------------
def _emit_column_lineage(
    results,
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
):
    """
    Emits a normalized lineage row.
    Resolution order:
      1) explicit_table_* overrides (if provided)
      2) qualifier (if present and resolvable)
      3) fallback_alias (e.g., JOIN alias) if provided
      4) sole entry in from_scope
    """
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

    results.append({
        "Database Name": db or "",
        "Table Name": table or "",
        "Table Alias Name": table_alias or (effective_qualifier or ""),
        "Column Name": column_name,
        "Alias Name": "",
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": ensure_list(remark_list),
    })

# -----------------------
# Context helper: nearest subquery alias for a SELECT
# -----------------------
def _nearest_subquery_alias(select_node: exp.Select) -> Optional[str]:
    """
    Finds the alias of the nearest enclosing Subquery for a given Select, if any.
    Useful for tagging outer/inner layers (e.g., TSR_TS_DATA).
    """
    parent_subq = select_node.parent
    while parent_subq and not isinstance(parent_subq, exp.Subquery):
        parent_subq = parent_subq.parent
    if isinstance(parent_subq, exp.Subquery):
        alias = parent_subq.alias_or_name if parent_subq.args.get("alias") else None
        return alias
    return None

# -----------------------
# Core extraction
# -----------------------
def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    ast = sqlglot.parse_one(sql)
    results: List[Dict] = []
    results.extend(
            emit_dataset_lineage(ast, regulation, metadatakey, view_name)
        )
    from_scope = build_from_scope_map(ast)
    selects = list(ast.find_all(exp.Select))

    for select in selects:
        # Optional: capture enclosing subquery alias (outer vs inner layer)
        enclosing_alias = _nearest_subquery_alias(select)
        # (We keep output schema the same; if you want, you can append SUBQUERY_LAYER to remarks.)

        # 1) SELECT list
        for col_text, col_alias, col_node in extract_select_list(select):
            # STAR
            if isinstance(col_node, exp.Star):
                results.extend(resolve_star(select, col_node, from_scope, regulation, metadatakey, view_name))
                continue

            # Direct column
            if isinstance(col_node, exp.Column):
                qualifier = col_node.table
                column_name = col_node.name
                db = ""
                table = ""
                table_alias = ""
                remarks = []
                if qualifier and qualifier in from_scope:
                    _, db, table, table_alias = from_scope[qualifier]
                    remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"])
                elif len(from_scope) == 1:
                    _, db, table, table_alias = next(iter(from_scope.values()))
                    remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"])
                else:
                    remarks.append(REMARKS["TABLE_AMBIGUOUS"])
                # Optionally tag subquery layer (commented to keep remarks tight)
                # if enclosing_alias: remarks.append(REMARKS["SUBQUERY_LAYER"])
                results.append({
                    "Database Name": db or "",
                    "Table Name": table or "",
                    "Table Alias Name": table_alias or "",
                    "Column Name": column_name,
                    "Alias Name": col_alias or "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": remarks,
                })
                continue

            # Derived / CASE expressions
            derived_columns = extract_columns_from_expression(col_node)
            if derived_columns:
                for dcol in derived_columns:
                    qualifier = dcol.table
                    column_name = dcol.name
                    db = ""
                    table = ""
                    table_alias = ""
                    if qualifier and qualifier in from_scope:
                        _, db, table, table_alias = from_scope[qualifier]
                    elif len(from_scope) == 1:
                        _, db, table, table_alias = next(iter(from_scope.values()))
                    remarks = [REMARKS["DERIVED_EXPR"]]
                    if isinstance(col_node, exp.Case):
                        remarks.append(REMARKS["CASE_EXPR"])
                    # if enclosing_alias: remarks.append(REMARKS["SUBQUERY_LAYER"])
                    results.append({
                        "Database Name": db or "",
                        "Table Name": table or "",
                        "Table Alias Name": table_alias or "",
                        "Column Name": column_name,
                        "Alias Name": col_alias or "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": remarks,
                    })
            else:
                if is_function_only_expression(col_node):
                    results.append({
                        "Database Name": "",
                        "Table Name": "",
                        "Table Alias Name": "",
                        "Column Name": col_node.sql(), 
                        "Alias Name": col_alias or "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": [
                            REMARKS["DERIVED_EXPR"],
                            REMARKS.get("FUNCTION_EXPR"),
                        ],
                    })
                else:
                    # Fallback safety (keeps old behavior)
                    results.append({
                        "Database Name": "",
                        "Table Name": "",
                        "Table Alias Name": "",
                        "Column Name": col_text,
                        "Alias Name": col_alias or "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": [REMARKS["DERIVED_EXPR"]],
                    })

        # 2) WHERE / GROUP BY / HAVING lineage
        def process_clause(expr_node, remark_key):
            if not expr_node:
                return
            for c in extract_columns_from_expression(expr_node):
                _emit_column_lineage(
                    results,
                    qualifier=c.table,
                    column_name=c.name,
                    from_scope=from_scope,
                    regulation=regulation,
                    metadatakey=metadatakey,
                    view_name=view_name,
                    remark_list=[remark_key],
                )
        process_clause(select.args.get("where"), REMARKS["WHERE_COLUMN"])
        if select.args.get("group"):
            for g in select.args["group"].expressions:
                process_clause(g, REMARKS["GROUP_BY_COLUMN"])
        process_clause(select.args.get("having"), REMARKS["HAVING_COLUMN"])

        # 3) JOIN lineage (inner select may carry joins even when the outer FROM is a subquery)
        joins = select.args.get("joins") or []
        for j in joins:
            # Join type
            kind = j.args.get("kind") or "INNER"
            join_type_tag = f"{REMARKS['JOIN_TYPE']}={str(kind).upper()}"

            # ON clause and right-side node alias (table or subquery alias)
            on_expr = j.args.get("on")
            right_node = j.this
            right_alias = None
            if isinstance(right_node, (exp.Table, exp.Subquery, exp.Alias)):
                right_alias = getattr(right_node, "alias_or_name", None)
                if not right_alias and isinstance(right_node, exp.Table):
                    right_alias = safe_name(right_node.this)

            # 3a) ON columns
            if on_expr:
                for c in extract_columns_from_expression(on_expr):
                    _emit_column_lineage(
                        results,
                        qualifier=c.table,
                        column_name=c.name,
                        from_scope=from_scope,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                        fallback_alias=right_alias,  # use JOIN alias if column is unqualified
                    )

                # 3b) Equality pairs a.col = b.col
                for eq in on_expr.find_all(exp.EQ):
                    left = getattr(eq, "this", None)
                    right = getattr(eq, "expression", None)
                    left_sql = left.sql() if isinstance(left, exp.Expression) else str(left)
                    right_sql = right.sql() if isinstance(right, exp.Expression) else str(right)
                    # Per-side lineage
                    if isinstance(left, exp.Column):
                        _emit_column_lineage(
                            results,
                            qualifier=left.table,
                            column_name=left.name,
                            from_scope=from_scope,
                            regulation=regulation,
                            metadatakey=metadatakey,
                            view_name=view_name,
                            remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                            fallback_alias=right_alias,
                        )
                    if isinstance(right, exp.Column):
                        _emit_column_lineage(
                            results,
                            qualifier=right.table,
                            column_name=right.name,
                            from_scope=from_scope,
                            regulation=regulation,
                            metadatakey=metadatakey,
                            view_name=view_name,
                            remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                            fallback_alias=right_alias,
                        )

            # 3c) NEW: subquery WHERE inside JOIN (e.g., ROW_NUM)
            if isinstance(right_node, exp.Subquery):
                # The subquery usually wraps a Select in .this
                sub_select = right_node.this if isinstance(right_node.this, exp.Select) else right_node.find(exp.Select)
                if sub_select:
                    sub_where = sub_select.args.get("where")
                    if sub_where:
                        # Resolve base table of the subquery for accurate Table Name
                        sub_db, sub_base_table = _pick_base_table_from_subquery(right_node)
                        for c in extract_columns_from_expression(sub_where):
                            # Prefer explicit alias/table if column is unqualified
                            _emit_column_lineage(
                                results,
                                qualifier=c.table,                  # may be None
                                column_name=c.name,                 # e.g., ROW_NUM
                                from_scope=from_scope,
                                regulation=regulation,
                                metadatakey=metadatakey,
                                view_name=view_name,
                                remark_list=[REMARKS["JOIN_SUBQUERY_WHERE_COLUMN"], join_type_tag],
                                fallback_alias=right_alias,         # e.g., PREVIOUS_RECORD
                                explicit_table_name=sub_base_table, # e.g., APP_REGHUB_RHOO_OTC_TSR_ESMA_TS_FACT_DATA
                                explicit_table_alias=right_alias,   # e.g., PREVIOUS_RECORD
                            )

    # 4) Normalize output rows
    normalized: List[Dict] = []
    for r in results:
        row = {}
        for k in OUTPUT_KEYS:
            if k == "Remarks":
                row[k] = ensure_list(r.get(k))
            else:
                row[k] = str(r.get(k, "")) if r.get(k) is not None else ""
        normalized.append(row)
    return normalized


def deduplicate_records(records: list[dict]) -> list[dict]:
    seen = set()
    unique = []

    for record in records:
        # 1️⃣ Drop junk STAR rows completely
        if (
            record.get("Database Name", "") == ""
            and record.get("Table Name", "") == ""
            and record.get("Table Alias Name", "") == ""
            and record.get("Column Name") == "*"
            and record.get("Alias Name", "") == ""
        ):
            continue  

        # 2️⃣ Convert dict → immutable canonical form for deduplication
        key = tuple(
            (k, tuple(v) if isinstance(v, list) else v)
            for k, v in sorted(record.items())
        )

        if key not in seen:
            seen.add(key)
            unique.append(record)

    return unique

# -----------------------
# Public API
# -----------------------
def parse_metadata_and_extract_lineage(
    metadata_json_str: str,
    regulation: str = "",
    metadatakey: str = "",
    view_name: str = "",
    sql_key: str = "sql_query",
) -> List[Dict]:
    sql = decode_base64_sql_from_metadata(metadata_json_str, sql_key)
    results = extract_lineage_rows(sql, regulation, metadatakey, view_name)
    return deduplicate_records(results)
    