"""
sql_lineage_extractor_enhanced_v6.py

Goal
- Keep existing behavior (DB/table/alias/column/alias output + remarks list[str])
- Add missing lineage for:
  1) Outer derived tables (e.g., FROM (SELECT ...) TSR_TS_DATA) so '*' and CASE columns map to TSR_TS_DATA (derived)
  2) JOIN subquery stars and WHERE columns:
        LEFT JOIN (SELECT * FROM TS_FACT WHERE ROW_NUM=2) PREVIOUS_RECORD
     -> '*' and ROW_NUM should map to table TS_FACT with Table Alias Name = PREVIOUS_RECORD
  3) Derived expressions with NO column refs (DATE_FORMAT(CURRENT_TIMESTAMP...)) should map to primary physical table
     in that SELECT (e.g., APP_REGHUB...ESMA_DATA alias TSR) rather than the outer derived alias.

Notes
- No hardcoding of TSR_TS_DATA / TSR / CURRENT_RECORD / PREVIOUS_RECORD
- Works across sqlglot versions where exp.Star may or may not have `.table`
"""

import base64
import json
from typing import List, Dict, Tuple, Optional, Any

import sqlglot
from sqlglot import exp


# -------------------------
# Remarks (kept stable)
# -------------------------

REMARKS = {
    "ALL_COLUMNS": "all_columns_selected",
    "DB_NOT_SPECIFIED": "database_not_specified_in_query",
    "INNER_ALIAS": "Inner Query Alias Layer",
    "SUBQUERY_LAYER": "Subquery Layer",
    "DERIVED_EXPR": "derived_expression",

    # enhancements (non-breaking)
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

def safe_name(x: Any) -> str:
    """Return a clean identifier-like name for sqlglot objects / strings."""
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    # sqlglot Identifier / Table / Column etc
    if hasattr(x, "name") and isinstance(getattr(x, "name"), str):
        return getattr(x, "name")
    if hasattr(x, "this"):
        try:
            return safe_name(getattr(x, "this"))
        except Exception:
            pass
    return str(x)


def decode_base64_sql_from_metadata(metadata_json_str: str, sql_key: str = "sql_query") -> str:
    meta = json.loads(metadata_json_str)
    raw = meta.get(sql_key, "") or ""
    # Accept either plain SQL or base64 SQL
    try:
        decoded = base64.b64decode(raw).decode("utf-8", errors="replace")
        # heuristic: if decoded looks like SQL, use it
        if "select" in decoded.lower():
            return decoded
    except Exception:
        pass
    return raw


def _remarks_with_db_flag(db: str, base_remarks: List[str]) -> List[str]:
    out = list(base_remarks or [])
    if not db:
        out.append(REMARKS["DB_NOT_SPECIFIED"])
    # de-dupe while keeping order
    seen = set()
    deduped = []
    for r in out:
        if r not in seen:
            seen.add(r)
            deduped.append(r)
    return deduped


# -------------------------
# Source mapping helpers
# -------------------------

def _pick_base_table_from_subquery(subq: exp.Subquery) -> Tuple[str, str, bool]:
    """
    Returns (db, table_name, is_derived_table)

    - If the subquery reads from exactly 1 physical table -> return that table (derived=False)
    - If it reads from 0 or multiple tables -> return ("", subquery_alias, derived=True)
    """
    subq_alias = subq.alias_or_name or ""
    tables = list(subq.find_all(exp.Table))

    uniq = []
    seen = set()
    for t in tables:
        db = safe_name(t.db)
        name = safe_name(t.this)
        key = (db, name)
        if name and key not in seen:
            seen.add(key)
            uniq.append(key)

    if len(uniq) == 1:
        return uniq[0][0], uniq[0][1], False

    # multi-table or no-table: treat as derived output table
    return "", (subq_alias or "__DERIVED__"), True


def _iter_immediate_sources(select_node: exp.Select):
    """
    Yield immediate FROM / JOIN sources of this SELECT without descending into subquery internals.
    """
    frm = select_node.args.get("from")
    if frm is not None:
        # FROM can contain Table or Subquery
        for child in frm.find_all(exp.Table):
            yield child
        for child in frm.find_all(exp.Subquery):
            yield child

    for j in (select_node.args.get("joins") or []):
        # join.this points to table/subquery; join expressions also contain columns
        jsrc = getattr(j, "this", None)
        if jsrc is None:
            continue
        if isinstance(jsrc, exp.Table):
            yield jsrc
        elif isinstance(jsrc, exp.Subquery):
            yield jsrc


def build_global_from_scope_map(ast_root) -> Dict[str, Tuple[Any, str, str, str, bool]]:
    """
    key -> (node, db, table_name, table_alias, is_derived_table)

    Global map: includes all physical tables and subquery aliases found anywhere.
    """
    from_map: Dict[str, Tuple[Any, str, str, str, bool]] = {}

    # physical tables
    for tbl in ast_root.find_all(exp.Table):
        table_name = safe_name(tbl.this)
        db = safe_name(tbl.db)
        table_alias = tbl.alias_or_name if tbl.args.get("alias") else ""
        key = table_alias or table_name
        if key:
            from_map[key] = (tbl, db, table_name, table_alias, False)

    # subquery aliases (CURRENT_RECORD, PREVIOUS_RECORD, TSR_TS_DATA, etc.)
    for subq in ast_root.find_all(exp.Subquery):
        alias = subq.alias_or_name if subq.args.get("alias") else ""
        if not alias:
            continue
        db, base_table, is_derived = _pick_base_table_from_subquery(subq)
        from_map[alias] = (subq, db, base_table, alias, is_derived)

    return from_map


def build_local_from_scope_map(select_node: exp.Select) -> Dict[str, Tuple[Any, str, str, str, bool]]:
    """
    Local map ONLY for immediate sources of this SELECT (FROM/JOIN).
    Prevents outer SELECT from accidentally resolving to inner tables.
    """
    from_map: Dict[str, Tuple[Any, str, str, str, bool]] = {}

    for src in _iter_immediate_sources(select_node):
        if isinstance(src, exp.Table):
            table_name = safe_name(src.this)
            db = safe_name(src.db)
            table_alias = src.alias_or_name if src.args.get("alias") else ""
            key = table_alias or table_name
            if key:
                from_map[key] = (src, db, table_name, table_alias, False)

        elif isinstance(src, exp.Subquery):
            alias = src.alias_or_name if src.args.get("alias") else ""
            if not alias:
                continue
            db, base_table, is_derived = _pick_base_table_from_subquery(src)
            from_map[alias] = (src, db, base_table, alias, is_derived)

    return from_map


def _get_parent_subquery_alias(select_node: exp.Select) -> str:
    """
    If this SELECT is directly inside a Subquery alias, return that alias.
    Example: (SELECT * FROM TS_FACT WHERE ROW_NUM=2) PREVIOUS_RECORD
    """
    node = select_node
    # sqlglot nodes usually have .parent
    while node is not None and getattr(node, "parent", None) is not None:
        parent = node.parent
        if isinstance(parent, exp.Subquery):
            alias = parent.alias_or_name if parent.args.get("alias") else ""
            return alias or ""
        node = parent
    return ""


# -------------------------
# STAR helpers
# -------------------------

def _star_qualifier(star_node: exp.Star) -> str:
    """
    Return qualifier for alias.* if present, else "".
    Works across sqlglot versions.
    """
    # Newer sqlglot sometimes has .table
    q = getattr(star_node, "table", None)
    if q:
        return safe_name(q)

    args = getattr(star_node, "args", {}) or {}
    this = args.get("this")
    if this is None:
        return ""
    return safe_name(this)


def _choose_default_scope_key(scope: Dict[str, Tuple[Any, str, str, str, bool]]) -> str:
    """
    If scope has 1 source, use it. Otherwise unknown.
    """
    keys = list(scope.keys())
    if len(keys) == 1:
        return keys[0]
    return ""


def resolve_star(
    select_node: exp.Select,
    star_node: exp.Star,
    local_scope: Dict[str, Tuple[Any, str, str, str, bool]],
    global_scope: Dict[str, Tuple[Any, str, str, str, bool]],
    regulation: str,
    metadatakey: str,
    view_name: str,
) -> List[Dict]:
    qualifier = _star_qualifier(star_node)  # alias.* or ""

    # choose which scope to use
    lookup_scope = local_scope if local_scope else global_scope

    key = qualifier or _choose_default_scope_key(lookup_scope)
    entry = lookup_scope.get(key) if key else None

    db = table_name = table_alias = ""
    is_derived = False
    if entry:
        _, db, table_name, table_alias, is_derived = entry

    row = {
        "Database Name": db,
        "Table Name": table_name,
        "Table Alias Name": table_alias,
        "Column Name": "*",
        "Alias Name": "",
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": _remarks_with_db_flag(db, [REMARKS["ALL_COLUMNS"]] + ([REMARKS["DERIVED_TABLE"]] if is_derived else [])),
    }

    # If this star is inside a join-subquery alias (CURRENT_RECORD/PREVIOUS_RECORD) and base table has no alias,
    # override Table Alias Name to subquery alias (requirement)
    parent_subq_alias = _get_parent_subquery_alias(select_node)
    if parent_subq_alias and (not row.get("Table Alias Name")):
        row["Table Alias Name"] = parent_subq_alias

    return [row]


# -------------------------
# Column / expression extraction
# -------------------------

def _column_qualifier(col: exp.Column) -> str:
    # col.table is stable in most sqlglot versions
    q = getattr(col, "table", None)
    return safe_name(q)


def _collect_column_refs(node: exp.Expression) -> List[exp.Column]:
    return list(node.find_all(exp.Column))


def _pick_primary_physical_source(local_scope: Dict[str, Tuple[Any, str, str, str, bool]]) -> Optional[Tuple[str, str, str, bool]]:
    """
    Pick a physical (non-derived) source from local scope.
    Prefer the first physical table; otherwise None.
    """
    for _, (_, db, tname, talias, is_derived) in local_scope.items():
        if tname and (not is_derived):
            return db, tname, talias, is_derived
    return None


def _resolve_column_to_source(
    select_node: exp.Select,
    col: exp.Column,
    local_scope: Dict[str, Tuple[Any, str, str, str, bool]],
    global_scope: Dict[str, Tuple[Any, str, str, str, bool]],
) -> Tuple[str, str, str, bool]:
    """
    Returns (db, table_name, table_alias, is_derived_table) for the column ref.
    Uses local scope first (prevents outer select leaking to inner tables),
    then global scope fallback.
    """
    qualifier = _column_qualifier(col)  # may be ""
    if qualifier:
        if qualifier in local_scope:
            _, db, tn, ta, d = local_scope[qualifier]
            # If this SELECT belongs to a JOIN subquery alias and base table has no alias, use subquery alias
            parent_subq_alias = _get_parent_subquery_alias(select_node)
            if parent_subq_alias and not ta:
                ta = parent_subq_alias

            return db, tn, ta, d
        if qualifier in global_scope:
            _, db, tn, ta, d = global_scope[qualifier]
            # If this SELECT belongs to a JOIN subquery alias and base table has no alias, use subquery alias
            parent_subq_alias = _get_parent_subquery_alias(select_node)
            if parent_subq_alias and not ta:
                ta = parent_subq_alias

            return db, tn, ta, d

    # unqualified column -> if local scope is a single source, attribute to it
    default_key = _choose_default_scope_key(local_scope) or _choose_default_scope_key(global_scope)
    if default_key and default_key in local_scope:
        _, db, tn, ta, d = local_scope[default_key]
            # If this SELECT belongs to a JOIN subquery alias and base table has no alias, use subquery alias
            parent_subq_alias = _get_parent_subquery_alias(select_node)
            if parent_subq_alias and not ta:
                ta = parent_subq_alias

        return db, tn, ta, d
    if default_key and default_key in global_scope:
        _, db, tn, ta, d = global_scope[default_key]
            # If this SELECT belongs to a JOIN subquery alias and base table has no alias, use subquery alias
            parent_subq_alias = _get_parent_subquery_alias(select_node)
            if parent_subq_alias and not ta:
                ta = parent_subq_alias

        return db, tn, ta, d

    return "", "", "", False


def _make_row(
    db: str,
    table_name: str,
    table_alias: str,
    column_name: str,
    alias_name: str,
    regulation: str,
    metadatakey: str,
    view_name: str,
    remarks: List[str],
) -> Dict:
    return {
        "Database Name": db,
        "Table Name": table_name,
        "Table Alias Name": table_alias,
        "Column Name": column_name,
        "Alias Name": alias_name,
        "Regulation": regulation,
        "Metadatakey": metadatakey,
        "View Name": view_name,
        "Remarks": _remarks_with_db_flag(db, remarks),
    }


def _rows_for_expression(
    select_node: exp.Select,
    expr_node: exp.Expression,
    out_alias: str,
    local_scope: Dict[str, Tuple[Any, str, str, str, bool]],
    global_scope: Dict[str, Tuple[Any, str, str, str, bool]],
    regulation: str,
    metadatakey: str,
    view_name: str,
    extra_remarks: List[str],
) -> List[Dict]:
    """
    For an expression in SELECT list (could be Column / CASE / function / literal):
    - If it contains column refs -> output rows for those columns
    - If it contains NO column refs -> attribute to primary physical source in local scope (requirement for DATE_FORMAT)
    """
    cols = _collect_column_refs(expr_node)
    rows: List[Dict] = []

    if cols:
        for c in cols:
            db, tn, ta, is_derived = _resolve_column_to_source(select_node, c, local_scope, global_scope)
            rows.append(
                _make_row(
                    db=db,
                    table_name=tn,
                    table_alias=ta,
                    column_name=safe_name(c.this),
                    alias_name=out_alias or "",
                    regulation=regulation,
                    metadatakey=metadatakey,
                    view_name=view_name,
                    remarks=list(dict.fromkeys([REMARKS["DERIVED_EXPR"]] + extra_remarks + ([REMARKS["DERIVED_TABLE"]] if is_derived else []))),
                )
            )
        return rows

    # No column refs: attach to primary physical table in THIS select (or single source)
    primary = _pick_primary_physical_source(local_scope)
    if primary:
        db, tn, ta, is_derived = primary
    else:
        # fallback: single available scope (could be derived)
        key = _choose_default_scope_key(local_scope) or _choose_default_scope_key(global_scope)
        if key and key in local_scope:
            _, db, tn, ta, is_derived = local_scope[key]
        elif key and key in global_scope:
            _, db, tn, ta, is_derived = global_scope[key]
        else:
            db = tn = ta = ""
            is_derived = False

    rows.append(
        _make_row(
            db=db,
            table_name=tn,
            table_alias=ta,
            column_name=out_alias or safe_name(expr_node) or "",
            alias_name=out_alias or "",
            regulation=regulation,
            metadatakey=metadatakey,
            view_name=view_name,
            remarks=list(dict.fromkeys([REMARKS["DERIVED_EXPR"]] + extra_remarks + ([REMARKS["DERIVED_TABLE"]] if is_derived else []))),
        )
    )
    return rows


# -------------------------
# Select list helper
# -------------------------

def extract_select_list(select_exp: exp.Select):
    projections = []
    for proj in (select_exp.expressions or []):
        alias = None
        node = proj
        if isinstance(proj, exp.Alias):
            alias = proj.alias_or_name
            node = proj.this
        projections.append((str(node), alias, node))
    return projections


# -------------------------
# WHERE/GROUP/HAVING helpers
# -------------------------

def _rows_for_filter_clause(
    select_node: exp.Select,
    clause_node: Optional[exp.Expression],
    local_scope,
    global_scope,
    regulation,
    metadatakey,
    view_name,
    remark_key: str,
) -> List[Dict]:
    if clause_node is None:
        return []
    rows = []
    for c in clause_node.find_all(exp.Column):
        db, tn, ta, is_derived = _resolve_column_to_source(select_node, c, local_scope, global_scope)
        rows.append(
            _make_row(
                db=db,
                table_name=tn,
                table_alias=ta,
                column_name=safe_name(c.this),
                alias_name="",
                regulation=regulation,
                metadatakey=metadatakey,
                view_name=view_name,
                remarks=[remark_key] + ([REMARKS["DERIVED_TABLE"]] if is_derived else []),
            )
        )
    return rows


# -------------------------
# Core extraction
# -------------------------

def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    ast = sqlglot.parse_one(sql)
    results: List[Dict] = []

    global_scope = build_global_from_scope_map(ast)

    for select_node in ast.find_all(exp.Select):
        local_scope = build_local_from_scope_map(select_node)

        # 1) SELECT list
        for col_text, col_alias, col_node in extract_select_list(select_node):

            # STAR
            if isinstance(col_node, exp.Star):
                results.extend(resolve_star(select_node, col_node, local_scope, global_scope, regulation, metadatakey, view_name))
                continue

            # CASE expression (still handled by generic expression resolver, but add remark)
            if isinstance(col_node, exp.Case):
                results.extend(
                    _rows_for_expression(
                        select_node,
                        col_node,
                        out_alias=col_alias or "",
                        local_scope=local_scope,
                        global_scope=global_scope,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        extra_remarks=[REMARKS["CASE_EXPR"]],
                    )
                )
                continue

            # plain column reference
            if isinstance(col_node, exp.Column):
                db, tn, ta, is_derived = _resolve_column_to_source(select_node, col_node, local_scope, global_scope)
                results.append(
                    _make_row(
                        db=db,
                        table_name=tn,
                        table_alias=ta,
                        column_name=safe_name(col_node.this),
                        alias_name=col_alias or "",
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        remarks=[] + ([REMARKS["DERIVED_TABLE"]] if is_derived else []),
                    )
                )
                continue

            # other derived expressions (functions, literals, arithmetic, etc.)
            results.extend(
                _rows_for_expression(
                    select_node,
                    col_node,
                    out_alias=col_alias or "",
                    local_scope=local_scope,
                    global_scope=global_scope,
                    regulation=regulation,
                    metadatakey=metadatakey,
                    view_name=view_name,
                    extra_remarks=[],
                )
            )

        # 2) WHERE / GROUP BY / HAVING columns
        results.extend(
            _rows_for_filter_clause(
                select_node,
                select_node.args.get("where"),
                local_scope,
                global_scope,
                regulation,
                metadatakey,
                view_name,
                REMARKS["WHERE_COLUMN"],
            )
        )

        # GROUP BY can be list of expressions
        group = select_node.args.get("group")
        if group is not None:
            for gexp in group.expressions or []:
                results.extend(
                    _rows_for_filter_clause(
                        select_node,
                        gexp,
                        local_scope,
                        global_scope,
                        regulation,
                        metadatakey,
                        view_name,
                        REMARKS["GROUP_BY_COLUMN"],
                    )
                )

        results.extend(
            _rows_for_filter_clause(
                select_node,
                select_node.args.get("having"),
                local_scope,
                global_scope,
                regulation,
                metadatakey,
                view_name,
                REMARKS["HAVING_COLUMN"],
            )
        )

    return results


# -------------------------
# Public API (same signature)
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
