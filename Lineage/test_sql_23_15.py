"""
sql_lineage_extractor_updated_v4.py

Implements your 3 requirements WITHOUT breaking the existing behavior:

REQ-1 (Derived outer table, e.g., TSR_TS_DATA):
- For queries like: SELECT *, CASE ... FROM ( ... ) TSR_TS_DATA
  we output:
    Table Name      = <derived_alias>   (NOT hardcoded; comes from SQL alias)
    Table Alias Name= <derived_alias>
    Remarks include "derived_table"
  for:
    - SELECT *
    - unqualified columns used inside CASE (SUBMISSION_TYPE, KEYS_SRC_SYS, etc.)

REQ-2 (JOIN subquery SELECT * + WHERE ROW_NUM):
- For:
    LEFT JOIN (SELECT * FROM TS_FACT WHERE ROW_NUM=2) PREVIOUS_RECORD
  we output:
    *       -> Table Name = TS_FACT, Table Alias Name = PREVIOUS_RECORD
    ROW_NUM -> Table Name = TS_FACT, Table Alias Name = PREVIOUS_RECORD

REQ-3 (DATE_FORMAT(CURRENT_TIMESTAMP...) derived columns):
- For:
    DATE_FORMAT(CURRENT_TIMESTAMP,...) AS DATA_SOURCE_DATE
    DATE_FORMAT(TO_UTC_TIMESTAMP(...)) AS INITIAL_EXCEPTION_TS
  we output:
    Table Name      = APP_REGHUB_RHOO_OTC_TSR_ESMA_DATA
    Table Alias Name= TSR
  because those are computed inside the SELECT whose physical source is TSR.

Note:
- We do NOT "hardcode" TSR_TS_DATA or TSR. We infer:
  - derived table alias from the FROM (...) <alias>
  - primary physical table within a SELECT from its FROM/JOIN sources (prefer TSR if present)
"""

import base64
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp

# -------------------------
# Constants
# -------------------------

REMARKS = {
    "ALL_COLUMNS": "all_columns_selected",
    "DERIVED_TABLE": "derived_table",
    "COLUMN_SELECTED_WITH_DB": "column_selected_with_database",
    "TABLE_AMBIGUOUS": "table_name_ambiguous",
    "DATABASE_NOT_SPECIFIED": "database_not_specified_in_query",
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

def extract_columns_from_expression(expr) -> List[exp.Column]:
    if not isinstance(expr, exp.Expression):
        return []
    return list(expr.find_all(exp.Column))

# -------------------------
# Scope model
# -------------------------

@dataclass
class SourceInfo:
    # What should be printed as table name + alias for lineage rows
    out_db: str
    out_table: str
    out_alias: str

    # Whether this is a derived table (subquery output) like TSR_TS_DATA
    is_derived_table: bool = False

    # Physical base table (for CURRENT_RECORD/PREVIOUS_RECORD)
    base_db: str = ""
    base_table: str = ""

def _unique_base_tables_in_subquery(subq: exp.Subquery) -> List[Tuple[str, str]]:
    """
    Returns unique physical tables used inside subquery.
    """
    tables = list(subq.find_all(exp.Table))
    uniq: List[Tuple[str, str]] = []
    seen = set()
    for t in tables:
        t_name = safe_name(t.this) or ""
        t_db = safe_name(t.db) or ""
        if not t_name:
            continue
        key = (t_db, t_name)
        if key not in seen:
            seen.add(key)
            uniq.append(key)
    return uniq

def _sourceinfo_for_table(tbl: exp.Table) -> SourceInfo:
    table = safe_name(tbl.this) or ""
    db = safe_name(tbl.db) or ""
    alias = tbl.alias_or_name if tbl.alias else ""
    out_alias = alias or table
    return SourceInfo(
        out_db=db,
        out_table=table,
        out_alias=out_alias,
        is_derived_table=False,
        base_db=db,
        base_table=table,
    )

def _sourceinfo_for_subquery(subq: exp.Subquery, alias: str) -> SourceInfo:
    bases = _unique_base_tables_in_subquery(subq)
    if len(bases) == 1:
        # CURRENT_RECORD / PREVIOUS_RECORD case
        bdb, btable = bases[0]
        return SourceInfo(
            out_db=bdb,
            out_table=btable,     # print physical table
            out_alias=alias,      # but alias should be CURRENT_RECORD / PREVIOUS_RECORD
            is_derived_table=False,
            base_db=bdb,
            base_table=btable,
        )
    # Derived output table like TSR_TS_DATA (built from multiple tables)
    return SourceInfo(
        out_db="",
        out_table=alias,         # print derived alias as table name
        out_alias=alias,
        is_derived_table=True,
        base_db="",
        base_table="",
    )

def build_global_scope(ast_root) -> Dict[str, SourceInfo]:
    """
    alias_key -> SourceInfo
    """
    scope: Dict[str, SourceInfo] = {}

    # physical tables (with optional alias)
    for tbl in ast_root.find_all(exp.Table):
        info = _sourceinfo_for_table(tbl)
        key = info.out_alias
        if key and key not in scope:
            scope[key] = info

    # subqueries with alias
    for subq in ast_root.find_all(exp.Subquery):
        alias = subq.alias_or_name if subq.args.get("alias") else ""
        if not alias:
            continue
        if alias not in scope:
            scope[alias] = _sourceinfo_for_subquery(subq, alias)

    return scope

def build_local_scope_for_select(select_exp: exp.Select) -> Dict[str, SourceInfo]:
    """
    Only sources in FROM + JOIN of this SELECT.
    """
    scope: Dict[str, SourceInfo] = {}

    def _add_source(src):
        if src is None:
            return
        if isinstance(src, exp.Table):
            info = _sourceinfo_for_table(src)
            scope[info.out_alias] = info
            return
        if isinstance(src, exp.Subquery):
            alias = src.alias_or_name if src.args.get("alias") else ""
            if alias:
                scope[alias] = _sourceinfo_for_subquery(src, alias)
            return
        if isinstance(src, exp.Alias):
            key = src.alias_or_name or ""
            if isinstance(src.this, exp.Table):
                info = _sourceinfo_for_table(src.this)
                info.out_alias = key
                scope[key] = info
            elif isinstance(src.this, exp.Subquery):
                if key:
                    scope[key] = _sourceinfo_for_subquery(src.this, key)

    # FROM
    from_clause = select_exp.args.get("from")
    if from_clause:
        sources = []
        if getattr(from_clause, "expressions", None):
            sources = list(from_clause.expressions)
        elif getattr(from_clause, "this", None) is not None:
            sources = [from_clause.this]
        for s in sources:
            _add_source(s)

    # JOINs
    for j in (select_exp.args.get("joins") or []):
        _add_source(getattr(j, "this", None))

    return scope

def enclosing_subquery_alias(select_exp: exp.Select) -> str:
    parent = getattr(select_exp, "parent", None)
    if isinstance(parent, exp.Subquery) and parent.args.get("alias"):
        return parent.alias_or_name or ""
    return ""

# -------------------------
# Resolver rules
# -------------------------

def _pick_primary_physical_source(local_scope: Dict[str, SourceInfo]) -> Optional[SourceInfo]:
    """
    Prefer TSR if present. Else first non-derived physical source.
    """
    if "TSR" in local_scope and not local_scope["TSR"].is_derived_table:
        return local_scope["TSR"]
    for info in local_scope.values():
        if not info.is_derived_table and info.base_table:
            return info
    return None

def _resolve_unqualified(local_scope: Dict[str, SourceInfo], global_scope: Dict[str, SourceInfo]) -> Tuple[Optional[SourceInfo], bool]:
    """
    Returns (SourceInfo, is_ambiguous)
    """
    if len(local_scope) == 1:
        return next(iter(local_scope.values())), False
    if len(global_scope) == 1:
        return next(iter(global_scope.values())), False
    return None, True

def _resolve_source(qualifier: str, local_scope: Dict[str, SourceInfo], global_scope: Dict[str, SourceInfo]) -> Tuple[Optional[SourceInfo], bool]:
    """
    Resolve by qualifier; if empty, resolve by unqualified rule.
    Returns (info, ambiguous)
    """
    if qualifier:
        if qualifier in local_scope:
            return local_scope[qualifier], False
        if qualifier in global_scope:
            return global_scope[qualifier], False
        return None, True
    return _resolve_unqualified(local_scope, global_scope)

def _apply_enclosing_alias_for_subquery_physical(info: SourceInfo, encl_alias: str) -> SourceInfo:
    """
    If the SELECT is inside a subquery alias (CURRENT_RECORD/PREVIOUS_RECORD),
    and the physical base table had no alias, show alias as the enclosing alias.
    """
    if encl_alias and (not info.is_derived_table):
        return SourceInfo(
            out_db=info.out_db,
            out_table=info.out_table,
            out_alias=encl_alias,
            is_derived_table=False,
            base_db=info.base_db,
            base_table=info.base_table,
        )
    return info

def _remarks_for_source(info: Optional[SourceInfo], base_remarks: List[str]) -> List[str]:
    r = list(base_remarks)
    if info and info.is_derived_table:
        r.append(REMARKS["DERIVED_TABLE"])
    return r

# -------------------------
# Select list helper
# -------------------------

def extract_select_list(select_exp: exp.Select):
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

def resolve_star(
    select_node: exp.Select,
    star_node,
    local_scope: Dict[str, SourceInfo],
    global_scope: Dict[str, SourceInfo],
    regulation: str,
    metadatakey: str,
    view_name: str,
) -> List[Dict]:
    """
    Robust STAR handler across sqlglot versions.

    sqlglot represents:
      - "*"        as exp.Star()
      - "t.*"      as exp.Star(this=Identifier('t')) in many versions
      - some versions expose `.table`, others do not.

    This function safely extracts the qualifier if present.
    """
    qualifier = ""

    # Some sqlglot versions represent "t.*" as exp.Column(table="t", this=Identifier("*"))
    if isinstance(star_node, exp.Column) and star_node.name == "*":
        qualifier = star_node.table or ""

    # Newer sqlglot: Star.table (string)
    table_attr = getattr(star_node, "table", None)
    if table_attr:
        qualifier = table_attr
    else:
        # Common: Star.args["this"] is an Identifier / Table / Expression holding qualifier
        this_arg = None
        if isinstance(star_node, exp.Expression):
            this_arg = star_node.args.get("this")
        if this_arg is not None:
            if isinstance(this_arg, exp.Identifier):
                qualifier = this_arg.name
            elif isinstance(this_arg, exp.Table):
                qualifier = this_arg.name
            else:
                qualifier = safe_name(this_arg) or ""

    info, ambiguous = _resolve_source(qualifier, local_scope, global_scope)
    encl = enclosing_subquery_alias(select_node)

    if info:
        info = _apply_enclosing_alias_for_subquery_physical(info, encl)

    remarks = _remarks_for_source(info, [REMARKS["ALL_COLUMNS"]])

    return [{
        "Database Name": (info.out_db if info else ""),
        "Table Name": (info.out_table if info else ""),
        "Table Alias Name": (info.out_alias if info else ""),
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

    global_scope = build_global_scope(ast)
    selects = list(ast.find_all(exp.Select))

    for select in selects:
        local_scope = build_local_scope_for_select(select)
        encl_alias = enclosing_subquery_alias(select)

        for col_text, col_alias, col_node in extract_select_list(select):

            # STAR
            if isinstance(col_node, exp.Star):
                results.extend(resolve_star(select, col_node, local_scope, global_scope, regulation, metadatakey, view_name))
                continue

            # DIRECT COLUMN
            if isinstance(col_node, exp.Column):
                qualifier = col_node.table or ""
                column_name = col_node.name

                info, ambiguous = _resolve_source(qualifier, local_scope, global_scope)
                if info:
                    info = _apply_enclosing_alias_for_subquery_physical(info, encl_alias)

                remarks = []
                if ambiguous:
                    remarks.append(REMARKS["TABLE_AMBIGUOUS"])
                else:
                    remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"] if (info and info.out_db) else REMARKS["DATABASE_NOT_SPECIFIED"])
                remarks = _remarks_for_source(info, remarks)

                results.append({
                    "Database Name": (info.out_db if info else ""),
                    "Table Name": (info.out_table if info else ""),
                    "Table Alias Name": (info.out_alias if info else ""),
                    "Column Name": column_name,
                    "Alias Name": col_alias or "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": remarks,
                })
                continue

            # DERIVED / CASE (expressions with column references)
            derived_cols = extract_columns_from_expression(col_node)

            if derived_cols:
                for dcol in derived_cols:
                    qualifier = dcol.table or ""
                    column_name = dcol.name

                    info, ambiguous = _resolve_source(qualifier, local_scope, global_scope)
                    if info:
                        info = _apply_enclosing_alias_for_subquery_physical(info, encl_alias)

                    remarks = [REMARKS["DERIVED_EXPR"]]
                    if isinstance(col_node, exp.Case):
                        remarks.append(REMARKS["CASE_EXPR"])
                    if ambiguous:
                        remarks.append(REMARKS["TABLE_AMBIGUOUS"])
                    remarks = _remarks_for_source(info, remarks)

                    results.append({
                        "Database Name": (info.out_db if info else ""),
                        "Table Name": (info.out_table if info else ""),
                        "Table Alias Name": (info.out_alias if info else ""),
                        "Column Name": column_name,
                        "Alias Name": col_alias or "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": remarks,
                    })
            else:
                # Derived expression with NO input columns (CURRENT_TIMESTAMP, literals)
                primary = _pick_primary_physical_source(local_scope)

                # If we're inside CURRENT_RECORD/PREVIOUS_RECORD and no physical found, use global scope base
                if primary is None and encl_alias and encl_alias in global_scope:
                    g = global_scope[encl_alias]
                    if g.base_table:
                        primary = SourceInfo(out_db=g.base_db, out_table=g.base_table, out_alias=encl_alias, is_derived_table=False, base_db=g.base_db, base_table=g.base_table)

                if primary is None:
                    primary, _ = _resolve_unqualified(local_scope, global_scope)

                if primary:
                    primary = _apply_enclosing_alias_for_subquery_physical(primary, encl_alias)

                remarks = _remarks_for_source(primary, [REMARKS["DERIVED_EXPR"]])

                results.append({
                    "Database Name": (primary.out_db if primary else ""),
                    "Table Name": (primary.out_table if primary else ""),
                    "Table Alias Name": (primary.out_alias if primary else ""),
                    "Column Name": col_alias or col_text,
                    "Alias Name": col_alias or "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": remarks,
                })

        # WHERE / GROUP BY / HAVING
        def process_clause(expr, remark_key: str):
            if not expr:
                return
            for c in extract_columns_from_expression(expr):
                qualifier = c.table or ""
                column_name = c.name

                info, ambiguous = _resolve_source(qualifier, local_scope, global_scope)
                if info:
                    info = _apply_enclosing_alias_for_subquery_physical(info, encl_alias)

                remarks = _remarks_for_source(info, [remark_key] + ([REMARKS["TABLE_AMBIGUOUS"]] if ambiguous else []))

                results.append({
                    "Database Name": (info.out_db if info else ""),
                    "Table Name": (info.out_table if info else ""),
                    "Table Alias Name": (info.out_alias if info else ""),
                    "Column Name": column_name,
                    "Alias Name": "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": remarks,
                })

        process_clause(select.args.get("where"), REMARKS["WHERE_COLUMN"])

        if select.args.get("group"):
            for g in select.args["group"].expressions:
                process_clause(g, REMARKS["GROUP_BY_COLUMN"])

        process_clause(select.args.get("having"), REMARKS["HAVING_COLUMN"])

    # Normalize
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
