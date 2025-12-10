"""
sql_lineage_parser_glot.py

Goal:
- Parse decoded SQL (from base64 sql_query in metadata JSON) and return lineage records
  with EXACT field names:

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

Key Fix:
- Handles SELECT * FROM ( SELECT ... ) alias  --> dives into subquery to extract columns.
- Produces:
  - all_columns_selected rows for STAR
  - Inner Query Alias Layer rows when STAR points to a subquery alias
  - Subquery Layer rows for columns extracted inside the subquery
  - database_not_specified_in_query when table has no db/schema prefix
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Iterable, Set

import re

try:
    import sqlglot
    from sqlglot import exp
except Exception as e:
    raise ImportError(
        "sqlglot is required. Install with: pip install sqlglot"
    ) from e


# -----------------------------
# Output row builder (EXACT KEYS)
# -----------------------------
def _make_row(
    db: str,
    table: str,
    column: str,
    alias: str,
    regulation: str,
    metadatakey: str,
    view_name: str,
    remarks: str,
) -> Dict[str, str]:
    return {
        "Database Name": db or "",
        "Table Name": table or "",
        "Column Name": column or "",
        "Alias Name": alias or "",
        "Regulation": regulation or "",
        "Metadatakey": metadatakey or "",
        "View Name": view_name or "",
        "Remarks": remarks or "",
    }


def _clean_sql(sql: str) -> str:
    """
    Keep this conservative: only normalize whitespace.
    We do NOT aggressively rewrite because your SQL includes Spark/Hive functions and placeholders.
    """
    if not sql:
        return ""
    # Remove null bytes, normalize newlines
    sql = sql.replace("\x00", " ").replace("\r\n", "\n").replace("\r", "\n")
    return sql.strip()


def _split_qualified_table(full_name: str) -> Tuple[str, str]:
    """
    Given something like:
      GFOLYREG_WORK.APP_REGHUB_RHOO_LIVETRADE_FACT_DATA_OTC_CURRENT_BATCH
    return:
      ("GFOLYREG_WORK", "APP_REGHUB_RHOO_LIVETRADE_FACT_DATA_OTC_CURRENT_BATCH")

    If more than 2 parts exist, we join all but last as "Database Name".
    If only 1 part exists, db is "N/A" (unknown / not specified).
    """
    parts = [p for p in full_name.split(".") if p]
    if len(parts) >= 2:
        return (".".join(parts[:-1]), parts[-1])
    return ("N/A", parts[0] if parts else "")


@dataclass
class SourceRef:
    """
    A FROM/JOIN source:
      - physical table: db/table filled, subquery_select None
      - subquery: subquery_select is exp.Select
    """
    db: str
    table: str
    subquery_select: Optional[exp.Select] = None


def _get_alias_name(node: exp.Expression) -> str:
    """
    For a projection expression, return alias if present, else "".
    Example: CURRENT_RECORD.ID AS REGHUB_ID -> "REGHUB_ID"
    """
    try:
        a = node.alias
        return a if a else ""
    except Exception:
        return ""


def _build_sources(select: exp.Select) -> Dict[str, SourceRef]:
    """
    Build alias -> SourceRef from FROM + JOIN.
    """
    sources: Dict[str, SourceRef] = {}

    def register_source(src_expr: exp.Expression) -> None:
        # Physical table
        if isinstance(src_expr, exp.Table):
            tbl_name = src_expr.name  # last part
            db_name = (src_expr.args.get("db") or "")  # may be None
            cat_name = (src_expr.args.get("catalog") or "")

            # If catalog exists, join it in db
            if cat_name and db_name:
                db = f"{cat_name}.{db_name}"
            elif cat_name and not db_name:
                db = cat_name
            else:
                db = db_name or "N/A"

            alias = (src_expr.alias or "").strip()
            if not alias:
                alias = tbl_name

            sources[alias] = SourceRef(db=db, table=tbl_name, subquery_select=None)
            return

        # Subquery
        if isinstance(src_expr, exp.Subquery):
            alias = (src_expr.alias or "").strip()
            inner = src_expr.this
            inner_select = inner if isinstance(inner, exp.Select) else inner.find(exp.Select)

            if not alias:
                alias = "SUBQUERY"

            sources[alias] = SourceRef(db="", table="", subquery_select=inner_select if isinstance(inner_select, exp.Select) else None)
            return

        # Sometimes FROM can be an Identifier or other expression; ignore safely
        return

    # FROM sources
    frm = select.args.get("from")
    if frm is not None:
        for e in getattr(frm, "expressions", []) or []:
            register_source(e)

    # JOIN sources
    joins = select.args.get("joins") or []
    for j in joins:
        right = j.this
        if right is not None:
            register_source(right)

    return sources


def _iter_projection_columns(proj: exp.Expression) -> Iterable[exp.Column]:
    """
    Yield all column references inside a projection expression.
    """
    for c in proj.find_all(exp.Column):
        yield c


def _is_star_projection(proj: exp.Expression) -> bool:
    return isinstance(proj, exp.Star) or (hasattr(exp, "Star") and isinstance(proj, exp.Star))


def _star_table_qualifier(star: exp.Star) -> str:
    """
    exp.Star can represent:
      *        -> no qualifier
      t.*      -> qualifier is Identifier("t")
    """
    t = star.args.get("this")
    if t is None:
        return ""
    # could be Identifier or Table
    if isinstance(t, exp.Identifier):
        return t.name
    if isinstance(t, exp.Table):
        return t.name
    return str(t)


def _extract_from_select(
    select: exp.Select,
    regulation: str,
    metadatakey: str,
    view_name: str,
    scope_remark: str = "",
    parent_star_alias: str = "",
    seen: Optional[Set[Tuple[str, str, str, str, str]]] = None,
) -> List[Dict[str, str]]:
    """
    Extract lineage rows from a Select.

    scope_remark:
      - "" for top-level / normal
      - "Subquery Layer" when we are diving inside a subquery due to SELECT *
    parent_star_alias:
      - if outer query does SELECT alias.* , we pass alias.* label down for child rows
    """
    if seen is None:
        seen = set()

    rows: List[Dict[str, str]] = []

    sources = _build_sources(select)
    physical_sources = {k: v for k, v in sources.items() if v.subquery_select is None and v.table}
    subquery_sources = {k: v for k, v in sources.items() if v.subquery_select is not None}

    projections = list(select.expressions or [])

    for proj in projections:
        alias_for_expr = _get_alias_name(proj)

        # -----------------
        # STAR handling
        # -----------------
        if isinstance(proj, exp.Star):
            star_qual = _star_table_qualifier(proj)  # "" or alias/table
            star_label = f"{star_qual}.*" if star_qual else "*"

            # If parent_star_alias is provided (outer alias.*), keep it consistent for inner outputs
            effective_star_label = parent_star_alias or star_label

            # If star is qualified and points to a physical table alias
            if star_qual and star_qual in physical_sources:
                src = physical_sources[star_qual]
                remark = scope_remark or "all_columns_selected"
                key = (src.db, src.table, "*", effective_star_label, remark)
                if key not in seen:
                    seen.add(key)
                    rows.append(_make_row(
                        db=src.db,
                        table=src.table,
                        column="*",
                        alias=effective_star_label,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        remarks=remark,
                    ))
                continue

            # If star points to a subquery alias => produce "all_columns_selected" + "Inner Query Alias Layer"
            if star_qual and star_qual in subquery_sources:
                # 1) all_columns_selected row (db/table unknown because it's derived)
                key1 = ("N/A", "", "*", effective_star_label, "all_columns_selected")
                if key1 not in seen:
                    seen.add(key1)
                    rows.append(_make_row(
                        db="N/A",
                        table=star_qual,
                        column="*",
                        alias=effective_star_label,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        remarks="all_columns_selected",
                    ))

                # 2) Inner Query Alias Layer row (blank db/table like your screenshot)
                key2 = ("", "", "*", effective_star_label, "Inner Query Alias Layer")
                if key2 not in seen:
                    seen.add(key2)
                    rows.append(_make_row(
                        db="",
                        table="",
                        column="*",
                        alias=effective_star_label,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        remarks="Inner Query Alias Layer",
                    ))

                # 3) Dive into subquery and extract its select columns as "Subquery Layer"
                inner_sel = subquery_sources[star_qual].subquery_select
                if inner_sel is not None:
                    rows.extend(_extract_from_select(
                        inner_sel,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        scope_remark="Subquery Layer",
                        parent_star_alias=effective_star_label,
                        seen=seen,
                    ))
                continue

            # Unqualified star: if there is exactly one subquery source, dive into it
            if not star_qual and len(subquery_sources) == 1:
                only_alias, only_src = next(iter(subquery_sources.items()))
                # Emit alias-layer row to match your pattern
                effective_star_label = parent_star_alias or f"{only_alias}.*"
                key2 = ("", "", "*", effective_star_label, "Inner Query Alias Layer")
                if key2 not in seen:
                    seen.add(key2)
                    rows.append(_make_row(
                        db="",
                        table="",
                        column="*",
                        alias=effective_star_label,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        remarks="Inner Query Alias Layer",
                    ))

                if only_src.subquery_select is not None:
                    rows.extend(_extract_from_select(
                        only_src.subquery_select,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        scope_remark="Subquery Layer",
                        parent_star_alias=effective_star_label,
                        seen=seen,
                    ))
                continue

            # Unqualified star but only physical sources => emit all_columns_selected for each
            if not star_qual and physical_sources:
                for a, src in physical_sources.items():
                    effective_star_label = parent_star_alias or f"{a}.*"
                    remark = scope_remark or "all_columns_selected"
                    key = (src.db, src.table, "*", effective_star_label, remark)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(_make_row(
                        db=src.db,
                        table=src.table,
                        column="*",
                        alias=effective_star_label,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        remarks=remark,
                    ))
            continue

        # -----------------
        # Non-STAR projections
        # -----------------
        cols = list(_iter_projection_columns(proj))
        if not cols:
            continue

        for c in cols:
            col_name = c.name
            tbl_qual = c.table  # alias or table name or ""

            # Default alias name for output:
            # - If projection has "AS something" use that (REGHUB_ID style)
            # - Else keep empty
            out_alias = alias_for_expr or ""

            # Case 1: tbl_qual matches a physical table alias in FROM/JOIN
            if tbl_qual and tbl_qual in physical_sources:
                src = physical_sources[tbl_qual]
                remark = scope_remark or ""
                key = (src.db, src.table, col_name, out_alias, remark)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(_make_row(
                    db=src.db,
                    table=src.table,
                    column=col_name,
                    alias=out_alias,
                    regulation=regulation,
                    metadatakey=metadatakey,
                    view_name=view_name,
                    remarks=remark,
                ))
                continue

            # Case 2: tbl_qual matches a subquery alias => we can’t directly map here
            if tbl_qual and tbl_qual in subquery_sources:
                remark = scope_remark or "Inner Query Alias Layer"
                key = ("", "", col_name, out_alias, remark)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(_make_row(
                    db="",
                    table="",
                    column=col_name,
                    alias=out_alias,
                    regulation=regulation,
                    metadatakey=metadatakey,
                    view_name=view_name,
                    remarks=remark,
                ))
                continue

            # Case 3: tbl_qual exists but not in sources:
            # Example: CURRENT_RECORD.ID (no db specified in query)
            if tbl_qual:
                remark = scope_remark or "database_not_specified_in_query"
                key = ("N/A", tbl_qual, col_name, out_alias, remark)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(_make_row(
                    db="N/A",
                    table=tbl_qual,
                    column=col_name,
                    alias=out_alias,
                    regulation=regulation,
                    metadatakey=metadatakey,
                    view_name=view_name,
                    remarks=remark,
                ))
                continue

            # Case 4: No qualifier on column:
            # If exactly one physical table exists, attribute it; else mark unknown.
            if not tbl_qual and len(physical_sources) == 1:
                _, src = next(iter(physical_sources.items()))
                remark = scope_remark or ""
                key = (src.db, src.table, col_name, out_alias, remark)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(_make_row(
                    db=src.db,
                    table=src.table,
                    column=col_name,
                    alias=out_alias,
                    regulation=regulation,
                    metadatakey=metadatakey,
                    view_name=view_name,
                    remarks=remark,
                ))
            else:
                remark = scope_remark or "database_not_specified_in_query"
                key = ("N/A", "", col_name, out_alias, remark)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(_make_row(
                    db="N/A",
                    table="",
                    column=col_name,
                    alias=out_alias,
                    regulation=regulation,
                    metadatakey=metadatakey,
                    view_name=view_name,
                    remarks=remark,
                ))

    return rows


def parse_sql_lineage(
    sql: str,
    regulation: str,
    metadatakey: str,
    view_name: str,
    dialect: str = "spark",
) -> List[Dict[str, str]]:
    """
    Public API:
    - Input: decoded SQL
    - Output: list of dict rows with exact keys
    """
    sql = _clean_sql(sql)
    if not sql:
        return []

    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        # If SQLGlot fails, return empty rather than breaking the API
        return []

    # Find the "outermost" SELECT
    top_select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if top_select is None:
        return []

    return _extract_from_select(
        top_select,
        regulation=regulation,
        metadatakey=metadatakey,
        view_name=view_name,
        scope_remark="",
    )


if __name__ == "__main__":
    # Quick local test (no API needed)
    sample_sql = """
    SELECT * FROM (
      SELECT
        T.TRADE_UIT_ID,
        T.FIRM_PARTY_GFCID,
        CURRENT_RECORD.ID AS REGHUB_ID
      FROM GFOLYREG_WORK.APP_REGHUB_RHOO_LIVETRADE_FACT_DATA_OTC_CURRENT_BATCH T
      LEFT JOIN SOME_DB.SOME_TABLE X ON X.ID = T.TRADE_UIT_ID
    ) A
    """
    rows = parse_sql_lineage(
        sample_sql,
        regulation="rhoo",
        metadatakey="rhoo_livetrade_sql_query_source_olympus_new",
        view_name="APP_REGHUB_RHOO_LIVETRADE_FACT_DATA_OTC_CURRENT_BATCH_VIEW",
    )
    for r in rows[:20]:
        print(r)
