# ============================================================
#                LINEAGE ENGINE (SQL + MONGO)
# ============================================================

import sqlglot
from sqlglot import exp
from typing import Any, Dict, List, Optional, Set


# ============================================================
#                COMMON RECORD FORMAT
# ============================================================

def _make_row(db, table, column, alias, regulation, metadatakey, view_name, remarks):
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


def _safe(node):
    if node is None:
        return ""
    if hasattr(node, "name"):
        return node.name
    try:
        return node.sql()
    except:
        return str(node)


def _get_alias_name(node):
    try:
        if hasattr(node, "alias_or_name") and node.alias_or_name:
            return node.alias_or_name
        alias_expr = node.args.get("alias")
        if alias_expr:
            return _safe(alias_expr)
    except:
        pass
    return ""


# ============================================================
#                  SQL: UNWRAP TOP-LEVEL SELECT
# ============================================================

def unwrap_select(node):
    if isinstance(node, exp.Select):
        return node
    if isinstance(node, exp.Subquery):
        if isinstance(node.this, exp.Select):
            return node.this
        return unwrap_select(node.this)
    if isinstance(node, exp.Paren):
        return unwrap_select(node.this)
    return node.find(exp.Select)


# ============================================================
#      SQL: REGISTER TABLES, SUBQUERIES, CTE SOURCES
# ============================================================

def _register_sources_recursive(node: exp.Expression, alias_map: dict):
    # CTEs
    for cte in node.find_all(exp.CTE):
        alias = getattr(cte, "alias_or_name", None) or _safe(cte.args.get("alias"))
        inner = cte.this
        inner_sel = inner.this if isinstance(inner, exp.Subquery) else inner
        if not isinstance(inner_sel, exp.Select):
            inner_sel = inner_sel.find(exp.Select)
        alias_map[alias] = {"type": "cte", "select": inner_sel, "database": "", "schema": "", "table": ""}

    # Tables
    for t in node.find_all(exp.Table):
        alias = getattr(t, "alias_or_name", None)
        if not alias:
            alias = _safe(t.args.get("alias")) or _safe(t.this)
        catalog = t.args.get("catalog")
        db_expr = t.args.get("db")
        database_name = _safe(catalog) if catalog else (_safe(db_expr) if db_expr else "")
        schema_name = _safe(db_expr) if db_expr else ""
        alias_map[alias] = {
            "type": "table",
            "database": database_name,
            "schema": schema_name,
            "table": _safe(t.this),
            "select": None
        }

    # Subqueries in FROM/JOIN
    for sub in node.find_all(exp.Subquery):
        alias = getattr(sub, "alias_or_name", None)
        if not alias:
            alias = _safe(sub.args.get("alias"))
        inner = sub.this
        inner_sel = inner if isinstance(inner, exp.Select) else inner.find(exp.Select)
        if alias:
            alias_map[alias] = {"type": "subquery", "database": "", "schema": "", "table": "", "select": inner_sel}


# ============================================================
#           SQL: PROJECTIONS (WITH RECURSION)
# ============================================================

def _extract_lineage_from_select(select: exp.Select, regulation: str, metadatakey: str,
                                 view_name: str, parent_alias: Optional[str], seen: Set):

    rows = []
    alias_map = {}
    _register_sources_recursive(select, alias_map)

    physical_tables = [v for v in alias_map.values() if v["type"] == "table"]

    for proj in select.expressions or []:

        # ★ STAR handling
        if isinstance(proj, exp.Star):
            qual = _safe(proj.args.get("this"))
            alias_label = parent_alias or (qual + ".*" if qual else "*")

            # qualified star (alias.*)
            if qual:
                entry = alias_map.get(qual)
                if entry:
                    if entry["type"] == "table":
                        key = (entry["database"], entry["table"], "*", alias_label, "all_columns_selected")
                        if key not in seen:
                            seen.add(key)
                            rows.append(_make_row(entry["database"], entry["table"], "*", alias_label,
                                                  regulation, metadatakey, view_name, "all_columns_selected"))
                    else:
                        # subquery or CTE
                        key1 = ("*", qual, "*", alias_label, "all_columns_selected")
                        key2 = ("*", qual, "*", alias_label, "Inner Query Alias Layer")
                        if key1 not in seen:
                            seen.add(key1)
                            rows.append(_make_row("", qual, "*", alias_label, regulation, metadatakey, view_name,
                                                  "all_columns_selected"))
                        if key2 not in seen:
                            seen.add(key2)
                            rows.append(_make_row("", "", "*", alias_label, regulation, metadatakey, view_name,
                                                  "Inner Query Alias Layer"))

                        if entry["select"]:
                            rows.extend(_extract_lineage_from_select(entry["select"],
                                                                     regulation, metadatakey, view_name,
                                                                     alias_label, seen))
                continue

            # unqualified *
            for name, entry in alias_map.items():
                if entry["type"] == "table":
                    alias_label2 = parent_alias or f"{name}.*"
                    key = (entry["database"], entry["table"], "*", alias_label2, "all_columns_selected")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row(entry["database"], entry["table"], "*", alias_label2,
                                              regulation, metadatakey, view_name, "all_columns_selected"))
                elif entry["type"] in ("subquery", "cte"):
                    alias_label2 = parent_alias or f"{name}.*"
                    # emit layer rows
                    k1 = ("*", name, "*", alias_label2, "all_columns_selected")
                    k2 = ("*", name, "*", alias_label2, "Inner Query Alias Layer")
                    if k1 not in seen:
                        seen.add(k1)
                        rows.append(_make_row("", name, "*", alias_label2, regulation, metadatakey, view_name,
                                              "all_columns_selected"))
                    if k2 not in seen:
                        seen.add(k2)
                        rows.append(_make_row("", "", "*", alias_label2, regulation, metadatakey, view_name,
                                              "Inner Query Alias Layer"))
                    if entry["select"]:
                        rows.extend(_extract_lineage_from_select(entry["select"],
                                                                 regulation, metadatakey, view_name,
                                                                 alias_label2, seen))
            continue

        # Non-star columns
        for col in proj.find_all(exp.Column):
            col_name = col.name
            qual = col.table or ""
            alias_name = _get_alias_name(proj) or ""

            if qual:
                entry = alias_map.get(qual)
                if entry and entry["type"] == "table":
                    key = (entry["database"], entry["table"], col_name, alias_name, "")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row(entry["database"], entry["table"],
                                              col_name, alias_name, regulation, metadatakey, view_name, ""))
                elif entry and entry["type"] in ("subquery", "cte"):
                    key = ("", "", col_name, alias_name, "Inner Query Alias Layer")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row("", "", col_name, alias_name,
                                              regulation, metadatakey, view_name, "Inner Query Alias Layer"))
                else:
                    key = ("", qual, col_name, alias_name, "database_not_specified_in_query")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row("", qual, col_name, alias_name,
                                              regulation, metadatakey, view_name,
                                              "database_not_specified_in_query"))
            else:
                # no qualifier
                if len(physical_tables) == 1:
                    entry = physical_tables[0]
                    key = (entry["database"], entry["table"], col_name, alias_name, "")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row(entry["database"], entry["table"],
                                              col_name, alias_name, regulation, metadatakey, view_name, ""))
                else:
                    key = ("", "", col_name, alias_name, "database_not_specified_in_query")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row("", "", col_name, alias_name,
                                              regulation, metadatakey, view_name, "database_not_specified_in_query"))

    return rows


def _generate_lineage_sql(sql: str, regulation: str, metadatakey: str,
                           view_name: str, dialect: Optional[str]) -> List[Dict[str, str]]:
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except:
        return []
    select = unwrap_select(tree)
    if not select:
        return []
    seen = set()
    return _extract_lineage_from_select(select, regulation, metadatakey, view_name, None, seen)


# ============================================================
#                      MONGO LINEAGE
# ============================================================

def _mongo_row(db, coll, col, alias, regulation, metadatakey, view_name, remarks):
    return _make_row(db, coll, col, alias, regulation, metadatakey, view_name, remarks)


def parse_mongo_find(collection, flt, proj, regulation, metadatakey, view_name):
    rows = []
    if not proj:
        rows.append(_mongo_row("", collection, "*", "", regulation, metadatakey, view_name, "all_columns_selected"))
        return rows
    for k, v in proj.items():
        if isinstance(v, int) and v == 1:
            rows.append(_mongo_row("", collection, k, "", regulation, metadatakey, view_name, "column_selected"))
        else:
            rows.append(_mongo_row("", collection, k, k, regulation, metadatakey, view_name, "computed_field"))
    return rows


def parse_mongo_aggregate(collection, pipeline, regulation, metadatakey, view_name):
    rows = []
    for stage in pipeline:
        if "$project" in stage:
            for k, v in stage["$project"].items():
                if isinstance(v, int) and v == 1:
                    rows.append(_mongo_row("", collection, k, "", regulation, metadatakey, view_name, "project_included"))
                else:
                    rows.append(_mongo_row("", collection, k, k, regulation, metadatakey, view_name, "project_computed"))
            continue
        if "$lookup" in stage:
            lk = stage["$lookup"]
            from_coll = lk.get("from")
            as_field = lk.get("as", from_coll)
            rows.append(_mongo_row("", collection, f"$lookup->{from_coll}", as_field, regulation, metadatakey, view_name, "lookup_join"))
            continue
    return rows


def parse_mongo_operation(op, regulation, metadatakey, view_name):
    if op.get("op") == "find":
        return parse_mongo_find(op.get("collection"), op.get("filter"), op.get("projection"),
                                regulation, metadatakey, view_name)
    if op.get("op") == "aggregate":
        return parse_mongo_aggregate(op.get("collection"), op.get("pipeline"),
                                     regulation, metadatakey, view_name)
    return []


# ============================================================
#                UNIFIED PUBLIC FUNCTION
# ============================================================

def parse_sql_lineage(source: Any, regulation: str,
                             metadatakey: str, view_name: str,
                             dialect: Optional[str] = None) -> List[Dict[str, str]]:

    # Mongo branch
    if isinstance(source, dict) and source.get("op"):
        return parse_mongo_operation(source, regulation, metadatakey, view_name)

    # SQL branch
    if isinstance(source, str):
        return _generate_lineage_sql(source, regulation, metadatakey, view_name, dialect)

    return []
if __name__ == "__main__":
    mongo_query = {
        "op": "find",
        "db": "crm",
        "collection": "customers",
        "filter": {"country": "IN"},
        "projection": {"name": 1, "email": 1}
    }
    rows_mongo = parse_sql_lineage(mongo_query, "GDPR", "MONGO1", "VW_CUSTOMERS")
    print(rows_mongo)
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
    print("==="*30)
    # print(extract_query_details(sql_txt))
    rows = parse_sql_lineage(
        sql_txt,
        regulation="SEC",
        metadatakey="KEY123",
        view_name="VW_SAMPLE"
    )

    import json
    print(json.dumps(rows, indent=2))