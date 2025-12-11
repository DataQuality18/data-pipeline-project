# import sqlglot

# tree = sqlglot.parse_one("SELECT a + b FROM x WHERE id = 10")
# print(tree)


# sql = sqlglot.transpile(
#     "select a,b from x where a>10 order by b",
#     pretty=True
# )[0]

# print(sql)
# print("------------------------table name column name ---------------------------------")
# import sqlglot

# tree = sqlglot.parse_one("SELECT a + b AS x FROM test WHERE c > 10")

# print(tree.find_all(sqlglot.exp.Column))
# print(tree.find_all(sqlglot.exp.Table))




# print("------------------------find all ---------------------------------")
# import sqlglot
# from sqlglot.expressions import Column, Table

# # def extract_query_details(sql):
# #     parsed = sqlglot.parse_one(sql)

# #     # Tables info
# #     tables = []
# #     for t in parsed.find_all(Table):
# #         tables.append({
# #             "db_name": t.catalog,
# #             "schema_name": t.db,
# #             "table_name": t.name
# #         })

# #     # Columns info (with alias if present)
# #     columns = []
# #     for c in parsed.find_all(Column):
# #         col_info = {
# #             "column_name": c.name,
# #             "alias": c.alias_or_name,
# #             "table": c.table,
# #             "schema": c.db,
# #             "database": c.catalog
# #         }
# #         columns.append(col_info)

# #     return {"tables": tables, "columns": columns}

# # import sqlglot
# # from sqlglot import exp
# # from typing import Dict, Any, List, Optional


# # def _safe_name(node) -> str:
# #     """Return identifier name or SQL text safely."""
# #     if node is None:
# #         return ""
# #     if hasattr(node, "name"):
# #         return node.name
# #     try:
# #         return node.sql()
# #     except Exception:
# #         return str(node)


# # def extract_query_details(sql: str, dialect: Optional[str] = None) -> Dict[str, Any]:
# #     """
# #     Parse SQL and return tables + columns with database/schema/table resolved.
# #     - database_name: prefers catalog (left-most qualifier) then db
# #     - schema_name: returns db if present
# #     """
# #     parsed = sqlglot.parse_one(sql, read=dialect)

# #     # Build table list and alias -> table mapping
# #     tables = []
# #     alias_map = {}  # alias -> dict with db/schema/table
# #     fullname_map = {}  # "catalog.db.table" or "db.table" or "table" -> same dict

# #     for t in parsed.find_all(exp.Table):
# #         # catalog and db may be expressions (Identifier) or None
# #         catalog_expr = t.args.get("catalog")
# #         db_expr = t.args.get("db")
# #         this_expr = t.this  # the table identifier (Identifier)

# #         catalog_name = _safe_name(catalog_expr) or ""
# #         db_name = _safe_name(db_expr) or ""
# #         table_name = _safe_name(this_expr) or ""

# #         # Decide what we want to call "database" and "schema"
# #         # Prefer catalog as Database Name (left-most), and db as Schema if present.
# #         database_name = catalog_name or db_name or ""
# #         schema_name = db_name or ""

# #         # alias: prefer alias_or_name, then args['alias'] if present, else table_name
# #         alias = getattr(t, "alias_or_name", None)
# #         if not alias:
# #             alias_expr = t.args.get("alias")
# #             if alias_expr is not None:
# #                 alias = _safe_name(alias_expr)
# #         alias = alias or table_name

# #         table_entry = {
# #             "db_name": database_name,
# #             "schema_name": schema_name,
# #             "table_name": table_name,
# #             "alias": alias,
# #             "raw_table_sql": t.sql(),
# #         }
# #         tables.append(table_entry)

# #         # register alias map and fullname map
# #         alias_map[alias] = table_entry

# #         # full keys to help resolve qualifiers like "GFOLYRE_MANAGED.APP_REGHUB_RHOO_TRADE"
# #         parts = []
# #         if catalog_name:
# #             parts.append(catalog_name)
# #         if db_name:
# #             parts.append(db_name)
# #         parts.append(table_name)
# #         full_key = ".".join([p for p in parts if p])
# #         fullname_map[full_key] = table_entry

# #         # also store db.table and table variants
# #         if db_name:
# #             fullname_map[f"{db_name}.{table_name}"] = table_entry
# #         fullname_map[table_name] = table_entry

# #     # Columns info (with alias if present) — resolve qualifier to table
# #     columns = []
# #     for c in parsed.find_all(exp.Column):
# #         col_name = _safe_name(c)  # this returns "T.COL" sometimes; better parse below
# #         # sqlglot Column has .name and .table attributes
# #         col_simple = getattr(c, "name", None) or col_name
# #         qualifier = getattr(c, "table", None) or ""  # may be alias or full qualifier

# #         resolved_db = ""
# #         resolved_schema = ""
# #         resolved_table = ""

# #         if qualifier:
# #             # try alias map first
# #             if qualifier in alias_map:
# #                 te = alias_map[qualifier]
# #                 resolved_db = te["db_name"]
# #                 resolved_schema = te["schema_name"]
# #                 resolved_table = te["table_name"]
# #             else:
# #                 # qualifier might be full dotted identifier like a.b.c
# #                 # try exact match in fullname_map
# #                 if qualifier in fullname_map:
# #                     te = fullname_map[qualifier]
# #                     resolved_db = te["db_name"]
# #                     resolved_schema = te["schema_name"]
# #                     resolved_table = te["table_name"]
# #                 else:
# #                     # try to match by taking last part as table
# #                     qparts = qualifier.split(".")
# #                     if len(qparts) >= 1:
# #                         last = qparts[-1]
# #                         if last in fullname_map:
# #                             te = fullname_map[last]
# #                             resolved_db = te["db_name"]
# #                             resolved_schema = te["schema_name"]
# #                             resolved_table = te["table_name"]
# #                         else:
# #                             # unknown qualifier -> put qualifier into table for traceability
# #                             resolved_table = qualifier
# #                             resolved_db = ""
# #                             resolved_schema = ""
# #         else:
# #             # no qualifier -> if exactly one table present, assign to that
# #             if len(tables) == 1:
# #                 te = tables[0]
# #                 resolved_db = te["db_name"]
# #                 resolved_schema = te["schema_name"]
# #                 resolved_table = te["table_name"]

# #         # column alias detection (if column expression is an Alias)
# #         # If the column node's parent is an exp.Alias, we can try to capture that alias
# #         col_alias = ""
# #         parent = getattr(c, "parent", None)
# #         # but often the Column node itself doesn't have parent attribute; safer to search upward:
# #         # we'll try to find an enclosing Alias expression by walking parents if available
# #         try:
# #             node = c
# #             while node is not None:
# #                 if isinstance(node, exp.Alias):
# #                     # alias could be Identifier or string
# #                     alias_expr = node.args.get("alias")
# #                     col_alias = _safe_name(alias_expr) or node.alias_or_name or ""
# #                     break
# #                 node = getattr(node, "parent", None)
# #         except Exception:
# #             col_alias = ""

# #         columns.append({
# #             "column_name": col_simple,
# #             "alias": col_alias or (col_simple),
# #             "qualifier": qualifier,
# #             "table": resolved_table,
# #             "schema": resolved_schema,
# #             "database": resolved_db,
# #             "raw_sql": c.sql(),
# #         })

# #     return {"tables": tables, "columns": columns}

# import sqlglot
# from sqlglot import exp
# from typing import Dict, Any, List, Optional


# def _safe_name(node) -> str:
#     """Return identifier name or SQL text safely."""
#     if node is None:
#         return ""
#     if hasattr(node, "name"):
#         return node.name
#     try:
#         return node.sql()
#     except Exception:
#         return str(node)


# def extract_query_details(sql: str, dialect: Optional[str] = None) -> Dict[str, Any]:
#     """
#     Parse SQL and return tables + columns with database/schema/table resolved.
#     - database_name: prefers catalog (left-most qualifier) then db (if catalog absent)
#     - schema_name: only set when a db (schema) part exists in the parsed node
#     """
#     parsed = sqlglot.parse_one(sql, read=dialect)

#     # Build table list and alias -> table mapping
#     tables: List[Dict[str, str]] = []
#     alias_map = {}  # alias -> dict with db/schema/table
#     fullname_map = {}  # "catalog.db.table" or "db.table" or "table" -> same dict

#     for t in parsed.find_all(exp.Table):
#         # catalog and db may be expressions (Identifier) or None
#         catalog_expr = t.args.get("catalog")
#         db_expr = t.args.get("db")
#         this_expr = t.this  # the table identifier (Identifier)

#         catalog_name = _safe_name(catalog_expr) if catalog_expr is not None else ""
#         db_name = _safe_name(db_expr) if db_expr is not None else ""
#         table_name = _safe_name(this_expr) or ""

#         # Decide database_name and schema_name:
#         # - If catalog present: database_name = catalog, schema_name = db (only if db_expr exists)
#         # - If no catalog but db present: database_name = db, schema_name = db (but user asked schema blank when not present,
#         #   so we set schema_name only if db_expr exists; this preserves schema blank when only catalog was provided)
#         if catalog_name:
#             database_name = catalog_name
#         elif db_name:
#             database_name = db_name
#         else:
#             database_name = ""

#         # Set schema_name ONLY if db_expr exists in the parsed Table node
#         schema_name = db_name if db_expr is not None else ""

#         # alias: prefer alias_or_name, then args['alias'] if present, else table_name
#         alias = getattr(t, "alias_or_name", None)
#         if not alias:
#             alias_expr = t.args.get("alias")
#             if alias_expr is not None:
#                 alias = _safe_name(alias_expr)
#         alias = alias or table_name

#         table_entry = {
#             "db_name": database_name,
#             "schema_name": schema_name,
#             "table_name": table_name,
#             "alias": alias,
#             "raw_table_sql": t.sql(),
#         }
#         tables.append(table_entry)

#         # register alias map and fullname map
#         alias_map[alias] = table_entry

#         # full keys to help resolve qualifiers like "GFOLYRE_MANAGED.APP_REGHUB_RHOO_TRADE"
#         parts = []
#         if catalog_name:
#             parts.append(catalog_name)
#         if db_name:
#             parts.append(db_name)
#         parts.append(table_name)
#         full_key = ".".join([p for p in parts if p])
#         fullname_map[full_key] = table_entry

#         # also store db.table and table variants
#         if db_name:
#             fullname_map[f"{db_name}.{table_name}"] = table_entry
#         fullname_map[table_name] = table_entry

#     # Columns info (with alias if present) — resolve qualifier to table
#     columns = []
#     for c in parsed.find_all(exp.Column):
#         # Column attributes
#         col_simple = getattr(c, "name", None) or _safe_name(c)
#         qualifier = getattr(c, "table", None) or ""  # may be alias or full qualifier

#         resolved_db = ""
#         resolved_schema = ""
#         resolved_table = ""

#         if qualifier:
#             # try alias map first
#             if qualifier in alias_map:
#                 te = alias_map[qualifier]
#                 resolved_db = te["db_name"]
#                 resolved_schema = te["schema_name"]
#                 resolved_table = te["table_name"]
#             else:
#                 # qualifier might be full dotted identifier like a.b.c
#                 if qualifier in fullname_map:
#                     te = fullname_map[qualifier]
#                     resolved_db = te["db_name"]
#                     resolved_schema = te["schema_name"]
#                     resolved_table = te["table_name"]
#                 else:
#                     # try to match by taking last part as table
#                     qparts = qualifier.split(".")
#                     if len(qparts) >= 1:
#                         last = qparts[-1]
#                         if last in fullname_map:
#                             te = fullname_map[last]
#                             resolved_db = te["db_name"]
#                             resolved_schema = te["schema_name"]
#                             resolved_table = te["table_name"]
#                         else:
#                             # unknown qualifier -> keep qualifier as table for traceability
#                             resolved_table = qualifier
#                             resolved_db = ""
#                             resolved_schema = ""
#         else:
#             # no qualifier -> if exactly one table present, map to it
#             if len(tables) == 1:
#                 te = tables[0]
#                 resolved_db = te["db_name"]
#                 resolved_schema = te["schema_name"]
#                 resolved_table = te["table_name"]

#         # column alias detection (best-effort)
#         col_alias = ""
#         # walk up parents to find enclosing Alias (safe)
#         try:
#             node = c
#             while node is not None:
#                 if isinstance(node, exp.Alias):
#                     alias_expr = node.args.get("alias")
#                     col_alias = _safe_name(alias_expr) or node.alias_or_name or ""
#                     break
#                 node = getattr(node, "parent", None)
#         except Exception:
#             col_alias = ""

#         columns.append({
#             "column_name": col_simple,
#             "alias": col_alias or col_simple,
#             "qualifier": qualifier,
#             "table": resolved_table,
#             "schema": resolved_schema,
#             "database": resolved_db,
#             "raw_sql": c.sql(),
#         })

#     return {"tables": tables, "columns": columns}


import sqlglot
from sqlglot import exp
from typing import List, Dict, Optional, Set


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe(node):
    if node is None:
        return ""
    if hasattr(node, "name"):
        return node.name
    try:
        return node.sql()
    except:
        return str(node)


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


def unwrap_select(node):
    if isinstance(node, exp.Select):
        return node
    if isinstance(node, exp.Subquery):
        if isinstance(node.this, exp.Select):
            return node.this
        return unwrap_select(node.this)
    if isinstance(node, exp.Paren):
        return unwrap_select(node.this)
    found = node.find(exp.Select)
    return found


# ---------------------------------------------------------------------------
# Table discovery
# ---------------------------------------------------------------------------
def extract_tables(parsed: exp.Expression):
    tables = []
    alias_map = {}

    for t in parsed.find_all(exp.Table):
        catalog = t.args.get("catalog")
        db_expr = t.args.get("db")

        catalog_name = _safe(catalog) if catalog else ""
        db_name = _safe(db_expr) if db_expr else ""
        table_name = _safe(t.this)

        # Database name = leftmost qualifier
        database_name = catalog_name or db_name or ""

        # Schema name = ONLY if db_expr exists
        schema_name = db_name if db_expr is not None else ""

        alias = getattr(t, "alias_or_name", None)
        if not alias:
            alias_expr = t.args.get("alias")
            alias = _safe(alias_expr) if alias_expr else table_name

        entry = {
            "database": database_name,
            "schema": schema_name,
            "table": table_name,
            "alias": alias,
        }
        tables.append(entry)
        alias_map[alias] = entry

    return tables, alias_map


# ---------------------------------------------------------------------------
# STAR expansion and column extraction
# ---------------------------------------------------------------------------
def expand_columns(select: exp.Select, tables, alias_map,
                   regulation, metadatakey, view_name, seen: Set):
    rows = []

    for proj in select.expressions:
        # -----------------------------------------------------------
        # 1. Process STAR (* or alias.*)
        # -----------------------------------------------------------
        if isinstance(proj, exp.Star):
            qualifier = _safe(proj.args.get("this"))

            if qualifier:  # alias.*
                if qualifier in alias_map:
                    t = alias_map[qualifier]
                    key = (t["database"], t["table"], "*", qualifier + ".*", "all_columns_selected")
                    if key not in seen:
                        seen.add(key)
                        rows.append(
                            _make_row(t["database"], t["table"], "*", qualifier + ".*",
                                      regulation, metadatakey, view_name, "all_columns_selected")
                        )
                continue

            # Unqualified * → expand all base tables
            for t in tables:
                alias = t["alias"]
                key = (t["database"], t["table"], "*", alias + ".*", "all_columns_selected")
                if key not in seen:
                    seen.add(key)
                    rows.append(
                        _make_row(t["database"], t["table"], "*", alias + ".*",
                                  regulation, metadatakey, view_name, "all_columns_selected")
                    )
            continue

        # -----------------------------------------------------------
        # 2. Non-* projections → extract Column nodes
        # -----------------------------------------------------------
        alias_name = getattr(proj, "alias", None)
        alias_name = alias_name or ""

        for col in proj.find_all(exp.Column):
            col_name = col.name
            qualifier = col.table or ""

            if qualifier and qualifier in alias_map:
                t = alias_map[qualifier]
                key = (t["database"], t["table"], col_name, alias_name, "")
                if key not in seen:
                    seen.add(key)
                    rows.append(
                        _make_row(t["database"], t["table"], col_name,
                                  alias_name, regulation, metadatakey, view_name, "")
                    )
            else:
                # Unqualified column → ambiguous if more than one table
                if len(tables) == 1:
                    t = tables[0]
                    key = (t["database"], t["table"], col_name, alias_name, "")
                    if key not in seen:
                        seen.add(key)
                        rows.append(
                            _make_row(t["database"], t["table"], col_name,
                                      alias_name, regulation, metadatakey, view_name, "")
                        )
                else:
                    key = ("", "", col_name, alias_name, "database_not_specified_in_query")
                    if key not in seen:
                        seen.add(key)
                        rows.append(
                            _make_row("", "", col_name, alias_name,
                                      regulation, metadatakey, view_name,
                                      "database_not_specified_in_query")
                        )

    return rows


# ---------------------------------------------------------------------------
# MAIN lineage API
# ---------------------------------------------------------------------------
def generate_lineage(sql: str, regulation: str, metadatakey: str, view_name: str, dialect=None):
    parsed = sqlglot.parse_one(sql, read=dialect)
    select = unwrap_select(parsed)
    if not select:
        return []

    tables, alias_map = extract_tables(parsed)

    seen = set()
    rows = expand_columns(select, tables, alias_map, regulation, metadatakey, view_name, seen)
    return rows


# ---- Test with your CREATE TABLE ----
sql = """
 select platform_level AS INTERFACE_PLATFORM, count(*) from IRIS_PRD.AEM.T_FACT_PLAYER_SPEND_BY_PLATFORM_DAILY where title_id = 356 and date_id <= '20251210' AND is_trial_base_spender_today='TRUE' group by 1 order by 1;
"""

# print(extract_query_details(sql))

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
rows = generate_lineage(
    sql_txt,
    regulation="SEC",
    metadatakey="KEY123",
    view_name="VW_SAMPLE"
)

import json
print(json.dumps(rows, indent=2))
