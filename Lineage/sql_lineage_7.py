"""
Production-Ready SQL Lineage Parser using SQLGlot AST
Backwards compatible with old sql_lineage_parser_22.py
Integrates cleanly with json_parse_sql (9).py
"""

import re
import pandas as pd
from sqlglot import parse_one, exp


# --------------------------------------------------- #
#                   UTILITIES
# --------------------------------------------------- #

def clean_sql(sql: str) -> str:
    """Normalize SQL by removing comments + collapsing whitespace."""
    if not sql:
        return ""
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"\s+", " ", sql).strip()
    return sql


def detect_query_type(sql: str) -> str:
    """Detect Mongo or Elastic queries."""
    s = sql.strip()
    if not s:
        return "empty"
    if s.startswith("{"):
        return "mongo"
    if s.startswith("["):
        return "elastic"
    return "sql"


def skip_map_complexity(sql: str) -> bool:
    """Detect Spark MAP() or STRUCT operations."""
    return bool(re.search(r"\bMAP\s*\(", sql, re.IGNORECASE))


# --------------------------------------------------- #
#               AST TABLE EXTRACTION
# --------------------------------------------------- #

def extract_tables_from_ast(ast):
    """Extract tables + aliases from SQL AST."""
    results = []

    for table in ast.find_all(exp.Table):
        db = table.db or ""
        table_name = table.name or ""
        alias = table.alias_or_name or ""

        if table_name.upper() in {"SELECT", "FROM", "WHERE", "JOIN", "AND", "OR"}:
            continue

        results.append({
            "database": db,
            "table": table_name,
            "alias": alias
        })

    return results


# --------------------------------------------------- #
#                AST COLUMN EXTRACTION
# --------------------------------------------------- #

def extract_columns_from_ast(ast):
    """Extract columns from SELECT statements."""
    results = []

    for select in ast.find_all(exp.Select):
        for proj in select.expressions:

            # SELECT *
            if isinstance(proj, exp.Star):
                results.append({
                    "column": "*",
                    "source": "",
                    "type": "all_columns"
                })
                continue

            # SELECT table.column
            if isinstance(proj, exp.Column):
                results.append({
                    "column": proj.name,
                    "source": proj.table or "",
                    "type": "regular"
                })
                continue

            # SELECT <expr> AS alias
            if isinstance(proj, exp.Alias):
                alias = proj.alias
                expr = proj.this

                # window functions
                if isinstance(expr, exp.Window):
                    results.append({
                        "column": alias,
                        "source": "",
                        "type": "window"
                    })
                else:
                    results.append({
                        "column": alias,
                        "source": "",
                        "type": "derived"
                    })
                continue

    return results


# --------------------------------------------------- #
#                     REMARKS
# --------------------------------------------------- #

def remark_for_column(col_type: str):
    if col_type == "all_columns":
        return "all_columns_selected"
    if col_type == "window":
        return "derived_column_window_function"
    if col_type == "derived":
        return "derived_column"
    return "base_column"


def remark_for_table():
    return "base_table"


# --------------------------------------------------- #
#                   BUILD ROWS
# --------------------------------------------------- #

def build_lineage_rows(tables, columns, view_name, metadatakey, regulation):
    rows = []

    # TABLE LEVEL ROWS
    for t in tables:
        rows.append({
            "Database Name": t["database"],
            "Table Name": t["table"],
            "Column Name": "",
            "Alias Name": t["alias"],
            "Regulation": regulation,
            "Metadatakey": metadatakey,
            "View Name": view_name,
            "Remarks": remark_for_table()
        })

    # COLUMN LEVEL ROWS
    for c in columns:
        rows.append({
            "Database Name": "",
            "Table Name": c.get("source", ""),
            "Column Name": c.get("column", ""),
            "Alias Name": "",
            "Regulation": regulation,
            "Metadatakey": metadatakey,
            "View Name": view_name,
            "Remarks": remark_for_column(c["type"])
        })

    return rows


# --------------------------------------------------- #
#         MAIN SQLLineageParser CLASS (SQLGLOT)
# --------------------------------------------------- #

class SQLLineageParser:

    def parse_single_sql(self, sql_text: str, filename: str,
                         view_name: str = "", regulation: str = ""):

        sql = clean_sql(sql_text)
        metadatakey = filename.replace(".sql", "")

        # empty SQL
        if not sql:
            return self._make_df([
                self._placeholder(filename, view_name, regulation, "empty_sql_content")
            ])

        # Mongo / Elastic
        qtype = detect_query_type(sql)
        if qtype == "mongo":
            return self._make_df([
                self._placeholder(filename, view_name, regulation, "mongo_query")
            ])
        if qtype == "elastic":
            return self._make_df([
                self._placeholder(filename, view_name, regulation, "elastic_query")
            ])

        # MAP skip
        if skip_map_complexity(sql):
            return self._make_df([
                self._placeholder(filename, view_name, regulation,
                                  "spark complex SQL - map function")
            ])

        # parse SQL with SQLGLOT
        try:
            ast = parse_one(sql, read="spark")
        except Exception as e:
            return self._make_df([
                self._placeholder(filename, view_name, regulation,
                                  f"failed_to_parse_sql: {str(e)}")
            ])

        # extract tables + columns
        try:
            tables = extract_tables_from_ast(ast)
            columns = extract_columns_from_ast(ast)
        except Exception as e:
            return self._make_df([
                self._placeholder(filename, view_name, regulation,
                                  f"lineage_extraction_error: {str(e)}")
            ])

        # build lineage rows
        lineage_rows = build_lineage_rows(
            tables=tables,
            columns=columns,
            view_name=view_name or filename,
            metadatakey=metadatakey,
            regulation=regulation
        )

        if not lineage_rows:
            return self._make_df([
                self._placeholder(filename, view_name, regulation,
                                  "no_columns_extracted")
            ])

        return self._make_df(lineage_rows)

    # --------------------------------------------------- #
    #                  INTERNAL HELPERS
    # --------------------------------------------------- #

    def _placeholder(self, filename, view_name, regulation, remark):
        return {
            "Database Name": "",
            "Table Name": "",
            "Column Name": "",
            "Alias Name": "",
            "Regulation": regulation,
            "Metadatakey": filename.replace(".sql", ""),
            "View Name": view_name or filename.replace(".sql", ""),
            "Remarks": remark
        }

    def _make_df(self, rows):
        columns = [
            "Database Name", "Table Name", "Column Name", "Alias Name",
            "Regulation", "Metadatakey", "View Name", "Remarks"
        ]
        return pd.DataFrame(rows)[columns]
