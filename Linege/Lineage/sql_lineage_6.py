"""
sql_lineage_api_wrapper.py

Refactor of your sql_lineage_4.py to be importable in an API.

Requirements:
  pip install sqllineage pandas numpy requests

What you get:
- parse_one_query(...) -> returns list[dict] rows (ready for JSON response)
- parse_many_queries(...) -> same but for multiple SQL strings
- run_lineage_from_metadata_api(...) -> calls an API, extracts queries, runs lineage, returns rows
"""

from __future__ import annotations

import re
import json
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import numpy as np
import requests
from sqllineage.runner import LineageRunner


# -----------------------------
# Constants / keyword sets
# -----------------------------

KEEP_KEYWORDS = {
    "select", "from", "join", "inner", "left", "right", "full", "outer",
    "cross", "on", "using", "as", "distinct"
}
SET_OPERATORS = {"union", "intersect", "except", "all"}
SKIP_KEYWORDS = {"where", "group", "having", "order", "limit", "offset", "fetch", "qualify", "window"}

SQL_KEYWORDS = {
    "where", "group", "order", "limit", "union", "intersect", "except",
    "join", "inner", "left", "right", "cross", "on", "and", "or", "by"
}

PARAMETER_PATTERNS = {
    r"#START_DATE#": "20231101",
    r"#END_DATE#": "20231130",
    r"#BATCH_DATE#": "20231101",
    r"#[A-Z_]+#": "20231101",
}

FUNCTION_MAPPINGS = {
    r"\bnow\s*\(\s*\)": "CURRENT_TIMESTAMP",
    r"\bgetdate\s*\(\s*\)": "CURRENT_TIMESTAMP",
    r"\bcurrent_time\s*\(\s*\)": "CURRENT_TIMESTAMP",
    r"\bisnull\s*\(": "nvl(",
    r"\blen\s*\(": "length(",
    r"\bdatediff\s*\(": "datediff(",
    r"\bdateadd\s*\(": "date_add(",
    r"\byear\s*\(": "year(",
    r"\bmonth\s*\(": "month(",
    r"\bday\s*\(": "day(",
    r"\bndv\s*\(": "approx_count_distinct(",
    r"\bappx_median\s*\(": "percentile_approx(",
}


# -----------------------------
# A) Query type detection + MAP skip
# -----------------------------

def detect_query_type(raw_query: str) -> str:
    """
    Your rule:
      - startswith '{' -> Mongo
      - startswith '[' -> Elastic
      - otherwise -> SQL
    """
    q = (raw_query or "").lstrip()
    if not q:
        return "EMPTY"
    if q.startswith("{"):
        return "Mongo"
    if q.startswith("["):
        return "Elastic"
    return "SQL"


def has_map_function(sql: str) -> bool:
    return bool(re.search(r"\bmap\s*\(", sql or "", re.IGNORECASE))


def extract_first_from_table(sql: str) -> Tuple[str, str]:
    """
    If we skip MAP, we still try to extract FROM db.table for a helpful row.
    Returns (db, table) or ("", "") if not found.
    """
    m = re.search(r"\bfrom\s+([\w_.]+)", sql or "", re.IGNORECASE)
    if not m:
        return "", ""
    full = m.group(1)
    parts = full.split(".")
    if len(parts) >= 2:
        return parts[0], parts[1]
    return "", parts[0]


# -----------------------------
# SQL cleanup & Impala -> Spark conversion (kept from your file)
# -----------------------------

def clean_and_fix_sql(query: str) -> str:
    for pattern, replacement in PARAMETER_PATTERNS.items():
        query = re.sub(pattern, replacement, query or "")

    cleanup_patterns = [
        (r"PERCENTILE_(CONT|DISC)\s*\(\s*([0-9.]+)\s*\)\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+)\s*\)\s*OVER\s*\(\s*\)",
         r"PERCENTILE(\2, \3)"),
        (r"(\w+)\s*\(\s*([^)]*)\s*\)\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+)\s*\)", r"\1(\2, \3)"),
        (r"(\bFROM\s+\w+(?:\.\w+)*)\s+by\s+\w+\s+(\bWHERE\b)", r"\1 \2"),
        (r"\bWHERE\s+([a-zA-Z_]\w*\.\w+)\s*=", r"WHERE \1 ="),
        (r"\bDATEADD\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)", r"DATE_ADD(\3, \2)"),
        (r"\bDATESUB\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)", r"DATE_SUB(\1, \2)"),
        (r"\bADD_MONTHS\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)", r"DATE_ADD(\1, INTERVAL \2 MONTH)"),
        (r"\bMONTHS_BETWEEN\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)", r"DATEDIFF(\1, \2) / 30"),
        (r"\bINTERVAL\s+(\d+)\s+YEARS?\b", r"INTERVAL \1 YEAR"),
        (r"\bINTERVAL\s+(\d+)\s+MONTHS?\b", r"INTERVAL \1 MONTH"),
        (r"\bINTERVAL\s+(\d+)\s+DAYS?\b", r"INTERVAL \1 DAY"),
    ]
    for pattern, replacement in cleanup_patterns:
        query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)

    return (query or "").strip()


def convert_impala_to_spark_sql(query: str) -> str:
    query = clean_and_fix_sql(query or "")

    for pattern, replacement in FUNCTION_MAPPINGS.items():
        query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)

    conversion_patterns = [
        (r"\bpercentile_approx\s*\(\s*([^)]+)\s*\)", r"percentile_approx(\1, 0.5)"),
        (r"\bGROUP_CONCAT\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)", r"concat_ws(\2, collect_list(\1))"),
        (r"\bGROUP_CONCAT\s*\(\s*DISTINCT\s+([^)\s]+)\s+ORDER\s+BY\s+[^,]+\s*,\s*([^)]+)\s*\)",
         r"concat_ws(\2, collect_list(DISTINCT \1))"),
        (r"\bGROUP_CONCAT\s*\(\s*([^)]+)\s*\)", r"concat_ws(',', collect_list(\1))"),
        (r"\bDATE\s+'([^']+)'", r"date('\1')"),
        (r"\bTIMESTAMP\s+'([^']+)'", r"timestamp('\1')"),
        (r"\bOFFSET\s+(\d+)\s+ROWS?\b", r"OFFSET \1"),
        (r"\bTRUE\b", "true"),
        (r"\bFALSE\b", "false"),
        (r"/\*\s*\+\s*[^*]*\*/", ""),
    ]
    for pattern, replacement in conversion_patterns:
        query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)

    return query.strip()


def split_sql_statements(sql_content: str) -> List[str]:
    content_no_comments = re.sub(r"--[^\n]*", "", sql_content or "")
    content_no_comments = re.sub(r"/\*.*?\*/", "", content_no_comments, flags=re.DOTALL)

    statements, current = [], ""
    in_single_quote = in_double_quote = False

    for i, char in enumerate(content_no_comments):
        is_escaped = i > 0 and content_no_comments[i - 1] == "\\"

        if char == "'" and not in_double_quote and not is_escaped:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote and not is_escaped:
            in_double_quote = not in_double_quote
        elif char == ";" and not (in_single_quote or in_double_quote):
            if current.strip():
                statements.append(current.strip())
            current = ""
            continue

        current += char

    if current.strip():
        statements.append(current.strip())

    return statements


def simplify_query_to_select_and_joins(query: str) -> str:
    """
    Keep SELECT/FROM/JOIN and remove WHERE/GROUP/HAVING/ORDER/LIMIT etc.
    Same logic you had, kept as-is (with minimal edits).
    """

    def process_parentheses_content(start_pos: int, original: str) -> Tuple[str, int]:
        depth = 1
        i = start_pos
        inner_content = ""
        in_single_quote = False
        in_double_quote = False

        while i < len(original) and depth > 0:
            char = original[i]

            if char == "'" and not in_double_quote and (i == 0 or original[i - 1] != "\\"):
                in_single_quote = not in_single_quote
                inner_content += char
            elif char == '"' and not in_single_quote and (i == 0 or original[i - 1] != "\\"):
                in_double_quote = not in_double_quote
                inner_content += char
            elif not in_single_quote and not in_double_quote:
                if char == "(":
                    depth += 1
                    inner_content += char
                elif char == ")":
                    depth -= 1
                    if depth > 0:
                        inner_content += char
                else:
                    inner_content += char
            else:
                inner_content += char

            i += 1

        if re.search(r"\bselect\b", inner_content, re.IGNORECASE):
            processed = simplify_single_query(inner_content)
            return f"({processed})", i
        return f"({inner_content})", i

    def simplify_single_query(sql: str) -> str:
        sql = (sql or "").strip()
        result_parts = []
        in_single_quote = in_double_quote = False

        cte_match = re.match(r"\s*with\s+", sql, re.IGNORECASE)
        if cte_match:
            i = cte_match.end()
            cte_part = "WITH "

            while i < len(sql):
                char = sql[i]

                if char == "'" and not in_double_quote:
                    in_single_quote = not in_single_quote
                    cte_part += char
                    i += 1
                elif char == '"' and not in_single_quote:
                    in_double_quote = not in_double_quote
                    cte_part += char
                    i += 1
                elif char == "(" and not in_single_quote and not in_double_quote:
                    processed_content, new_pos = process_parentheses_content(i + 1, sql)
                    cte_part += processed_content
                    i = new_pos
                elif not in_single_quote and not in_double_quote:
                    remaining = sql[i:].lstrip()
                    if re.match(r"select\b", remaining, re.IGNORECASE) and not re.match(r",", sql[i:].lstrip()):
                        result_parts.append(cte_part.rstrip())
                        sql = sql[i:]
                        break
                    else:
                        cte_part += char
                        i += 1
                else:
                    cte_part += char
                    i += 1

        tokens = []
        current_token = ""
        i = 0
        in_single_quote = False
        in_double_quote = False

        while i < len(sql):
            char = sql[i]

            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
                current_token += char
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
                current_token += char
            elif char == "(" and not in_single_quote and not in_double_quote:
                processed_content, new_pos = process_parentheses_content(i + 1, sql)
                current_token += processed_content
                i = new_pos - 1
            elif (char.isspace() or char in "(),") and not in_single_quote and not in_double_quote:
                if current_token:
                    tokens.append(current_token)
                    current_token = ""
                if char in "(),":
                    tokens.append(char)
            else:
                current_token += char

            i += 1

        if current_token:
            tokens.append(current_token)

        filtered_tokens = []
        skip_mode = False
        skip_depth = 0
        paren_depth = 0

        for idx, token in enumerate(tokens):
            token_lower = token.lower()

            if token == "(":
                paren_depth += 1
                if not skip_mode:
                    filtered_tokens.append(token)
                else:
                    skip_depth += 1
            elif token == ")":
                paren_depth -= 1
                if skip_mode and skip_depth > 0:
                    skip_depth -= 1
                elif not skip_mode:
                    filtered_tokens.append(token)

                if skip_mode and skip_depth == 0 and paren_depth == 0:
                    skip_mode = False
            elif token_lower in SET_OPERATORS and paren_depth == 0:
                skip_mode = False
                filtered_tokens.append(token)
            elif token_lower in SKIP_KEYWORDS and paren_depth == 0:
                skip_mode = True
                skip_depth = 0

                if token_lower in ["group", "order"] and idx + 1 < len(tokens):
                    if tokens[idx + 1].lower() == "by":
                        continue
            elif token_lower == "by" and skip_mode:
                continue
            elif not skip_mode:
                filtered_tokens.append(token)

        result = ""
        for i, token in enumerate(filtered_tokens):
            if i == 0:
                result = token
            elif token in "(),":
                result += token
            elif filtered_tokens[i - 1] in "(,":
                result += token
            else:
                result += " " + token

        if result_parts:
            return "\n".join(result_parts) + "\n" + result
        return result

    try:
        simplified = simplify_single_query(query)
        return simplified.strip()
    except Exception:
        return (query or "").strip()


# -----------------------------
# Static value extraction (kept)
# -----------------------------

def extract_static_value_edges(query: str, result: LineageRunner) -> List[Dict]:
    static_edges = []
    edge_id_counter = [0]

    def extract_select_items(select_clause: str) -> List[str]:
        items, current, paren_depth = [], "", 0
        in_quote, quote_char = False, None

        for char in select_clause:
            if char in ("'", '"') and not in_quote:
                in_quote, quote_char = True, char
            elif in_quote and char == quote_char:
                in_quote, quote_char = False, None
            elif not in_quote:
                if char == "(":
                    paren_depth += 1
                elif char == ")":
                    paren_depth -= 1
                elif char == "," and paren_depth == 0:
                    if current.strip():
                        items.append(current.strip())
                    current = ""
                    continue
            current += char

        if current.strip():
            items.append(current.strip())
        return items

    def create_edge(source: str, target: str, edge_counter: List[int]) -> Dict:
        return {"data": {"id": f"static_e{edge_counter[0]}", "source": source, "target": target}}

    def add_edge(static_value: str, column: str, target_alias: str):
        static_edges.append(create_edge(f"{static_value}.{column.upper()}", f"{target_alias}.{column.upper()}", edge_id_counter))
        edge_id_counter[0] += 1

    def process_select_clause(select_clause: str, target_alias: str):
        patterns = [
            (r"^'([^']*)'(?:\s+as\s+(\w+))?$", lambda m: (m.group(1).upper() or "EMPTY_STRING", m.group(2) or "unknown_column")),
            (r"^null\s+as\s+(\w+)$", lambda m: ("NULL", m.group(1))),
            (r"^(true|false)\s+as\s+(\w+)$", lambda m: (m.group(1).upper(), m.group(2))),
            (r"^date\s+'([^']+)'(?:\s+as\s+(\w+))?$", lambda m: (f"DATE_{m.group(1)}", m.group(2) or "unknown_column")),
            (r"^timestamp\s+'([^']+)'(?:\s+as\s+(\w+))?$", lambda m: (f"TIMESTAMP_{m.group(1).replace(' ', '_').replace(':', '-')}", m.group(2) or "unknown_column")),
            (r"^interval\s+'([^']+)'\s+(\w+)(?:\s+as\s+(\w+))?$", lambda m: (f"INTERVAL_{m.group(1)}_{m.group(2).upper()}", m.group(3) or "unknown_column")),
            (r"^(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s+as\s+(\w+)$", lambda m: (m.group(1), m.group(2))),
            (r"^(0x[0-9a-fA-F]+)\s+as\s+(\w+)$", lambda m: (m.group(1).upper(), m.group(2))),
            (r"^array\s*\[([^\]]+)\](?:\s+as\s+(\w+))?$", lambda m: (f"ARRAY_{m.group(1).strip().replace(',', '_').replace(' ', '')}", m.group(2) or "unknown_column")),
            (r"^map\s*\(([^)]+)\)(?:\s+as\s+(\w+))?$", lambda m: (f"MAP_{m.group(1).strip().replace(',', '_').replace(' ', '').replace(chr(39), '')}", m.group(2) or "unknown_column")),
        ]

        for item in extract_select_items(select_clause):
            item = item.strip()
            matched = False

            for pattern, extractor in patterns:
                match = re.match(pattern, item, re.IGNORECASE)
                if match:
                    static_value, column_name = extractor(match)
                    add_edge(static_value, column_name, target_alias)
                    matched = True
                    break

            if not matched and re.search(r"\bcase\b", item, re.IGNORECASE):
                alias_match = re.search(r"\bas\s+(\w+)$", item, re.IGNORECASE)
                if alias_match:
                    column_name = alias_match.group(1)
                    then_values = re.findall(r"\bthen\s+'([^']+)'", item, re.IGNORECASE)
                    then_numbers = re.findall(r"\bthen\s+(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b", item, re.IGNORECASE)

                    for val in then_values + then_numbers:
                        add_edge(str(val).upper(), column_name, target_alias)

    # MAIN
    try:
        if not result.target_tables:
            return static_edges

        select_match = re.search(r"select\s+(.*?)\s+from", query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return static_edges

        target_table = str(result.target_tables[0])
        target_alias = target_table.split(".")[-1] if "." in target_table else target_table
        process_select_clause(select_match.group(1), target_alias)

    except Exception:
        return static_edges

    return static_edges


# -----------------------------
# C) Keyword-node filtering (prevents table="and")
# -----------------------------

def _sanitize_entity_parts(parts: List[str]) -> List[str]:
    """
    If LineageRunner produced weird nodes like:
      "and.col"  -> parts[0] == 'and' (keyword)
    We rewrite it to:
      "N/A.col" -> so table won't become 'and'
    """
    if not parts:
        return parts

    # only sanitize the table-ish segment:
    # - for 2 parts: parts[0] is "table"
    # - for 3 parts: parts[1] is "table"
    if len(parts) == 2 and parts[0].lower() in SQL_KEYWORDS:
        return ["N/A", parts[1]]
    if len(parts) == 3 and parts[1].lower() in SQL_KEYWORDS:
        return [parts[0], "N/A", parts[2]]
    return parts


def create_lineage_dataframe(all_edges: List[Dict], query_id: str) -> pd.DataFrame:
    def parse_entity(entity: str, is_source: bool, edge_id: str) -> Tuple[str, str, str]:
        parts = (entity or "").split(".")
        parts = _sanitize_entity_parts(parts)

        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            # Special handling for static values in source
            if is_source and "static" in edge_id:
                return "", "", parts[0]
            return "", parts[0], parts[1]
        elif len(parts) == 1:
            return "", "", parts[0]
        return "", "", entity

    def determine_type(parts: List[str], db: str, table: str, col: str, edge_id: str, is_source: bool) -> str:
        if not is_source and len(parts) == 3 and db == "dummy":
            return "Select"
        if len(parts) == 3:
            return "Table"
        if len(parts) == 2 and db == "":
            return "CTE/Subquery"
        if len(parts) == 1:
            return "CTE/Subquery"
        return "CTE/Subquery"

    def determine_remark(parts: List[str], db: str, table: str, col: str, edge_id: str, is_source: bool) -> str:
        if "*" in col:
            return "All columns selected"
        if is_source and "static" in edge_id:
            return "Static"
        return "N/A"

    lineage_data = []
    for edge in all_edges:
        edge_id = edge["data"]["id"]
        source = str(edge["data"]["source"])
        target = str(edge["data"]["target"])

        source_parts = _sanitize_entity_parts(source.split("."))
        target_parts = _sanitize_entity_parts(target.split("."))

        source_db, source_table, source_col = parse_entity(".".join(source_parts), True, edge_id)
        target_db, target_table, target_col = parse_entity(".".join(target_parts), False, edge_id)

        source_type = determine_type(source_parts, source_db, source_table, source_col, edge_id, True)
        target_type = determine_type(target_parts, target_db, target_table, target_col, edge_id, False)

        source_remark = determine_remark(source_parts, source_db, source_table, source_col, edge_id, True)
        target_remark = determine_remark(target_parts, target_db, target_table, target_col, edge_id, False)

        source_db = "" if source_db == "<default>" else source_db
        target_db = "" if target_db == "<default>" else target_db

        lineage_data.append({
            "Query_Key": query_id,
            "Database_Name": source_db,
            "Table_Name": source_table,
            "Column_Name": source_col,
            "Source_Type": source_type,
            "Source_Remark": source_remark,
            "Alias": target_col,
            "Target_Type": target_type,
            "Target_Remark": target_remark,
        })

    df = pd.DataFrame(lineage_data)
    if not df.empty:
        df["Source_Table_Remark"] = np.where(df["Table_Name"].isin(["", "N/A", "unknown", "unknown_target"]), "Table Name ambiguous", "")
    return df


# -----------------------------
# Main callable functions (API-friendly)
# -----------------------------

def _row_for_non_sql(query_key: str, qtype: str, view_name: str, metadatakey: str, regulation: str, class_name: str) -> List[Dict[str, Any]]:
    return [{
        "Query_Key": query_key,
        "Database_Name": "",
        "Table_Name": "",
        "Column_Name": "N/A",
        "Source_Type": qtype,
        "Source_Remark": f"{qtype} query",
        "Source_Table_Remark": "",
        "Alias": "N/A",
        "Target_Type": "N/A",
        "Target_Remark": "N/A",
        "View_Name": view_name,
        "Metadatakey": metadatakey,
        "Regulation": regulation,
        "Class_Name": class_name,
        "status": "success",
    }]


def _row_for_map_skip(query_key: str, sql: str, view_name: str, metadatakey: str, regulation: str, class_name: str) -> List[Dict[str, Any]]:
    db, table = extract_first_from_table(sql)
    return [{
        "Query_Key": query_key,
        "Database_Name": db,
        "Table_Name": table or "N/A",
        "Column_Name": "N/A",
        "Source_Type": "SQL",
        "Source_Remark": "spark complex query (MAP)",
        "Source_Table_Remark": "",
        "Alias": "N/A",
        "Target_Type": "N/A",
        "Target_Remark": "N/A",
        "View_Name": view_name,
        "Metadatakey": metadatakey,
        "Regulation": regulation,
        "Class_Name": class_name,
        "status": "success",
    }]


def parse_one_query(
    sql_text: str,
    query_key: str,
    view_name: str = "",
    metadatakey: str = "",
    regulation: str = "",
    class_name: str = "",
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    This is the one you call from FastAPI.

    Returns: list of dict rows (JSON-ready)
    """

    raw = sql_text or ""
    qtype = detect_query_type(raw)

    if verbose:
        print(f"[parse_one_query] query_key={query_key} qtype={qtype}")

    # Non-SQL
    if qtype in ("Mongo", "Elastic"):
        return _row_for_non_sql(query_key, qtype, view_name, metadatakey, regulation, class_name)

    # Empty
    if qtype == "EMPTY" or not raw.strip():
        return _row_for_non_sql(query_key, "EMPTY", view_name, metadatakey, regulation, class_name)

    # MAP skip
    if has_map_function(raw):
        if verbose:
            print(f"[parse_one_query] MAP detected -> skipping lineage for {query_key}")
        return _row_for_map_skip(query_key, raw, view_name, metadatakey, regulation, class_name)

    # Normal SQL lineage
    try:
        converted = convert_impala_to_spark_sql(raw.strip())
        simplified = simplify_query_to_select_and_joins(converted)

        # sqllineage often works better if wrapped as INSERT
        has_dml = any(kw in simplified.lower() for kw in ["insert into", "insert overwrite", "create table"])
        final_query = simplified if has_dml else f"insert into dummy.dummy_table \n\n{simplified}"

        if verbose:
            print(f"[parse_one_query] Running LineageRunner for {query_key}")

        result = LineageRunner(sql=final_query, verbose=False)

        # force init
        _ = result.get_column_lineage()
        graph = result._sql_holder.column_lineage_graph

        column_edges = [{"data": {"id": f"e{i}", "source": str(a), "target": str(b)}} for i, (a, b) in enumerate(graph.edges)]
        static_edges = extract_static_value_edges(raw, result)

        all_edges = column_edges + static_edges
        if not all_edges:
            # fallback row
            return [{
                "Query_Key": query_key,
                "Database_Name": "",
                "Table_Name": "N/A",
                "Column_Name": "N/A",
                "Source_Type": "SQL",
                "Source_Remark": "no_edges_extracted",
                "Source_Table_Remark": "Table Name ambiguous",
                "Alias": "N/A",
                "Target_Type": "N/A",
                "Target_Remark": "N/A",
                "View_Name": view_name,
                "Metadatakey": metadatakey,
                "Regulation": regulation,
                "Class_Name": class_name,
                "status": "success",
            }]

        df = create_lineage_dataframe(all_edges, query_key)

        # attach metadata
        df["View_Name"] = view_name
        df["Metadatakey"] = metadatakey
        df["Regulation"] = regulation
        df["Class_Name"] = class_name
        df["status"] = "success"

        # return JSON-ready
        return df.to_dict(orient="records")

    except Exception as e:
        if verbose:
            print(f"[parse_one_query] ERROR {query_key}: {e}")

        return [{
            "Query_Key": query_key,
            "Database_Name": "Failed",
            "Table_Name": "Failed",
            "Column_Name": "Failed",
            "Source_Type": "Failed",
            "Source_Remark": f"lineage_failed: {str(e)}",
            "Source_Table_Remark": "",
            "Alias": "Failed",
            "Target_Type": "Failed",
            "Target_Remark": "Failed",
            "View_Name": view_name,
            "Metadatakey": metadatakey,
            "Regulation": regulation,
            "Class_Name": class_name,
            "status": "failure",
        }]


def parse_many_queries(
    queries: Dict[str, str],
    metadata: Optional[Dict[str, Dict[str, str]]] = None,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    queries:
      { "Q1": "select ...", "Q2": "select ..." }

    metadata (optional):
      { "Q1": {"view_name": "...", "metadatakey": "...", "regulation":"...", "class_name":"..."} }
    """
    metadata = metadata or {}
    out: List[Dict[str, Any]] = []

    for qk, sql in queries.items():
        md = metadata.get(qk, {})
        rows = parse_one_query(
            sql_text=sql,
            query_key=qk,
            view_name=md.get("view_name", ""),
            metadatakey=md.get("metadatakey", ""),
            regulation=md.get("regulation", ""),
            class_name=md.get("class_name", ""),
            verbose=verbose,
        )
        out.extend(rows)

    return out


# -----------------------------
# API: call metadata endpoint + extract create_query/select_query
# -----------------------------

def extract_queries_from_metadata_payload(payload: Any) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    """
    Very defensive extraction.
    Supports common shapes like:
      - list of records
      - {"data": [ ... ]}
      - record has {"value": {"create_query":[{"query": "..."}], "select_query":{"query":"..."}}}
    """
    if payload is None:
        return {}, {}

    records = payload
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], list):
        records = payload["data"]

    if isinstance(records, dict):
        records = [records]

    if not isinstance(records, list):
        return {}, {}

    queries: Dict[str, str] = {}
    meta: Dict[str, Dict[str, str]] = {}

    for idx, rec in enumerate(records):
        if not isinstance(rec, dict):
            continue

        regulation = rec.get("regulation", rec.get("Regulation", "")) or ""
        view_name = rec.get("view_name", rec.get("ViewName", rec.get("viewName", ""))) or ""
        metadatakey = rec.get("metadatakey", rec.get("metadataKey", rec.get("metadatakey_name", ""))) or ""
        class_name = rec.get("class_name", rec.get("ClassName", rec.get("className", ""))) or ""

        value = rec.get("value") if isinstance(rec.get("value"), dict) else rec

        create_q = value.get("create_query", [])
        select_q = value.get("select_query")

        # create_query can be list or dict
        if isinstance(create_q, dict):
            create_q = [create_q]

        if isinstance(create_q, list):
            for j, item in enumerate(create_q):
                if not isinstance(item, dict):
                    continue
                sql = item.get("query") or item.get("sql") or ""
                qk = f"{view_name or 'VIEW'}__CREATE__{j+1}__{idx+1}"
                queries[qk] = sql
                meta[qk] = {
                    "view_name": view_name or qk,
                    "metadatakey": metadatakey,
                    "regulation": regulation,
                    "class_name": class_name,
                }

        if isinstance(select_q, dict):
            sql = select_q.get("query") or select_q.get("sql") or ""
            qk = f"{view_name or 'VIEW'}__SELECT__{idx+1}"
            queries[qk] = sql
            meta[qk] = {
                "view_name": view_name or qk,
                "metadatakey": metadatakey,
                "regulation": regulation,
                "class_name": class_name,
            }

    return queries, meta


def run_lineage_from_metadata_api(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout_sec: int = 30,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Calls metadata endpoint, extracts queries, runs lineage, returns JSON-ready rows.
    """
    resp = requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout_sec)
    resp.raise_for_status()
    payload = resp.json()

    queries, meta = extract_queries_from_metadata_payload(payload)

    if verbose:
        print(f"[run_lineage_from_metadata_api] extracted {len(queries)} queries")

    return parse_many_queries(queries, meta, verbose=verbose)


# -----------------------------
# Optional: local test runner
# -----------------------------
if __name__ == "__main__":
    # quick local sanity test
    test_sql = "select a.id as id, 'X' as flag from db.table a"
    rows = parse_one_query(test_sql, query_key="TEST_1", view_name="v_test", regulation="SEC", verbose=True)
    print(json.dumps(rows[:5], indent=2))
