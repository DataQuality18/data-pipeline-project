"""
SQL Lineage Parser with Impala to Spark SQL conversion

pip install sqllineage
"""

import pandas as pd
import sqllineage
import numpy as np
import re
import argparse
import sys
import json
from typing import Dict, List, Tuple, Any

from sqllineage.runner import LineageRunner

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def simplify_query_to_select_and_joins(query: str) -> str:
    """
    Remove WHERE, GROUP BY, HAVING, ORDER BY, LIMIT clauses from SQL query.
    Keeps only SELECT, FROM, and JOIN clauses at all nesting levels (CTEs, subqueries).
    
    Args:
        query: SQL query string
        
    Returns:
        Simplified query with only SELECT and JOIN logic
    """
    
    def process_parentheses_content(content: str, start_pos: int, original: str) -> Tuple[str, int]:
        """
        Recursively process content within parentheses (subqueries).
        Returns (processed_content, end_position)
        """
        depth = 1
        i = start_pos
        inner_content = ""
        in_single_quote = False
        in_double_quote = False
        
        while i < len(original) and depth > 0:
            char = original[i]
            
            # Handle quotes
            if char == "'" and not in_double_quote and (i == 0 or original[i-1] != '\\'):
                in_single_quote = not in_single_quote
                inner_content += char
            elif char == '"' and not in_single_quote and (i == 0 or original[i-1] != '\\'):
                in_double_quote = not in_double_quote
                inner_content += char
            elif not in_single_quote and not in_double_quote:
                if char == '(':
                    depth += 1
                    inner_content += char
                elif char == ')':
                    depth -= 1
                    if depth > 0:
                        inner_content += char
                else:
                    inner_content += char
            else:
                inner_content += char
            
            i += 1
        
        # Check if this is a subquery (contains SELECT)
        if re.search(r'\bselect\b', inner_content, re.IGNORECASE):
            processed = simplify_single_query(inner_content)
            return f"({processed})", i
        else:
            # Not a subquery, keep as is (e.g., function arguments, IN clauses)
            return f"({inner_content})", i
    
    def simplify_single_query(sql: str) -> str:
        """
        Simplify a single SQL query (not containing outer parentheses).
        Handles CTEs and removes filtering/grouping/sorting clauses.
        """
        sql = sql.strip()
        result_parts = []
        i = 0
        in_single_quote = False
        in_double_quote = False
        
        # Check for CTE (WITH clause)
        cte_match = re.match(r'\s*with\s+', sql, re.IGNORECASE)
        if cte_match:
            # Process CTE definitions
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
                elif char == '(' and not in_single_quote and not in_double_quote:
                    # Start of CTE definition
                    processed_content, new_pos = process_parentheses_content("", i + 1, sql)
                    cte_part += processed_content
                    i = new_pos
                elif not in_single_quote and not in_double_quote:
                    # Check if we've reached the main SELECT
                    remaining = sql[i:].lstrip()
                    if re.match(r'select\b', remaining, re.IGNORECASE) and not re.match(r',', sql[i:].lstrip()):
                        # Found main SELECT, break
                        result_parts.append(cte_part.rstrip())
                        sql = sql[i:]
                        break
                    else:
                        cte_part += char
                        i += 1
                else:
                    cte_part += char
                    i += 1
        
        # Now process the main query
        # Build tokens while respecting parentheses and quotes
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
            elif char == '(' and not in_single_quote and not in_double_quote:
                # Process subquery or function
                processed_content, new_pos = process_parentheses_content("", i + 1, sql)
                current_token += processed_content
                i = new_pos - 1
            elif (char.isspace() or char in '(),') and not in_single_quote and not in_double_quote:
                if current_token:
                    tokens.append(current_token)
                    current_token = ""
                if char in '(),':
                    tokens.append(char)
            else:
                current_token += char
            
            i += 1
        
        if current_token:
            tokens.append(current_token)
        
        # Filter tokens to keep only relevant clauses
        keep_keywords = ['select', 'from', 'join', 'inner', 'left', 'right', 'full', 'outer', 
                        'cross', 'on', 'using', 'as', 'distinct']
        set_operators = ['union', 'intersect', 'except', 'all']  # Set operators that break clauses
        skip_keywords = ['where', 'group', 'having', 'order', 'limit', 'offset', 'fetch', 'qualify', 'window']
        
        filtered_tokens = []
        skip_mode = False
        skip_depth = 0
        paren_depth = 0
        
        for idx, token in enumerate(tokens):
            token_lower = token.lower()
            
            # Track parentheses depth
            if token == '(':
                paren_depth += 1
                if not skip_mode:
                    filtered_tokens.append(token)
                else:
                    skip_depth += 1
            elif token == ')':
                paren_depth -= 1
                if skip_mode and skip_depth > 0:
                    skip_depth -= 1
                elif not skip_mode:
                    filtered_tokens.append(token)
                
                # Exit skip mode if we're back at the same level
                if skip_mode and skip_depth == 0 and paren_depth == 0:
                    skip_mode = False
            elif token_lower in set_operators and paren_depth == 0:
                # Set operators (UNION, INTERSECT, EXCEPT) end skip mode
                skip_mode = False
                filtered_tokens.append(token)
            elif token_lower in skip_keywords and paren_depth == 0:
                # Start skipping
                skip_mode = True
                skip_depth = 0
                
                # Special handling for "group by", "order by"
                if token_lower in ['group', 'order'] and idx + 1 < len(tokens):
                    next_token = tokens[idx + 1].lower()
                    if next_token == 'by':
                        continue
            elif token_lower == 'by' and skip_mode:
                # Skip 'by' in 'group by' or 'order by'
                continue
            elif not skip_mode:
                filtered_tokens.append(token)
        
        # Reconstruct the query
        result = ""
        for i, token in enumerate(filtered_tokens):
            if i == 0:
                result = token
            elif token in '(),':
                result += token
            elif filtered_tokens[i-1] in '(,':
                result += token
            else:
                result += " " + token
        
        if result_parts:
            return "\n".join(result_parts) + "\n" + result
        return result
    
    # Main processing
    try:
        simplified = simplify_single_query(query)
        return simplified.strip()
    except Exception as e:
        print(f"Error simplifying query: {e}")
        return query


def clean_and_fix_sql(query: str) -> str:
    """Clean and fix common SQL syntax issues before processing."""
    cleaned_query = query
    
    parameter_replacements = {
        r'#START_DATE#': '20231101',
        r'#END_DATE#': '20231130',
        r'#BATCH_DATE#': '20231101',
        r'#[A-Z_]+#': '20231101'
    }
    
    for pattern, replacement in parameter_replacements.items():
        cleaned_query = re.sub(pattern, replacement, cleaned_query)
    
    cleaned_query = re.sub(
        r'PERCENTILE_CONT\s*\(\s*([0-9.]+)\s*\)\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+)\s*\)\s*OVER\s*\(\s*\)',
        r'PERCENTILE(\1, \2)',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'PERCENTILE_DISC\s*\(\s*([0-9.]+)\s*\)\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+)\s*\)\s*OVER\s*\(\s*\)',
        r'PERCENTILE(\1, \2)',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'(\w+)\s*\(\s*([^)]*)\s*\)\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+)\s*\)',
        r'\1(\2, \3)',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'(\bFROM\s+\w+(?:\.\w+)*)\s+by\s+\w+\s+(\bWHERE\b)',
        r'\1 \2',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'\bWHERE\s+([a-zA-Z_]\w*\.\w+)\s*=',
        r'WHERE \1 =',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'\bDATEADD\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
        r'DATE_ADD(\3, \2)',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'\bDATESUB\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
        r'DATE_SUB(\1, \2)',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'\bADD_MONTHS\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
        r'DATE_ADD(\1, INTERVAL \2 MONTH)',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'\bMONTHS_BETWEEN\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
        r'DATEDIFF(\1, \2) / 30',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'\bINTERVAL\s+(\d+)\s+YEARS?\b',
        r'INTERVAL \1 YEAR',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'\bINTERVAL\s+(\d+)\s+MONTHS?\b',
        r'INTERVAL \1 MONTH',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    cleaned_query = re.sub(
        r'\bINTERVAL\s+(\d+)\s+DAYS?\b',
        r'INTERVAL \1 DAY',
        cleaned_query,
        flags=re.IGNORECASE
    )
    
    return cleaned_query.strip()


def convert_impala_to_spark_sql(query: str) -> str:
    """Convert Impala SQL syntax to Spark SQL syntax."""
    converted_query = clean_and_fix_sql(query)
    
    function_mappings = {
        r'\bnow\s*\(\s*\)': 'CURRENT_TIMESTAMP',
        r'\bgetdate\s*\(\s*\)': 'CURRENT_TIMESTAMP',
        r'\bcurrent_time\s*\(\s*\)': 'CURRENT_TIMESTAMP',
        r'\bisnull\s*\(': 'nvl(',
        r'\blen\s*\(': 'length(',
        r'\bdatediff\s*\(': 'datediff(',
        r'\bdateadd\s*\(': 'date_add(',
        r'\byear\s*\(': 'year(',
        r'\bmonth\s*\(': 'month(',
        r'\bday\s*\(': 'day(',
        r'\bndv\s*\(': 'approx_count_distinct(',
        r'\bappx_median\s*\(': 'percentile_approx(',
    }
    
    for pattern, replacement in function_mappings.items():
        converted_query = re.sub(pattern, replacement, converted_query, flags=re.IGNORECASE)
    
    converted_query = re.sub(
        r'\bpercentile_approx\s*\(\s*([^)]+)\s*\)',
        r'percentile_approx(\1, 0.5)',
        converted_query,
        flags=re.IGNORECASE
    )
    
    converted_query = re.sub(
        r'\bGROUP_CONCAT\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
        r'concat_ws(\2, collect_list(\1))',
        converted_query,
        flags=re.IGNORECASE
    )
    
    converted_query = re.sub(
        r'\bGROUP_CONCAT\s*\(\s*DISTINCT\s+([^)\s]+)\s+ORDER\s+BY\s+[^,]+\s*,\s*([^)]+)\s*\)',
        r'concat_ws(\2, collect_list(DISTINCT \1))',
        converted_query,
        flags=re.IGNORECASE
    )
    
    converted_query = re.sub(
        r'\bGROUP_CONCAT\s*\(\s*([^)]+)\s*\)',
        r'concat_ws(\',\', collect_list(\1))',
        converted_query,
        flags=re.IGNORECASE
    )
    
    converted_query = re.sub(
        r"\bDATE\s+'([^']+)'", 
        r"date('\1')", 
        converted_query, 
        flags=re.IGNORECASE
    )
    
    converted_query = re.sub(
        r"\bTIMESTAMP\s+'([^']+)'", 
        r"timestamp('\1')", 
        converted_query, 
        flags=re.IGNORECASE
    )
    
    converted_query = re.sub(
        r'\bOFFSET\s+(\d+)\s+ROWS?\b', 
        r'OFFSET \1', 
        converted_query, 
        flags=re.IGNORECASE
    )
    
    converted_query = re.sub(r'\bTRUE\b', 'true', converted_query, flags=re.IGNORECASE)
    converted_query = re.sub(r'\bFALSE\b', 'false', converted_query, flags=re.IGNORECASE)
    
    converted_query = re.sub(
        r'/\*\s*\+\s*[^*]*\*/', 
        '', 
        converted_query, 
        flags=re.IGNORECASE
    )
    
    return converted_query.strip()


def detect_sql_dialect(query: str) -> str:
    """Detect SQL dialect based on function patterns."""
    query_lower = query.lower()
    
    impala_indicators = [
        'compute stats', 'invalidate metadata', 'refresh functions',
        'show table stats', 'show column stats', 'upsert into',
        'parquet_file(', 'appx_median(', 'impala_version()',
        'ndv(', 'group_concat(', 'isnull(', 'len(', 'now()', 'getdate()'
    ]
    
    spark_indicators = [
        'cache table', 'uncache table', 'refresh table',
        'analyze table compute statistics', 'create or replace temporary view',
        'spark_version()', 'collect_list(', 'collect_set('
    ]
    
    impala_score = sum(1 for indicator in impala_indicators if indicator in query_lower)
    spark_score = sum(1 for indicator in spark_indicators if indicator in query_lower)
    
    if impala_score > spark_score and impala_score > 0:
        return 'impala'
    elif spark_score > impala_score and spark_score > 0:
        return 'sparksql'
    else:
        return 'unknown'


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='SQL Lineage Parser with Impala to Spark SQL conversion')
    parser.add_argument('--file', '-f', 
                       default='',
                       help='Path to SQL file to parse')
    parser.add_argument('--input-dialect', '-d',
                       choices=['auto', 'impala', 'sparksql'],
                       default='auto',
                       help='Input SQL dialect (auto-detect by default)')
    parser.add_argument('--verbose', '-v',
                       action='store_true',
                       help='Verbose output')
    return parser.parse_args()


def split_sql_statements(sql_content: str) -> list:
    """Split SQL content into individual statements, handling comments and quoted strings properly."""
    # First, remove comments before splitting
    # Remove single-line comments (-- ...)
    content_no_single_comments = re.sub(r'--[^\n]*', '', sql_content)
    
    # Remove multi-line comments (/* ... */)
    content_no_comments = re.sub(r'/\*.*?\*/', '', content_no_single_comments, flags=re.DOTALL)
    
    splitted_statements = []
    current_statement = ""
    in_single_quote = False
    in_double_quote = False
    i = 0
    
    while i < len(content_no_comments):
        char = content_no_comments[i]
        
        # Handle single quotes
        if char == "'" and not in_double_quote:
            if i > 0 and content_no_comments[i-1] == '\\':
                current_statement += char
            else:
                in_single_quote = not in_single_quote
                current_statement += char
        # Handle double quotes
        elif char == '"' and not in_single_quote:
            if i > 0 and content_no_comments[i-1] == '\\':
                current_statement += char
            else:
                in_double_quote = not in_double_quote
                current_statement += char
        # Handle semicolon (statement delimiter)
        elif char == ';' and not in_single_quote and not in_double_quote:
            if current_statement.strip():
                splitted_statements.append(current_statement.strip().lower())
            current_statement = ""
        else:
            current_statement += char
        
        i += 1
    
    # Add last statement if exists
    if current_statement.strip():
        splitted_statements.append(current_statement.strip().lower())
    
    return splitted_statements


def parse_column_name_with_type(col_str, node_type_lookup):
    """Parse column name into database, table, column components."""
    col_str = str(col_str)
    parts = col_str.split('.')
    
    if len(parts) >= 3:
        database_name = parts[0] if parts[0] != '<default>' else 'default'
        table_name = parts[1]
        column_name = parts[2]
    elif len(parts) == 2:
        database_name = 'default'
        table_name = parts[0]
        column_name = parts[1]
    else:
        database_name = 'default'
        table_name = 'unknown'
        column_name = parts[0]
    
    object_type = node_type_lookup.get(col_str, 'unknown')
    
    parent_type = 'unknown'
    if len(parts) >= 2:
        parent_key = '.'.join(parts[:-1])
        parent_type = node_type_lookup.get(parent_key, 'unknown')
        
    return database_name, table_name, column_name, object_type, parent_type


def extract_static_value_edges(query: str, result: LineageRunner) -> List[Dict]:
    """
    Extract static value lineage as graph edges in JSON format.
    Handles CTEs, subqueries, and nested queries at all levels.
    
    Args:
        query: Original SQL query
        result: LineageRunner result object
        
    Returns:
        List of edge dictionaries with format:
        {"data": {"id": "eX", "source": "<static_value>.column_name", "target": "table.column"}}
    """
    static_edges = []
    edge_id_counter = [0]  # Use list to make it mutable in nested functions
    
    def extract_select_items(select_clause: str) -> List[str]:
        """Extract individual SELECT items from a SELECT clause."""
        items = []
        current_item = ""
        paren_depth = 0
        in_quote = False
        quote_char = None
        
        for char in select_clause:
            if char in ("'", '"') and not in_quote:
                in_quote = True
                quote_char = char
                current_item += char
            elif in_quote and char == quote_char:
                in_quote = False
                quote_char = None
                current_item += char
            elif char == '(' and not in_quote:
                paren_depth += 1
                current_item += char
            elif char == ')' and not in_quote:
                paren_depth -= 1
                current_item += char
            elif char == ',' and paren_depth == 0 and not in_quote:
                if current_item.strip():
                    items.append(current_item.strip())
                current_item = ""
            else:
                current_item += char
        
        if current_item.strip():
            items.append(current_item.strip())
        
        return items
    
    def process_select_clause(select_clause: str, target_alias: str, static_edges: List[Dict], edge_counter: List[int]):
        """Process a SELECT clause to extract static values."""
        select_items = extract_select_items(select_clause)
        
        for item in select_items:
            static_value = None
            column_name = None
            item_stripped = item.strip()
            
            # Pattern 1: String literal with AS alias (including empty strings)
            string_match = re.match(r"^'([^']*)'(?:\s+as\s+(\w+))?$", item_stripped, re.IGNORECASE)
            if string_match:
                static_value = string_match.group(1).upper() if string_match.group(1) else 'EMPTY_STRING'
                column_name = string_match.group(2) if string_match.group(2) else 'unknown_column'
            
            # Pattern 2: NULL literal with AS alias
            if not static_value:
                null_match = re.match(r'^null\s+as\s+(\w+)$', item_stripped, re.IGNORECASE)
                if null_match:
                    static_value = 'NULL'
                    column_name = null_match.group(1)
            
            # Pattern 3: Boolean literal with AS alias
            if not static_value:
                bool_match = re.match(r'^(true|false)\s+as\s+(\w+)$', item_stripped, re.IGNORECASE)
                if bool_match:
                    static_value = bool_match.group(1).upper()
                    column_name = bool_match.group(2)
            
            # Pattern 4: DATE literal with AS alias
            if not static_value:
                date_match = re.match(r"^date\s+'([^']+)'(?:\s+as\s+(\w+))?$", item_stripped, re.IGNORECASE)
                if date_match:
                    static_value = f"DATE_{date_match.group(1)}"
                    column_name = date_match.group(2) if date_match.group(2) else 'unknown_column'
            
            # Pattern 5: TIMESTAMP literal with AS alias
            if not static_value:
                timestamp_match = re.match(r"^timestamp\s+'([^']+)'(?:\s+as\s+(\w+))?$", item_stripped, re.IGNORECASE)
                if timestamp_match:
                    static_value = f"TIMESTAMP_{timestamp_match.group(1).replace(' ', '_').replace(':', '-')}"
                    column_name = timestamp_match.group(2) if timestamp_match.group(2) else 'unknown_column'
            
            # Pattern 6: INTERVAL literal with AS alias
            if not static_value:
                interval_match = re.match(r"^interval\s+'([^']+)'\s+(\w+)(?:\s+as\s+(\w+))?$", item_stripped, re.IGNORECASE)
                if interval_match:
                    static_value = f"INTERVAL_{interval_match.group(1)}_{interval_match.group(2).upper()}"
                    column_name = interval_match.group(3) if interval_match.group(3) else 'unknown_column'
            
            # Pattern 7: Number literal (including negative, decimal, scientific notation) with AS alias
            if not static_value:
                # Matches: -5, 100, 3.14, 1.5e10, -2.3e-5
                number_match = re.match(r'^(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s+as\s+(\w+)$', item_stripped, re.IGNORECASE)
                if number_match:
                    static_value = number_match.group(1)
                    column_name = number_match.group(2)
            
            # Pattern 8: Hex literal with AS alias
            if not static_value:
                hex_match = re.match(r'^(0x[0-9a-fA-F]+)\s+as\s+(\w+)$', item_stripped, re.IGNORECASE)
                if hex_match:
                    static_value = hex_match.group(1).upper()
                    column_name = hex_match.group(2)
            
            # Pattern 9: ARRAY literal with AS alias
            if not static_value:
                array_match = re.match(r'^array\s*\[([^\]]+)\](?:\s+as\s+(\w+))?$', item_stripped, re.IGNORECASE)
                if array_match:
                    array_content = array_match.group(1).strip()
                    static_value = f"ARRAY_{array_content.replace(',', '_').replace(' ', '')}"
                    column_name = array_match.group(2) if array_match.group(2) else 'unknown_column'
            
            # Pattern 10: MAP literal with AS alias
            if not static_value:
                map_match = re.match(r'^map\s*\(([^)]+)\)(?:\s+as\s+(\w+))?$', item_stripped, re.IGNORECASE)
                if map_match:
                    map_content = map_match.group(1).strip()
                    static_value = f"MAP_{map_content.replace(',', '_').replace(' ', '').replace(chr(39), '')}"
                    column_name = map_match.group(2) if map_match.group(2) else 'unknown_column'
            
            # Pattern 11: CASE with static values
            if not static_value and re.search(r'\bcase\b', item_stripped, re.IGNORECASE):
                then_values = re.findall(r'\bthen\s+\'([^\']+)\'', item_stripped, re.IGNORECASE)
                then_numbers = re.findall(r'\bthen\s+(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b', item_stripped, re.IGNORECASE)
                
                alias_match = re.search(r'\bas\s+(\w+)$', item_stripped, re.IGNORECASE)
                if alias_match:
                    column_name = alias_match.group(1)
                    
                    for val in then_values:
                        source = f"{val.upper()}.{column_name.upper()}"
                        target = f"{target_alias}.{column_name.upper()}"
                        
                        edge = {
                            "data": {
                                "id": f"static_e{edge_counter[0]}",
                                "source": source,
                                "target": target
                            }
                        }
                        static_edges.append(edge)
                        edge_counter[0] += 1
                    
                    for val in then_numbers:
                        source = f"{val}.{column_name.upper()}"
                        target = f"{target_alias}.{column_name.upper()}"
                        
                        edge = {
                            "data": {
                                "id": f"static_e{edge_counter[0]}",
                                "source": source,
                                "target": target
                            }
                        }
                        static_edges.append(edge)
                        edge_counter[0] += 1
                    
                    continue
            
            # Create edge for simple static value
            if static_value and column_name:
                source = f"{static_value}.{column_name.upper()}"
                target = f"{target_alias}.{column_name.upper()}"
                
                edge = {
                    "data": {
                        "id": f"static_e{edge_counter[0]}",
                        "source": source,
                        "target": target
                    }
                }
                static_edges.append(edge)
                edge_counter[0] += 1
    
    def find_subqueries_with_aliases(text: str) -> List[Tuple[str, str]]:
        """
        Extract all subqueries (SELECT within parentheses) from text along with their aliases.
        Returns list of tuples: (subquery_text, alias)
        """
        subqueries = []
        depth = 0
        current_subquery = ""
        in_subquery = False
        in_quote = False
        subquery_start_pos = -1
        i = 0
        
        while i < len(text):
            char = text[i]
            
            if char == "'" and (i == 0 or text[i-1] != '\\'):
                in_quote = not in_quote
                if in_subquery:
                    current_subquery += char
            elif not in_quote:
                if char == '(':
                    # Check if this starts a SELECT subquery
                    remaining = text[i:].lstrip('(').lstrip()
                    if re.match(r'select\b', remaining, re.IGNORECASE):
                        if depth == 0:
                            in_subquery = True
                            current_subquery = ""
                            subquery_start_pos = i
                        else:
                            current_subquery += char
                        depth += 1
                    elif in_subquery:
                        current_subquery += char
                        depth += 1
                elif char == ')' and in_subquery:
                    depth -= 1
                    if depth == 0:
                        in_subquery = False
                        if current_subquery.strip():
                            # Extract alias after the closing parenthesis
                            alias = "subquery"
                            remaining_text = text[i+1:].lstrip()
                            # Look for AS alias or direct alias
                            alias_match = re.match(r'(?:as\s+)?(\w+)', remaining_text, re.IGNORECASE)
                            if alias_match and alias_match.group(1).lower() not in ['where', 'group', 'order', 'limit', 'union', 'intersect', 'except', 'join', 'inner', 'left', 'right', 'cross', 'on']:
                                alias = alias_match.group(1).lower()  # Normalize to lowercase
                            
                            subqueries.append((current_subquery.strip(), alias))
                        current_subquery = ""
                        subquery_start_pos = -1
                    else:
                        current_subquery += char
                elif in_subquery:
                    current_subquery += char
            else:
                if in_subquery:
                    current_subquery += char
            
            i += 1
        
        return subqueries
    
    def process_query_level(query_text: str, is_main_query: bool = False, subquery_alias: str = None):
        """Recursively process a query at any level (main, CTE, subquery)."""
        # Extract table aliases
        table_aliases = {}
        alias_pattern = r'(?:from|join)\s+(\S+)\s+(?:as\s+)?(\w+)(?:\s|,|$)'
        for match in re.finditer(alias_pattern, query_text, re.IGNORECASE):
            table_name = match.group(1)
            alias = match.group(2)
            if alias and alias.lower() not in ['where', 'inner', 'left', 'right', 'full', 'cross', 'on', 'using', 'and', 'or']:
                table_aliases[table_name] = alias
        
        # Find SELECT clause
        select_match = re.search(r'select\s+(.*?)\s+from', query_text, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return
        
        select_clause = select_match.group(1)
        
        # Determine target alias for this query level
        if is_main_query:
            target_tables = result.target_tables
            if target_tables:
                target_table = str(target_tables[0])
                target_table_parts = target_table.split('.')
                target_table_name = target_table_parts[-1] if target_table_parts else target_table
                target_alias = table_aliases.get(target_table, target_table_name)
            else:
                target_alias = "unknown_target"
        else:
            # For CTEs and subqueries, use the provided alias or default
            target_alias = subquery_alias if subquery_alias else "subquery"
        
        # Process this SELECT clause for static values
        process_select_clause(select_clause, target_alias, static_edges, edge_id_counter)
        
        # Look for subqueries in the entire query text with their aliases
        subqueries_with_aliases = find_subqueries_with_aliases(query_text)
        for subquery_text, subquery_alias_found in subqueries_with_aliases:
            # Recursively process each subquery with its alias
            process_query_level(subquery_text, is_main_query=False, subquery_alias=subquery_alias_found)
    
    try:
        # Get target table
        target_tables = result.target_tables
        if not target_tables:
            return static_edges
        
        query_lower = query.lower()
        
        # Check for CTEs (WITH clause)
        with_match = re.match(r'\s*with\s+', query_lower, re.IGNORECASE)
        if with_match:
            # Process CTEs
            cte_pattern = r'with\s+(.*?)(?:select\s+.*?from)'
            cte_match = re.search(cte_pattern, query, re.IGNORECASE | re.DOTALL)
            
            if cte_match:
                cte_section = cte_match.group(1)
                
                # Extract individual CTEs
                cte_definitions = []
                current_cte = ""
                paren_depth = 0
                in_quote = False
                
                for char in cte_section:
                    if char in ("'", '"') and not in_quote:
                        in_quote = True
                        current_cte += char
                    elif in_quote and (char == "'" or char == '"'):
                        in_quote = False
                        current_cte += char
                    elif char == '(' and not in_quote:
                        paren_depth += 1
                        current_cte += char
                    elif char == ')' and not in_quote:
                        paren_depth -= 1
                        current_cte += char
                        if paren_depth == 0:
                            cte_definitions.append(current_cte)
                            current_cte = ""
                    else:
                        current_cte += char
                
                # Process each CTE
                for cte_def in cte_definitions:
                    # Extract CTE name/alias and the SELECT part
                    cte_name_match = re.match(r'\s*(\w+)\s+as\s*\(', cte_def, re.IGNORECASE)
                    cte_alias = cte_name_match.group(1).lower() if cte_name_match else "cte"
                    
                    # Extract the SELECT part from CTE
                    cte_select_match = re.search(r'\(\s*(select\s+.*)\s*\)', cte_def, re.IGNORECASE | re.DOTALL)
                    if cte_select_match:
                        cte_query = cte_select_match.group(1)
                        process_query_level(cte_query, is_main_query=False, subquery_alias=cte_alias)
        
        # Process main query
        process_query_level(query, is_main_query=True)
    
    except Exception as e:
        print(f"Error extracting static value edges: {e}")
        import traceback
        traceback.print_exc()
    
    return static_edges


def create_lineage_dataframe(all_edges: List[Dict]) -> pd.DataFrame:
    """
    Convert all_edges into a structured dataframe with lineage information.
    
    Args:
        all_edges: List of edge dictionaries with source and target
        
    Returns:
        DataFrame with columns: source_database, source_table, source_column,
                                target_database, target_table, target_column,
                                column_remarks, edge_id
    """
    lineage_data = []
    
    for edge in all_edges:
        edge_id = edge['data']['id']
        source = edge['data']['source']
        target = edge['data']['target']
        
        # Parse source
        source_parts = source.split('.')
        if len(source_parts) == 3:
            source_db, source_table, source_col = source_parts
        elif len(source_parts) == 2:
            source_db = ""
            source_table, source_col = source_parts
            if 'static' in edge_id:
                source_col = source_table 
                source_table = ''
        elif len(source_parts) == 1:
            source_db = ""
            source_table = ""
            source_col = source_parts[0]
        else:
            source_db = ""
            source_table = ""
            source_col = source
        
        # Parse target
        target_parts = target.split('.')
        if len(target_parts) == 3:
            target_db, target_table, target_col = target_parts
        elif len(target_parts) == 2:
            target_db = ""
            target_table, target_col = target_parts
        elif len(target_parts) == 1:
            target_db = ""
            target_table = ""
            target_col = target_parts[0]
        else:
            target_db = ""
            target_table = ""
            target_col = target
        
        # Determine source_remarks
        source_remarks = ""
        
        # Check if source column is "*"
        if '*' in source_col:
            source_remarks = "All columns selected"
        # Check for static values
        elif 'static' in edge_id:
            source_remarks = "Static"
        # Check for CTE/Subquery source
        elif len(source_parts) == 3 and source_db == '<default>':
            source_remarks = "Table"
        # Check if source is from a table
        elif len(source_parts) == 3 and source_db != '<default>' and source_db != '':
            source_remarks = "Table"
        # Check if source has 2 parts (likely CTE/Subquery alias)
        elif len(source_parts) == 2 and source_db == '':
            # Check if source_table is a subquery/CTE alias
            if len(source_table) <= 3 or source_table.lower() in ['cte', 'subquery', 'inner_data', 'outer_data', 'main_query', 'select']:
                source_remarks = "CTE/Subquery"
            else:
                source_remarks = "Table"
        # Check if source has only 1 part (no table reference)
        elif len(source_parts) == 1:
            source_remarks = "CTE/Subquery"
        # Default to Table
        else:
            source_remarks = "Table"
        
        # Determine target_remarks
        target_remarks = ""
        
        # Check if target column is "*"
        if '*' in target_col:
            target_remarks = "All columns selected"
        elif len(target_parts) == 3 and target_db == 'dummy':
            target_remarks = "select"
        # Check for CTE/Subquery target
        elif len(target_parts) == 3 and target_db == '<default>':
            target_remarks = "Table"
        # Check if target is from a table
        elif len(target_parts) == 3 and target_db != '<default>' and target_db != '':
            target_remarks = "Table"
        # Check if target has 2 parts (likely CTE/Subquery alias)
        elif len(target_parts) == 2 and target_db == '':
            # Check if target_table is a subquery/CTE alias
            if len(target_table) <= 3 or target_table.lower() in ['cte', 'subquery', 'inner_data', 'outer_data', 'main_query', 'select']:
                target_remarks = "CTE/Subquery"
            else:
                target_remarks = "Table"
        # Check if target has only 1 part (no table reference)
        elif len(target_parts) == 1:
            target_remarks = "CTE/Subquery"
        # Default to Table
        else:
            target_remarks = "Table"
        
        # Clean up <default> database names
        if source_db == '<default>':
            source_db = ""
        if target_db == '<default>':
            target_db = ""
        
        lineage_data.append({
            'edge_id': edge_id,
            'source_database': source_db,
            'source_table': source_table,
            'source_column': source_col,
            'source_remarks': source_remarks,
            'target_database': target_db,
            'target_table': target_table,
            'target_column': target_col,
            'target_remarks': target_remarks
        })
    
    df = pd.DataFrame(lineage_data)
    return df


def extract_static_values_simple(query: str) -> List[Dict]:
    """
    Simple extraction of static values when LineageRunner is not available.
    Extracts basic literals from SELECT clause.
    """
    static_edges = []
    edge_counter = 0
    
    try:
        # Find SELECT clause
        select_match = re.search(r'select\s+(.*?)\s+from', query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return static_edges
        
        select_clause = select_match.group(1)
        
        # Simple pattern matching for common literals with AS aliases
        patterns = [
            (r"'([^']*)'\s+as\s+(\w+)", lambda m: (m.group(1).upper() if m.group(1) else 'EMPTY_STRING', m.group(2))),  # String (including empty)
            (r'(\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s+as\s+(\w+)', lambda m: (m.group(1), m.group(2))),  # Number
            (r'(true|false)\s+as\s+(\w+)', lambda m: (m.group(1).upper(), m.group(2))),  # Boolean
            (r'null\s+as\s+(\w+)', lambda m: ('NULL', m.group(1))),  # NULL
        ]
        
        for pattern, extract_fn in patterns:
            for match in re.finditer(pattern, select_clause, re.IGNORECASE):
                if len(match.groups()) == 2:
                    value, column = extract_fn(match)
                else:
                    value = match.group(1)
                    column = match.group(2) if len(match.groups()) > 1 else 'unknown_column'
                
                edge = {
                    "data": {
                        "id": f"static_e{edge_counter}",
                        "source": f"{value}.{column.upper()}",
                        "target": f"dummy_table.{column.upper()}"
                    }
                }
                static_edges.append(edge)
                edge_counter += 1
    
    except Exception as e:
        print(f"Error in simple static value extraction: {e}")
    
    return static_edges


def main():
    """Main execution function."""
    if len(sys.argv) > 1:
        args = parse_arguments()
        file_path = args.file
        input_dialect = args.input_dialect
        verbose_mode = args.verbose
    else:
        file_path = 'C:/Users/koush/Downloads/lineage/test.sql'
        input_dialect = 'auto'
        verbose_mode = True

    try:
        with open(file_path, 'r') as f:
            sql_content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return
    
    sql_statements = split_sql_statements(sql_content)

    for query in sql_statements:
        
        original_query = query.strip().lower()
                
        if not original_query:
            continue
        
        converted_query = convert_impala_to_spark_sql(original_query)
        keep_select_and_joins = simplify_query_to_select_and_joins(converted_query)
        
        final_processed_query = keep_select_and_joins
        
        if ("insert into" in final_processed_query) or ("insert overwrite" in final_processed_query) or ("create table" in final_processed_query):
            final_query = final_processed_query
        else:
            # if final_processed_query.startswith("--") or final_processed_query.startswith("/*"):
            #     final_query = final_processed_query
            # else:
            #     final_query = "insert into dummy.dummy_table \n\n" + final_processed_query
                
            final_query = "insert into dummy.dummy_table \n\n" + final_processed_query
                                
        # Comments already removed in split_sql_statements, just strip whitespace
        final_query_no_comments = final_query.strip()
        
        column_edges = []
        static_edges = []
        
        try:
            result = LineageRunner(sql = final_query_no_comments, verbose=False)
            
            column_lineage = result.get_column_lineage()
            
            graph = result._sql_holder.column_lineage_graph
            
            # Get column lineage edges
            column_edges = [
                    {"data": {"id": f"e{i}", "source": str(edge[0]), "target": str(edge[1])}}
                    for i, edge in enumerate(graph.edges)
                ]
            
            # Get static value edges (with successful LineageRunner result)
            static_edges = extract_static_value_edges(query, result)
            
        except Exception as e:
            # If LineageRunner fails, try to extract static values without it
            if verbose_mode:
                print(f"Warning: LineageRunner failed: {e}")
                print("Attempting to extract static values from original query...")
            
            # Create a minimal mock result for static value extraction
            class MockResult:
                def __init__(self):
                    self.target_tables = []
            
            # Try to extract target table from the query
            target_match = re.search(r'insert\s+into\s+(\S+)', query, re.IGNORECASE)
            if target_match:
                mock_result = MockResult()
                mock_result.target_tables = [target_match.group(1)]
                static_edges = extract_static_value_edges(query, mock_result)
            else:
                # Extract static values without target table info
                static_edges = extract_static_values_simple(query)
        
        # Combine column and static edges
        all_edges = column_edges + static_edges
        
        # Create lineage dataframe
        if all_edges:
            lineage_df = create_lineage_dataframe(all_edges)
            
            selective_column = lineage_df.loc[:, ['source_database', 'source_table', 'source_column', 'source_remarks', 'target_column', 'target_remarks']]
            
            selective_column.rename(columns={
                'source_database': 'Database_Name',
                'source_table': 'Table_Name',
                'source_column': 'Column_Name',
                'source_remarks': 'Source_Type',
                'target_column': 'Alias',
                'target_remarks': 'Target_Type'
            }, inplace=True)
            
            # Display dataframe
            print("LINEAGE TABLE")
            print(selective_column.sort_values(by='Alias').to_string(index=False))

        
if __name__ == "__main__":
    main()
