"""
SQL Lineage Parser with Impala to Spark SQL conversion

pip install sqllineage
"""

import pandas as pd
import re
import argparse
import sys
from typing import Dict, List, Tuple
from sqllineage.runner import LineageRunner

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Constants
KEEP_KEYWORDS = {'select', 'from', 'join', 'inner', 'left', 'right', 'full', 'outer', 
                 'cross', 'on', 'using', 'as', 'distinct'}
SET_OPERATORS = {'union', 'intersect', 'except', 'all'}
SKIP_KEYWORDS = {'where', 'group', 'having', 'order', 'limit', 'offset', 'fetch', 'qualify', 'window'}
SQL_KEYWORDS = {'where', 'group', 'order', 'limit', 'union', 'intersect', 'except', 
                'join', 'inner', 'left', 'right', 'cross', 'on', 'and', 'or'}
CTE_INDICATORS = {'cte', 'subquery', 'inner_data', 'outer_data', 'main_query', 'select'}

PARAMETER_PATTERNS = {
    r'#START_DATE#': '20231101',
    r'#END_DATE#': '20231130',
    r'#BATCH_DATE#': '20231101',
    r'#[A-Z_]+#': '20231101'
}

FUNCTION_MAPPINGS = {
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


def simplify_query_to_select_and_joins(query: str) -> str:
    """
    Remove WHERE, GROUP BY, HAVING, ORDER BY, LIMIT clauses from SQL query.
    Keeps only SELECT, FROM, and JOIN clauses at all nesting levels (CTEs, subqueries).
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
        """Simplify a single SQL query, handling CTEs and removing filter/group/sort clauses."""
        sql = sql.strip()
        result_parts = []
        in_single_quote = in_double_quote = False
        
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
    # Apply all parameter replacements
    for pattern, replacement in PARAMETER_PATTERNS.items():
        query = re.sub(pattern, replacement, query)
    
    # Apply SQL cleanup patterns
    cleanup_patterns = [
        (r'PERCENTILE_(CONT|DISC)\s*\(\s*([0-9.]+)\s*\)\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+)\s*\)\s*OVER\s*\(\s*\)', r'PERCENTILE(\2, \3)'),
        (r'(\w+)\s*\(\s*([^)]*)\s*\)\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+)\s*\)', r'\1(\2, \3)'),
        (r'(\bFROM\s+\w+(?:\.\w+)*)\s+by\s+\w+\s+(\bWHERE\b)', r'\1 \2'),
        (r'\bWHERE\s+([a-zA-Z_]\w*\.\w+)\s*=', r'WHERE \1 ='),
        (r'\bDATEADD\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)', r'DATE_ADD(\3, \2)'),
        (r'\bDATESUB\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)', r'DATE_SUB(\1, \2)'),
        (r'\bADD_MONTHS\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)', r'DATE_ADD(\1, INTERVAL \2 MONTH)'),
        (r'\bMONTHS_BETWEEN\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)', r'DATEDIFF(\1, \2) / 30'),
        (r'\bINTERVAL\s+(\d+)\s+YEARS?\b', r'INTERVAL \1 YEAR'),
        (r'\bINTERVAL\s+(\d+)\s+MONTHS?\b', r'INTERVAL \1 MONTH'),
        (r'\bINTERVAL\s+(\d+)\s+DAYS?\b', r'INTERVAL \1 DAY'),
    ]
    
    for pattern, replacement in cleanup_patterns:
        query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
    
    return query.strip()


def convert_impala_to_spark_sql(query: str) -> str:
    """Convert Impala SQL syntax to Spark SQL syntax."""
    query = clean_and_fix_sql(query)
    
    # Apply function mappings
    for pattern, replacement in FUNCTION_MAPPINGS.items():
        query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
    
    # Additional conversion patterns
    conversion_patterns = [
        (r'\bpercentile_approx\s*\(\s*([^)]+)\s*\)', r'percentile_approx(\1, 0.5)'),
        (r'\bGROUP_CONCAT\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)', r'concat_ws(\2, collect_list(\1))'),
        (r'\bGROUP_CONCAT\s*\(\s*DISTINCT\s+([^)\s]+)\s+ORDER\s+BY\s+[^,]+\s*,\s*([^)]+)\s*\)', r'concat_ws(\2, collect_list(DISTINCT \1))'),
        (r'\bGROUP_CONCAT\s*\(\s*([^)]+)\s*\)', r"concat_ws(',', collect_list(\1))"),
        (r"\bDATE\s+'([^']+)'", r"date('\1')"),
        (r"\bTIMESTAMP\s+'([^']+)'", r"timestamp('\1')"),
        (r'\bOFFSET\s+(\d+)\s+ROWS?\b', r'OFFSET \1'),
        (r'\bTRUE\b', 'true'),
        (r'\bFALSE\b', 'false'),
        (r'/\*\s*\+\s*[^*]*\*/', ''),
    ]
    
    for pattern, replacement in conversion_patterns:
        query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
    
    return query.strip()


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
                       default='C:/Users/koush/Downloads/lineage/test.sql',
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
    """Split SQL content into individual statements, handling comments and quoted strings."""
    # Remove comments
    content_no_comments = re.sub(r'--[^\n]*', '', sql_content)
    content_no_comments = re.sub(r'/\*.*?\*/', '', content_no_comments, flags=re.DOTALL)
    
    statements, current = [], ""
    in_single_quote = in_double_quote = False
    
    for i, char in enumerate(content_no_comments):
        is_escaped = i > 0 and content_no_comments[i-1] == '\\'
        
        if char == "'" and not in_double_quote and not is_escaped:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote and not is_escaped:
            in_double_quote = not in_double_quote
        elif char == ';' and not (in_single_quote or in_double_quote):
            if current.strip():
                statements.append(current.strip().lower())
            current = ""
            continue
        
        current += char
    
    if current.strip():
        statements.append(current.strip().lower())
    
    return statements


def parse_column_name_with_type(col_str, node_type_lookup):
    """Parse column name into database, table, column components."""
    parts = str(col_str).split('.')
    
    if len(parts) >= 3:
        database_name, table_name, column_name = parts[0], parts[1], parts[2]
        database_name = 'default' if database_name == '<default>' else database_name
    elif len(parts) == 2:
        database_name, table_name, column_name = 'default', parts[0], parts[1]
    else:
        database_name, table_name, column_name = 'default', 'unknown', parts[0]
    
    object_type = node_type_lookup.get(col_str, 'unknown')
    parent_type = node_type_lookup.get('.'.join(parts[:-1]), 'unknown') if len(parts) >= 2 else 'unknown'
        
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
        items, current, paren_depth = [], "", 0
        in_quote, quote_char = False, None
        
        for char in select_clause:
            if char in ("'", '"') and not in_quote:
                in_quote, quote_char = True, char
            elif in_quote and char == quote_char:
                in_quote, quote_char = False, None
            elif not in_quote:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    if current.strip():
                        items.append(current.strip())
                    current = ""
                    continue
            current += char
        
        if current.strip():
            items.append(current.strip())
        
        return items
    
    def create_edge(source: str, target: str, edge_counter: List[int]) -> Dict:
        """Create an edge dictionary."""
        return {"data": {"id": f"static_e{edge_counter[0]}", "source": source, "target": target}}
    
    def add_edge(static_value: str, column: str, target_alias: str, static_edges: List[Dict], edge_counter: List[int]):
        """Add an edge to the static edges list."""
        edge = create_edge(f"{static_value}.{column.upper()}", f"{target_alias}.{column.upper()}", edge_counter)
        static_edges.append(edge)
        edge_counter[0] += 1
    
    def process_select_clause(select_clause: str, target_alias: str, static_edges: List[Dict], edge_counter: List[int]):
        """Process a SELECT clause to extract static values."""
        # Define patterns with value extractor functions
        patterns = [
            (r"^'([^']*)'(?:\s+as\s+(\w+))?$", lambda m: (m.group(1).upper() or 'EMPTY_STRING', m.group(2) or 'unknown_column')),
            (r'^null\s+as\s+(\w+)$', lambda m: ('NULL', m.group(1))),
            (r'^(true|false)\s+as\s+(\w+)$', lambda m: (m.group(1).upper(), m.group(2))),
            (r"^date\s+'([^']+)'(?:\s+as\s+(\w+))?$", lambda m: (f"DATE_{m.group(1)}", m.group(2) or 'unknown_column')),
            (r"^timestamp\s+'([^']+)'(?:\s+as\s+(\w+))?$", lambda m: (f"TIMESTAMP_{m.group(1).replace(' ', '_').replace(':', '-')}", m.group(2) or 'unknown_column')),
            (r"^interval\s+'([^']+)'\s+(\w+)(?:\s+as\s+(\w+))?$", lambda m: (f"INTERVAL_{m.group(1)}_{m.group(2).upper()}", m.group(3) or 'unknown_column')),
            (r'^(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s+as\s+(\w+)$', lambda m: (m.group(1), m.group(2))),
            (r'^(0x[0-9a-fA-F]+)\s+as\s+(\w+)$', lambda m: (m.group(1).upper(), m.group(2))),
            (r'^array\s*\[([^\]]+)\](?:\s+as\s+(\w+))?$', lambda m: (f"ARRAY_{m.group(1).strip().replace(',', '_').replace(' ', '')}", m.group(2) or 'unknown_column')),
            (r'^map\s*\(([^)]+)\)(?:\s+as\s+(\w+))?$', lambda m: (f"MAP_{m.group(1).strip().replace(',', '_').replace(' ', '').replace(chr(39), '')}", m.group(2) or 'unknown_column')),
        ]
        
        for item in extract_select_items(select_clause):
            item = item.strip()
            
            # Try all patterns
            matched = False
            for pattern, extractor in patterns:
                match = re.match(pattern, item, re.IGNORECASE)
                if match:
                    static_value, column_name = extractor(match)
                    add_edge(static_value, column_name, target_alias, static_edges, edge_counter)
                    matched = True
                    break
            
            # Handle CASE statements separately
            if not matched and re.search(r'\bcase\b', item, re.IGNORECASE):
                alias_match = re.search(r'\bas\s+(\w+)$', item, re.IGNORECASE)
                if alias_match:
                    column_name = alias_match.group(1)
                    then_values = re.findall(r'\bthen\s+\'([^\']+)\'', item, re.IGNORECASE)
                    then_numbers = re.findall(r'\bthen\s+(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b', item, re.IGNORECASE)
                    
                    for val in then_values + then_numbers:
                        add_edge(str(val).upper() if isinstance(val, str) and not val.replace('.', '').replace('-', '').isdigit() else str(val),
                                column_name, target_alias, static_edges, edge_counter)
    
    def find_subqueries_with_aliases(text: str) -> List[Tuple[str, str]]:
        """Extract all subqueries (SELECT within parentheses) with their aliases."""
        subqueries, depth, current, in_subquery = [], 0, "", False
        in_quote, i = False, 0
        
        while i < len(text):
            char = text[i]
            
            if char == "'" and (i == 0 or text[i-1] != '\\'):
                in_quote = not in_quote
                if in_subquery:
                    current += char
            elif not in_quote:
                if char == '(':
                    remaining = text[i:].lstrip('(').lstrip()
                    if re.match(r'select\b', remaining, re.IGNORECASE):
                        if depth == 0:
                            in_subquery, current = True, ""
                        else:
                            current += char
                        depth += 1
                    elif in_subquery:
                        current += char
                        depth += 1
                elif char == ')' and in_subquery:
                    depth -= 1
                    if depth == 0:
                        in_subquery = False
                        if current.strip():
                            alias = "subquery"
                            alias_match = re.match(r'(?:as\s+)?(\w+)', text[i+1:].lstrip(), re.IGNORECASE)
                            if alias_match and alias_match.group(1).lower() not in SQL_KEYWORDS:
                                alias = alias_match.group(1).lower()
                            subqueries.append((current.strip(), alias))
                        current = ""
                    else:
                        current += char
                elif in_subquery:
                    current += char
            elif in_subquery:
                current += char
            i += 1
        
        return subqueries
    
    def process_query_level(query_text: str, is_main_query: bool = False, subquery_alias: str = None):
        """Recursively process a query at any level (main, CTE, subquery)."""
        # Extract table aliases
        table_aliases = {match.group(1): match.group(2) 
                        for match in re.finditer(r'(?:from|join)\s+(\S+)\s+(?:as\s+)?(\w+)(?:\s|,|$)', 
                                                query_text, re.IGNORECASE)
                        if match.group(2) and match.group(2).lower() not in SQL_KEYWORDS}
        
        # Find SELECT clause
        select_match = re.search(r'select\s+(.*?)\s+from', query_text, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return
        
        # Determine target alias
        if is_main_query:
            target_tables = result.target_tables
            if target_tables:
                target_table = str(target_tables[0])
                target_table_name = target_table.split('.')[-1] if '.' in target_table else target_table
                target_alias = table_aliases.get(target_table, target_table_name)
            else:
                target_alias = "unknown_target"
        else:
            target_alias = subquery_alias or "subquery"
        
        # Process SELECT clause and subqueries
        process_select_clause(select_match.group(1), target_alias, static_edges, edge_id_counter)
        
        for subquery_text, subquery_alias_found in find_subqueries_with_aliases(query_text):
            process_query_level(subquery_text, is_main_query=False, subquery_alias=subquery_alias_found)
    
    try:
        if not result.target_tables:
            return static_edges
        
        # Process CTEs if present
        if re.match(r'\s*with\s+', query, re.IGNORECASE):
            cte_match = re.search(r'with\s+(.*?)(?:select\s+.*?from)', query, re.IGNORECASE | re.DOTALL)
            if cte_match:
                # Extract individual CTEs
                cte_definitions, current, paren_depth, in_quote = [], "", 0, False
                
                for char in cte_match.group(1):
                    if char in ("'", '"'):
                        in_quote = not in_quote if not in_quote or char in ("'", '"') else in_quote
                    elif not in_quote:
                        if char == '(':
                            paren_depth += 1
                        elif char == ')':
                            paren_depth -= 1
                            current += char
                            if paren_depth == 0:
                                cte_definitions.append(current)
                                current = ""
                                continue
                    current += char
                
                # Process each CTE
                for cte_def in cte_definitions:
                    cte_name_match = re.match(r'\s*(\w+)\s+as\s*\(', cte_def, re.IGNORECASE)
                    cte_alias = cte_name_match.group(1).lower() if cte_name_match else "cte"
                    
                    cte_select_match = re.search(r'\(\s*(select\s+.*)\s*\)', cte_def, re.IGNORECASE | re.DOTALL)
                    if cte_select_match:
                        process_query_level(cte_select_match.group(1), is_main_query=False, subquery_alias=cte_alias)
        
        # Process main query
        process_query_level(query, is_main_query=True)
    
    except Exception as e:
        print(f"Error extracting static value edges: {e}")
        import traceback
        traceback.print_exc()
    
    return static_edges


def create_lineage_dataframe(all_edges: List[Dict]) -> pd.DataFrame:
    """Convert all_edges into a structured dataframe with lineage information."""
    
    def parse_entity(entity: str, is_source: bool, edge_id: str) -> Tuple[str, str, str]:
        """Parse entity string into database, table, column."""
        parts = entity.split('.')
        
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            # Special handling for static values in source
            if is_source and 'static' in edge_id:
                return "", "", parts[0]
            return "", parts[0], parts[1]
        elif len(parts) == 1:
            return "", "", parts[0]
        else:
            return "", "", entity
    
    def determine_remarks(parts: List[str], db: str, table: str, col: str, edge_id: str, is_source: bool) -> str:
        """Determine remarks based on entity components."""
        if '*' in col:
            return "All columns selected"
        if is_source and 'static' in edge_id:
            return "Static"
        if not is_source and len(parts) == 3 and db == 'dummy':
            return "Select"
        if len(parts) == 3 and db in ('', '<default>'):
            return "Table"
        if len(parts) == 3 and db not in ('', '<default>'):
            return "Table"
        if len(parts) == 2 and db == '':
            return "CTE/Subquery" if len(table) <= 3 or table.lower() in CTE_INDICATORS else "Table"
        if len(parts) == 1:
            return "CTE/Subquery"
        return "Table"
    
    lineage_data = []
    for edge in all_edges:
        edge_id = edge['data']['id']
        source, target = edge['data']['source'], edge['data']['target']
        
        # Parse source and target
        source_parts = source.split('.')
        target_parts = target.split('.')
        source_db, source_table, source_col = parse_entity(source, True, edge_id)
        target_db, target_table, target_col = parse_entity(target, False, edge_id)
        
        # Determine remarks
        source_remarks = determine_remarks(source_parts, source_db, source_table, source_col, edge_id, True)
        target_remarks = determine_remarks(target_parts, target_db, target_table, target_col, edge_id, False)
        
        # Clean up <default> database names
        source_db = "" if source_db == '<default>' else source_db
        target_db = "" if target_db == '<default>' else target_db
        
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
    
    return pd.DataFrame(lineage_data)


def extract_static_values_simple(query: str) -> List[Dict]:
    """Simple extraction of static values when LineageRunner is not available."""
    static_edges, edge_counter = [], 0
    
    try:
        select_match = re.search(r'select\s+(.*?)\s+from', query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return static_edges
        
        # Pattern matching for common literals with AS aliases
        patterns = [
            (r"'([^']*)'\s+as\s+(\w+)", lambda m: (m.group(1).upper() or 'EMPTY_STRING', m.group(2))),
            (r'(\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s+as\s+(\w+)', lambda m: (m.group(1), m.group(2))),
            (r'(true|false)\s+as\s+(\w+)', lambda m: (m.group(1).upper(), m.group(2))),
            (r'null\s+as\s+(\w+)', lambda m: ('NULL', m.group(1))),
        ]
        
        for pattern, extract_fn in patterns:
            for match in re.finditer(pattern, select_match.group(1), re.IGNORECASE):
                value, column = extract_fn(match)
                static_edges.append({
                    "data": {
                        "id": f"static_e{edge_counter}",
                        "source": f"{value}.{column.upper()}",
                        "target": f"dummy_table.{column.upper()}"
                    }
                })
                edge_counter += 1
    
    except Exception as e:
        print(f"Error in simple static value extraction: {e}")
    
    return static_edges


def main():
    """Main execution function."""
    # Parse arguments
    if len(sys.argv) > 1:
        args = parse_arguments()
        file_path, verbose_mode = args.file, args.verbose
    else:
        file_path, verbose_mode = 'C:/Users/koush/Downloads/lineage/test.sql', True

    # Read SQL file
    try:
        with open(file_path, 'r') as f:
            sql_content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return
    
    # Process each SQL statement
    for query in split_sql_statements(sql_content):
        if not query.strip():
            continue
        
        # Convert and simplify query
        converted_query = convert_impala_to_spark_sql(query.strip().lower())
        simplified_query = simplify_query_to_select_and_joins(converted_query)
        
        # Add INSERT INTO wrapper if not present
        has_dml = any(kw in simplified_query for kw in ['insert into', 'insert overwrite', 'create table'])
        final_query = simplified_query if has_dml else f"insert into dummy.dummy_table \n\n{simplified_query}"
        
        column_edges, static_edges = [], []
        
        # Try to extract lineage using LineageRunner
        try:
            result = LineageRunner(sql=final_query.strip(), verbose=False)
            column_lineage = result.get_column_lineage()  # Initialize _sql_holder
            graph = result._sql_holder.column_lineage_graph
            
            column_edges = [{"data": {"id": f"e{i}", "source": str(edge[0]), "target": str(edge[1])}}
                          for i, edge in enumerate(graph.edges)]
            static_edges = extract_static_value_edges(query, result)
            
        except Exception as e:
            if verbose_mode:
                print(f"Warning: LineageRunner failed: {e}")
                print("Attempting to extract static values from original query...")
            
            # Fallback: Try to extract static values with mock result
            target_match = re.search(r'insert\s+into\s+(\S+)', query, re.IGNORECASE)
            if target_match:
                class MockResult:
                    target_tables = [target_match.group(1)]
                static_edges = extract_static_value_edges(query, MockResult())
            else:
                static_edges = extract_static_values_simple(query)
        
        # Create and display lineage dataframe
        if all_edges := column_edges + static_edges:
            lineage_df = create_lineage_dataframe(all_edges)
            
            output_df = lineage_df[['source_database', 'source_table', 'source_column', 
                                    'source_remarks', 'target_column', 'target_remarks']].rename(columns={
                'source_database': 'Database_Name',
                'source_table': 'Table_Name',
                'source_column': 'Column_Name',
                'source_remarks': 'Source_Type',
                'target_column': 'Alias',
                'target_remarks': 'Target_Type'
            })
            
            print("LINEAGE TABLE")
            print(output_df.sort_values(by='Alias').to_string(index=False))


if __name__ == "__main__":
    main()
