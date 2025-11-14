"""
SQL Lineage Parser
==================
A production-ready tool to parse SQL queries and extract column-level lineage information.

Author: Optimized Version
Date: November 14, 2025
Version: 3.1

Features:
- Query key tagging for tracking lineage by query
- Ambiguous/Internal remark classification
- Mongo and Elastic query detection
- Failed query tracking
- Optimized regex patterns for better performance
- Helper methods to reduce code duplication

Usage:
    python sql_lineage_parser.py <sql_file1> <sql_file2> ...
    
Example:
    python sql_lineage_parser.py query.sql
    python sql_lineage_parser.py query1.sql query2.sql query3.sql
"""

import re
import sqlparse
import pandas as pd
import os
import sys
from typing import List, Dict, Union
from pathlib import Path


class SQLLineageParser:
    """Main parser class for extracting SQL column lineage"""
    
    # Pre-compiled regex patterns for performance
    SUBQUERY_PATTERN = re.compile(r'^\s*SELECT\s+.*?\s+FROM\s*\(', re.IGNORECASE | re.DOTALL)
    FROM_PATTERN = re.compile(r'\bFROM\s+([^()]+?)(?:\s+(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN|\s+WHERE|\s+GROUP\s+BY|\s+HAVING|\s+ORDER\s+BY|\s*$)', re.IGNORECASE | re.DOTALL)
    SELECT_PATTERN = re.compile(r'\bSELECT\s+(.*?)(?=\s+FROM\s+)', re.IGNORECASE | re.DOTALL)
    JOIN_PATTERN = re.compile(r'(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN\s+([\w_.]+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.*?)(?=\s+(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN|\s+WHERE|\s+GROUP\s+BY|\s+ORDER\s+BY|\s*$)', re.IGNORECASE | re.DOTALL)
    ALIAS_PATTERN = re.compile(r'(.*?)\s+AS\s+([\w_]+)', re.IGNORECASE)
    STAR_PATTERN = re.compile(r'(\w+)\.\*')
    WINDOW_FUNCTION_PATTERN = re.compile(r'\bOVER\s*\(', re.IGNORECASE)
    
    def __init__(self):
        self.ignored_keywords = ['elastic_query', 'mongo_query']
    
    def detect_query_type(self, query_text: str) -> str:
        """
        Detect if query is SQL, Mongo, or Elastic
        
        Returns: 'SQL', 'Mongo', or 'Elastic'
        """
        query_trimmed = query_text.strip()
        
        # Check for Mongo query (starts with { or contains MongoDB patterns)
        if query_trimmed.startswith('{') or 'db.' in query_trimmed[:50]:
            return 'Mongo'
        
        # Check for Elastic query (starts with "Kly" or contains Elasticsearch patterns)
        if query_trimmed.startswith('Kly') or query_trimmed.startswith('GET ') or query_trimmed.startswith('POST '):
            return 'Elastic'
        
        # Default to SQL
        return 'SQL'
    
    def _create_non_sql_record(self, query_type: str, query_key: str, view_name: str) -> Dict[str, str]:
        """
        Create a record for non-SQL queries (Mongo or Elastic)
        
        Args:
            query_type: 'Mongo' or 'Elastic'
            query_key: Unique identifier for the query
            view_name: Name or path for display
            
        Returns:
            Dictionary with lineage record for non-SQL query
        """
        return {
            'query_key': query_key,
            'database_name': '',
            'table_name': '',
            'column_name': '',
            'alias_name': '',
            'view_name': view_name,
            'remarks': f'{query_type} Query',
            'status': 'success',
            'layer_order': 0
        }
    
    def _create_error_record(self, query_key: str, error_message: str, view_name: str = '') -> Dict[str, str]:
        """
        Create a record for failed query processing
        
        Args:
            query_key: Unique identifier for the query
            error_message: Description of the error
            view_name: Name or path for display
            
        Returns:
            Dictionary with error record
        """
        return {
            'query_key': query_key,
            'database_name': 'ERROR',
            'table_name': 'ERROR',
            'column_name': 'ERROR',
            'alias_name': 'ERROR',
            'view_name': view_name,
            'remarks': error_message,
            'status': 'failed',
            'layer_order': 0
        }
    
    def parse_sql_files(self, sql_files: List[str]) -> pd.DataFrame:
        """
        Parse multiple SQL files and extract column lineage information
        
        Args:
            sql_files: List of SQL file paths to parse
            
        Returns:
            DataFrame with columns: Query Key, Database Name, Table Name, Column Name, 
                                   Alias Name, Remarks, Status
        """
        all_lineage_data = []
        
        for sql_file in sql_files:
            if not os.path.exists(sql_file):
                print(f"WARNING: File {sql_file} not found, skipping...")
                continue
                
            try:
                with open(sql_file, 'r', encoding='utf-8') as file:
                    sql_content = file.read()
                
                # Generate query key from filename
                query_key = Path(sql_file).stem.upper()
                
                print(f"Processing file: {sql_file} (Query Key: {query_key})")
                
                # Detect query type
                query_type = self.detect_query_type(sql_content)
                
                if query_type in ['Mongo', 'Elastic']:
                    # Handle non-SQL queries
                    lineage_data = [self._create_non_sql_record(query_type, query_key, sql_file)]
                    print(f"SUCCESS: Detected {query_type} Query")
                else:
                    # Handle SQL query
                    lineage_data = self.parse_single_sql(sql_content, sql_file, query_key)
                    # Add status to all records
                    for record in lineage_data:
                        record['status'] = 'success'
                    print(f"SUCCESS: Extracted {len(lineage_data)} column mappings")
                
                all_lineage_data.extend(lineage_data)
                
            except Exception as e:
                print(f"ERROR processing file {sql_file}: {str(e)}")
                query_key = Path(sql_file).stem.upper()
                all_lineage_data.append(self._create_error_record(
                    query_key, 
                    f'failure_processing_file: {str(e)}',
                    sql_file
                ))
            
        return self.create_lineage_dataframe(all_lineage_data)
    
    def parse_query_dictionary(self, queries: Dict[str, str]) -> pd.DataFrame:
        """
        Parse multiple SQL queries from a dictionary
        
        Args:
            queries: Dictionary where key is query identifier (e.g., 'SQ1', 'SQ2')
                    and value is the SQL query text
                    
        Returns:
            DataFrame with columns: Database Name, Table Name, Column Name, 
                                   Alias Name, Remarks, Status, View Name
                                   
        Example:
            queries = {
                'SQ1': 'SELECT id, name FROM customers',
                'SQ2': 'SELECT * FROM products',
                'SQ3': 'db.collection.find({})'  # Mongo query
            }
            df = parser.parse_query_dictionary(queries)
        """
        all_lineage_data = []
        
        for query_key, sql_text in queries.items():
            print(f"Processing query: {query_key}")
            
            try:
                # Detect query type
                query_type = self.detect_query_type(sql_text)
                
                if query_type in ['Mongo', 'Elastic']:
                    # Handle non-SQL queries
                    lineage_data = [self._create_non_sql_record(query_type, query_key, query_key)]
                    print(f"SUCCESS: Detected {query_type} Query for {query_key}")
                else:
                    # Handle SQL query
                    lineage_data = self.parse_single_sql(sql_text, query_key, query_key)
                    # Add status to all records
                    for record in lineage_data:
                        record['status'] = 'success'
                    print(f"SUCCESS: Extracted {len(lineage_data)} column mappings for {query_key}")
                
                all_lineage_data.extend(lineage_data)
                
            except Exception as e:
                print(f"ERROR processing query {query_key}: {str(e)}")
                all_lineage_data.append(self._create_error_record(
                    query_key,
                    f'query_failed: {str(e)}',
                    query_key
                ))
        
        return self.create_lineage_dataframe(all_lineage_data)
    
    def parse_single_sql(self, sql_content: str, filename: str, query_key: str = 'UNKNOWN') -> List[Dict[str, str]]:
        """Parse a single SQL query and extract lineage information"""
        lineage_data = []
        
        # Clean and normalize SQL
        sql_content = self.clean_sql(sql_content)
        
        if not sql_content.strip():
            return [{
                'query_key': query_key,
                'database_name': 'N/A',
                'table_name': 'N/A', 
                'column_name': 'N/A',
                'alias_name': 'N/A',
                'remarks': 'empty_sql_content',
                'layer_order': 0
            }]
        
        try:
            # Parse SQL using sqlparse
            parsed = sqlparse.parse(sql_content)
            
            if not parsed:
                return [{
                    'query_key': query_key,
                    'database_name': 'N/A',
                    'table_name': 'N/A',
                    'column_name': 'N/A',
                    'alias_name': 'N/A',
                    'remarks': 'failed_to_parse_sql',
                    'layer_order': 0
                }]
                
            # Process each statement
            for stmt in parsed:
                if not str(stmt).strip() or str(stmt).strip() == ';':
                    continue
                    
                stmt_lineage = self.process_sql_statement(stmt, filename, query_key)
                lineage_data.extend(stmt_lineage)
                    
        except Exception as e:
            print(f"ERROR parsing SQL: {str(e)}")
            lineage_data.append({
                'query_key': query_key,
                'database_name': 'ERROR',
                'table_name': 'ERROR',
                'column_name': 'ERROR', 
                'alias_name': 'ERROR',
                'remarks': f'failure_sql_lineage_tech: {str(e)}',
                'layer_order': 0
            })
            
        return lineage_data
    
    def process_sql_statement(self, stmt, filename: str, query_key: str = 'UNKNOWN') -> List[Dict[str, str]]:
        """Process a single SQL statement"""
        lineage_data = []
        sql_text = str(stmt).strip()
        
        # Check if this is a SELECT from subquery (captures both SELECT * and SELECT columns)
        if self.SUBQUERY_PATTERN.match(sql_text):
            # This is selecting from a subquery - trace through to actual sources
            
            # Extract outer SELECT columns
            outer_select_match = re.search(r'^\s*SELECT\s+(.*?)\s+FROM\s*\(', sql_text, re.IGNORECASE | re.DOTALL)
            outer_columns_text = outer_select_match.group(1).strip() if outer_select_match else None
            
            # Extract the subquery and its alias
            subquery_match = re.search(r'FROM\s*\((.*)\)\s+(\w+)', sql_text, re.IGNORECASE | re.DOTALL)
            if subquery_match:
                subquery_text = subquery_match.group(1).strip()
                subquery_alias = subquery_match.group(2).strip()
                
                # Build mapping of inner column aliases to outer final names
                # Key: inner query alias name -> Value: outer query final name
                outer_col_map = {}
                select_all_from_subquery = False  # Flag for SELECT alias.* pattern
                
                if outer_columns_text and outer_columns_text.upper() != 'DISTINCT':
                    # Check if it's SELECT alias.* pattern (e.g., "SELECT al.*")
                    star_pattern = re.match(r'^(\w+)\.\*$', outer_columns_text.strip(), re.IGNORECASE)
                    if star_pattern and star_pattern.group(1).upper() == subquery_alias.upper():
                        # SELECT al.* - means select all columns from subquery with their inner aliases
                        select_all_from_subquery = True
                    elif outer_columns_text != '*':
                        outer_cols = self.split_sql_columns(outer_columns_text)
                        
                        for outer_col in outer_cols:
                            # Parse outer column: subquery_alias.column_name [AS final_alias]
                            outer_parsed = self.parse_column_expression(outer_col)
                            
                            if outer_parsed and outer_parsed.get('table_name') == subquery_alias:
                                # Column referenced from subquery
                                inner_ref_name = outer_parsed.get('column_name')  # What the outer query calls from inner
                                final_name = outer_parsed.get('alias_name')  # Get explicit alias or None
                                
                                # If no explicit alias, use the column name from outer query
                                if not final_name:
                                    final_name = inner_ref_name
                                
                                if inner_ref_name and final_name:
                                    # Map: inner_ref_name -> final_name
                                    outer_col_map[inner_ref_name.upper()] = final_name
                
                # Parse the inner query to get actual source columns
                inner_parsed = sqlparse.parse(subquery_text)
                if inner_parsed:
                    for inner_stmt in inner_parsed:
                        inner_lineage = self.process_inner_statement(inner_stmt, filename, query_key)
                        
                        # Update aliases based on outer query mapping
                        filtered_lineage = []
                        for entry in inner_lineage:
                            # Use alias if present, otherwise use column name for matching
                            inner_output_name = entry.get('alias_name') or entry.get('column_name', '')
                            
                            # If outer query is SELECT alias.*, include all inner columns
                            if select_all_from_subquery:
                                matched_outer = inner_output_name  # Keep the same alias from inner query
                            else:
                                # Check if outer query references this column (exact match or partial match)
                                matched_outer = None
                                best_match_score = 0
                                best_match = None
                                
                                for outer_ref, outer_final in outer_col_map.items():
                                    match_score = 0
                                    match_type = None
                                    
                                    # Try exact match first (highest priority)
                                    if inner_output_name.upper() == outer_ref:
                                        match_score = 1000
                                        match_type = "EXACT"
                                        matched_outer = outer_final
                                        break  # Perfect match, stop searching
                                    
                                    # Try substring match
                                    if inner_output_name.upper() in outer_ref:
                                        # Inner is substring of outer (e.g., INCORPORATED_COUNTRY in EMP_INCORPORATED_ADDRESS_COUNTRY)
                                        match_score = 100
                                        match_type = "SUBSTRING"
                                    elif outer_ref in inner_output_name.upper():
                                        # Outer is substring of inner (less likely but possible)
                                        match_score = 90
                                        match_type = "SUBSTRING_REV"
                                    
                                    # Try fuzzy match based on common words (but only if no substring match)
                                    if match_score == 0:
                                        inner_parts = set(inner_output_name.upper().split('_'))
                                        outer_parts = set(outer_ref.split('_'))
                                        common = inner_parts & outer_parts
                                        
                                        # Filter out common prefixes like EMP, TBL, etc.
                                        common_meaningful = common - {'EMP', 'TBL', 'DIM', 'FACT', 'ACTV'}
                                        
                                        if common_meaningful:
                                            # Calculate match quality
                                            min_parts = min(len(inner_parts), len(outer_parts))
                                            overlap_ratio = len(common_meaningful) / min_parts
                                            
                                            # Require at least 50% overlap of meaningful parts
                                            if overlap_ratio >= 0.5:
                                                match_score = int(overlap_ratio * 50)  # Score 25-50
                                                match_type = "FUZZY"
                                    
                                    # Track best match
                                    if match_score > best_match_score:
                                        best_match_score = match_score
                                        best_match = (outer_ref, outer_final, match_type, common if match_type == "FUZZY" else None)
                                
                                # Use best match if found
                                if not matched_outer and best_match and best_match_score >= 25:  # Minimum threshold
                                    outer_ref, outer_final, match_type, common = best_match
                                    matched_outer = outer_final
                            
                            if matched_outer:
                                # Layer 1: Source table (physical database.table.column)
                                source_row = entry.copy()
                                source_row['query_key'] = query_key
                                source_row['subquery_alias'] = ''
                                source_row['subquery_column'] = ''
                                source_row['layer_order'] = 1  # For sorting
                                filtered_lineage.append(source_row)
                                
                                # Layer 2: Table Alias in inner query (e.g., T.DMH_BUSINESS_DATE or just the column for constants)
                                # Get the table alias used in the inner query
                                inner_table_alias = entry.get('table_alias', '')  # This is the alias like 'T', 'PARTY', 'EMP'
                                inner_col_name = entry.get('column_name')
                                inner_col_alias = entry.get('alias_name')  # Alias in inner query like 'BUSINESS_DATE'
                                
                                # Always add Layer 2 (for both table columns and constants/derived)
                                table_alias_row = {
                                    'query_key': query_key,
                                    'database_name': '',
                                    'table_name': inner_table_alias,  # Table alias (e.g., T) or empty for constants
                                    'column_name': inner_col_name,  # Column name (e.g., DMH_BUSINESS_DATE or constant value)
                                    'subquery_alias': '',
                                    'subquery_column': '',
                                    'alias_name': inner_col_alias if (inner_col_alias and inner_col_alias != inner_col_name) else '',
                                    'remarks': 'Inner Query Alias Layer',
                                    'layer_order': 2  # For sorting
                                }
                                filtered_lineage.append(table_alias_row)
                                
                                # Layer 3: Subquery alias (e.g., EMP.INCORPORATED_COUNTRY)
                                # Only set alias if it's different from column name
                                final_alias = matched_outer if matched_outer != inner_output_name else ''
                                
                                subquery_row = {
                                    'query_key': query_key,
                                    'database_name': '',
                                    'table_name': subquery_alias,  # Subquery alias as table name (e.g., EMP)
                                    'column_name': inner_output_name,  # Column from subquery (e.g., INCORPORATED_COUNTRY)
                                    'subquery_alias': '',
                                    'subquery_column': '',
                                    'alias_name': final_alias,  # Final alias from outer query
                                    'remarks': 'Subquery Layer',
                                    'layer_order': 3  # For sorting
                                }
                                filtered_lineage.append(subquery_row)
                        
                        lineage_data.extend(filtered_lineage)
            
            return lineage_data
        
        # Extract main query information
        main_tables = self.extract_main_tables(stmt)
        join_info = self.extract_join_info(stmt)
        
        # Process SELECT columns
        select_columns = self.extract_select_columns(stmt)
        
        for col_info in select_columns:
            lineage_entry = self.process_column_lineage(col_info, main_tables, join_info, filename, 0, query_key)
            if lineage_entry:
                lineage_data.append(lineage_entry)
                
        return lineage_data
    
    def process_inner_statement(self, stmt, filename: str, query_key: str = 'UNKNOWN') -> List[Dict[str, str]]:
        """Process an inner/nested SQL statement"""
        lineage_data = []
        
        # Extract tables and joins from inner query
        main_tables = self.extract_main_tables(stmt)
        join_info = self.extract_join_info(stmt)
        
        # Process SELECT columns
        select_columns = self.extract_select_columns(stmt)
        
        for col_info in select_columns:
            lineage_entry = self.process_column_lineage(col_info, main_tables, join_info, filename, 0, query_key)
            if lineage_entry:
                lineage_data.append(lineage_entry)
                
        return lineage_data
    
    def clean_sql(self, sql_content: str) -> str:
        """Clean SQL content by removing comments and normalizing"""
        # Remove single line comments
        sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
        # Remove multi-line comments (including hints like /*+ BROADCAST */)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        # Remove extra whitespace but keep newlines for parsing
        sql_content = re.sub(r'\s+', ' ', sql_content)
        return sql_content.strip()
    
    def get_remarks(self, sql_content: str) -> str:
        """Determine remarks based on SQL patterns"""
        sql_lower = sql_content.lower()
        
        if sql_lower.strip().startswith('{'):
            return "ignored_elastic_query"
            
        return ""
    
    def extract_main_tables(self, stmt) -> List[Dict[str, str]]:
        """Extract main tables with their aliases and database information"""
        tables = []
        sql_text = str(stmt).strip()
        
        # Enhanced FROM clause extraction
        from_match = self.FROM_PATTERN.search(sql_text)
        
        if from_match:
            from_clause = from_match.group(1).strip()
            
            # Extract tables with optional database.schema.table patterns and aliases
            table_pattern = r'([\w_]+(?:\.[\w_]+)*)(?:\s+(?:AS\s+)?(\w+))?'
            table_matches = re.finditer(table_pattern, from_clause, re.IGNORECASE)
            
            for match in table_matches:
                full_table_name = match.group(1)
                table_alias = match.group(2)
                
                # Skip SQL keywords that might match
                if full_table_name.upper() in ['AS', 'ON', 'AND', 'OR']:
                    continue
                
                # Parse database.schema.table pattern
                parts = full_table_name.split('.')
                if len(parts) >= 3:
                    database_name = parts[0]
                    schema_name = parts[1]
                    table_name = parts[2]
                elif len(parts) == 2:
                    database_name = parts[0]
                    schema_name = 'dbo'
                    table_name = parts[1]
                else:
                    database_name = 'N/A'
                    schema_name = 'dbo'
                    table_name = parts[0]
                
                tables.append({
                    'full_name': full_table_name,
                    'database_name': database_name,
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'alias': table_alias or table_name
                })
                        
        return tables
    
    def extract_select_columns(self, stmt) -> List[Dict[str, str]]:
        """Extract columns from SELECT clause with improved parsing"""
        columns = []
        sql_text = str(stmt).strip()
        
        # Skip if not a SELECT statement
        if not self.SELECT_PATTERN.search(sql_text):
            return columns
        
        # Find SELECT clause
        select_match = self.SELECT_PATTERN.search(sql_text)
        if not select_match:
            return columns
            
        select_clause = select_match.group(1).strip()
        
        # Split columns properly
        column_expressions = self.split_sql_columns(select_clause)
        
        for col_expr in column_expressions:
            col_info = self.parse_column_expression(col_expr)
            if col_info:
                columns.append(col_info)
                
        return columns
    
    def split_sql_columns(self, select_clause: str) -> List[str]:
        """Split SQL SELECT clause into individual column expressions"""
        columns = []
        current = ""
        paren_depth = 0
        in_quotes = False
        quote_char = None
        
        for char in select_clause:
            if char in ['"', "'"] and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
            elif char == '(' and not in_quotes:
                paren_depth += 1
            elif char == ')' and not in_quotes:
                paren_depth -= 1
            elif char == ',' and paren_depth == 0 and not in_quotes:
                if current.strip():
                    columns.append(current.strip())
                current = ""
                continue
                
            current += char
            
        if current.strip():
            columns.append(current.strip())
            
        return columns
    
    def parse_column_expression(self, col_expr: str) -> Dict[str, str]:
        """Parse a single column expression into its components"""
        col_expr = col_expr.strip()
        if not col_expr:
            return None
            
        table_name = None
        column_name = None
        alias_name = None
        
        # Check for window functions (ROW_NUMBER, RANK, etc.)
        if self.WINDOW_FUNCTION_PATTERN.search(col_expr):
            # Extract the alias after the window function
            alias_match = re.search(r'\)\s+AS\s+([\w_]+)', col_expr, re.IGNORECASE)
            if alias_match:
                alias_name = alias_match.group(1).strip()
            return {
                'original_expression': col_expr,
                'table_name': None,
                'column_name': alias_name or 'window_function',
                'alias_name': alias_name or 'window_function',
                'is_star': False,
                'is_derived': True
            }
        
        # Check for table.* pattern first
        star_match = self.STAR_PATTERN.match(col_expr)
        if star_match:
            table_name = star_match.group(1)
            column_name = '*'
            alias_name = f"{table_name}.*"
            return {
                'original_expression': col_expr,
                'table_name': table_name,
                'column_name': column_name,
                'alias_name': alias_name,
                'is_star': True,
                'is_derived': False
            }
        
        # Handle AS alias
        alias_match = self.ALIAS_PATTERN.search(col_expr)
        if alias_match:
            base_expr = alias_match.group(1).strip()
            alias_name = alias_match.group(2).strip()
        else:
            base_expr = col_expr
            alias_name = None  # No explicit alias
        
        # Check if this is a function or derived column
        is_derived = bool(re.search(r'\(.*\)', base_expr))
        
        # Extract table and column from base expression
        if '.' in base_expr:
            # Handle table.column pattern
            parts = re.findall(r'[\w_]+', base_expr)
            if len(parts) >= 2:
                table_name = parts[-2]
                column_name = parts[-1]
            elif len(parts) == 1:
                column_name = parts[0]
        else:
            # Simple column reference
            parts = re.findall(r'[\w_]+', base_expr)
            if parts:
                column_name = parts[0]
        
        return {
            'original_expression': col_expr,
            'table_name': table_name,
            'column_name': column_name or base_expr,
            'alias_name': alias_name,  # Keep None if no explicit alias
            'is_star': False,
            'is_derived': is_derived
        }
    
    def extract_join_info(self, stmt) -> List[Dict[str, str]]:
        """Extract JOIN information with database context"""
        joins = []
        sql_text = str(stmt).strip()
        
        # Use pre-compiled pattern to match different types of JOINs
        join_matches = self.JOIN_PATTERN.finditer(sql_text)
        
        for match in join_matches:
            full_table_name = match.group(1)
            table_alias = match.group(2)
            join_condition = match.group(3).strip()
            
            # Parse database.schema.table pattern
            parts = full_table_name.split('.')
            if len(parts) >= 3:
                database_name = parts[0]
                schema_name = parts[1]
                table_name = parts[2]
            elif len(parts) == 2:
                database_name = parts[0]
                schema_name = 'dbo'
                table_name = parts[1]
            else:
                database_name = 'N/A'
                schema_name = 'dbo'
                table_name = parts[0]
            
            joins.append({
                'full_name': full_table_name,
                'database_name': database_name,
                'schema_name': schema_name,
                'table_name': table_name,
                'table_alias': table_alias or table_name,
                'condition': join_condition
            })
            
        return joins
    
    def process_column_lineage(self, col_info: Dict, parent_tables: List, join_info: List, filename: str, level: int, query_key: str = 'UNKNOWN') -> Dict[str, str]:
        """Process a single column to determine its lineage"""
        remarks = ""
        
        # Handle derived columns (window functions, etc.)
        if col_info.get('is_derived'):
            # Check if it's a window function
            original_expr = col_info.get('original_expression', '').upper()
            if 'OVER(' in original_expr or 'OVER (' in original_expr:
                return {
                    'query_key': query_key,
                    'database_name': 'N/A',
                    'table_name': 'N/A',
                    'column_name': col_info.get('alias_name', 'derived'),
                    'alias_name': col_info.get('alias_name', 'derived'),
                    'remarks': 'derived_column_window_function',
                    'layer_order': 0
                }
            else:
                remarks = "derived_column"
        
        # Skip single character table names unless they're valid aliases
        if col_info.get('table_name') and len(col_info['table_name']) == 1:
            valid_alias = any(t['alias'] == col_info['table_name'] for t in parent_tables) or \
                         any(j['table_alias'] == col_info['table_name'] for j in join_info)
            if not valid_alias:
                return None
        
        # Determine database and table names
        original_table_alias = col_info.get('table_name')  # Store original alias (e.g., 'PARTY')
        database_name, table_name = self.resolve_table_reference(col_info, parent_tables, join_info)
        
        # Handle column names
        column_name = col_info.get('column_name', '')
        alias_name = col_info.get('alias_name', '')
        
        # Handle star expressions - if table.*, leave it as is
        if col_info.get('is_star'):
            return {
                'query_key': query_key,
                'database_name': database_name or 'unknown',
                'table_name': table_name or 'unknown',
                'column_name': '*',
                'alias_name': f"{table_name}.*" if table_name != 'unknown' else '*',
                'layer_order': 0,
                'remarks': 'all_columns_selected'
            }
        
        # Determine remarks for derived columns
        original_expr = col_info.get('original_expression', '').upper()
        if not remarks:
            if 'ROW_NUMBER()' in original_expr or 'RANK()' in original_expr or 'DENSE_RANK()' in original_expr:
                remarks = "derived_column_window_function"
            elif any(func in original_expr for func in ['COALESCE(', 'CASE WHEN', 'NVL(', 'CONCAT(', 'CAST(']):
                remarks = "derived_column"
            elif any(keyword in original_expr for keyword in ["'ACCOUNTMNEMONIC'", "'PRIMO'", "'L'", "'BR'", "'FALSE'", "'TRUE'"]):
                remarks = "constant_value"
            elif database_name == 'N/A':
                remarks = "database_not_specified_in_query"
            elif not table_name or table_name == 'unknown':
                if len(parent_tables) + len(join_info) > 1:
                    remarks = "table_name_ambiguous"
                else:
                    remarks = ""
            
        # Only set alias_name if it's different from column_name
        final_alias = alias_name if (alias_name and alias_name != column_name) else ''
        
        return {
            'query_key': query_key,
            'database_name': database_name or 'unknown',
            'table_name': table_name or 'unknown',
            'table_alias': original_table_alias,  # Keep the original alias
            'column_name': column_name or 'unknown',
            'alias_name': final_alias,
            'remarks': remarks,
            'layer_order': 0
        }
    
    def resolve_table_reference(self, col_info: Dict, parent_tables: List, join_info: List) -> tuple:
        """Resolve table references to get actual database and table names"""
        col_table_name = col_info.get('table_name')
        
        if not col_table_name:
            # If no table specified and only one table, use that
            if len(parent_tables) == 1:
                return parent_tables[0]['database_name'], parent_tables[0]['table_name']
            return 'unknown', 'unknown'
        
        # Check if this matches any main table alias
        for table in parent_tables:
            if table['alias'] == col_table_name:
                return table['database_name'], table['table_name']
        
        # Check if this matches any join table alias
        for join in join_info:
            if join.get('table_alias') == col_table_name:
                return join['database_name'], join['table_name']
        
        # Check if this is an actual table name (not an alias)
        for table in parent_tables:
            if table['table_name'] == col_table_name:
                return table['database_name'], table['table_name']
        
        # Check joins for actual table name
        for join in join_info:
            if join['table_name'] == col_table_name:
                return join['database_name'], join['table_name']
        
        # If we have a table name but can't resolve it, try to extract from context
        if '.' in col_table_name:
            parts = col_table_name.split('.')
            if len(parts) >= 3:
                return parts[0], parts[2]
            elif len(parts) == 2:
                return parts[0], parts[1]
        
        return 'unknown', col_table_name
    
    def create_lineage_dataframe(self, lineage_data: List[Dict[str, str]]) -> pd.DataFrame:
        """Create final DataFrame in the desired format with ambiguous/internal logic"""
        if not lineage_data:
            return pd.DataFrame(columns=[
                'Database Name', 'Table Name', 'Column Name', 
                'Alias Name', 'Remarks'
            ])
            
        df = pd.DataFrame(lineage_data)
        
        # Apply ambiguous/internal logic before renaming
        # Rule 1: If both database_name and table_name are empty/missing but column_name exists -> ambiguous
        # Rule 2: If database_name is missing but table_name exists -> internal
        def apply_remark_logic(row):
            db_name = str(row.get('database_name', '')).strip()
            tbl_name = str(row.get('table_name', '')).strip()
            col_name = str(row.get('column_name', '')).strip()
            current_remark = str(row.get('remarks', '')).strip()
            
            # Skip if already has specific remarks
            if current_remark and current_remark not in ['', 'N/A']:
                return current_remark
            
            # Rule 1: Ambiguous case
            if (not db_name or db_name == '' or db_name == 'unknown') and \
               (not tbl_name or tbl_name == '' or tbl_name == 'unknown') and \
               col_name and col_name != '' and col_name != 'unknown':
                return 'ambiguous'
            
            # Rule 2: Internal case
            if (not db_name or db_name == '' or db_name == 'unknown') and \
               tbl_name and tbl_name != '' and tbl_name != 'unknown':
                return 'internal'
            
            return current_remark
        
        if 'remarks' in df.columns:
            df['remarks'] = df.apply(apply_remark_logic, axis=1)
        
        # Rename columns to final format
        df = df.rename(columns={
            'database_name': 'Database Name',
            'table_name': 'Table Name', 
            'column_name': 'Column Name',
            'alias_name': 'Alias Name',
            'remarks': 'Remarks',
            'view_name': 'View Name'
        })
        
        # Define column order (excluding Query Key and Status from output)
        column_order = [
            'Database Name', 'Table Name', 'Column Name',
            'Alias Name', 'Remarks', 'View Name'
        ]
        
        # Keep only the columns that exist in the DataFrame
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        # Remove duplicates and sort by layer order to maintain proper sequence
        df = df.drop_duplicates().reset_index(drop=True)
        
        # Sort by layer_order if it exists (query_key kept internally but not in output)
        if 'layer_order' in df.columns:
            df = df.sort_values(['layer_order'])
            df = df.drop(columns=['layer_order'])  # Remove helper column from final output
        
        return df


def main():
    """Main entry point for command-line usage"""
    print("=" * 80)
    print("SQL LINEAGE PARSER v2.0")
    print("=" * 80)
    print()
    
    # Check if files are provided as command-line arguments
    if len(sys.argv) > 1:
        sql_files = sys.argv[1:]
    else:
        # Interactive mode - ask for files
        print("No SQL files provided as arguments.")
        print()
        file_input = input("Enter SQL file path(s) separated by spaces: ").strip()
        if not file_input:
            print("ERROR: No files specified. Exiting.")
            sys.exit(1)
        sql_files = file_input.split()
    
    # Validate files exist
    valid_files = []
    for f in sql_files:
        if os.path.exists(f):
            valid_files.append(f)
        else:
            print(f"Warning: File '{f}' not found, skipping...")
    
    if not valid_files:
        print("Error: No valid SQL files found. Exiting.")
        sys.exit(1)
    
    print(f"\nProcessing {len(valid_files)} file(s)...")
    print()
    
    # Parse SQL files
    parser = SQLLineageParser()
    result_df = parser.parse_sql_files(valid_files)
    
    # Display results
    print()
    print("=" * 80)
    print("LINEAGE ANALYSIS RESULTS")
    print("=" * 80)
    print()
    print(result_df.to_string(index=False))
    
    # Save to CSV
    output_file = 'sql_lineage_output.csv'
    result_df.to_csv(output_file, index=False)
    
    # Summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"SUCCESS: Total columns found: {len(result_df)}")
    print(f"SUCCESS: Unique databases: {result_df['Database Name'].nunique()}")
    print(f"SUCCESS: Unique tables: {result_df['Table Name'].nunique()}")
    print(f"SUCCESS: Unique columns: {result_df['Column Name'].nunique()}")
    print()
    print(f"SAVED: Results saved to: {output_file}")
    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
