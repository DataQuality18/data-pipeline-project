"""
SQL Lineage Parser
==================
A production-ready tool to parse SQL queries and extract column-level lineage information.

Author: Optimized Version
Date: November 12, 2025
Version: 2.0

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
from typing import List, Dict
from pathlib import Path


class SQLLineageParser:
    """Main parser class for extracting SQL column lineage"""
    
    def __init__(self):
        self.ignored_keywords = ['elastic_query', 'mongo_query']
        
    def parse_sql_files(self, sql_files: List[str]) -> pd.DataFrame:
        """
        Parse multiple SQL files and extract column lineage information
        
        Args:
            sql_files: List of SQL file paths to parse
            
        Returns:
            DataFrame with columns: Database Name, Table Name, Column Name, Alias Name, Remarks
        """
        all_lineage_data = []
        
        for sql_file in sql_files:
            if not os.path.exists(sql_file):
                print(f"⚠️  Warning: File {sql_file} not found, skipping...")
                continue
                
            try:
                with open(sql_file, 'r', encoding='utf-8') as file:
                    sql_content = file.read()
                    
                print(f"📄 Processing file: {sql_file}")
                lineage_data = self.parse_single_sql(sql_content, sql_file)
                all_lineage_data.extend(lineage_data)
                print(f"✅ Extracted {len(lineage_data)} column mappings")
                
            except Exception as e:
                print(f"❌ Error processing file {sql_file}: {str(e)}")
                all_lineage_data.append({
                    'database_name': 'ERROR',
                    'table_name': 'ERROR',
                    'column_name': 'ERROR', 
                    'alias_name': 'ERROR',
                    'remarks': f'failure_processing_file: {str(e)}'
                })
            
        return self.create_lineage_dataframe(all_lineage_data)
    
    def parse_single_sql(self, sql_content: str, filename: str) -> List[Dict[str, str]]:
        """Parse a single SQL query and extract lineage information"""
        lineage_data = []
        
        # Clean and normalize SQL
        sql_content = self.clean_sql(sql_content)
        
        if not sql_content.strip():
            return [{
                'database_name': 'N/A',
                'table_name': 'N/A', 
                'column_name': 'N/A',
                'alias_name': 'N/A',
                'remarks': 'empty_sql_content'
            }]
        
        # Check for ignored patterns
        remarks = self.get_remarks(sql_content)
        
        if "ignored" in remarks:
            return [{
                'database_name': 'N/A',
                'table_name': 'N/A', 
                'column_name': 'N/A',
                'alias_name': 'N/A',
                'remarks': remarks
            }]
        
        try:
            # Parse SQL using sqlparse
            parsed = sqlparse.parse(sql_content)
            
            if not parsed:
                return [{
                    'database_name': 'N/A',
                    'table_name': 'N/A',
                    'column_name': 'N/A',
                    'alias_name': 'N/A',
                    'remarks': 'failed_to_parse_sql'
                }]
                
            # Process each statement
            for stmt in parsed:
                if not str(stmt).strip() or str(stmt).strip() == ';':
                    continue
                    
                stmt_lineage = self.process_sql_statement(stmt, filename)
                lineage_data.extend(stmt_lineage)
                    
        except Exception as e:
            print(f"❌ Error parsing SQL: {str(e)}")
            lineage_data.append({
                'database_name': 'ERROR',
                'table_name': 'ERROR',
                'column_name': 'ERROR', 
                'alias_name': 'ERROR',
                'remarks': f'failure_sql_lineage_tech: {str(e)}'
            })
            
        return lineage_data
    
    def process_sql_statement(self, stmt, filename: str) -> List[Dict[str, str]]:
        """Process a single SQL statement"""
        lineage_data = []
        sql_text = str(stmt).strip()
        
        # Check if this is a subquery wrapped statement
        if re.match(r'^\s*SELECT\s+\*\s+FROM\s*\(', sql_text, re.IGNORECASE):
            # This is selecting from a subquery
            subquery_match = re.search(r'FROM\s*\((.*)\)\s+\w+', sql_text, re.IGNORECASE | re.DOTALL)
            if subquery_match:
                subquery_text = subquery_match.group(1).strip()
                # Parse the inner query
                inner_parsed = sqlparse.parse(subquery_text)
                if inner_parsed:
                    for inner_stmt in inner_parsed:
                        inner_lineage = self.process_inner_statement(inner_stmt, filename)
                        lineage_data.extend(inner_lineage)
            
            return lineage_data
        
        # Extract main query information
        main_tables = self.extract_main_tables(stmt)
        join_info = self.extract_join_info(stmt)
        
        # Process SELECT columns
        select_columns = self.extract_select_columns(stmt)
        
        for col_info in select_columns:
            lineage_entry = self.process_column_lineage(col_info, main_tables, join_info, filename, 0)
            if lineage_entry:
                lineage_data.append(lineage_entry)
                
        return lineage_data
    
    def process_inner_statement(self, stmt, filename: str) -> List[Dict[str, str]]:
        """Process an inner/nested SQL statement"""
        lineage_data = []
        
        # Extract tables and joins from inner query
        main_tables = self.extract_main_tables(stmt)
        join_info = self.extract_join_info(stmt)
        
        # Process SELECT columns
        select_columns = self.extract_select_columns(stmt)
        
        for col_info in select_columns:
            lineage_entry = self.process_column_lineage(col_info, main_tables, join_info, filename, 0)
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
        
        # Enhanced FROM clause extraction - stop at JOIN, WHERE, GROUP BY, etc.
        from_pattern = r'\bFROM\s+([^()]+?)(?:\s+(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN|\s+WHERE|\s+GROUP\s+BY|\s+HAVING|\s+ORDER\s+BY|\s*$)'
        from_match = re.search(from_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
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
        if not re.search(r'\bSELECT\b', sql_text, re.IGNORECASE):
            return columns
        
        # Find SELECT clause more robustly - handle subqueries
        select_match = re.search(r'\bSELECT\s+(.*?)(?=\s+FROM\s+)', sql_text, re.IGNORECASE | re.DOTALL)
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
        if re.search(r'\bOVER\s*\(', col_expr, re.IGNORECASE):
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
        star_match = re.match(r'(\w+)\.\*', col_expr)
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
        alias_match = re.search(r'(.*?)\s+AS\s+([\w_]+)', col_expr, re.IGNORECASE)
        if alias_match:
            base_expr = alias_match.group(1).strip()
            alias_name = alias_match.group(2).strip()
        else:
            base_expr = col_expr
            # For non-aliased columns, extract the last identifier
            match = re.search(r'[\w_]+$', base_expr)
            if match:
                alias_name = match.group(0)
        
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
            'alias_name': alias_name or column_name,
            'is_star': False,
            'is_derived': is_derived
        }
    
    def extract_join_info(self, stmt) -> List[Dict[str, str]]:
        """Extract JOIN information with database context"""
        joins = []
        sql_text = str(stmt).strip()
        
        # Pattern to match different types of JOINs
        join_pattern = r'(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN\s+([\w_.]+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.*?)(?=\s+(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN|\s+WHERE|\s+GROUP\s+BY|\s+ORDER\s+BY|\s*$)'
        
        join_matches = re.finditer(join_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
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
    
    def process_column_lineage(self, col_info: Dict, parent_tables: List, join_info: List, filename: str, level: int) -> Dict[str, str]:
        """Process a single column to determine its lineage"""
        remarks = ""
        
        # Handle derived columns (window functions, etc.)
        if col_info.get('is_derived'):
            # Check if it's a window function
            original_expr = col_info.get('original_expression', '').upper()
            if 'OVER(' in original_expr or 'OVER (' in original_expr:
                return {
                    'database_name': 'N/A',
                    'table_name': 'N/A',
                    'column_name': col_info.get('alias_name', 'derived'),
                    'alias_name': col_info.get('alias_name', 'derived'),
                    'remarks': 'derived_column_window_function'
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
        database_name, table_name = self.resolve_table_reference(col_info, parent_tables, join_info)
        
        # Handle column names
        column_name = col_info.get('column_name', '')
        alias_name = col_info.get('alias_name', '')
        
        # Handle star expressions - if table.*, leave it as is
        if col_info.get('is_star'):
            return {
                'database_name': database_name or 'unknown',
                'table_name': table_name or 'unknown',
                'column_name': '*',
                'alias_name': f"{table_name}.*" if table_name != 'unknown' else '*',
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
            
        return {
            'database_name': database_name or 'unknown',
            'table_name': table_name or 'unknown',
            'column_name': column_name or 'unknown',
            'alias_name': alias_name or column_name,
            'remarks': remarks
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
        """Create final DataFrame in the desired format"""
        if not lineage_data:
            return pd.DataFrame(columns=['Database Name', 'Table Name', 'Column Name', 'Alias Name', 'Remarks'])
            
        df = pd.DataFrame(lineage_data)
        df = df.rename(columns={
            'database_name': 'Database Name',
            'table_name': 'Table Name', 
            'column_name': 'Column Name',
            'alias_name': 'Alias Name',
            'remarks': 'Remarks'
        })
        
        # Remove duplicates and sort
        df = df.drop_duplicates().reset_index(drop=True)
        df = df.sort_values(['Database Name', 'Table Name', 'Column Name'])
        
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
            print("❌ No files specified. Exiting.")
            sys.exit(1)
        sql_files = file_input.split()
    
    # Validate files exist
    valid_files = []
    for f in sql_files:
        if os.path.exists(f):
            valid_files.append(f)
        else:
            print(f"⚠️  Warning: File '{f}' not found, skipping...")
    
    if not valid_files:
        print("❌ No valid SQL files found. Exiting.")
        sys.exit(1)
    
    print(f"\n📁 Processing {len(valid_files)} file(s)...")
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
    print(f"✅ Total columns found: {len(result_df)}")
    print(f"✅ Unique databases: {result_df['Database Name'].nunique()}")
    print(f"✅ Unique tables: {result_df['Table Name'].nunique()}")
    print(f"✅ Unique columns: {result_df['Column Name'].nunique()}")
    print()
    print(f"💾 Results saved to: {output_file}")
    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
