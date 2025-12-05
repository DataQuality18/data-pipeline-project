
"""
Generate Lineage CSV from SQL or JSON File
===========================================
Parse a SQL or JSON file and generate lineage information in CSV format.

Usage:
    python generate_lineage_csv.py <file> <regulation> [output_csv]
    
Example:
    python generate_lineage_csv.py query.sql rhoo
    python generate_lineage_csv.py rhoo_controls.json rhoo
    python generate_lineage_csv.py "my query.sql" emir lineage_output.csv
"""

import sys
import os
from pathlib import Path
from sql_lineage_parser import SQLLineageParser
from json_parse_sql import JSONSQLParser


def generate_lineage_csv(input_file: str, regulation: str, output_csv: str = None):
    """
    Generate lineage CSV from SQL or JSON file
    
    Args:
        input_file: Path to SQL or JSON file
        regulation: Regulation name (e.g., 'rhoo', 'emir')
        output_csv: Optional output CSV filename (default: <file>_lineage.csv)
    """
    # Validate input file
    if not os.path.exists(input_file):
        print(f"ERROR: File '{input_file}' not found")
        return False
    
    # Determine file type
    file_path = Path(input_file)
    file_ext = file_path.suffix.lower()
    
    # Generate output filename if not provided
    if not output_csv:
        output_csv = f"{file_path.stem}_lineage.csv"
    
    print("=" * 100)
    print("LINEAGE CSV GENERATOR")
    print("=" * 100)
    print(f"Input File: {input_file}")
    print(f"File Type: {file_ext}")
    print(f"Regulation: {regulation}")
    print(f"Output CSV: {output_csv}")
    print("=" * 100)
    print()
    
    try:
        if file_ext == '.json':
            # Parse JSON file
            print(f"Processing JSON file: {input_file}")
            json_parser = JSONSQLParser(directory=str(file_path.parent))
            results = json_parser.parse_json_files(regulation=regulation)
            
            if not results:
                print("WARNING: No SQL queries extracted from JSON")
                return False
            
            # Parse SQL queries and generate lineage
            sql_parser = SQLLineageParser()
            sql_dict = {f"SQL_{idx}": result['sql_query'] for idx, result in enumerate(results, 1)}
            metadata_dict = {
                f"SQL_{idx}": {
                    'view_name': result.get('view_name', ''),
                    'metadatakey': result.get('metadatakey', ''),
                    'regulation': result.get('regulation', '')
                } for idx, result in enumerate(results, 1)
            }
            
            df = sql_parser.parse_query_dictionary(sql_dict, metadata_dict)
            
        else:
            # Parse SQL file
            print(f"Processing SQL file: {input_file}")
            sql_parser = SQLLineageParser()
            df = sql_parser.parse_sql_files([input_file], regulation=regulation)
        
        if df.empty:
            print("WARNING: No lineage data extracted")
            return False
        
        # Save to CSV
        df.to_csv(output_csv, index=False)
        
        print()
        print("=" * 100)
        print("SUCCESS!")
        print("=" * 100)
        print(f"Total records: {len(df)}")
        print(f"CSV saved to: {output_csv}")
        print()
        print("Preview (first 5 rows):")
        print("-" * 100)
        print(df.head().to_string())
        print()
        
        return True
        
    except Exception as e:
        print()
        print("=" * 100)
        print("ERROR!")
        print("=" * 100)
        print(f"Failed to generate lineage CSV: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point"""
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage:")
        print("  For SQL: python generate_lineage_csv.py <sql_file> [output_csv]")
        print("  For JSON: python generate_lineage_csv.py <json_file> <regulation> [output_csv]")
        print()
        print("Arguments:")
        print("  file        : Path to SQL or JSON file")
        print("  regulation  : Regulation name (required for JSON only)")
        print("  output_csv  : Optional output CSV filename")
        print()
        print("Examples:")
        print("  python generate_lineage_csv.py query.sql")
        print("  python generate_lineage_csv.py query.sql output.csv")
        print("  python generate_lineage_csv.py rhoo_controls.json rhoo")
        print("  python generate_lineage_csv.py rhoo_controls.json rhoo lineage_output.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    file_ext = Path(input_file).suffix.lower()
    
    # For JSON files, regulation is required
    if file_ext == '.json':
        if len(sys.argv) < 3:
            print("ERROR: Regulation parameter is required for JSON files")
            print("Usage: python generate_lineage_csv.py <json_file> <regulation> [output_csv]")
            sys.exit(1)
        regulation = sys.argv[2]
        output_csv = sys.argv[3] if len(sys.argv) > 3 else None
    else:
        # For SQL files, regulation is not used
        regulation = ''
        output_csv = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Generate lineage CSV
    success = generate_lineage_csv(input_file, regulation, output_csv)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
generate_lineage_csv.py
Displaying json_parse_sql.py.
