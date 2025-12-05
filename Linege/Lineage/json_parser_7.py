
"""
JSON SQL Query Parser
=====================
Parse JSON configuration from files or URL endpoints to extract SQL queries.

Features:
- Parse JSON files in a directory OR fetch from URL endpoint
- Handle large data from API endpoints with streaming
- Filter by regulation, name, and view_name (USER INPUTS)
- Support multiple view_names as tuple input
- Extract SQL queries from nested JSON structures

Usage:
    User provides filter parameters as input:
    - regulation (e.g., "rhoo")
    - name (e.g., "rhoo_controls_field_sql_query")
    - view_names (e.g., ("VIEW1", "VIEW2"))
    
    Function returns list of dictionaries with matching SQL queries.
"""

import json
import sys
import base64
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union
import urllib.parse
import ipaddress
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel, Field
from sql_lineage_parser import SQLLineageParser

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Initialize FastAPI app
app = FastAPI(
    title="JSON SQL Query Parser API",
    description="Parse JSON metadata and extract SQL queries with filtering",
    version="2.0"
)


class JSONSQLParser:
    """Parser for extracting SQL queries from JSON configuration files or URL endpoints"""
    
    def __init__(self, directory: str = ".", timeout: int = 30, max_retries: int = 3, 
                 allowed_domains: Optional[List[str]] = None, enforce_https: bool = True):
        """
        Initialize the parser
        
        Args:
            directory: Directory containing JSON files (default: current directory)
            timeout: HTTP request timeout in seconds (default: 30)
            max_retries: Number of retries for failed HTTP requests (default: 3)
            allowed_domains: Optional list of allowed domains for URL validation
                           Example: ['api.example.com', 'trusted-api.company.com']
                           If None, all domains are allowed (less secure)
            enforce_https: If True, only HTTPS URLs are allowed (default: True, recommended)
        """
        self.directory = directory
        self.timeout = timeout
        self.max_retries = max_retries
        self.allowed_domains = allowed_domains
        self.enforce_https = enforce_https
        self._session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic for handling large data"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def _validate_url(self, url: str, allowed_domains: Optional[List[str]] = None, enforce_https: bool = True) -> bool:
        """Validate URL to prevent SSRF attacks"""
        try:
            parsed = urllib.parse.urlparse(url)
            
            # Validate scheme
            if enforce_https and parsed.scheme != 'https':
                raise ValueError("Only HTTPS URLs are allowed for security")
            if not enforce_https and parsed.scheme not in ['http', 'https']:
                raise ValueError(f"Invalid URL scheme: {parsed.scheme}")
            
            if not parsed.hostname:
                raise ValueError("URL must contain a valid hostname")
            
            hostname_lower = parsed.hostname.lower()
            
            # Block dangerous hosts
            blocked_hosts = {'localhost', '0.0.0.0', '127.0.0.1', '::1', 'metadata.google.internal', '169.254.169.254'}
            if hostname_lower in blocked_hosts:
                raise ValueError("Internal network access not allowed")
            
            # Block internal domain patterns
            if any(hostname_lower.endswith(pattern) for pattern in ['.local', '.internal', '.localhost']):
                raise ValueError(f"Access to internal domains not allowed: {parsed.hostname}")
            
            # Block private IP ranges
            private_prefixes = ('192.168.', '10.') + tuple(f'172.{i}.' for i in range(16, 32))
            if hostname_lower.startswith(private_prefixes):
                raise ValueError("Private IP ranges blocked")
            
            # Validate IP addresses
            try:
                ip = ipaddress.ip_address(parsed.hostname)
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                    raise ValueError(f"Private/reserved IP blocked: {parsed.hostname}")
            except ValueError as ip_error:
                if "blocked" in str(ip_error).lower():
                    raise
            
            # Domain allowlist
            if allowed_domains and hostname_lower not in [d.lower() for d in allowed_domains]:
                raise ValueError(f"Domain {parsed.hostname} not in allowlist")
            
            return True
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid URL: {str(e)}")
        
    def parse_from_url(
        self,
        url: str,
        regulation: Optional[str] = None,
        name: Optional[str] = None,
        class_name: Optional[str] = None,
        view_names: Optional[Union[Tuple[str, ...], List[str]]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, str]]:
        """Fetch data from URL endpoint and extract SQL queries based on filters"""
        # Validate URL
        self._validate_url(url, self.allowed_domains, self.enforce_https)
        
        # Convert view_names to tuple
        if isinstance(view_names, list):
            view_names = tuple(view_names)
        
        print(f"Fetching data from URL: {url}")
        
        try:
            response = self._session.get(url, headers=headers, timeout=self.timeout, stream=True)
            response.raise_for_status()
            results = self._process_data_structure(response.json(), "url", regulation, name, class_name, view_names)
            print(f"Total SQL queries extracted: {len(results)}")
            return results
        except requests.exceptions.Timeout:
            print(f"ERROR: Request timed out after {self.timeout} seconds")
            return []
        except Exception as e:
            print(f"ERROR: {str(e)}")
            return []
    
    def parse_json_files(
        self,
        regulation: Optional[str] = None,
        name: Optional[str] = None,
        class_name: Optional[str] = None,
        view_names: Optional[Union[Tuple[str, ...], List[str]]] = None
    ) -> List[Dict[str, str]]:
        """Parse all JSON files in directory and extract SQL queries based on filters"""
        # Convert view_names to tuple
        if isinstance(view_names, list):
            view_names = tuple(view_names)
        
        json_files = self._find_json_files()
        print(f"Found {len(json_files)} JSON file(s) in '{self.directory}'\n")
        
        results = []
        for json_file in json_files:
            print(f"Processing: {json_file}")
            file_results = self._parse_single_json(json_file, regulation, name, class_name, view_names)
            results.extend(file_results)
            print(f"  -> Found {len(file_results)} matching SQL queries")
        
        print(f"\nTotal SQL queries extracted: {len(results)}")
        return results
    
    def _find_json_files(self) -> List[str]:
        """Find all JSON files in the specified directory"""
        return sorted([str(f) for f in Path(self.directory).glob("*.json")])
    
    def _parse_single_json(
        self,
        json_file: str,
        regulation: Optional[str],
        name: Optional[str],
        class_name: Optional[str],
        view_names: Optional[Tuple[str, ...]]
    ) -> List[Dict[str, str]]:
        """Parse a single JSON file and extract matching SQL queries"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            file_name = Path(json_file).name
            return self._process_data_structure(data, file_name, regulation, name, class_name, view_names)
        except (json.JSONDecodeError, Exception) as e:
            print(f"  X ERROR processing {json_file}: {str(e)}")
            return []
    
    def _decode_sql(self, encoded_sql: str) -> str:
        """Decode base64 encoded SQL or return as-is"""
        if not encoded_sql or not isinstance(encoded_sql, str):
            return ""
        
        try:
            decoded_bytes = base64.b64decode(encoded_sql, validate=True)
            decoded_sql = decoded_bytes.decode('utf-8')
        except (ValueError, base64.binascii.Error, UnicodeDecodeError):
            decoded_sql = encoded_sql
        
        # Normalize whitespace and validate
        cleaned_sql = ' '.join(decoded_sql.split())
        return cleaned_sql if len(cleaned_sql) >= 5 else ""
    
    def _process_data_structure(
        self,
        data: Union[Dict, List],
        source_name: str,
        regulation: Optional[str],
        name: Optional[str],
        class_name: Optional[str],
        view_names: Optional[Tuple[str, ...]]
    ) -> List[Dict[str, str]]:
        """Process data structure (list, metadataList, or single object) uniformly"""
        results = []
        
        # Normalize to list of items
        if 'metadataList' in data:
            items = data['metadataList']
            print(f"Received metadataList with {len(items)} records")
        elif isinstance(data, list):
            items = data
            print(f"Received list with {len(items)} records")
        else:
            items = [data]
            print("Received single record")
        
        # Process all items
        for idx, item in enumerate(items, 1):
            if len(items) > 1 and idx % 100 == 0:
                print(f"  Processing record {idx}/{len(items)}...")
            
            item_source = f"{source_name}_record_{idx}" if len(items) > 1 else source_name
            item_results = self._parse_single_data_object(
                item, item_source, regulation, name, class_name, view_names
            )
            results.extend(item_results)
        
        return results
    
    def _parse_single_data_object(
        self,
        data: Dict,
        source_name: str,
        regulation: Optional[str],
        name: Optional[str],
        class_name: Optional[str],
        view_names: Optional[Tuple[str, ...]]
    ) -> List[Dict[str, str]]:
        """Parse a single data object and extract matching SQL queries"""
        # Early return for filter mismatches
        if (regulation and data.get('regulation') != regulation) or \
           (name and data.get('name', '') != name):
            return []
        
        value = data.get('value', {})
        if class_name and value.get('classname', '') != class_name:
            return []
        
        common_metadata = {
            'source': source_name,
            'regulation': data.get('regulation', ''),
            'metadatakey': data.get('name', ''),
            'class_name': value.get('classname', '')
        }
        
        results = []
        # Process create_query list
        for query_obj in value.get('create_query', []):
            if not view_names or query_obj.get('view_name', '') in view_names:
                results.append(self._build_result(query_obj, common_metadata))
        
        # Process select_query object
        if select_query := value.get('select_query'):
            if not view_names or select_query.get('view_name', '') in view_names:
                results.append(self._build_result(select_query, common_metadata))
        
        return results
    
    def _build_result(self, query_obj: Dict, common_metadata: Dict) -> Dict[str, str]:
        """Build result dictionary from query object"""
        return {
            **common_metadata,
            'view_name': query_obj.get('view_name', ''),
            'sql_query': self._decode_sql(query_obj.get('sql_query', ''))
        }
    
    def get_sql_queries_dict(self, results: List[Dict[str, str]]) -> Dict[str, str]:
        """
        Convert results to a simple dictionary format
        
        Args:
            results: List of dictionaries from parse_json_files()
            
        Returns:
            Dictionary with format: {'SQL 1': 'query1', 'SQL 2': 'query2', ...}
        """
        return {f"SQL {idx}": result['sql_query'] for idx, result in enumerate(results, 1)}
    
    def print_results(self, results: List[Dict[str, str]]):
        """Pretty print the results"""
        if not results:
            print("No results found")
            return
        
        print("=" * 100)
        print("SQL QUERY EXTRACTION RESULTS")
        print("=" * 100 + "\n")
        
        for idx, result in enumerate(results, 1):
            print(f"{idx}. Source: {result['source']}")
            print(f"   Regulation: {result['regulation']}")
            print(f"   Name: {result['name']}")
            print(f"   View Name: {result['view_name']}")
            print(f"   SQL Query: {result['sql_query']}\n")


def main():
    """Example usage"""
    print("=" * 100)
    print("JSON SQL QUERY PARSER")
    print("=" * 100)
    print()
    
    # Initialize parser
    parser = JSONSQLParser(directory=".", timeout=30, max_retries=3)
    
    # Example 1: Parse from local JSON files
    print("Example 1: Parse from local JSON files")
    print("-" * 100)
    user_regulation = "rhoo"
    user_name = "rhoo_controls_field_sql_query"
    user_view_names = ("APP_REGHUB_RHOO_CONTROLS_FIELD_GRU", "APP_REGHUB_RHOO_CONTROLS_FIELD_REG")
    
    results1 = parser.parse_json_files(
        regulation=user_regulation,
        name=user_name,
        view_names=user_view_names
    )
    parser.print_results(results1)
    
    print()
    print("=" * 100)
    print("Example 2: Parse from URL endpoint (if available)")
    print("-" * 100)
    
    # Example URL endpoint (replace with your actual endpoint)
    # url = "https://your-api.example.com/metadata/rhoo"
    # 
    # # Optional: Add authentication headers
    # headers = {
    #     "Authorization": "Bearer your_token_here",
    #     "Content-Type": "application/json"
    # }
    # 
    # results2 = parser.parse_from_url(
    #     url=url,
    #     regulation=user_regulation,
    #     name=user_name,
    #     view_names=user_view_names,
    #     headers=headers
    # )
    # parser.print_results(results2)
    
    print("URL parsing example commented out - replace with your actual endpoint")
    
    print()
    print("=" * 100)
    print("SQL QUERIES AS DICTIONARY")
    print("=" * 100)
    
    # Get results as dictionary
    sql_dict = parser.get_sql_queries_dict(results1)
    print("\nDictionary format:")
    print(sql_dict)


# -----------------------------------------
# Pydantic Models for API
# -----------------------------------------

class URLParseRequest(BaseModel):
    """Request model for parsing from URL endpoint"""
    url: str = Field(..., description="API endpoint URL to fetch data from", example="https://api.example.com/metadata")
    regulation: str = Field(..., description="Filter by regulation (required)", example="rhoo")
    metadatakey: Optional[str] = Field(None, description="Filter by top-level name field", example="rhoo_controls_field_sql_query")
    class_name: Optional[str] = Field(None, description="Filter by class name from value.classname", example="com.citi.olympus.reg.spark.source.olympus.common.SparkStepSqlMeta")
    view_names: Optional[List[str]] = Field(None, description="List of view names to filter (will be converted to tuple)", example=["APP_REGHUB_RHOO_CONTROLS_FIELD_GRU", "APP_REGHUB_RHOO_CONTROLS_FIELD_REG"])
    headers: Optional[Dict[str, str]] = Field(None, description="Optional HTTP headers for authentication", example={"Authorization": "Bearer token123"})

class FileParseRequest(BaseModel):
    """Request model for parsing from local files"""
    directory: str = Field(".", description="Directory containing JSON files")
    regulation: str = Field(..., description="Filter by regulation (required)")
    metadatakey: Optional[str] = Field(None, description="Filter by top-level name field")
    class_name: Optional[str] = Field(None, description="Filter by class name from value.classname")
    view_names: Optional[List[str]] = Field(None, description="List of view names to filter (will be converted to tuple)")

class SimpleSQLResponse(BaseModel):
    """Simple response with only SQL queries"""
    success: bool
    message: str
    total_queries: int
    sql_queries: List[str]

class LineageRecord(BaseModel):
    """Single lineage record"""
    database_name: str = Field(..., alias="Database Name")
    table_name: str = Field(..., alias="Table Name")
    column_name: str = Field(..., alias="Column Name")
    alias_name: str = Field(..., alias="Alias Name")
    regulation: str = Field(..., alias="Regulation")
    metadatakey: str = Field(..., alias="Metadatakey")
    view_name: str = Field(..., alias="View Name")
    remarks: str = Field(..., alias="Remarks")

    class Config:
        populate_by_name = True

class LineageResponse(BaseModel):
    """Response with SQL lineage information"""
    success: bool
    message: str
    total_records: int
    lineage_data: List[Dict[str, str]]


# -----------------------------------------
# FastAPI Endpoints
# -----------------------------------------

# Global parser instance
_parser = JSONSQLParser(directory=".", timeout=60, max_retries=3)

def _process_lineage(results: List[Dict]) -> List[Dict[str, str]]:
    """
    Helper function to process SQL results and generate lineage records.
    
    Args:
        results: List of dictionaries containing sql_query, view_name, name, regulation
        
    Returns:
        List of lineage records with all required fields
    """
    if not results:
        return []
    
    # Create dictionary of SQL queries and metadata for lineage parser
    sql_dict = {f"SQL_{idx}": result['sql_query'] for idx, result in enumerate(results, 1)}
    metadata_dict = {
        f"SQL_{idx}": {
            'view_name': result.get('view_name', ''),
            'metadatakey': result.get('metadatakey', ''),
            'regulation': result.get('regulation', '')
        } for idx, result in enumerate(results, 1)
    }
    
    # Parse lineage using SQLLineageParser with metadata
    lineage_parser = SQLLineageParser()
    lineage_df = lineage_parser.parse_query_dictionary(sql_dict, metadata_dict)
    
    # Convert DataFrame to list of dictionaries, filter out N/A records
    lineage_records = []
    for _, row in lineage_df.iterrows():
        record = {k: str(row.get(k) or '') for k in ['Database Name', 'Table Name', 'Column Name', 'Alias Name', 'Regulation', 'Metadatakey', 'View Name', 'Remarks']}
        
        # Skip records where all key fields are 'N/A'
        if not all(record[k] == 'N/A' for k in ['Database Name', 'Table Name', 'Column Name', 'Alias Name']):
            lineage_records.append(record)
    
    return lineage_records

@app.post("/parse-lineage-from-url", response_model=LineageResponse, summary="Parse SQL Lineage from URL")
def api_parse_lineage_from_url(request: URLParseRequest):
    """
    Fetch data from URL, extract SQL queries, and generate lineage information.
    
    Returns lineage data including database, table, column information along with
    name and regulation metadata.
    """
    try:
        # Extract SQL queries with metadata
        results = _parser.parse_from_url(
            url=request.url,
            regulation=request.regulation,
            name=request.metadatakey,
            class_name=request.class_name,
            view_names=tuple(request.view_names) if request.view_names else None,
            headers=request.headers
        )
        
        # Process lineage
        lineage_records = _process_lineage(results)
        
        return LineageResponse(
            success=True,
            message=f"Successfully extracted lineage for {len(results)} SQL queries",
            total_records=len(lineage_records),
            lineage_data=lineage_records
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse lineage from URL: {str(e)}")

@app.post("/parse-lineage-from-files", response_model=LineageResponse, summary="Parse SQL Lineage from Local Files")
def api_parse_lineage_from_files(request: FileParseRequest):
    """
    Parse SQL queries from local files and generate lineage information.
    
    Returns lineage data including database, table, column information along with
    name and regulation metadata.
    """
    try:
        # Extract SQL queries with metadata
        results = _parser.parse_json_files(
            regulation=request.regulation,
            name=request.metadatakey,
            class_name=request.class_name,
            view_names=tuple(request.view_names) if request.view_names else None
        )
        
        # Process lineage
        lineage_records = _process_lineage(results)
        
        return LineageResponse(
            success=True,
            message=f"Successfully extracted lineage for {len(results)} SQL queries",
            total_records=len(lineage_records),
            lineage_data=lineage_records
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse lineage from files: {str(e)}")


# -----------------------------------------
# Main Entry Point
# -----------------------------------------

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "api":
        # API mode: start FastAPI server
        import uvicorn
        print("Starting JSON SQL Parser API Server...")
        print("API Documentation: http://127.0.0.1:8000/docs")
        print("Endpoints:")
        print("  - POST /parse-lineage-from-url    (Parse lineage from URL endpoint)")
        print("  - POST /parse-lineage-from-files  (Parse lineage from local files)")
        print()
        uvicorn.run(app, host="127.0.0.1", port=8001)
    else:
        # Console mode: run examples
        main()
json_parse_sql.py
Displaying json_parse_sql.py.
