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
        """
        Validate URL to prevent SSRF attacks with optional domain allowlisting
        
        Args:
            url: URL to validate
            allowed_domains: Optional list of allowed domain names. If provided, only these domains are permitted.
                           Example: ['api.example.com', 'trusted-api.company.com']
            enforce_https: If True, only HTTPS URLs are allowed (recommended for production)
            
        Returns:
            True if URL is valid and safe
            
        Raises:
            ValueError: If URL is invalid or potentially dangerous
        """
        try:
            parsed = urllib.parse.urlparse(url)
            
            # Enforce HTTPS only (more secure)
            if enforce_https:
                if parsed.scheme != 'https':
                    raise ValueError("Only HTTPS URLs are allowed for security")
            else:
                # Allow HTTP/HTTPS but block other schemes
                if parsed.scheme not in ['http', 'https']:
                    raise ValueError(f"Invalid URL scheme: {parsed.scheme}. Only http and https are allowed.")
            
            # Check if hostname exists
            if not parsed.hostname:
                raise ValueError("URL must contain a valid hostname")
            
            hostname_lower = parsed.hostname.lower()
            
            # Block localhost and loopback addresses
            if hostname_lower in ['localhost', '0.0.0.0', '127.0.0.1', '::1']:
                raise ValueError("Internal network access not allowed: localhost/loopback addresses are blocked")
            
            # Block cloud metadata endpoints
            if hostname_lower in ['metadata.google.internal', '169.254.169.254']:
                raise ValueError("Access to cloud metadata endpoints is not allowed")
            
            # Block internal domain patterns
            internal_patterns = ['.local', '.internal', '.localhost']
            if any(hostname_lower.endswith(pattern) for pattern in internal_patterns):
                raise ValueError(f"Access to internal domains is not allowed: {parsed.hostname}")
            
            # Block private IP address ranges (optimized check)
            private_prefixes = ('192.168.', '10.') + tuple(f'172.{i}.' for i in range(16, 32))
            if hostname_lower.startswith(private_prefixes):
                raise ValueError("Internal network access not allowed: private IP ranges are blocked")
            
            # More thorough IP address validation using ipaddress module
            try:
                ip = ipaddress.ip_address(parsed.hostname)
                # Block private, loopback, link-local, and reserved addresses
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                    raise ValueError(f"Internal network access not allowed: {parsed.hostname} is a private/reserved IP")
            except ValueError as ip_error:
                # If it's our custom error message, re-raise it
                if "Internal network access not allowed" in str(ip_error):
                    raise
                # Otherwise, hostname is not an IP address, which is fine - continue validation
                pass
            
            # Domain allowlist validation (optional but recommended)
            if allowed_domains:
                if hostname_lower not in [domain.lower() for domain in allowed_domains]:
                    raise ValueError(f"Domain {parsed.hostname} is not in the allowlist")
            
            return True
            
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid URL format: {str(e)}")
        
    def parse_from_url(
        self,
        url: str,
        regulation: Optional[str] = None,
        name: Optional[str] = None,
        view_names: Optional[Union[Tuple[str, ...], List[str]]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, str]]:
        """
        Fetch data from URL endpoint and extract SQL queries based on filters
        
        Args:
            url: API endpoint URL that returns JSON data (single object or list)
            regulation: Filter by regulation field (e.g., "rhoo")
            name: Filter by top-level name field (e.g., "rhoo_controls_field_sql_query")
            view_names: Tuple or list of view names to filter
            headers: Optional HTTP headers for the request (e.g., authentication)
        
        Returns:
            List of dictionaries containing matched SQL queries with metadata
        """
        results = []
        
        # Convert view_names to tuple if it's a list
        if isinstance(view_names, list):
            view_names = tuple(view_names)
        
        # Validate URL to prevent SSRF attacks
        try:
            self._validate_url(url, allowed_domains=self.allowed_domains, enforce_https=self.enforce_https)
        except ValueError as e:
            print(f"ERROR: URL validation failed: {str(e)}")
            raise ValueError(f"Invalid or unsafe URL: {str(e)}")
        
        print(f"Fetching data from URL: {url}")
        
        try:
            # Use streaming for large data
            response = self._session.get(
                url, 
                headers=headers, 
                timeout=self.timeout,
                stream=True
            )
            response.raise_for_status()
            
            # Parse JSON with streaming for memory efficiency
            data = response.json()
            
            # Normalize data structure
            results = self._process_data_structure(
                data, "url", regulation, name, view_names
            )
            
            print(f"Total SQL queries extracted: {len(results)}")
            
        except requests.exceptions.Timeout:
            print(f"ERROR: Request timed out after {self.timeout} seconds")
        except (requests.exceptions.RequestException, json.JSONDecodeError, Exception) as e:
            print(f"ERROR: {str(e)}")
        
        return results
    
    def parse_json_files(
        self,
        regulation: Optional[str] = None,
        name: Optional[str] = None,
        view_names: Optional[Union[Tuple[str, ...], List[str]]] = None
    ) -> List[Dict[str, str]]:
        """
        Parse all JSON files in directory and extract SQL queries based on filters
        
        Args:
            regulation: Filter by regulation field (e.g., "rhoo")
            name: Filter by top-level name field (e.g., "rhoo_controls_field_sql_query")
            view_names: Tuple or list of view names to filter
        
        Returns:
            List of dictionaries containing matched SQL queries with metadata
        """
        results = []
        
        # Convert view_names to tuple if it's a list
        if isinstance(view_names, list):
            view_names = tuple(view_names)
        
        # Find all JSON files in directory
        json_files = self._find_json_files()
        
        print(f"Found {len(json_files)} JSON file(s) in '{self.directory}'")
        print()
        
        # Process each JSON file
        for json_file in json_files:
            print(f"Processing: {json_file}")
            file_results = self._parse_single_json(
                json_file, 
                regulation, 
                name, 
                view_names
            )
            results.extend(file_results)
            print(f"  -> Found {len(file_results)} matching SQL queries")
        
        print()
        print(f"Total SQL queries extracted: {len(results)}")
        return results
    
    def _find_json_files(self) -> List[str]:
        """Find all JSON files in the specified directory"""
        return sorted([str(f) for f in Path(self.directory).glob("*.json")])
    
    def _parse_single_json(
        self,
        json_file: str,
        regulation: Optional[str],
        name: Optional[str],
        view_names: Optional[Tuple[str, ...]]
    ) -> List[Dict[str, str]]:
        """Parse a single JSON file and extract matching SQL queries"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            file_name = Path(json_file).name
            return self._process_data_structure(data, file_name, regulation, name, view_names)
        except (json.JSONDecodeError, Exception) as e:
            print(f"  X ERROR processing {json_file}: {str(e)}")
            return []
    
    def _decode_sql(self, encoded_sql: str) -> str:
        """
        Decode base64 encoded SQL with validation and security checks
        
        Args:
            encoded_sql: Base64 encoded SQL string or plain SQL
            
        Returns:
            Decoded and cleaned SQL string, or empty string if decoding fails
        """
        if not encoded_sql or not isinstance(encoded_sql, str):
            logger.warning("Empty or invalid SQL input")
            return ""
        
        # Try to decode as base64
        try:
            decoded_bytes = base64.b64decode(encoded_sql, validate=True)
            decoded_sql = decoded_bytes.decode('utf-8')
        except (ValueError, base64.binascii.Error):
            # Not base64 encoded, use as-is
            decoded_sql = encoded_sql
        except (UnicodeDecodeError, Exception) as e:
            logger.warning(f"Error decoding SQL: {e}")
            return ""
        
        # Normalize whitespace
        cleaned_sql = ' '.join(decoded_sql.split())
        
        # Validate minimum length
        if len(cleaned_sql) < 5:
            logger.warning("Decoded SQL suspiciously short or empty")
            return ""
        
        return cleaned_sql
    
    def _is_safe_sql(self, sql: str) -> bool:
        """
        Optional: Basic SQL injection protection (can be enhanced)
        
        Args:
            sql: SQL string to validate
            
        Returns:
            True if SQL appears safe, False otherwise
        """
        # Basic checks for dangerous patterns
        sql_upper = sql.upper()
        
        # Block potentially dangerous commands
        dangerous_patterns = [
            'DROP TABLE', 'DROP DATABASE', 'TRUNCATE',
            'DELETE FROM', 'UPDATE SET',
            'EXEC ', 'EXECUTE ', 'xp_cmdshell',
            'GRANT ', 'REVOKE ',
            '; DROP', '; DELETE', '; UPDATE'
        ]
        
        for pattern in dangerous_patterns:
            if pattern in sql_upper:
                logger.warning(f"Potentially unsafe SQL pattern detected: {pattern}")
                return False
        
        return True
    
    def _process_data_structure(
        self,
        data: Union[Dict, List],
        source_name: str,
        regulation: Optional[str],
        name: Optional[str],
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
                item, item_source, regulation, name, view_names
            )
            results.extend(item_results)
        
        return results
    
    def _parse_single_data_object(
        self,
        data: Dict,
        source_name: str,
        regulation: Optional[str],
        name: Optional[str],
        view_names: Optional[Tuple[str, ...]]
    ) -> List[Dict[str, str]]:
        """Parse a single data object (from file or URL) and extract matching SQL queries"""
        # Early return for filter mismatches
        if regulation and data.get('regulation') != regulation:
            return []
        if name and data.get('name', '') != name:
            return []
        
        # Extract metadata once
        value = data.get('value', {})
        common_metadata = {
            'source': source_name,
            'regulation': data.get('regulation', ''),
            'metadatakey': data.get('name', '')
        }
        
        results = []
        
        # Process create_query list
        for query_obj in value.get('create_query', []):
            if self._matches_view_filter(query_obj.get('view_name', ''), view_names):
                results.append(self._build_result(query_obj, common_metadata))
        
        # Process select_query object
        if select_query := value.get('select_query'):
            if self._matches_view_filter(select_query.get('view_name', ''), view_names):
                results.append(self._build_result(select_query, common_metadata))
        
        return results
    
    def _matches_view_filter(self, view_name: str, view_names: Optional[Tuple[str, ...]]) -> bool:
        """Check if view name matches filter"""
        return not view_names or view_name in view_names
    
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
        """
        Pretty print the results
        
        Args:
            results: List of dictionaries from parse_json_files() or parse_from_url()
        """
        if not results:
            print("No results found")
            return
        
        print("=" * 100)
        print("SQL QUERY EXTRACTION RESULTS")
        print("=" * 100)
        print()
        
        for idx, result in enumerate(results, 1):
            print(f"{idx}. Source: {result['source']}")
            print(f"   Regulation: {result['regulation']}")
            print(f"   Name: {result['name']}")
            print(f"   View Name: {result['view_name']}")
            print(f"   SQL Query: {result['sql_query']}")
            print()


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
    regulation: Optional[str] = Field(None, description="Filter by regulation", example="rhoo")
    metadatakey: Optional[str] = Field(None, description="Filter by top-level name field", example="rhoo_controls_field_sql_query")
    view_names: Optional[List[str]] = Field(None, description="List of view names to filter (will be converted to tuple)", example=["APP_REGHUB_RHOO_CONTROLS_FIELD_GRU", "APP_REGHUB_RHOO_CONTROLS_FIELD_REG"])
    headers: Optional[Dict[str, str]] = Field(None, description="Optional HTTP headers for authentication", example={"Authorization": "Bearer token123"})

class FileParseRequest(BaseModel):
    """Request model for parsing from local files"""
    directory: str = Field(".", description="Directory containing JSON files")
    regulation: Optional[str] = Field(None, description="Filter by regulation")
    metadatakey: Optional[str] = Field(None, description="Filter by top-level name field")
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
    
    # Convert DataFrame to list of dictionaries
    lineage_records = []
    for _, row in lineage_df.iterrows():
        record = {
            'Database Name': str(row.get('Database Name') or ''),
            'Table Name': str(row.get('Table Name') or ''),
            'Column Name': str(row.get('Column Name') or ''),
            'Alias Name': str(row.get('Alias Name') or ''),
            'Regulation': str(row.get('Regulation') or ''),
            'Metadatakey': str(row.get('Metadatakey') or ''),
            'View Name': str(row.get('View Name') or ''),
            'Remarks': str(row.get('Remarks') or '')
        }
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
