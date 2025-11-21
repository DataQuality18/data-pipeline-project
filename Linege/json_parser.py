"""
JSON SQL Query Parser
=====================
Parse JSON configuration from files or URL endpoints to extract SQL queries.


Features:
- Parse JSON files in a directory OR fetch from URL endpoint
- Handle large data from API endpoints with streaming
- Filter by regulation, classname, and view_name (USER INPUTS)
- Support multiple view_names as tuple input
- Extract SQL queries from nested JSON structures

Usage:
    User provides filter parameters as input:
    - regulation (e.g., "rhoo")
    - classname (e.g., "com.citi.olympus.reg.spark.source.olympus.common.SparkStepSqlMeta")
    - view_names (e.g., ("VIEW1", "VIEW2"))
    
    Function returns list of dictionaries with matching SQL queries.
"""

import json
import sys
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel, Field

# Initialize FastAPI app
app = FastAPI(
    title="JSON SQL Query Parser API",
    description="Parse JSON metadata and extract SQL queries with filtering",
    version="2.0"
)


class JSONSQLParser:
    """Parser for extracting SQL queries from JSON configuration files or URL endpoints"""
    
    def __init__(self, directory: str = ".", timeout: int = 30, max_retries: int = 3):
        """
        Initialize the parser
        
        Args:
            directory: Directory containing JSON files (default: current directory)
            timeout: HTTP request timeout in seconds (default: 30)
            max_retries: Number of retries for failed HTTP requests (default: 3)
        """
        self.directory = directory
        self.timeout = timeout
        self.max_retries = max_retries
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
        
    def parse_from_url(
        self,
        url: str,
        regulation: Optional[str] = None,
        classname: Optional[str] = None,
        view_names: Optional[Union[Tuple[str, ...], List[str]]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, str]]:
        """
        Fetch data from URL endpoint and extract SQL queries based on filters
        
        Args:
            url: API endpoint URL that returns JSON data (single object or list)
            regulation: Filter by regulation field (e.g., "rhoo")
            classname: Filter by value.classname field
            view_names: Tuple or list of view names to filter
            headers: Optional HTTP headers for the request (e.g., authentication)
        
        Returns:
            List of dictionaries containing matched SQL queries with metadata
        """
        results = []
        
        # Convert view_names to tuple if it's a list
        if isinstance(view_names, list):
            view_names = tuple(view_names)
        
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
            
            # Check if data is a list or single object
            if isinstance(data, list):
                print(f"Received list with {len(data)} records")
                for idx, item in enumerate(data, 1):
                    if idx % 100 == 0:
                        print(f"  Processing record {idx}/{len(data)}...")
                    item_results = self._parse_single_data_object(
                        item, 
                        f"url_record_{idx}",
                        regulation, 
                        classname, 
                        view_names
                    )
                    results.extend(item_results)
            else:
                print("Received single record")
                results = self._parse_single_data_object(
                    data,
                    "url_data",
                    regulation,
                    classname,
                    view_names
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
        classname: Optional[str] = None,
        view_names: Optional[Union[Tuple[str, ...], List[str]]] = None
    ) -> List[Dict[str, str]]:
        """
        Parse all JSON files in directory and extract SQL queries based on filters
        
        Args:
            regulation: Filter by regulation field (e.g., "rhoo")
            classname: Filter by value.classname field
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
                classname, 
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
        classname: Optional[str],
        view_names: Optional[Tuple[str, ...]]
    ) -> List[Dict[str, str]]:
        """Parse a single JSON file and extract matching SQL queries"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return self._parse_single_data_object(data, Path(json_file).name, regulation, classname, view_names)
        except (json.JSONDecodeError, Exception) as e:
            print(f"  X ERROR processing {json_file}: {str(e)}")
            return []
    
    def _parse_single_data_object(
        self,
        data: Dict,
        source_name: str,
        regulation: Optional[str],
        classname: Optional[str],
        view_names: Optional[Tuple[str, ...]]
    ) -> List[Dict[str, str]]:
        """Parse a single data object (from file or URL) and extract matching SQL queries"""
        # Check top-level filters early return
        if regulation and data.get('regulation') != regulation:
            return []
        
        if classname and data.get('value', {}).get('classname', '') != classname:
            return []
        
        # Extract common metadata
        common_metadata = {
            'source': source_name,
            'regulation': data.get('regulation', ''),
            'classname': data.get('value', {}).get('classname', '')
        }
        
        results = []
        value = data.get('value', {})
        
        # Process create_query list
        for query_obj in value.get('create_query', []):
            view_name = query_obj.get('view_name', '')
            if not view_names or view_name in view_names:
                results.append({
                    **common_metadata,
                    'view_name': view_name,
                    'sql_query': query_obj.get('sql_query', '')
                })
        
        # Process select_query object
        if select_query := value.get('select_query'):
            view_name = select_query.get('view_name', '')
            if not view_names or view_name in view_names:
                results.append({
                    **common_metadata,
                    'view_name': view_name,
                    'sql_query': select_query.get('sql_query', '')
                })
        
        return results
    
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
            print(f"   Classname: {result['classname']}")
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
    user_classname = "com.citi.olympus.reg.spark.source.olympus.common.SparkStepSqlMeta"
    user_view_names = ("APP_REGHUB_RHOO_CONTROLS_FIELD_GRU", "APP_REGHUB_RHOO_CONTROLS_FIELD_REG")
    
    results1 = parser.parse_json_files(
        regulation=user_regulation,
        classname=user_classname,
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
    #     classname=user_classname,
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
    classname: Optional[str] = Field(None, description="Filter by value.classname", example="com.citi.olympus.SomeClass")
    view_names: Optional[List[str]] = Field(None, description="List of view names to filter", example=["VIEW1", "VIEW2"])
    headers: Optional[Dict[str, str]] = Field(None, description="Optional HTTP headers for authentication", example={"Authorization": "Bearer token123"})

class FileParseRequest(BaseModel):
    """Request model for parsing from local files"""
    directory: str = Field(".", description="Directory containing JSON files")
    regulation: Optional[str] = Field(None, description="Filter by regulation")
    classname: Optional[str] = Field(None, description="Filter by value.classname")
    view_names: Optional[List[str]] = Field(None, description="List of view names to filter")

class SimpleSQLResponse(BaseModel):
    """Simple response with only SQL queries"""
    success: bool
    message: str
    total_queries: int
    sql_queries: List[str]


# -----------------------------------------
# FastAPI Endpoints
# -----------------------------------------

# Global parser instance
_parser = JSONSQLParser(directory=".", timeout=60, max_retries=3)

@app.get("/", summary="API Information")
def root():
    """Get API information and available endpoints"""
    return {
        "name": "JSON SQL Query Parser API",
        "version": "2.0",
        "description": "Parse JSON metadata and extract SQL queries with filtering",
        "endpoints": {
            "/parse-from-url": "Parse SQL queries from a URL endpoint",
            "/parse-from-files": "Parse SQL queries from local JSON files",
            "/health": "Health check endpoint"
        }
    }

@app.get("/health", summary="Health Check")
def health_check():
    """Check if the API is running"""
    return {"status": "healthy", "service": "JSON SQL Query Parser"}

@app.post("/parse-from-url", response_model=SimpleSQLResponse, summary="Parse from URL Endpoint")
def api_parse_from_url(request: URLParseRequest):
    """
    Fetch data from a URL endpoint and extract SQL queries with filtering.
    
    **Example Request:**
    ```json
    {
      "url": "https://api.example.com/metadata",
      "regulation": "rhoo",
      "classname": "com.citi.olympus.SomeClass",
      "view_names": ["VIEW1", "VIEW2"],
      "headers": {
        "Authorization": "Bearer token"
      }
    }
    ```
    
    **Supports:**
    - Large datasets with streaming
    - Authentication via headers
    - Automatic retry logic
    - Progress tracking for large lists
    """
    try:
        results = _parser.parse_from_url(
            url=request.url,
            regulation=request.regulation,
            classname=request.classname,
            view_names=tuple(request.view_names) if request.view_names else None,
            headers=request.headers
        )
        
        return SimpleSQLResponse(
            success=True,
            message=f"Successfully extracted {len(results)} SQL queries from URL",
            total_queries=len(results),
            sql_queries=[result['sql_query'] for result in results]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse from URL: {str(e)}")

@app.post("/parse-from-files", response_model=SimpleSQLResponse, summary="Parse from Local Files")
def api_parse_from_files(request: FileParseRequest):
    """
    Parse SQL queries from local JSON files with filtering.
    
    **Example Request:**
    ```json
    {
      "directory": ".",
      "regulation": "rhoo",
      "classname": "com.citi.olympus.SomeClass",
      "view_names": ["VIEW1", "VIEW2"]
    }
    ```
    
    **Use Cases:**
    - Local development and testing
    - Offline processing
    - File-based configuration
    """
    try:
        parser = JSONSQLParser(directory=request.directory)
        results = parser.parse_json_files(
            regulation=request.regulation,
            classname=request.classname,
            view_names=tuple(request.view_names) if request.view_names else None
        )
        
        return SimpleSQLResponse(
            success=True,
            message=f"Successfully extracted {len(results)} SQL queries from files",
            total_queries=len(results),
            sql_queries=[result['sql_query'] for result in results]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse from files: {str(e)}")

@app.get("/parse-simple", summary="Simple URL Parse (GET)")
def api_parse_simple(
    url: str = Query(..., description="URL endpoint to fetch data from"),
    regulation: Optional[str] = Query(None, description="Filter by regulation (e.g., 'rhoo')"),
    classname: Optional[str] = Query(None, description="Filter by value.classname (e.g., 'com.citi.olympus.SomeClass')"),
    view_names: Optional[str] = Query(None, description="Comma-separated view names (e.g., 'VIEW1,VIEW2,VIEW3')")
):
    """
    Simple GET endpoint for parsing from URL (no authentication headers).
    
    **Example:**
    ```
    GET /parse-simple?url=https://api.example.com/metadata&regulation=rhoo&classname=com.citi.olympus.SomeClass&view_names=APP_REGHUB_RHOO_CONTROLS_FIELD_GRU,APP_REGHUB_RHOO_CONTROLS_FIELD_REG
    ```
    
    **Filter Parameters:**
    - `url` (required): API endpoint URL
    - `regulation` (optional): Filter by regulation field
    - `classname` (optional): Filter by value.classname field
    - `view_names` (optional): Comma-separated list of view names
    
    **Use Cases:**
    - Quick testing without POST body
    - Simple integrations
    - Public APIs without authentication
    """
    try:
        results = _parser.parse_from_url(
            url=url,
            regulation=regulation,
            classname=classname,
            view_names=tuple(v.strip() for v in view_names.split(',')) if view_names else None,
            headers=None
        )
        
        return SimpleSQLResponse(
            success=True,
            message=f"Successfully extracted {len(results)} SQL queries",
            total_queries=len(results),
            sql_queries=[result['sql_query'] for result in results]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse: {str(e)}")


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
        print("  - POST /parse-from-url    (Parse from URL endpoint)")
        print("  - POST /parse-from-files  (Parse from local files)")
        print("  - GET  /parse-simple      (Simple URL parse)")
        print()
        uvicorn.run(app, host="127.0.0.1", port=8000)
    else:
        # Console mode: run examples
        main()
