from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from pydantic import BaseModel

# 👇 Import from your wrapper file
from sql_lineage_6 import (
    parse_one_query,
    parse_many_queries,
    run_lineage_from_metadata_api,
)

app = FastAPI(title="SQL Lineage API", version="1.0.0")


# ---------- Pydantic models ----------

class LineageRequest(BaseModel):
    sql_text: str
    query_key: str
    view_name: str = ""
    metadatakey: str = ""
    regulation: str = ""
    class_name: str = ""
    verbose: bool = False


class BatchLineageRequest(BaseModel):
    # {"Q1": "select ...", "Q2": "select ..."}
    queries: Dict[str, str]
    # {"Q1": {"view_name": "...", "metadatakey": "...", ...}, ...}
    metadata: Optional[Dict[str, Dict[str, str]]] = None
    verbose: bool = False


class MetadataLineageRequest(BaseModel):
    url: str
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    timeout_sec: int = 30
    verbose: bool = False


# You could also define a LineageRow model, but your wrapper
# already returns JSON-serializable dicts, so we'll just return them directly.


# ---------- Endpoints ----------

@app.post("/lineage/query")
def lineage_for_single_query(req: LineageRequest) -> List[Dict[str, Any]]:
    """
    Run lineage for one SQL string using parse_one_query.
    """
    rows = parse_one_query(
        sql_text=req.sql_text,
        query_key=req.query_key,
        view_name=req.view_name,
        metadatakey=req.metadatakey,
        regulation=req.regulation,
        class_name=req.class_name,
        verbose=req.verbose,
    )
    return rows


@app.post("/lineage/batch")
def lineage_for_many_queries(req: BatchLineageRequest) -> List[Dict[str, Any]]:
    """
    Run lineage for multiple queries at once using parse_many_queries.
    """
    rows = parse_many_queries(
        queries=req.queries,
        metadata=req.metadata,
        verbose=req.verbose,
    )
    return rows


@app.post("/lineage/from-metadata")
def lineage_from_metadata(req: MetadataLineageRequest) -> List[Dict[str, Any]]:
    """
    Call a metadata API endpoint, extract queries, and run lineage.
    Uses run_lineage_from_metadata_api.
    """
    rows = run_lineage_from_metadata_api(
        url=req.url,
        params=req.params,
        headers=req.headers,
        timeout_sec=req.timeout_sec,
        verbose=req.verbose,
    )
    return rows
