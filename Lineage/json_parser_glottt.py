"""

FastAPI wrapper that:
- Accepts request with SAME shape you showed (url + regulation mandatory)
- Fetches metadata JSON from URL
- Finds (view_name, sql_query) pairs anywhere in JSON
- Uses base64 decode on sql_query
- Runs SQLGlot lineage parser
- Returns response in the SAME output style you showed:
  {
    "success": true,
    "message": "...",
    "total_records": <int>,
    "lineage_data": [ ... rows ... ]
  }

IMPORTANT:
- regulation is NOT hardcoded.
- Metadatakey: "Yes" -> read from metadata JSON "name" and put into output.
  (If missing in JSON, falls back to request.metadatakey if provided.)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Iterable, Tuple
import base64
import json

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from sql_lineage_parser_glot import parse_sql_lineage


app = FastAPI(title="SQL Lineage Parser (SQLGlot)")


class ParseLineageRequest(BaseModel):
    url: str = Field(..., description="Metadata JSON endpoint URL")
    regulation: str = Field(..., description="Regulation (mandatory)")
    metadatakey: Optional[str] = Field(None, description="Optional fallback metadatakey for testing")
    class_name: Optional[str] = Field(None, description="Optional (kept for compatibility/testing)")
    view_names: Optional[List[str]] = Field(None, description="Optional list of view_name values to filter")
    headers: Optional[Dict[str, str]] = Field(None, description="Optional HTTP headers")
    
class ParseLineageencodedsql(BaseModel):
    sql: str = Field(..., description=" encoded sql")


def _safe_b64decode_to_text(b64_str: str) -> str:
    if not b64_str:
        return ""

    s = b64_str.strip()

    # Some payloads may be missing padding
    pad = len(s) % 4
    if pad:
        s += "=" * (4 - pad)

    # Try standard decode, then urlsafe
    try:
        return base64.b64decode(s).decode("utf-8", errors="replace")
    except Exception:
        return base64.urlsafe_b64decode(s).decode("utf-8", errors="replace")


def _iter_view_sql_objects(obj: Any) -> Iterable[Dict[str, Any]]:
    """
    Recursively find dicts that contain BOTH:
      - view_name
      - sql_query
    """
    if isinstance(obj, dict):
        if "view_name" in obj and "sql_query" in obj:
            yield obj

        for v in obj.values():
            yield from _iter_view_sql_objects(v)

    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_view_sql_objects(item)


def _extract_entries_with_metadatakey(payload: Any) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Returns list of: (metadatakey, view_sql_object)

    Supports common real payload shapes:
    - payload is a dict with keys like: {"name": "...", "value": {...}}
    - payload is a list of many such dicts
    - nested occurrences
    """
    results: List[Tuple[str, Dict[str, Any]]] = []

    def walk(node: Any, current_key: str = "") -> None:
        if isinstance(node, dict):
            # If this dict has a metadatakey-ish "name", carry it down
            new_key = current_key
            if isinstance(node.get("name"), str) and node.get("name").strip():
                new_key = node["name"].strip()

            # If it has a "value", inspect value with this key context
            if "value" in node:
                for vobj in _iter_view_sql_objects(node["value"]):
                    results.append((new_key, vobj))

            # Continue walking
            for v in node.values():
                walk(v, new_key)

        elif isinstance(node, list):
            for item in node:
                walk(item, current_key)

    walk(payload, "")
    return results


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/parse_lineage_from_url")
def parse_lineage_from_url(req: ParseLineageRequest):
    # url + regulation are mandatory by schema
    url = req.url.strip()
    regulation = req.regulation.strip()

    if not url:
        raise HTTPException(status_code=400, detail="url is required")
    if not regulation:
        raise HTTPException(status_code=400, detail="regulation is required")

    headers = req.headers or {}

    # Fetch metadata JSON
    try:
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to fetch URL: {e}")

    try:
        payload = r.json()
    except Exception:
        # Sometimes response might be text JSON
        try:
            payload = json.loads(r.text)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Response is not valid JSON: {e}")

    # Extract all (metadatakey, {view_name, sql_query}) entries
    extracted = _extract_entries_with_metadatakey(payload)

    # Filter by view_names if provided
    wanted = set([v.strip() for v in (req.view_names or []) if v and v.strip()])
    filtered: List[Tuple[str, Dict[str, Any]]] = []
    for mk, obj in extracted:
        vn = (obj.get("view_name") or "").strip()
        if not vn:
            continue
        if wanted and vn not in wanted:
            continue
        filtered.append((mk, obj))

    # If view_names were provided but nothing matched, still return clean response
    lineage_rows: List[Dict[str, str]] = []
    sql_count = 0

    for mk, obj in filtered:
        view_name = (obj.get("view_name") or "").strip()
        b64_sql = (obj.get("sql_query") or "").strip()
        if not b64_sql:
            continue

        sql_count += 1
        decoded_sql = _safe_b64decode_to_text(b64_sql)

        # Metadatakey rule:
        # - Always prefer metadatakey found from metadata JSON "name"
        # - If missing, fallback to request.metadatakey (testing convenience)
        effective_mk = mk or (req.metadatakey or "")

        rows = parse_sql_lineage(
            decoded_sql,
            regulation=regulation,
            metadatakey=effective_mk,
            view_name=view_name,
            dialect="spark",
        )
        lineage_rows.extend(rows)

    return {
        "success": True,
        "message": f"Successfully extracted lineage for {sql_count} SQL queries",
        "total_records": len(lineage_rows),
        "lineage_data": lineage_rows,
    }
    
@app.post("/parse_lineage_from_sql")
def parse_lineage_from_sql(req: ParseLineageencodedsql):
    # url + regulation are mandatory by schema
    decoded_sql = _safe_b64decode_to_text(b64_sql)
    url = req.sql.strip()
    decoded_sql = _safe_b64decode_to_text(b64_sql)
    # Metadatakey rule:
    # - Always prefer metadatakey found from metadata JSON "name"
    # - If missing, fallback to request.metadatakey (testing convenience)
    # effective_mk = mk or (req.metadatakey or "")
    lineage_rows = list()
    rows = parse_sql_lineage(
        decoded_sql,
        regulation='',
        metadatakey='',
        view_name='',
        dialect='',
    )
    lineage_rows.extend(rows)

    return {
        "success": True,
        "message": f"Successfully extracted lineage for {sql_count} SQL queries",
        "total_records": len(lineage_rows),
        "lineage_data": lineage_rows,
    }
    
class LineageRequestEncodedSql(BaseModel):
sql_text: str

@router.post("/parse_lineage_from_sql")
def parse_lineage_from_url(req: LineageRequestEncodedSql):
    b64_sql = req.sql_text.strip()
    decoded_sql = _safe_b64decode_to_text(b64_sql)
    lineage_rows: List[Dict[str, str]] = []
    rows = parse_sql_lineage(
        decoded_sql
       
    )
    lineage_rows.extend(rows)

    return {
        "success": True,
        "message": f"Successfully extracted lineage for 1 SQL queries",
        "total_records": len(lineage_rows),
        "lineage_data": lineage_rows,
    }
    
    
  kk  
    
#     def parse_sql_lineage(
#     source: Any,
#     regulation: Optional[str] = None,
#     metadatakey: Optional[str] = None,
#     view_name: Optional[str] = None,
#     dialect: Optional[str] = None
# ) -> List[Dict[str, str]]:  this  need to replace sql_lineage_glot.py file at line 62
    
    
