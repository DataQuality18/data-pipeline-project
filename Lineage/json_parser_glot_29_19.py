"""
json_parser_glot.py

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
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from test3 import parse_sql_lineage


# app = FastAPI(title="SQL Lineage Parser (SQLGlot)")
router =  APIRouter()


class ParseLineageRequest(BaseModel):
    url: str = Field(..., description="Metadata JSON endpoint URL")
    regulation: str = Field(..., description="Regulation (mandatory)")
    metadatakey: Optional[str] = Field(None, description="Optional fallback metadatakey for testing")
    class_name: Optional[str] = Field(None, description="Optional (kept for compatibility/testing)")
    view_names: Optional[List[str]] = Field(None, description="Optional list of view_name values to filter")
    headers: Optional[Dict[str, str]] = Field(None, description="Optional HTTP headers")

class LineageRequestEncodedSql(BaseModel):
    sql_text: str
    

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

import base64

def encode_text_to_base64(text: str, encoding: str = "utf-8") -> str:
    encoded_bytes = base64.b64encode(text.encode(encoding))
    return encoded_bytes.decode("ascii")

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


# def _extract_entries_with_metadatakey(payload: Any) -> List[Tuple[str, Dict[str, Any]]]:
#     """
#     Returns list of: (metadatakey, view_sql_object)

#     Supports common real payload shapes:
#     - payload is a dict with keys like: {"name": "...", "value": {...}}
#     - payload is a list of many such dicts
#     - nested occurrences
#     """
#     results: List[Tuple[str, Dict[str, Any]]] = []

#     def walk(node: Any, current_key: str = "") -> None:
#         if isinstance(node, dict):
#             # If this dict has a metadatakey-ish "name", carry it down
#             new_key = current_key
#             if isinstance(node.get("name"), str) and node.get("name").strip():
#                 new_key = node["name"].strip()

#             # If it has a "value", inspect value with this key context
#             if "value" in node:
#                 for vobj in _iter_view_sql_objects(node["value"]):
#                     results.append((new_key, vobj))

#             # Continue walking
#             for v in node.values():
#                 walk(v, new_key)

#         elif isinstance(node, list):
#             for item in node:
#                 walk(item, current_key)

#     walk(payload, "")
#     return results
def _extract_entries_with_context(
    payload: Any,
    requested_metadatakey: Optional[str] = None
) -> List[Tuple[str, Optional[str], Dict[str, Any]]]:
    """
    Returns list of:
      (effective_metadatakey, classname, view_sql_object)

    - Honors requested_metadatakey if provided
    - Safely walks ANY JSON structure
    """

    results: List[Tuple[str, Optional[str], Dict[str, Any]]] = []

    def walk(
        node: Any,
        current_key: Optional[str] = None,
        current_class: Optional[str] = None
    ):
        if isinstance(node, dict):
            mk = current_key
            cls = current_class

            # Capture metadatakey from payload
            if isinstance(node.get("name"), str) and node["name"].strip():
                mk = node["name"].strip()

            # Capture classname
            if isinstance(node.get("classname"), str):
                cls = node["classname"]

            # If value exists, extract SQLs under this metadata context
            if "value" in node:
                for vobj in _iter_view_sql_objects(node["value"]):
                    effective_mk = mk or requested_metadatakey or ""
                    results.append((effective_mk, cls, vobj))

            # Continue traversal
            for v in node.values():
                walk(v, mk, cls)

        elif isinstance(node, list):
            for item in node:
                walk(item, current_key, current_class)

    walk(payload)
    return results



@router.get("/health")
def health():
    return {"status": "ok"}


@router.post("/parse_lineage_from_url")
def parse_lineage_from_url(req: ParseLineageRequest):
    lineage_rows: List[Dict[str, str]] = []
    sql_count = 0

    try:
        if not req.url.strip():
            raise HTTPException(status_code=400, detail="url is required")
        if not req.regulation.strip():
            raise HTTPException(status_code=400, detail="regulation is required")

        headers = req.headers or {}

        try:
            r = requests.get(req.url, headers=headers, timeout=60)
            r.raise_for_status()
            payload = r.json()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to fetch URL: {e}")

        # extracted = _extract_entries_with_context(payload)
        extracted = _extract_entries_with_context(
                payload,
                requested_metadatakey=req.metadatakey
            )


        wanted_views = set(v.strip() for v in (req.view_names or []) if v.strip())

        for mk, cls, obj in extracted:
            
            # metadatakey filter
            if req.metadatakey and mk and req.metadatakey != mk:
                continue
            
            view_name = (obj.get("view_name") or "").strip()
            if not view_name:
                continue

            # view_name filter (optional)
            if wanted_views and view_name not in wanted_views:
                continue

            # class_name filter (optional)
            if req.class_name and cls and req.class_name != cls:
                continue

            b64_sql = (obj.get("sql_query") or "").strip()
            if not b64_sql:
                continue

            decoded_sql = _safe_b64decode_to_text(b64_sql)
            sql_count += 1

            effective_mk = mk or (req.metadatakey or "")

            rows = parse_sql_lineage(
                decoded_sql,
                regulation=req.regulation,
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

    except Exception as err:
        return {
            "success": False,
            "message": f"Error: {err}",
            "total_records": len(lineage_rows),
            "lineage_data": lineage_rows,
        }


@router.post("/parse_lineage_from_sql")
def parse_lineage_from_url(req: LineageRequestEncodedSql):
    decoded_sql = req.sql_text.strip()
    # decoded_sql = _safe_b64decode_to_text(b64_sql)
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
