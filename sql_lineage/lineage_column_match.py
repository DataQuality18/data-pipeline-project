from fastapi import FastAPI,APIRouter, UploadFile, File, HTTPException
from typing import List, Dict, Any, Optional
import json
import os
import difflib
from fastapi import APIRouter, UploadFile, File, Form
import re
from pydantic import BaseModel, Field
# app = FastAPI(title="SQL Lineage Column Matcher")
from metadata_service import (
    MapperInfoRequest,
    MetadataResponse,
    build_metadata_url,
    call_gateway,
    
)
router =  APIRouter()

# =====================================================
# Fuzzy matching helpers (stdlib only)
# =====================================================
class MetadataResponse(BaseModel):
    """Generic wrapper for successful gateway responses."""
    status: str = Field(description="Outcome: 'success' or 'error'")
    regulation: str
    mapper: str
    gateway_url: str = Field(description="The full URL that was called")
    data: Optional[dict] = Field(default=None, description="Raw gateway payload")
    message: Optional[str] = Field(default=None, description="Human-readable note")


def tokenize(col: str) -> set:
    if not col:
        return set()

    # 1. Normalize separators
    col = col.upper().replace("_", " ")

    # 2. Split alpha-numeric boundaries: COUNTERPARTY1 → COUNTERPARTY 1
    col = re.sub(r"([A-Z]+)(\d+)", r"\1 \2", col)

    # 3. Tokenize
    tokens = set(col.split())

    # 4. Remove pure numbers
    tokens = {t for t in tokens if not t.isdigit()}

    return tokens



def has_meaningful_token_overlap(a: str, b: str) -> bool:
    STOP_WORDS = {
        "ID", "IDENTIFIER", "CODE", "KEY", "SK", "NO", "NUM"
    }
    #  PRODUCT_ID_TOXNOMY  PRODUCT
    tokens_a = tokenize(a) - STOP_WORDS
    tokens_b = tokenize(b) - STOP_WORDS

    return bool(tokens_a & tokens_b)


def normalize_col(col: str) -> str:
    return col.upper().replace("_", "").replace(" ", "").strip()

def fuzzy_score(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio() * 100

# =====================================================
# Core matching logic
# =====================================================

def match_columns_with_lineage(
    lineage_rows: List[Dict[str, Any]],
    field_mappings: List[Dict[str, Any]],
    threshold: int = 80
) -> Dict[str, Any]:
    HIGH_THRESHOLD = 80
    LOW_THRESHOLD = 0
    strong_matches = []
    nvl_report = []
    # lineage_rows = lineage_rows.get("lineage_data",{})
    field_mappings =  field_mappings.get("value", {}).get("table_columns", [])
    for mapping in field_mappings:
        target_col_raw = mapping.get("column_name", "")
        target_col = normalize_col(target_col_raw)

        best_score = 0
        best_match = None
        candidates = []

        for row in lineage_rows:
            lineage_col_raw = row.get("Column Name", "")

            # Skip wildcard columns
            if lineage_col_raw.strip() == "*":
                continue

            lineage_col = normalize_col(lineage_col_raw)
            score = fuzzy_score(target_col, lineage_col)

            # Track best match (even if weak)
            is_semantic = has_meaningful_token_overlap(
                target_col_raw, lineage_col_raw
            )

            if not  has_meaningful_token_overlap(target_col_raw, lineage_col_raw):
                continute

            if score > best_score:
                best_score = score
                best_match = {
                    "dbName": row.get("Database Name", ""),
                    "tableName": row.get("Table Name", ""),
                    "columnName": lineage_col_raw,
                    "matchPercentage": round(score, 2)
                }
            # ONLY keep semantically meaningful weak matches
            if (LOW_THRESHOLD <= score < HIGH_THRESHOLD and has_meaningful_token_overlap(target_col_raw, lineage_col_raw)):
                candidates.append({
                    "dbName": row.get("Database Name", ""),
                    "tableName": row.get("Table Name", ""),
                    "columnName": lineage_col_raw,
                    "matchPercentage": round(score, 2)
                })
            
        strong_matches.append({
            "mappedColumn": target_col_raw,
            "mapperfiled":mapping.get("field", ""),
                
            "matchedColumn":[best_match, sorted(
                    candidates,
                    key=lambda x: x["matchPercentage"],
                    reverse=True
                )]
        })
       

    return strong_matches

# =====================================================
# JSON loaders
# =====================================================

async def load_uploaded_json(file: UploadFile) -> List[Dict]:
    if not file.filename.endswith(".json"):
        raise HTTPException(400, f"{file.filename} is not a JSON file")

    try:
        content = await file.read()
        data = json.loads(content)
    except Exception:
        raise HTTPException(400, f"Invalid JSON in {file.filename}")
    return data

def load_local_json(filename: str) -> List[Dict]:
    if not os.path.exists(filename):
        raise HTTPException(400, f"Local file not found: {filename}")

    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        raise HTTPException(400, f"Invalid JSON in local file: {filename}")

    return data

# =====================================================
# FastAPI endpoint
# =====================================================

@router.post(
    "/match-lineage",
    response_model=MetadataResponse,
    summary="Match SQL lineage columns against mapper metadata",
    tags=["Lineage"],
)
async def match_lineage(
    # SQL lineage is still uploaded as a file
    sql_lineage: UploadFile = File(..., description="JSON file containing SQL lineage rows"),
    # Mapper params come from the request body — passed straight to fetch-mapper-info
    mapper_request: MapperInfoRequest = ...,
):
    """
    If files are uploaded → use them
    Else → read sql_lineage.json & fields_mapping.json from same directory
    """

    # 🔹 Load SQL lineage
    
    if sql_lineage:
        print(f"reading data from uploaded file {sql_lineage}")
        lineage_data = await load_uploaded_json(sql_lineage)
        print("file uploaded ")
    else:
        print(f"prepairing data from loacal file:")
        lineage_data = load_local_json(sql_lineage_path)

    # 🔹 Load field mapping
    # ── Step 2: Fetch mapper metadata from the metadata service ──────────────
    gateway_url = build_metadata_url(
        regulation=mapper_request.regulation,
        mapper=mapper_request.mapper,
    )
     # Reuse the shared call_gateway helper — consistent error handling & logging
    gateway_data: dict = await call_gateway(url=gateway_url, method="GET")
    
    matches = match_columns_with_lineage(
        lineage_rows=lineage_data,
        field_mappings=gateway_data
    )

    return {
            "success": True,
            "message": f"Successfully extracted lineage for {sql_count} SQL queries",
            "lineage_data": matches,
        }


