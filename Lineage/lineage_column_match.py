from fastapi import FastAPI,APIRouter, UploadFile, File, HTTPException
from typing import List, Dict, Any, Optional
import json
import os
import difflib

# app = FastAPI(title="SQL Lineage Column Matcher")

router =  APIRouter()

# =====================================================
# Fuzzy matching helpers (stdlib only)
# =====================================================

def normalize_col(col: str) -> str:
    return col.upper().replace("_", "").replace(" ", "").strip()

def fuzzy_score(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio() * 100

# =====================================================
# Core matching logic
# =====================================================

def match_columns_with_lineage(
    lineage_rows: List[Dict[str, Any]],
    field_mappings: List[Dict[str, Any]]
) -> List[Dict[str, str]]:

    results = []

    for mapping in field_mappings:
        target_col = normalize_col(mapping.get("column", ""))
        best_match = None
        best_score = 0

        for row in lineage_rows:
            lineage_col = normalize_col(row.get("Column Name", ""))
            score = fuzzy_score(target_col, lineage_col)
            results.append({
                "dbName": row.get("Database Name", ""),
                "tableName": row.get("Table Name", ""),
                "columnName": row.get("Column Name", ""),
                "matchPrecentage": round(score, 2)
            })

    return results

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

    if not isinstance(data, list):
        raise HTTPException(400, f"{file.filename} must contain a JSON array")

    return data

def load_local_json(filename: str) -> List[Dict]:
    if not os.path.exists(filename):
        raise HTTPException(400, f"Local file not found: {filename}")

    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        raise HTTPException(400, f"Invalid JSON in local file: {filename}")

    if not isinstance(data, list):
        raise HTTPException(400, f"{filename} must contain a JSON array")

    return data

# =====================================================
# FastAPI endpoint
# =====================================================

@router.post("/match-lineage")
async def match_lineage(
    sql_lineage: Optional[UploadFile] = File(None),
    fields_mapping: Optional[UploadFile] = File(None)
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
        lineage_data = load_local_json("sql_lineage.json")

    # 🔹 Load field mapping
    if fields_mapping:
        print("preapairing mapper data from loaded data")
        mapping_data = await load_uploaded_json(fields_mapping)
    else:
        print("prepairing mapper data from loacl field")
        mapping_data = load_local_json("fields_mapping.json")

    matches = match_columns_with_lineage(
        lineage_rows=lineage_data,
        field_mappings=mapping_data
    )

    return matches


