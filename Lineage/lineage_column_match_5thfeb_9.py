from fastapi import FastAPI,APIRouter, UploadFile, File, HTTPException
from typing import List, Dict, Any, Optional
import json
import os
import difflib
from fastapi import APIRouter, UploadFile, File, Form
import re

# app = FastAPI(title="SQL Lineage Column Matcher")

router =  APIRouter()

# =====================================================
# Fuzzy matching helpers (stdlib only)
# =====================================================



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
    LOW_THRESHOLD = 50
    strong_matches = []
    nvl_report = []
    lineage_rows = lineage_rows.get("lineage_data",{})
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
            

        # Strong match → keep existing behavior
        if best_match and best_score >= HIGH_THRESHOLD:
            strong_matches.append({
                "mappedColumn": target_col_raw,
                **best_match
            })

        # Weak match → NVL / COALESCE-style output
        else:
            nvl_report.append({
                "mappedColumn": target_col_raw,
                "candidateColumns": sorted(
                    candidates,
                    key=lambda x: x["matchPercentage"],
                    reverse=True
                )
            })

    return {
        "strongMatches": strong_matches,
        "nvlStyleReport": nvl_report}

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

@router.post("/match-lineage")
async def match_lineage(
    sql_lineage: Optional[UploadFile] = File(None),
    # sql_lineage_path: Optional[str] = Form(None),

    fields_mapping: Optional[UploadFile] = File(None),
    # fields_mapping_path: Optional[str] = Form(None),
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
    if fields_mapping:
        print("preapairing mapper data from loaded data")
        mapping_data = await load_uploaded_json(fields_mapping)
    else:
        print("prepairing mapper data from loacl field")
        mapping_data = load_local_json(fields_mapping_path)

    matches = match_columns_with_lineage(
        lineage_rows=lineage_data,
        field_mappings=mapping_data
    )

    return matches



