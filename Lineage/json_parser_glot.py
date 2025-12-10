import requests
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sql_lineage_7 import SQLLineageParser

app = FastAPI()
parser = SQLLineageParser()


# ------------------------------- #
#         REQUEST MODEL
# ------------------------------- #

class LineageRequest(BaseModel):
    url: str                     # REQUIRED
    regulation: str              # REQUIRED
    metadatakey: str | None = None
    view_name: str | None = None


# ------------------------------- #
#         API ENDPOINT
# ------------------------------- #

@app.post("/parse-lineage-from-url")
def parse_lineage(req: LineageRequest):

    # 1) Validate URL
    try:
        resp = requests.get(req.url)
        resp.raise_for_status()
        meta = resp.json()
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to fetch metadata: {str(e)}"
        )

    # 2) Validate regulation
    if not req.regulation or req.regulation.strip() == "":
        raise HTTPException(400, detail="regulation is required")

    # 3) Extract optional metadata
    metadatakey = req.metadatakey or meta.get("metadatakey", "unknown_metadata")
    view_name = req.view_name or meta.get("view_name", "unknown_view")

    # 4) Extract SQL blocks
    sql_blocks = []
    sql_keys = ["sql", "query", "sql_query", "select_query", "create_query"]

    for key in sql_keys:
        if key in meta:
            val = meta[key]

            if isinstance(val, str):
                sql_blocks.append(val)

            elif isinstance(val, dict) and "query" in val:
                sql_blocks.append(val["query"])

            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, dict) and "query" in item:
                        sql_blocks.append(item["query"])

    if not sql_blocks:
        raise HTTPException(400, detail="No SQL queries found in metadata response")

   
