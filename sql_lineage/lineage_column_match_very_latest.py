"""
lineage_column_match.py
─────────────────────────────────────────────────────────────────────────────
POST /match-lineage

Metadata-service response shape:

    {
      "name": "olympus_aniretd_source_query",
      "value": {
        "pos_markets": { "mapper": "source_olympus_aniretd_pos_mapper",
                         "sql_files": ["aniretd_pos_sql_query_source_olympus_new"] },
        "pos_sip":     { "mapper": "source_olympus_aniretd_pos_mapper",
                         "sql_files": ["aniretd_pos_sip_sql_query_source_olympus_new"] },
        "trade_cds":   { "mapper": "source_olympus_aniretd_trade_mapper",
                         "sql_files": ["aniretd_trade_sql_query_source_olympus_cds"] },
        ...
      }
    }

For every key inside "value" (concurrently):
  A. GET  mapper field definitions
  B. POST ODS lineage  (one request per key, filtered to that key's sql_files)
  C. match_columns_with_lineage() → records tagged with the key name

Final response aggregates all keys into a single "lineage_data" list.
─────────────────────────────────────────────────────────────────────────────
"""

import asyncio
import difflib
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field, model_serializer

from metadata_service import (
    MapperInfoRequest,   # noqa: F401 – keep for external import compat
    MetadataResponse,    # noqa: F401
    build_metadata_url,
    call_gateway,
)

router = APIRouter()

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

ODS_FACT_SERVICE_URL: str = (
    "http://rhoo-gateway-vip-uat.nam.nsroot.net"
    "/reghub-api/rhoo/ods-fact-service/facts"
)

_MATCH_THRESHOLD: float = 40.0   # scores below this → "no_match"
_EXACT_SCORE:     float = 100.0


# ─────────────────────────────────────────────────────────────────────────────
# Fuzzy-matching helpers  (stdlib only)
# ─────────────────────────────────────────────────────────────────────────────

def tokenize(col: str) -> set:
    """
    Tokenize a column name into meaningful uppercase tokens.
      - Normalises underscores → spaces
      - Splits alpha-numeric boundaries  e.g. COUNTERPARTY1 → COUNTERPARTY 1
      - Removes pure numeric tokens
    """
    if not col:
        return set()
    col = col.upper().replace("_", " ")
    col = re.sub(r"([A-Z]+)(\d+)", r"\1 \2", col)
    tokens = set(col.split())
    return {t for t in tokens if not t.isdigit()}


def has_meaningful_token_overlap(a: str, b: str) -> bool:
    """
    True only when the two names share ≥1 token that is NOT a stop-word.
    Prevents false-positives like TRADE_ID ↔ PRODUCT_ID.
    """
    STOP_WORDS = {"ID", "IDENTIFIER", "CODE", "KEY", "SK", "NO", "NUM"}
    return bool((tokenize(a) - STOP_WORDS) & (tokenize(b) - STOP_WORDS))


def normalize_col(col: str) -> str:
    """Strip underscores / spaces and uppercase for fuzzy comparison."""
    return col.upper().replace("_", "").replace(" ", "").strip()


def fuzzy_score(a: str, b: str) -> float:
    """Return a 0-100 similarity score between two normalised strings."""
    return difflib.SequenceMatcher(None, a, b).ratio() * 100


# ─────────────────────────────────────────────────────────────────────────────
# ODS response → flat lineage rows
# ─────────────────────────────────────────────────────────────────────────────

def parse_windata_row(
    windata_item: Dict[str, Any],
    winkeys: Dict[str, Any],
) -> Dict[str, Any]:
    """Map a raw ODS windata item + its winkeys into a normalised row dict."""
    return {
        # core fields used for column matching
        "Column Name":   windata_item.get("a4", ""),
        "Table Name":    windata_item.get("a2", ""),
        "Database Name": windata_item.get("a3", ""),
        # traceability
        "sql_file":        windata_item.get("a1", ""),
        "sql_file_ref":    windata_item.get("a6", ""),
        "source_column":   windata_item.get("a5", ""),
        "source_table":    windata_item.get("a7", ""),
        "lineage_actions": windata_item.get("lal", []),
        # winkeys context
        "currBranch":         winkeys.get("currBranch", ""),
        "winkey_id":          winkeys.get("id", ""),
        "version":            winkeys.get("version", ""),
        "olympusApplication": winkeys.get("olympusApplication", ""),
        "windowType":         winkeys.get("windowType", ""),
    }


def extract_lineage_rows_from_response(
    api_response: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Flatten every windata item from every factContainer into a single list.

    ODS response shape:
        {
          "totalRecords": N,
          "factContainers": [
            { "winkeys": {...}, "windata": [...] },
            ...
          ]
        }
    """
    rows: List[Dict[str, Any]] = []
    for container in api_response.get("factContainers", []):
        winkeys: Dict[str, Any]       = container.get("winkeys", {})
        windata: List[Dict[str, Any]] = container.get("windata", [])
        for item in windata:
            rows.append(parse_windata_row(item, winkeys))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Mapper field extractor  (handles multiple gateway response shapes)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_mapper_fields(
    field_mappings: Any,
) -> List[Dict[str, str]]:
    """
    Normalise the mapper API response into:
        [ { "mapperColumn": str, "mapperFactField": str }, ... ]

    Supported shapes
    ─────────────────
    A  flat dict    { "TRADE_ID": "TRADE_IDENTIFIER", ... }
    B  list of objs [ { "column": "TRADE_ID", "factField": "..." }, ... ]
    C  envelope     { "fields": [...] / "columns": [...] / "data": [...] }
    """
    if not field_mappings:
        return []

    # Shape C — unwrap known envelope keys
    if isinstance(field_mappings, dict):
        for key in ("fields", "columns", "mappings", "data", "fieldMappings"):
            if key in field_mappings and isinstance(field_mappings[key], list):
                field_mappings = field_mappings[key]
                break  # fall through to Shape B

    # Shape B — list of field-definition objects
    if isinstance(field_mappings, list):
        result = []
        for item in field_mappings:
            if not isinstance(item, dict):
                continue
            col = (
                item.get("column")
                or item.get("columnName")
                or item.get("name")
                or item.get("field")
                or item.get("mapperColumn")
                or ""
            )
            fact = (
                item.get("factField")
                or item.get("fact_field")
                or item.get("target")
                or item.get("mapperFactField")
                or ""
            )
            if col:
                result.append({"mapperColumn": str(col), "mapperFactField": str(fact)})
        return result

    # Shape A — flat {column: factField} dict
    if isinstance(field_mappings, dict):
        return [
            {"mapperColumn": str(col), "mapperFactField": str(fact)}
            for col, fact in field_mappings.items()
            if not col.startswith("@") and col not in ("totalRecords", "status")
        ]

    return []


# ─────────────────────────────────────────────────────────────────────────────
# Core matching logic
# ─────────────────────────────────────────────────────────────────────────────

def match_columns_with_lineage(
    lineage_rows:   List[Dict[str, Any]],
    field_mappings: Dict[str, Any],
    *,
    metadata_key: str = "",   # e.g. "pos_markets" — tag injected by caller
    regulation:   str = "",
) -> List[Dict[str, Any]]:
    """
    For every mapper field find the single best-matching lineage row.

    Scoring tiers
    ─────────────
    1. Exact normalised match                 → 100 %   matchType: exact
    2. Shared meaningful token + fuzzy ≥ 40 % → score   matchType: token+fuzzy
    3. Pure fuzzy ≥ 40 %                      → score   matchType: fuzzy
    4. Best score < threshold                 → 0 %     matchType: no_match

    One record is always emitted per mapper field (including no_match) so
    downstream consumers can see which mapper fields had no coverage.

    Output record
    ─────────────
    {
        "regulation":      str,
        "key":             str,    ← metadata key, e.g. "pos_markets"
        "mapperColumn":    str,
        "mapperFactField": str,
        "dbName":          str,
        "tableName":       str,
        "columnName":      str,    ← lineage-side column (a4)
        "sourceColumn":    str,    ← upstream source column (a5)
        "sourceTable":     str,    ← upstream source table  (a7)
        "sqlFile":         str,    ← sql_file (a1)
        "sqlFileRef":      str,    ← sql_file_ref (a6)
        "matchPercentage": str,    ← e.g. "87.5%"
        "matchType":       str,    ← exact | token+fuzzy | fuzzy | no_match
    }
    """
    mapper_fields = _extract_mapper_fields(field_mappings)
    results: List[Dict[str, Any]] = []

    for mf in mapper_fields:
        mapper_col  = mf["mapperColumn"]
        mapper_fact = mf["mapperFactField"]
        norm_mapper = normalize_col(mapper_col)

        best_score: float          = -1.0
        best_row:   Optional[Dict] = None
        best_type:  str            = "no_match"

        for row in lineage_rows:
            lineage_col = row.get("Column Name", "")
            if not lineage_col:
                continue

            norm_lineage = normalize_col(lineage_col)

            # Tier 1: exact
            if norm_mapper == norm_lineage:
                score, match_type = _EXACT_SCORE, "exact"
            else:
                score      = fuzzy_score(norm_mapper, norm_lineage)
                match_type = (
                    "token+fuzzy"
                    if has_meaningful_token_overlap(mapper_col, lineage_col)
                    else "fuzzy"
                )

            if score > best_score:
                best_score, best_row, best_type = score, row, match_type

        if best_row is not None and best_score >= _MATCH_THRESHOLD:
            results.append({
                "regulation":      regulation or best_row.get("olympusApplication", ""),
                "key":             metadata_key,
                "mapperColumn":    mapper_col,
                "mapperFactField": mapper_fact,
                "dbName":          best_row.get("Database Name", ""),
                "tableName":       best_row.get("Table Name", ""),
                "columnName":      best_row.get("Column Name", ""),
                "sourceColumn":    best_row.get("source_column", ""),
                "sourceTable":     best_row.get("source_table", ""),
                "sqlFile":         best_row.get("sql_file", ""),
                "sqlFileRef":      best_row.get("sql_file_ref", ""),
                "matchPercentage": f"{best_score:.1f}%",
                "matchType":       best_type,
            })
        
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Per-key async processor
# ─────────────────────────────────────────────────────────────────────────────

async def _process_metadata_key(
    key:         str,
    entry:       Dict[str, Any],
    regulation:  str,
    curr_branch: str,
) -> List[Dict[str, Any]]:
    """
    Process one key from the metadata "value" dict.

    entry = {
        "mapper":    "source_olympus_aniretd_pos_mapper",
        "sql_files": ["aniretd_pos_sql_query_source_olympus_new"]
    }

    A. GET  mapper field definitions
    B. POST ODS lineage (filtered to this key's sql_files via windata:a6)
    C. match_columns_with_lineage()
    """
    mapper    = entry.get("mapper", "").strip().lower()
    sql_files = entry.get("sql_files", [])

    if not mapper:
        print(f"  [{key}] ⚠  no mapper — skipping")
        return []

    print(f"  [{key}] mapper={mapper}  sql_files={sql_files}")

    # A. Mapper URL
    mapper_url = build_metadata_url(regulation=regulation, mapper=mapper)
    print(f"  [{key}] GET mapper → {mapper_url}")

    # B. ODS payload — scope lineage to this key's sql_files
    ods_payload: Dict[str, Any] = {
        "filterCriteria": {
            "winkeys:currBranch":         curr_branch,
            "winkeys:windowType":         "sql_lineage",
            "winkeys:olympusApplication": regulation,
            **({"windata:a6": sql_files} if sql_files else {}),
        },
        "header": {
            "regulation": regulation,
            "stream":     "window",
        },
    }
    print(f"  [{key}] POST ODS → {ODS_FACT_SERVICE_URL}")

    # Fetch both concurrently; degrade gracefully on failure
    mapper_result, ods_response = await asyncio.gather(
        call_gateway(url=mapper_url, method="GET"),
        call_gateway(url=ODS_FACT_SERVICE_URL, method="POST", payload=ods_payload),
        return_exceptions=True,
    )

    if isinstance(mapper_result, Exception):
        print(f"  [{key}] ✗ mapper failed: {mapper_result}")
        mapper_result = {}

    if isinstance(ods_response, Exception):
        print(f"  [{key}] ✗ ODS failed: {ods_response}")
        ods_response = {}

    lineage_rows = extract_lineage_rows_from_response(
        ods_response if isinstance(ods_response, dict) else {}
    )
    total_records = (ods_response.get("totalRecords", 0)
                     if isinstance(ods_response, dict) else 0)
    print(f"  [{key}] totalRecords={total_records}  lineage_rows={len(lineage_rows)}")

    # C. Match
    matches = match_columns_with_lineage(
        lineage_rows   = lineage_rows,
        field_mappings = mapper_result,
        metadata_key   = key,
        regulation     = regulation,
    )
    print(f"  [{key}] → {len(matches)} records "
          f"(matched={sum(1 for m in matches if m['matchType'] != 'no_match')})")
    return matches


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic request models
# ─────────────────────────────────────────────────────────────────────────────

class MatchLineageRequest(BaseModel):
    """
    Request body for POST /match-lineage.

    base_url        — reghub gateway root
    regulation_meta — metadata key, e.g. "rhoo_emiretd_metadata_batch_sourcing"
    regulation      — e.g. "rhoo"
    current_branch  — e.g. "1.26.3.2"
    """
    base_url:        str            = "http://rhoo-gateway-vip-uat.nam.nsroot.net/reghub-api"
    regulation_meta: str            = "rhoo_emiretd_metadata_batch_sourcing"
    regulation: str                 = "rhoo"
    current_branch: str             = "1.26.3.2"

    


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint  POST /match-lineage
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/match-lineage")
async def match_lineage(request: MatchLineageRequest):
    """
    Full pipeline
    ─────────────
    1. GET  metadata-service → { name, value: { key: {mapper, sql_files} } }
    2. For every key in value — concurrently:
         A. GET  mapper field definitions
         B. POST ODS lineage (scoped to key's sql_files)
         C. match_columns_with_lineage()
    3. Aggregate all match records, attach summary stats, return.

    Every record in "lineage_data" carries a "key" field that identifies
    which metadata key (e.g. pos_markets, trade_cds) it originated from.
    """
    regulation  = request.regulation
    curr_branch = request.current_branch

    # ── Step 1: master metadata ────────────────────────────────────────────────
    metadata_url = (
        f"{request.base_url.rstrip('/')}"
        f"/{regulation}/metadata-service/metadata/{request.regulation_meta}"
    )
    print(f"[match-lineage] Step 1 — GET metadata\n  → {metadata_url}")

    metadata_response: Dict[str, Any] = await call_gateway(
        url=metadata_url, method="GET"
    )

    # The payload we care about lives under "value"
    metadata_value: Dict[str, Any] = metadata_response.get("value", {})
    if not metadata_value:
        return {
            "success": False,
            "message": (
                f"metadata-service returned no 'value' for "
                f"{request.regulation_meta}"
            ),
            "lineage_data": [],
        }

    print(
        f"[match-lineage] name={metadata_response.get('name')}  "
        f"keys={list(metadata_value.keys())}"
    )

    # ── Steps 2A/B/C: all metadata keys in parallel ────────────────────────────
    print(
        f"[match-lineage] Step 2 — processing {len(metadata_value)} keys concurrently"
    )
    key_results: List[List[Dict[str, Any]]] = await asyncio.gather(
        *[
            _process_metadata_key(
                key         = key,
                entry       = entry,
                regulation  = regulation,
                curr_branch = curr_branch,
            )
            for key, entry in metadata_value.items()
            if isinstance(entry, dict)
        ]
    )

    # ── Step 3: aggregate ─────────────────────────────────────────────────────
    all_matches: List[Dict[str, Any]] = [
        record
        for per_key in key_results
        for record in per_key
    ]

    exact     = sum(1 for m in all_matches if m["matchType"] == "exact")
   

    # Per-key summary  { key: { total, matched, no_match } }
    key_summary: Dict[str, Dict[str, int]] = {}
    for m in all_matches:
        k = m["key"]
        if k not in key_summary:
            key_summary[k] = {"total": 0, "matched": 0, "no_match": 0}
        key_summary[k]["total"] += 1
        if m["matchType"] != "no_match":
            key_summary[k]["matched"] += 1
        else:
            key_summary[k]["no_match"] += 1

    print(f"[match-lineage] Done — {len(all_matches)} total records")

    return {
        "success":           True,
        "regulation":        regulation,
        "regulationMeta":    request.regulation_meta,
        "currBranch":        curr_branch,
        "metadataName":      metadata_response.get("name", ""),
        "keysProcessed":     list(metadata_value.keys()),
        "totalMatchRecords": len(all_matches),
        "matchSummary": {
            "exact":       exact,
          
        },
        "keySummary": key_summary,
        "message": (
            f"Processed {len(metadata_value)} metadata keys — "
            f"{len(all_matches)} total records "
            f"(exact={exact}"
        ),
        "lineage_data": all_matches,
    }