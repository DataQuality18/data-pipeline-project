from pydantic import BaseModel, Field
from fastapi import APIRouter, Form
from typing import List, Dict, Any, Optional
import difflib
import re

from metadata_service import (
    MapperInfoRequest,
    MetadataResponse,
    build_metadata_url,
    call_gateway,
)

router = APIRouter()

ODS_FACT_SERVICE_URL: str = (
    "http://rhoo-gateway-vip-uat.nam.nsroot.net"
    "/reghub-api/rhoo/ods-fact-service/facts"
)


# =====================================================
# Fuzzy matching helpers (stdlib only)
# =====================================================

def tokenize(col: str) -> set:
    """
    Tokenize a column name into meaningful uppercase tokens.
      - Normalizes underscores → spaces
      - Splits alpha-numeric boundaries  e.g. COUNTERPARTY1 → COUNTERPARTY 1
      - Removes pure numeric tokens
    """
    if not col:
        return set()
    col = col.upper().replace("_", " ")
    col = re.sub(r"([A-Z]+)(\d+)", r"\1 \2", col)
    tokens = set(col.split())
    tokens = {t for t in tokens if not t.isdigit()}
    return tokens


def has_meaningful_token_overlap(a: str, b: str) -> bool:
    """
    True only when the two column names share at least one token
    that is NOT a stop-word (ID, CODE, KEY …).
    Prevents false-positives like TRADE_ID <-> PRODUCT_ID.
    """
    STOP_WORDS = {"ID", "IDENTIFIER", "CODE", "KEY", "SK", "NO", "NUM"}
    tokens_a = tokenize(a) - STOP_WORDS
    tokens_b = tokenize(b) - STOP_WORDS
    return bool(tokens_a & tokens_b)


def normalize_col(col: str) -> str:
    """Strip underscores / spaces and uppercase for fuzzy comparison."""
    return col.upper().replace("_", "").replace(" ", "").strip()


def fuzzy_score(a: str, b: str) -> float:
    """Return a 0-100 similarity score between two normalised strings."""
    return difflib.SequenceMatcher(None, a, b).ratio() * 100


# =====================================================
# windata parser
# =====================================================

def parse_windata_row(
    windata_item: Dict[str, Any],
    winkeys: Dict[str, Any],
) -> Dict[str, Any]:
    """
    """
    return {
        # ── core fields consumed by match_columns_with_lineage ────────────
        "Column Name":   windata_item.get("a4", ""),   # matched against mapper
        "Table Name":    windata_item.get("a2", ""),
        "Database Name": windata_item.get("a3", ""),   # alias / schema as db proxy

        # ── traceability fields carried into match results ─────────────────
        "sql_file":        windata_item.get("a1", ""),
        "sql_file_ref":    windata_item.get("a6", ""),
        "source_column":   windata_item.get("a5", ""),
        "source_table":    windata_item.get("a7", ""),
        "lineage_actions": windata_item.get("lal", []),

        # ── winkeys context ────────────────────────────────────────────────
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
    Walk the ODS fact-service response and flatten every windata item
    from every factContainer into a single list of normalised lineage rows.

    Response structure:
        {
          "totalRecords": 1000,
          "factContainers": [
            {
              "windata": [ ... ],
              "winkeys": { currBranch, id, version, olympusApplication, windowType }
            },
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


# =====================================================
# Core matching logic
# =====================================================

def match_columns_with_lineage(
    lineage_rows: List[Dict[str, Any]],
    field_mappings: Dict[str, Any],
) -> List[Dict[str, Any]]:
    "copy here existing api code"
    pass


# =====================================================
# FastAPI endpoint — POST /match-lineage
# =====================================================
# ── Pydantic Models ────────────────────────────────────────────────────────────

class WinKeys(BaseModel):
    curr_branch: str = "1.26.3.2"           # winkeys:currBranch
    window_type: str = "sql_lineage"         # winkeys:windowType
    olympus_application: str = "rhoo"        # winkeys:olympusApplication


class WinData(BaseModel):
    a6: List[str] = Field(
        default_factory=list,
        description="SQL query source names (windata:a6)"
    )


class FilterCriteria(BaseModel):
    winkeys: WinKeys = Field(default_factory=WinKeys)
    windata: WinData = Field(default_factory=WinData)


class Header(BaseModel):
    regulation: str = "rhoo"
    stream: str = "window"


class MatchLineageRequest(BaseModel):
    filter_criteria: FilterCriteria = Field(default_factory=FilterCriteria)
    header: Header = Field(default_factory=Header)
    mapper: str = "source_olympus_rhoo_dmat_mapper"


# ── Endpoint (JSON body instead of Form) ──────────────────────────────────────
@router.post("/match-lineage")
async def match_lineage(request: MatchLineageRequest):
    # Access fields naturally via the nested model
    regulation = request.header.regulation
    stream = request.header.stream
    curr_branch = request.filter_criteria.winkeys.curr_branch
    window_type = request.filter_criteria.winkeys.window_type
    olympus_application = request.filter_criteria.winkeys.olympus_application
    a6_sources = request.filter_criteria.windata.a6
    mapper = request.mapper

    ods_payload: Dict[str, Any] = {
        "header": {
            "regulation": regulation,
            "stream":     stream,
        },
        "filter_criteria": {
            "winkeys:olympusApplication": olympus_application,
            "winkeys:windowType":         window_type,
            "winkeys:currBranch":         curr_branch,
            "windata": {
                "a6": a6_sources
    }
        },
    }

    print(f"[match-lineage] Step 1 — POST {ODS_FACT_SERVICE_URL}")
    print(f"[match-lineage]   payload → {ods_payload}")

    ods_response: Dict[str, Any] = await call_gateway(
        url=ODS_FACT_SERVICE_URL,
        method="POST",
        payload=ods_payload,
    )

    total_records: int            = ods_response.get("totalRecords", 0)
    fact_containers: List[Dict]   = ods_response.get("factContainers", [])
    print(f"[match-lineage]   totalRecords={total_records}  "
          f"factContainers={len(fact_containers)}")

    # ── Step 2: Flatten all windata rows from all factContainers ──────────────
    all_lineage_rows: List[Dict[str, Any]] = extract_lineage_rows_from_response(
        ods_response
    )
    print(f"[match-lineage] Step 2 — {len(all_lineage_rows)} lineage rows extracted "
          f"from {len(fact_containers)} factContainers")

    # ── Step 3: Fetch mapper column definitions ────────────────────────────────
    mapper      = mapper.strip().lower()
    gateway_url = build_metadata_url(regulation=regulation, mapper=mapper)
    print(f"[match-lineage] Step 3 — GET mapper metadata: {gateway_url}")

    gateway_data: Dict[str, Any] = await call_gateway(
        url=gateway_url, method="GET"
    )

    # ── Step 4: Fuzzy column matching ─────────────────────────────────────────
    print("[match-lineage] Step 4 — running fuzzy match ...")
    matches: List[Dict[str, Any]] = match_columns_with_lineage(
        lineage_rows=all_lineage_rows,
        field_mappings=gateway_data,
    )
    print(f"[match-lineage] Done — {len(matches)} mapper fields processed")

    return {
        "success":            True,
        "regulation":         regulation,
        "currBranch":         curr_branch,
        "olympusApplication": olympus_application,
        "windowType":         window_type,
        "totalRecords":       total_records,
        "totalLineageRows":   len(all_lineage_rows),
        "totalMapperFields":  len(matches),
        "message": (
            f"Matched {len(matches)} mapper fields against "
            f"{len(all_lineage_rows)} lineage rows "
            f"(branch: {curr_branch})"
        ),
        "lineage_data": matches,
    }