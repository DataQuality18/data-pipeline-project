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
    """
    For every mapper column (field_mappings → value → table_columns),
    scan ALL lineage rows (extracted from windata) and produce:

      bestMatch   — the single highest-scoring row that passes
                    the semantic-overlap gate (score can be any value)
      candidates  — all rows that pass the gate but score < 80,
                    sorted descending by matchPercentage

    Every match entry carries the full traceability chain:
      sqlFile, sqlFileRef, tableName, dbName,
      sourceColumn, sourceTable, lineageActions,
      currBranch, olympusApplication
    """
    HIGH_THRESHOLD = 80
    strong_matches: List[Dict[str, Any]] = []

    columns_list: List[Dict[str, Any]] = (
        field_mappings.get("value", {}).get("table_columns", [])
    )

    for mapping in columns_list:
        target_col_raw: str = mapping.get("column_name", "")
        target_col: str     = normalize_col(target_col_raw)

        best_score: float              = 0.0
        best_match: Optional[Dict]     = None
        candidates: List[Dict[str, Any]] = []

        for row in lineage_rows:
            lineage_col_raw: str = row.get("Column Name", "")

            # Skip SQL wildcard columns
            if lineage_col_raw.strip() == "*":
                continue

            # Semantic gate — must share a meaningful token
            if not has_meaningful_token_overlap(target_col_raw, lineage_col_raw):
                continue

            lineage_col: str  = normalize_col(lineage_col_raw)
            score: float      = fuzzy_score(target_col, lineage_col)

            match_entry: Dict[str, Any] = {
                # ── match identity ─────────────────────────────────────────
                "columnName":      lineage_col_raw,
                "matchPercentage": round(score, 2),
                # ── lineage traceability (from windata + winkeys) ──────────
                "sqlFile":            row.get("sql_file", ""),
                "sqlFileRef":         row.get("sql_file_ref", ""),
                "tableName":          row.get("Table Name", ""),
                "dbName":             row.get("Database Name", ""),
                "sourceColumn":       row.get("source_column", ""),
                "sourceTable":        row.get("source_table", ""),
                "lineageActions":     row.get("lineage_actions", []),
                "currBranch":         row.get("currBranch", ""),
                "olympusApplication": row.get("olympusApplication", ""),
            }

            if score > best_score:
                best_score = score
                best_match = match_entry

            if score < HIGH_THRESHOLD:
                candidates.append(match_entry)

        strong_matches.append({
            "mappedColumn": target_col_raw,
            "mapperField":  mapping.get("field", ""),
            "matchedColumn": {
                "bestMatch": best_match,
                "candidates": sorted(
                    candidates,
                    key=lambda x: x["matchPercentage"],
                    reverse=True,
                ),
            },
        })

    return strong_matches


# =====================================================
# FastAPI endpoint — POST /match-lineage
# =====================================================

@router.post("/match-lineage")
async def match_lineage(
    # ── ODS Fact-Service filter params (mirror the SoapUI request body) ────────
    regulation: str = Form(
        default="rhoo",
        description=(
            "Sent as header.regulation AND used to build the mapper URL. "
            "e.g. 'rhoo'"
        ),
    ),
    stream: str = Form(
        default="window",
        description="header.stream — e.g. 'window'",
    ),
    olympus_application: str = Form(
        default="rhoo",
        description="filter_criteria → winkeys:olympusApplication — e.g. 'rhoo'",
    ),
    curr_branch: str = Form(
        default="1.26.3.2",
        description="filter_criteria → winkeys:currBranch — e.g. '1.26.3.2'",
    ),
    window_type: str = Form(
        default="sql_lineage",
        description="filter_criteria → winkeys:windowType — always 'sql_lineage'",
    ),
    # ── Mapper metadata param ──────────────────────────────────────────────────
    mapper: str = Form(
        default="source_olympus_rhoo_dmat_mapper",
        description="Mapper name used to fetch column definitions from the gateway",
    ),
):
    """
    POST /match-lineage
    """

    # ── Step 1: Build payload and POST to ODS fact-service ────────────────────
    regulation          = regulation.strip().lower()
    stream              = stream.strip().lower()
    olympus_application = olympus_application.strip().lower()
    curr_branch         = curr_branch.strip()
    window_type         = window_type.strip().lower()

    ods_payload: Dict[str, Any] = {
        "header": {
            "regulation": regulation,
            "stream":     stream,
        },
        "filter_criteria": {
            "winkeys:olympusApplication": olympus_application,
            "winkeys:windowType":         window_type,
            "winkeys:currBranch":         curr_branch,
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