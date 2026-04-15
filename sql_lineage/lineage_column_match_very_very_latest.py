"""
lineage_column_match.py  — with full exception handling
─────────────────────────────────────────────────────────────────────────────
POST /match-lineage

For every key inside metadata "value" (concurrently):
  A. GET  mapper field definitions
  B. POST ODS lineage  (scoped to that key's sql_files)
  C. match_columns_with_lineage() → records tagged with the key name

Final response aggregates all keys into a single "lineage_data" list.
─────────────────────────────────────────────────────────────────────────────
"""

import asyncio
import difflib
import logging
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from metadata_service import (
    build_metadata_url,
    call_gateway,
)

router = APIRouter()
log    = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

ODS_FACT_SERVICE_URL: str = (
    "http://rhoo-gateway-vip-uat.nam.nsroot.net"
    "/reghub-api/rhoo/ods-fact-service/facts"
)

_MATCH_THRESHOLD: float = 40.0
_EXACT_SCORE:     float = 100.0


# ─────────────────────────────────────────────────────────────────────────────
# Fuzzy-matching helpers
# ─────────────────────────────────────────────────────────────────────────────

def tokenize(col: str) -> set:
    """
    Tokenize a column name into uppercase tokens.
    FIX: returns set() for non-str / None instead of raising AttributeError.
    """
    if not col or not isinstance(col, str):
        return set()
    col    = col.upper().replace("_", " ")
    col    = re.sub(r"([A-Z]+)(\d+)", r"\1 \2", col)
    tokens = set(col.split())
    return {t for t in tokens if not t.isdigit()}


def has_meaningful_token_overlap(a: str, b: str) -> bool:
    """
    True when a and b share ≥1 non-stop-word token.
    FIX: coerces inputs to str; returns False instead of raising on bad types.
    FIX: guards empty token sets (both sides must have ≥1 meaningful token).
    """
    STOP_WORDS = {"ID", "IDENTIFIER", "CODE", "KEY", "SK", "NO", "NUM"}
    try:
        tokens_a = tokenize(str(a) if a is not None else "") - STOP_WORDS
        tokens_b = tokenize(str(b) if b is not None else "") - STOP_WORDS
    except Exception as exc:
        log.warning("has_meaningful_token_overlap(%r, %r) failed: %s", a, b, exc)
        return False
    if not tokens_a or not tokens_b:   # FIX: empty set → False, not True
        return False
    return bool(tokens_a & tokens_b)


def normalize_col(col: str) -> str:
    """
    Strip underscores/spaces and uppercase.
    FIX: returns "" for non-str / None instead of raising AttributeError.
    """
    if not col or not isinstance(col, str):
        return ""
    return col.upper().replace("_", "").replace(" ", "").strip()


def fuzzy_score(a: str, b: str) -> float:
    """
    0-100 similarity score.
    FIX: returns 0.0 for non-str / empty instead of crashing SequenceMatcher.
    """
    if not a or not b or not isinstance(a, str) or not isinstance(b, str):
        return 0.0
    try:
        return difflib.SequenceMatcher(None, a, b).ratio() * 100
    except Exception as exc:
        log.warning("fuzzy_score(%r, %r) failed: %s", a, b, exc)
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# ODS response → flat lineage rows
# ─────────────────────────────────────────────────────────────────────────────

def parse_windata_row(
    windata_item: Dict[str, Any],
    winkeys:      Dict[str, Any],
) -> Dict[str, Any]:
    """
    Map one ODS windata item into a normalised row dict.
    FIX: guards both args against None / non-dict.
    """
    if not isinstance(windata_item, dict):
        windata_item = {}
    if not isinstance(winkeys, dict):
        winkeys = {}
    return {
        "Column Name":        windata_item.get("a4") or "",
        "Column Alias Name":  windata_item.get("a6") or "",
        "Table Name":         windata_item.get("a2") or "",
        "Database Name":      windata_item.get("a3") or "",
        "sql_file":           windata_item.get("a1") or "",
        "sql_file_ref":       windata_item.get("a6") or "",
        "source_column":      windata_item.get("a5") or "",
        "source_table":       windata_item.get("a7") or "",
        "lineage_actions":    windata_item.get("lal") or [],
        "currBranch":         winkeys.get("currBranch")         or "",
        "winkey_id":          winkeys.get("id")                 or "",
        "version":            winkeys.get("version")            or "",
        "olympusApplication": winkeys.get("olympusApplication") or "",
        "windowType":         winkeys.get("windowType")         or "",
    }


def extract_lineage_rows_from_response(
    api_response: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Flatten every windata item from every factContainer.
    FIX: guards against non-dict response, non-list factContainers,
         malformed containers, and per-item parse failures.
    """
    if not isinstance(api_response, dict):
        log.warning("extract_lineage_rows: expected dict, got %s", type(api_response).__name__)
        return []

    fact_containers = api_response.get("factContainers") or []
    if not isinstance(fact_containers, list):
        log.warning("factContainers is not a list: %s", type(fact_containers).__name__)
        return []

    rows: List[Dict[str, Any]] = []
    for idx, container in enumerate(fact_containers):
        if not isinstance(container, dict):
            log.warning("factContainers[%d] is not a dict — skipping", idx)
            continue
        winkeys = container.get("winkeys") or {}
        windata = container.get("windata") or []
        if not isinstance(windata, list):
            log.warning("factContainers[%d].windata is not a list — skipping", idx)
            continue
        for item in windata:
            try:
                rows.append(parse_windata_row(item, winkeys))
            except Exception as exc:
                log.warning("parse_windata_row failed (item=%r): %s", item, exc)
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Mapper field extractor
# ─────────────────────────────────────────────────────────────────────────────

def _extract_mapper_fields(field_mappings: Any) -> List[Dict[str, str]]:
    """
    Normalise mapper API response →
        [ { "mapperColumn", "mapperFactField", "columnType" }, ... ]

    Real API shape  { "name", "value": { "table_columns": [
        { "column_name": "TRADE_ID", "field": "src:tradeId", "column_type": "..." }
    ]}}

    FIX: always returns [] instead of None when no shape matches (was missing
         explicit return after the primary shape block → implicit None).
    FIX: wrapped in try/except so malformed data never propagates.
    """
    if not field_mappings:
        return []
    try:
        # ── Primary: { "name", "value": { "table_columns": [...] } } ──────────
        if isinstance(field_mappings, dict):
            value = field_mappings.get("value") or {}
            if isinstance(value, dict) and "table_columns" in value:
                table_columns = value["table_columns"]
                if isinstance(table_columns, list):
                    result = []
                    for item in table_columns:
                        if not isinstance(item, dict):
                            continue
                        col  = (item.get("column_name") or "").strip()
                        fact = (item.get("field")       or "").strip()
                        if col:
                            result.append({
                                "mapperColumn":    col,
                                "mapperFactField": fact,
                                "columnType":      (item.get("column_type") or ""),
                            })
                    return result
    except Exception as exc:
        log.error("_extract_mapper_fields failed: %s", exc, exc_info=True)
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Core matching logic
# ─────────────────────────────────────────────────────────────────────────────

def _no_match_record(mapper_col: str, mapper_fact: str, key: str, regulation: str) -> Dict[str, Any]:
    """Single source of truth for a no_match result record."""
    return {
        "regulation":      regulation,
        "key":             key,
        "mapperColumn":    mapper_col,
        "mapperFactField": mapper_fact,
        "dbName":          "",
        "tableName":       "",
        "columnName":      "",
        "columnAliasName": "",
        "matchedOn":       "",
        "matchPercentage": "0.0%",
        "matchType":       "no_match", 
    }


def match_columns_with_lineage(
    lineage_rows:   List[Dict[str, Any]],
    field_mappings: Dict[str, Any],
    *,
    metadata_key: str = "",
    regulation:   str = "",
) -> List[Dict[str, Any]]:
    """
    For every mapper field, scan ALL lineage rows and emit results using:

    Case 1 — EXACT / ABOVE THRESHOLD  (final_score >= _MATCH_THRESHOLD)
        ALL rows whose re-score >= threshold are emitted.
        matchType = "exact"  if final_score == 100.0
        matchType = "fuzzy"  otherwise

    Case 2 — WEAK MATCH  (0 < best_score < _MATCH_THRESHOLD)
        Only the single best-scoring row below threshold is emitted.
        matchType = "weak"

    Case 3 — NO MATCH  (no row passed filters or all scores <= 0)
        One no_match sentinel record is emitted.
        matchType = "no_match", all location fields empty.

    Note: Cases 1 and 2 are NOT mutually exclusive — if above-threshold
    rows exist they are always emitted; the weak row is only emitted when
    there are NO above-threshold rows (it is the best-available fallback).
    """
    try:
        mapper_fields = _extract_mapper_fields(field_mappings) or []
    except Exception as exc:
        log.error("[%s] _extract_mapper_fields raised: %s", metadata_key, exc, exc_info=True)
        mapper_fields = []

    results: List[Dict[str, Any]] = []
    log.info("[%s] mapper_fields_count=%d", metadata_key, len(mapper_fields))

    for mf in mapper_fields:
        mapper_col  = mf.get("mapperColumn",    "")
        mapper_fact = mf.get("mapperFactField", "")

        if not mapper_col:
            log.warning("[%s] skipping entry with empty mapperColumn", metadata_key)
            continue

        target_col_raw = mapper_col
        target_col     = normalize_col(mapper_col)

        # ── Two accumulators replace the old single best_row ──────────────────
        above_threshold_rows: List[Dict[str, Any]] = []   # Case 1 candidates
        best_weak_score: float          = -1.0             # Case 2 tracker
        best_weak_row:   Optional[Dict] = None

        skipped_db = skipped_wild = skipped_token = tried = 0

        # ── Inner loop: score every lineage row ───────────────────────────────
        for row in lineage_rows:
            try:
                dbName = row.get("Database Name") or ""
                if not dbName or "gfolyreg_work" in dbName.lower():
                    skipped_db += 1
                    continue

                lineage_col_row: str = row.get("Column Alias Name") or ""
                if not lineage_col_row.strip():
                    lineage_col_row = row.get("Column Name") or ""

                if lineage_col_row.strip() == "*":
                    skipped_wild += 1
                    continue

                lineage_col = normalize_col(lineage_col_row)

                # no token overlap on alias → retry with raw Column Name
                if not has_meaningful_token_overlap(target_col_raw, lineage_col_row):
                    fallback = row.get("Column Name")
                    if fallback is None or fallback.strip() == "*":
                        continue
                    lineage_col_row = fallback
                    lineage_col     = normalize_col(lineage_col_row)

                # still no overlap → skip
                if not has_meaningful_token_overlap(target_col_raw, lineage_col_row):
                    skipped_token += 1
                    continue

                tried += 1

                # ── Re-score against the resolved compare column ──────────────
                col_name  = row.get("Column Name",       "") or ""
                col_alias = row.get("Column Alias Name", "") or ""
                compare   = col_alias if col_alias.strip() else col_name

                m_norm = normalize_col(mapper_col)
                c_norm = normalize_col(compare)

                if m_norm == c_norm:
                    final_score = 100.0
                    match_type  = "exact"
                else:
                    final_score = min(fuzzy_score(m_norm, c_norm), 99.0)
                    match_type  = "fuzzy"

                # ── Route into the correct accumulator ────────────────────────
                if final_score >= _MATCH_THRESHOLD:
                    # Case 1: collect ALL qualifying rows
                    above_threshold_rows.append({
                        "regulation":      regulation or (row.get("olympusApplication") or ""),
                        "key":             metadata_key,
                        "mapperColumn":    mapper_col,
                        "mapperFactField": mapper_fact,
                        "dbName":          row.get("Database Name",     "") or "",
                        "tableName":       row.get("Table Name",        "") or "",
                        "columnName":      col_name,
                        "columnAliasName": col_alias,
                        "matchedOn":       "alias" if col_alias.strip() else "columnName",
                        "matchPercentage": f"{final_score:.1f}%",
                        "matchType":       match_type,   # "exact" | "fuzzy"
                    })

                elif final_score > 0:
                    # Case 2: keep only the single best weak candidate
                    if final_score > best_weak_score or (
                        final_score == best_weak_score
                        and best_weak_row is not None
                        and len(compare) > len(
                            (best_weak_row.get("Column Alias Name") or "")
                            or (best_weak_row.get("Column Name") or "")
                        )
                    ):
                        best_weak_score = final_score
                        best_weak_row   = row

            except Exception as exc:
                log.warning("[%s] row error: %s  row=%r", metadata_key, exc, row)
                continue

        log.debug(
            "[%s] col=%r  skipped_db=%d  wild=%d  token=%d  tried=%d"
            "  above_threshold=%d  weak_score=%.1f",
            metadata_key, mapper_col,
            skipped_db, skipped_wild, skipped_token, tried,
            len(above_threshold_rows), max(best_weak_score, 0),
        )

        # ── Emit results for this mapper field ────────────────────────────────

        if above_threshold_rows:
            # Case 1: one record per qualifying row (exact or fuzzy)
            log.debug(
                "[%s] col=%r → Case 1: %d above-threshold record(s)",
                metadata_key, mapper_col, len(above_threshold_rows),
            )
            results.extend(above_threshold_rows)

            # Case 2 is suppressed when Case 1 rows exist — the weak candidate
            # is only a fallback, not an addition alongside strong matches.

        elif best_weak_row is not None and best_weak_score > 0:
            # Case 2: no strong match found — emit the single best weak row
            col_name  = best_weak_row.get("Column Name",       "") or ""
            col_alias = best_weak_row.get("Column Alias Name", "") or ""

            log.debug(
                "[%s] col=%r → Case 2: weak  %.1f%%",
                metadata_key, mapper_col, best_weak_score,
            )
            results.append({
                "regulation":      regulation or (best_weak_row.get("olympusApplication") or ""),
                "key":             metadata_key,
                "mapperColumn":    mapper_col,
                "mapperFactField": mapper_fact,
                "dbName":          best_weak_row.get("Database Name",  "") or "",
                "tableName":       best_weak_row.get("Table Name",     "") or "",
                "columnName":      col_name,
                "columnAliasName": col_alias,
                "matchedOn":       "alias" if col_alias.strip() else "columnName",
                "matchPercentage": f"{best_weak_score:.1f}%",
                "matchType":       "weak",
            })

        else:
            # Case 3: nothing matched at all
            log.debug("[%s] col=%r → Case 3: no_match", metadata_key, mapper_col)
            results.append(_no_match_record(mapper_col, mapper_fact, metadata_key, regulation))

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
    Process one metadata key end-to-end.
    FIX: outer try/except so one bad key never propagates to asyncio.gather.
    FIX: isinstance checks on both gateway responses before calling .get().
    """
    try:
        mapper    = (entry.get("mapper") or "").strip().lower()
        sql_files = entry.get("sql_files") or []

        if not mapper:
            log.warning("[%s] no mapper — skipping", key)
            return []

        if not isinstance(sql_files, list):
            log.warning("[%s] sql_files is %s — treating as []", key, type(sql_files).__name__)
            sql_files = []

        mapper_url  = build_metadata_url(regulation=regulation, mapper=mapper)
        ods_payload = {
            "filterCriteria": {
                "winkeys:currBranch":         curr_branch,
                "winkeys:windowType":         "sql_lineage",
                "winkeys:olympusApplication": regulation,
                **({"windata:a6": sql_files} if sql_files else {}),
            },
            "header": {"regulation": regulation, "stream": "window"},
        }

        log.info("[%s] GET %s  |  POST %s", key, mapper_url, ODS_FACT_SERVICE_URL)

        mapper_result, ods_response = await asyncio.gather(
            call_gateway(url=mapper_url,           method="GET"),
            call_gateway(url=ODS_FACT_SERVICE_URL, method="POST", payload=ods_payload),
            return_exceptions=True,
        )

        # FIX: log exception type explicitly, not just str()
        if isinstance(mapper_result, Exception):
            log.error("[%s] mapper failed (%s): %s", key, type(mapper_result).__name__, mapper_result)
            mapper_result = {}
        if isinstance(ods_response, Exception):
            log.error("[%s] ODS failed (%s): %s", key, type(ods_response).__name__, ods_response)
            ods_response = {}

        # FIX: isinstance guard before .get() calls
        if not isinstance(mapper_result, dict):
            log.error("[%s] mapper response not dict: %s", key, type(mapper_result).__name__)
            mapper_result = {}
        if not isinstance(ods_response, dict):
            log.error("[%s] ODS response not dict: %s", key, type(ods_response).__name__)
            ods_response = {}

        lineage_rows  = extract_lineage_rows_from_response(ods_response)
        total_records = ods_response.get("totalRecords", 0)
        log.info("[%s] totalRecords=%s  lineage_rows=%d", key, total_records, len(lineage_rows))

        matches = match_columns_with_lineage(
            lineage_rows   = lineage_rows,
            field_mappings = mapper_result,
            metadata_key   = key,
            regulation     = regulation,
        )
        matched = sum(1 for m in matches if m.get("matchType") != "no_match")
        log.info("[%s] → %d records (matched=%d)", key, len(matches), matched)
        return matches

    except Exception as exc:
        # FIX: catch-all so this key's failure doesn't propagate up
        log.error("[%s] _process_metadata_key failed: %s", key, exc, exc_info=True)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic request model
# ─────────────────────────────────────────────────────────────────────────────

class MatchLineageRequest(BaseModel):
    base_url:        str = "http://rhoo-gateway-vip-uat.nam.nsroot.net/reghub-api"
    regulation_meta: str = "rhoo_emiretd_metadata_batch_sourcing"
    regulation:      str = "rhoo"
    current_branch:  str = "1.26.3.2"


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint  POST /match-lineage
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/match-lineage")
async def match_lineage(request: MatchLineageRequest):
    regulation  = request.regulation
    curr_branch = request.current_branch

    metadata_url = (
        f"{request.base_url.rstrip('/')}"
        f"/{regulation}/metadata-service/metadata/{request.regulation_meta}"
    )
    log.info("[match-lineage] GET metadata → %s", metadata_url)

    # FIX: HTTP errors → 502 instead of unhandled 500
    try:
        metadata_response: Dict[str, Any] = await call_gateway(url=metadata_url, method="GET")
    except Exception as exc:
        log.error("[match-lineage] metadata fetch failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Metadata fetch failed: {exc}")

    # FIX: validate response type before .get()
    if not isinstance(metadata_response, dict):
        raise HTTPException(
            status_code=502,
            detail=f"Unexpected metadata response type: {type(metadata_response).__name__}",
        )

    metadata_value = metadata_response.get("value") or {}
    if not metadata_value:
        raise HTTPException(
            status_code=404,
            detail=f"No 'value' in metadata response for {request.regulation_meta}",
        )

    if not isinstance(metadata_value, dict):
        raise HTTPException(
            status_code=502,
            detail=f"metadata 'value' is not a dict: {type(metadata_value).__name__}",
        )

    # Only process entries that are dicts
    valid_keys = {k: v for k, v in metadata_value.items() if isinstance(v, dict)}
    if not valid_keys:
        raise HTTPException(status_code=422, detail="No valid key entries found in metadata 'value'")

    log.info("[match-lineage] keys=%s", list(valid_keys.keys()))

    # FIX: return_exceptions=True so one failing key doesn't abort all others
    key_results: List[Any] = await asyncio.gather(
        *[
            _process_metadata_key(key=k, entry=v, regulation=regulation, curr_branch=curr_branch)
            for k, v in valid_keys.items()
        ],
        return_exceptions=True,
    )

    all_matches: List[Dict[str, Any]] = []
    for k, result in zip(valid_keys.keys(), key_results):
        if isinstance(result, Exception):
            log.error("[match-lineage] key=%r raised: %s", k, result)
            continue
        if isinstance(result, list):
            all_matches.extend(result)

    # FIX: use .get() so missing "matchType" key never raises KeyError
    exact    = sum(1 for m in all_matches if m.get("matchType") == "exact")
    fuzzy    = sum(1 for m in all_matches if m.get("matchType") == "fuzzy")
    no_match = sum(1 for m in all_matches if m.get("matchType") == "no_match")

    key_summary: Dict[str, Dict[str, int]] = {}
    for m in all_matches:
        k = m.get("key", "unknown")
        if k not in key_summary:
            key_summary[k] = {"total": 0, "matched": 0, "no_match": 0}
        key_summary[k]["total"] += 1
        if m.get("matchType") != "no_match":
            key_summary[k]["matched"] += 1
        else:
            key_summary[k]["no_match"] += 1

    log.info("[match-lineage] done — %d records", len(all_matches))

    return {
        "success":           True,
        "regulation":        regulation,
        "regulationMeta":    request.regulation_meta,
        "currBranch":        curr_branch,
        "metadataName":      metadata_response.get("name", ""),
        "keysProcessed":     list(valid_keys.keys()),
        "totalMatchRecords": len(all_matches),
        "matchSummary":      {"exact": exact, "fuzzy": fuzzy, "no_match": no_match},
        "keySummary":        key_summary,
        "message": (
            f"Processed {len(valid_keys)} keys — {len(all_matches)} records "
            f"(exact={exact}, fuzzy={fuzzy}, no_match={no_match})"
        ),
        "lineage_data": all_matches,
    }