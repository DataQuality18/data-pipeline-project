"""
================================================================================
ZHOO Regulation - Metadata API Integration Service
================================================================================
Author      : Senior Developer
Description : FastAPI service that integrates with the ZHOO regulation metadata
              gateway (Olympus source system). Supports:
                - Fetching mapper info (Step 1: Fetch Mapper Info)
                - Windows environment queries (Step 1: Windows)
                - POST requests with filter criteria for SQL query sources
              All endpoints are fully exception-handled with structured logging.
================================================================================
"""

import logging
import sys
import httpx

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# Structured logger outputs to stdout with timestamp, level, and module name.
# ─────────────────────────────────────────────────────────────────────────────

def setup_logger(name: str) -> logging.Logger:
    """Configure and return a named logger with console handler."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if logger already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


logger = setup_logger("zhoo_metadata_service")


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# Centralised configuration extracted from the API Configuration Reference doc.
# ─────────────────────────────────────────────────────────────────────────────

# Default regulation & mapper values (from configuration object in the doc)
DEFAULT_REGULATION: str = "zhoo"
DEFAULT_STREAM: str = "txaas"
DEFAULT_MAPPER: str = "source_olympus_zhoo_dmat_mapper"

# Non-production gateway base URL (section 3 of the reference doc)
BASE_URL_NON_PROD: str = (
    "http://zhoo-gateway-wig-n01.nam.nsroot.net/repodb-api"
)

# Three SQL query source files referenced in apiFiles (section 2)
API_FILES: list[str] = [
    "zhoo_eod_del_inactv_refdata_dmat_sql_query_source_olympus",
    "zhoo_eod_del_inactv_refdata_dmat_key_eod_query_source_olympus",
    "zhoo_eod_load_refdata_dmat_sql_query_source_olympus",
]

# HTTP timeout in seconds for outbound gateway calls
HTTP_TIMEOUT_SECONDS: float = 30.0


# ─────────────────────────────────────────────────────────────────────────────
# PYDANTIC MODELS — Request / Response schemas
# ─────────────────────────────────────────────────────────────────────────────

class MapperInfoRequest(BaseModel):
    """
    Request model for fetching mapper metadata.
    Maps to the URL pattern:
        /repodb-api/{request.regulation}/metadata-service/metadata/{request.mapper}
    """
    regulation: str = Field(
        default=DEFAULT_REGULATION,
        description="Regulation identifier, e.g. 'zhoo'",
        min_length=1,
    )
    mapper: str = Field(
        default=DEFAULT_MAPPER,
        description="Mapper name, e.g. 'source_olympus_zhoo_dmat_mapper'",
        min_length=1,
    )

    @field_validator("regulation", "mapper")
    @classmethod
    def strip_and_lower(cls, value: str) -> str:
        """Normalise string fields: strip whitespace and lowercase."""
        return value.strip().lower()


class WindowsQueryRequest(BaseModel):
    """
    Request model for the Windows environment step.
    Uses the same gateway endpoint pattern as Fetch Mapper Info (section 5).
    Includes POST body with filter_criteria and custom headers (section 6).
    """
    regulation: str = Field(
        default=DEFAULT_REGULATION,
        description="Regulation identifier sent in POST headers",
        min_length=1,
    )
    mapper: str = Field(
        default=DEFAULT_MAPPER,
        description="Mapper name used in URL path construction",
        min_length=1,
    )
    stream: str = Field(
        default="window",
        description="Stream type; typically 'window' for Windows step",
        min_length=1,
    )
    winkeys_id: str = Field(
        default=API_FILES[0],                    # first SQL file as default
        description="Filter criteria winkeys_id — must match one of the apiFiles",
    )

    @field_validator("regulation", "mapper", "stream")
    @classmethod
    def strip_and_lower(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("winkeys_id")
    @classmethod
    def validate_winkeys_id(cls, value: str) -> str:
        """Ensure winkeys_id is one of the known SQL query source files."""
        if value not in API_FILES:
            raise ValueError(
                f"winkeys_id '{value}' is not a recognised apiFile. "
                f"Valid values: {API_FILES}"
            )
        return value


class MetadataResponse(BaseModel):
    """Generic wrapper for successful gateway responses."""
    status: str = Field(description="Outcome: 'success' or 'error'")
    regulation: str
    mapper: str
    gateway_url: str = Field(description="The full URL that was called")
    data: Optional[dict] = Field(default=None, description="Raw gateway payload")
    message: Optional[str] = Field(default=None, description="Human-readable note")


class HealthResponse(BaseModel):
    """Health-check response schema."""
    service: str
    status: str
    default_regulation: str
    default_mapper: str
    api_files: list[str]


# ─────────────────────────────────────────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def build_metadata_url(regulation: str, mapper: str) -> str:
    """
    Construct the full metadata endpoint URL using dynamic path substitution.

    Pattern (sections 4 & 5 of the reference doc):
        {BASE_URL}/{regulation}/metadata-service/metadata/{mapper}

    Args:
        regulation: The regulation code (e.g. 'zhoo').
        mapper    : The mapper identifier.

    Returns:
        Fully-formed URL string.
    """
    url = f"{BASE_URL_NON_PROD}/{regulation}/metadata-service/metadata/{mapper}"
    logger.debug("Built metadata URL: %s", url)
    return url


def build_post_payload(winkeys_id: str, regulation: str, stream: str) -> dict:
    """
    Build the POST request body structure as specified in section 6.

    Structure:
        {
            "filter_criteria": { "winkeys_id": "<SQL_FILE_NAME>" },
            "headers":         { "regulation": "<REG>", "stream": "<STREAM>" }
        }

    Args:
        winkeys_id : SQL query source file identifier.
        regulation : Regulation code used in the POST headers block.
        stream     : Stream type (e.g. 'window', 'txaas').

    Returns:
        Dict representing the POST body.
    """
    payload = {
        "filter_criteria": {
            "winkeys_id": winkeys_id,   # maps to one of the three apiFiles
        },
        "headers": {
            "regulation": regulation,   # sent in POST body headers section
            "stream": stream,           # 'window' for Windows step per section 6
        },
    }
    logger.debug("Built POST payload: %s", payload)
    return payload


async def call_gateway(
    url: str,
    method: str = "GET",
    payload: Optional[dict] = None,
) -> dict:
    """
    Async HTTP client wrapper around the ZHOO metadata gateway.

    Raises HTTPException on:
        - Connection / timeout errors (502 Bad Gateway)
        - Non-2xx HTTP responses from the gateway (forwarded status code)
        - Unexpected runtime errors (500 Internal Server Error)

    Args:
        url     : Full target URL.
        method  : HTTP method — 'GET' or 'POST'.
        payload : JSON body for POST requests (ignored for GET).

    Returns:
        Parsed JSON response as a dict.
    """
    logger.info("Calling gateway | method=%s | url=%s", method, url)

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
            if method.upper() == "POST":
                logger.debug("POST body: %s", payload)
                response = await client.post(url, json=payload)
            else:
                response = await client.get(url)

            logger.info(
                "Gateway responded | status_code=%s | url=%s",
                response.status_code,
                url,
            )

            # Raise for any 4xx / 5xx responses from the gateway
            response.raise_for_status()

            return response.json()

    except httpx.TimeoutException as exc:
        logger.error("Gateway timeout | url=%s | error=%s", url, exc)
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Gateway timed out after {HTTP_TIMEOUT_SECONDS}s: {url}",
        )

    except httpx.ConnectError as exc:
        logger.error("Gateway connection error | url=%s | error=%s", url, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Unable to connect to metadata gateway: {url}. Error: {exc}",
        )

    except httpx.HTTPStatusError as exc:
        # Propagate the upstream HTTP error code so the caller gets context
        upstream_code = exc.response.status_code
        logger.error(
            "Gateway returned error | status=%s | url=%s | body=%s",
            upstream_code,
            url,
            exc.response.text,
        )
        raise HTTPException(
            status_code=upstream_code,
            detail=(
                f"Metadata gateway returned HTTP {upstream_code} for {url}. "
                f"Response: {exc.response.text}"
            ),
        )

    except Exception as exc:
        # Catch-all for any unexpected error (serialisation, SSL, etc.)
        logger.exception("Unexpected error calling gateway | url=%s", url)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error while contacting gateway: {exc}",
        )


# ─────────────────────────────────────────────────────────────────────────────
# APPLICATION LIFESPAN — startup / shutdown hooks
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Log service startup and shutdown events."""
    logger.info(
        "ZHOO Metadata Service starting up | regulation=%s | mapper=%s",
        DEFAULT_REGULATION,
        DEFAULT_MAPPER,
    )
    yield
    logger.info("ZHOO Metadata Service shutting down.")


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APPLICATION
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="ZHOO Regulation — Metadata API Service",
    description=(
        "Integrates with the Olympus source system via the ZHOO regulation "
        "metadata gateway. Supports Fetch Mapper Info (GET) and Windows POST "
        "query flows as per the API Configuration & Integration Reference."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL EXCEPTION HANDLER
# Catches any unhandled exception that escapes route handlers.
# ─────────────────────────────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Fallback handler — logs and returns a safe 500 response."""
    logger.exception(
        "Unhandled exception | path=%s | method=%s",
        request.url.path,
        request.method,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "An unexpected internal error occurred.", "error": str(exc)},
    )


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    tags=["Utility"],
)
async def health_check() -> HealthResponse:
    """
    Returns service health status and current static configuration.
    Useful for liveness / readiness probes in container environments.
    """
    logger.info("Health check endpoint invoked.")
    return HealthResponse(
        service="zhoo-metadata-service",
        status="healthy",
        default_regulation=DEFAULT_REGULATION,
        default_mapper=DEFAULT_MAPPER,
        api_files=API_FILES,
    )


@app.post(
    "/fetch-mapper-info",
    response_model=MetadataResponse,
    summary="Step 1 — Fetch Mapper Info",
    tags=["Metadata"],
)
async def fetch_mapper_info(request_body: MapperInfoRequest) -> MetadataResponse:
    """
    **Step 1 — Fetch Mapper Info** (Section 4 of the reference doc).

    Constructs the gateway URL dynamically using `regulation` and `mapper`
    values from the request payload, then performs a GET to retrieve mapper
    metadata from the Olympus source system.

    URL pattern:
        `{BASE_URL}/{request.regulation}/metadata-service/metadata/{request.mapper}`
    """
    logger.info(
        "fetch_mapper_info invoked | regulation=%s | mapper=%s",
        request_body.regulation,
        request_body.mapper,
    )

    # Build the target URL using values from the incoming request payload
    gateway_url = build_metadata_url(
        regulation=request_body.regulation,
        mapper=request_body.mapper,
    )

    # Call the gateway — exceptions are raised and handled inside call_gateway
    gateway_data = await call_gateway(url=gateway_url, method="GET")

    logger.info(
        "fetch_mapper_info successful | regulation=%s | mapper=%s",
        request_body.regulation,
        request_body.mapper,
    )

    return MetadataResponse(
        status="success",
        regulation=request_body.regulation,
        mapper=request_body.mapper,
        gateway_url=gateway_url,
        data=gateway_data,
        message="Mapper metadata fetched successfully from Olympus gateway.",
    )


@app.post(
    "/windows-query",
    response_model=MetadataResponse,
    summary="Step 1 — Windows Environment Query",
    tags=["Metadata"],
)
async def windows_query(request_body: WindowsQueryRequest) -> MetadataResponse:
    """
    **Step 1 — Windows** (Sections 5 & 6 of the reference doc).

    Uses the same unified gateway endpoint pattern as Fetch Mapper Info
    but issues a POST request with a structured body containing:
      - `filter_criteria.winkeys_id` — one of the three SQL apiFiles
      - `headers.regulation`         — regulation code
      - `headers.stream`             — stream type ('window')

    This query targets EOD delta/load processes for ZHOO reference data.
    """
    logger.info(
        "windows_query invoked | regulation=%s | mapper=%s | winkeys_id=%s",
        request_body.regulation,
        request_body.mapper,
        request_body.winkeys_id,
    )

    # Same endpoint pattern as Fetch Mapper Info (unified gateway — section 5)
    gateway_url = build_metadata_url(
        regulation=request_body.regulation,
        mapper=request_body.mapper,
    )

    # Build POST body as defined in section 6 of the reference doc
    post_payload = build_post_payload(
        winkeys_id=request_body.winkeys_id,
        regulation=request_body.regulation,
        stream=request_body.stream,
    )

    # POST to the gateway with the structured payload
    gateway_data = await call_gateway(
        url=gateway_url,
        method="POST",
        payload=post_payload,
    )

    logger.info(
        "windows_query successful | regulation=%s | winkeys_id=%s",
        request_body.regulation,
        request_body.winkeys_id,
    )

    return MetadataResponse(
        status="success",
        regulation=request_body.regulation,
        mapper=request_body.mapper,
        gateway_url=gateway_url,
        data=gateway_data,
        message=(
            f"Windows EOD query successful for SQL source: {request_body.winkeys_id}"
        ),
    )


@app.get(
    "/api-files",
    summary="List all configured SQL apiFiles",
    tags=["Utility"],
)
async def list_api_files() -> dict:
    """
    Returns the three SQL query source files defined in apiFiles (section 2).
    Useful for populating dropdowns / validating winkeys_id values upstream.
    """
    logger.info("api-files endpoint invoked — returning %d files.", len(API_FILES))
    return {
        "regulation": DEFAULT_REGULATION,
        "api_files": API_FILES,
        "count": len(API_FILES),
    }


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT — local development runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    logger.info("Starting Uvicorn development server on http://0.0.0.0:8000")
    uvicorn.run(
        "zhoo_metadata_service:app",
        host="0.0.0.0",
        port=8000,
        reload=True,          # Hot-reload enabled for local dev
        log_level="debug",
    )
