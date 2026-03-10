"""
================================================================================
Unit Tests — ZHOO Metadata API Service
================================================================================
Run with:  pytest test_zhoo_metadata_service.py -v
================================================================================
"""

import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient

from zhoo_metadata_service import (
    app,
    build_metadata_url,
    build_post_payload,
    DEFAULT_REGULATION,
    DEFAULT_MAPPER,
    API_FILES,
    BASE_URL_NON_PROD,
)

client = TestClient(app)


# ─────────────────────────────────────────────────────────────────────────────
# HELPER / UTILITY TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildMetadataUrl:
    def test_correct_url_pattern(self):
        """URL must follow the pattern from sections 4 & 5 of the ref doc."""
        url = build_metadata_url("zhoo", "source_olympus_zhoo_dmat_mapper")
        expected = (
            f"{BASE_URL_NON_PROD}/zhoo/metadata-service/metadata/"
            "source_olympus_zhoo_dmat_mapper"
        )
        assert url == expected

    def test_dynamic_regulation_substitution(self):
        """Regulation value must be injected into the URL path."""
        url = build_metadata_url("test_reg", "some_mapper")
        assert "test_reg" in url

    def test_dynamic_mapper_substitution(self):
        """Mapper value must appear at the end of the URL path."""
        url = build_metadata_url("zhoo", "custom_mapper")
        assert url.endswith("custom_mapper")


class TestBuildPostPayload:
    def test_filter_criteria_present(self):
        """POST body must contain filter_criteria with winkeys_id."""
        payload = build_post_payload(API_FILES[0], "zhoo", "window")
        assert "filter_criteria" in payload
        assert payload["filter_criteria"]["winkeys_id"] == API_FILES[0]

    def test_headers_block_present(self):
        """POST body must contain headers with regulation and stream."""
        payload = build_post_payload(API_FILES[0], "zhoo", "window")
        assert payload["headers"]["regulation"] == "zhoo"
        assert payload["headers"]["stream"] == "window"


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestHealthCheck:
    def test_returns_200(self):
        response = client.get("/health")
        assert response.status_code == 200

    def test_returns_default_config(self):
        data = client.get("/health").json()
        assert data["default_regulation"] == DEFAULT_REGULATION
        assert data["default_mapper"] == DEFAULT_MAPPER
        assert data["api_files"] == API_FILES


class TestListApiFiles:
    def test_returns_three_files(self):
        data = client.get("/api-files").json()
        assert data["count"] == 3
        assert len(data["api_files"]) == 3

    def test_all_known_files_present(self):
        data = client.get("/api-files").json()
        for f in API_FILES:
            assert f in data["api_files"]


class TestFetchMapperInfo:
    @patch("zhoo_metadata_service.call_gateway", new_callable=AsyncMock)
    def test_success(self, mock_gateway):
        """Should return 200 and wrap gateway response in MetadataResponse."""
        mock_gateway.return_value = {"mapper": "ok", "fields": []}
        response = client.post(
            "/fetch-mapper-info",
            json={"regulation": "zhoo", "mapper": DEFAULT_MAPPER},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        assert body["regulation"] == "zhoo"

    def test_validation_error_empty_regulation(self):
        """Empty regulation string must fail Pydantic validation."""
        response = client.post(
            "/fetch-mapper-info",
            json={"regulation": "", "mapper": DEFAULT_MAPPER},
        )
        assert response.status_code == 422   # Unprocessable Entity


class TestWindowsQuery:
    @patch("zhoo_metadata_service.call_gateway", new_callable=AsyncMock)
    def test_success(self, mock_gateway):
        """Should POST correct payload and return wrapped gateway response."""
        mock_gateway.return_value = {"result": "window_data"}
        response = client.post(
            "/windows-query",
            json={
                "regulation": "zhoo",
                "mapper": DEFAULT_MAPPER,
                "stream": "window",
                "winkeys_id": API_FILES[0],
            },
        )
        assert response.status_code == 200
        assert response.json()["status"] == "success"

    def test_invalid_winkeys_id_rejected(self):
        """winkeys_id not in API_FILES must fail validation."""
        response = client.post(
            "/windows-query",
            json={
                "regulation": "zhoo",
                "mapper": DEFAULT_MAPPER,
                "stream": "window",
                "winkeys_id": "invalid_sql_file",
            },
        )
        assert response.status_code == 422
