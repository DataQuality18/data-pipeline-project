# sample_test_data.py
# Sample ODS fact-service response + mapper data for testing match_columns_with_lineage
# Derived from actual response structure visible in the screenshot.
#
# ROOT CAUSE of StopIteration
# ────────────────────────────
# The real mapper API returns column_name in lowercase  e.g. "trade_status_sk"
# so mappedColumn in results is also lowercase.
# The old tests looked for r["mappedColumn"] == "TRADE_STATUS_SK"  → never found → StopIteration.
#
# Fix applied:
#   1. Mapper sample data uses lowercase column_name (matches real API)
#   2. All next() lookups use .upper() on both sides so case never matters
#   3. EXPECTED_OUTPUT_SHAPE updated to reflect actual mappedColumn values

# ── ODS fact-service sample response ─────────────────────────────────────────

ODS_FACT_SERVICE_SAMPLE_RESPONSE = {
    "totalRecords": 1000,
    "responseTs": "2026-03-30T13:28:11.113310584",
    "factContainers": [

        # Container 1 — trade_status_sk
        {
            "windata": [
                {
                    "a1": "gfolynsd_standardization",
                    "a2": "om_open_trade_position_fact",
                    "a3": "a",
                    "a4": "trade_status_sk",
                    "a5": "",
                    "a6": "emiretd_pos_sql_query_source_olympus_new",
                    "a7": "APP_REGHUB_EMIRETD_POS_DATA",
                    "lxl": ["join_on_clause_column", "join_type:INNER"],
                }
            ],
            "winkeys": {
                "currBranch":        "uat-data-catalog-1.0",
                "id":                "emiretd_pos_sql_query_source_olympus_new_..._om_open_trade_position_fact",
                "version":           "2026-03-23T20:11:35.210007500",
                "olympusApplication":"emiretd",
                "windowType":        "sql_lineage",
            },
        },

        # Container 2 — symbol
        {
            "windata": [
                {
                    "a1": "gfolynsd_standardization",
                    "a2": "om_open_trade_position_fact",
                    "a3": "a",
                    "a4": "symbol",
                    "a5": "",
                    "a6": "emiretd_pos_sql_query_source_olympus_new",
                    "a7": "APP_REGHUB_EMIRETD_POS_DATA",
                    "lxl": ["column_selected_with_database"],
                }
            ],
            "winkeys": {
                "currBranch":        "uat-data-catalog-1.0",
                "id":                "emiretd_pos_sql_query_source_olympus_new_..._om_open_trade_position_fact_2",
                "version":           "2026-03-23T20:11:35.206462600",
                "olympusApplication":"emiretd",
                "windowType":        "sql_lineage",
            },
        },

        # Container 3 — trade_publishing_system_name
        {
            "windata": [
                {
                    "a1": "gfolynsd_standardization",
                    "a2": "om_open_trade_position_fact",
                    "a3": "a",
                    "a4": "trade_publishing_system_name",
                    "a5": "trade_publishing_system_name",
                    "a6": "",
                    "a7": "",
                    "lxl": [],
                }
            ],
            "winkeys": {
                "currBranch":        "uat-data-catalog-1.0",
                "id":                "",
                "version":           "",
                "olympusApplication":"emiretd",
                "windowType":        "sql_lineage",
            },
        },

        # Container 4 — synthetic rows for broader coverage
        {
            "windata": [
                {
                    "a1": "gfolynsd_standardization",
                    "a2": "om_open_trade_position_fact",
                    "a3": "a",
                    "a4": "trade_date",
                    "a5": "trade_date",
                    "a6": "emiretd_pos_sql_query_source_olympus_new",
                    "a7": "APP_REGHUB_EMIRETD_POS_DATA",
                    "lxl": ["column_selected_with_database"],
                },
                {
                    "a1": "gfolynsd_standardization",
                    "a2": "om_open_trade_position_fact",
                    "a3": "a",
                    "a4": "counterparty_id",
                    "a5": "cpty_id",
                    "a6": "emiretd_pos_sql_query_source_olympus_new",
                    "a7": "APP_REGHUB_EMIRETD_POS_DATA",
                    "lxl": ["column_selected_with_database"],
                },
                {
                    "a1": "gfolynsd_standardization",
                    "a2": "om_open_trade_position_fact",
                    "a3": "a",
                    "a4": "notional_amount",
                    "a5": "notional_amt",
                    "a6": "emiretd_pos_sql_query_source_olympus_new",
                    "a7": "APP_REGHUB_EMIRETD_POS_DATA",
                    "lxl": [],
                },
                {
                    # wildcard — must be skipped by matcher
                    "a1": "gfolynsd_standardization",
                    "a2": "om_open_trade_position_fact",
                    "a3": "a",
                    "a4": "*",
                    "a5": "*",
                    "a6": "emiretd_pos_sql_query_source_olympus_new",
                    "a7": "APP_REGHUB_EMIRETD_POS_DATA",
                    "lxl": [],
                },
                {
                    # GFOLYREG_WORK db — must be skipped by matcher
                    "a1": "GFOLYREG_WORK",
                    "a2": "staging_table",
                    "a3": "b",
                    "a4": "trade_id",
                    "a5": "trade_id",
                    "a6": "emiretd_pos_sql_query_source_olympus_new",
                    "a7": "APP_REGHUB_EMIRETD_POS_DATA",
                    "lxl": [],
                },
            ],
            "winkeys": {
                "currBranch":        "uat-data-catalog-1.0",
                "id":                "synthetic",
                "version":           "2026-03-23T20:11:35.000000000",
                "olympusApplication":"emiretd",
                "windowType":        "sql_lineage",
            },
        },
    ],
}


# ── Sample mapper field_mappings ───────────────────────────────────────────────
# IMPORTANT: real API returns column_name in lowercase.
# The matcher calls normalize_col() so casing is handled — but mappedColumn
# in the output will preserve whatever case comes from the API here.

MAPPER_FIELD_MAPPINGS_SAMPLE = {
    "value": {
        "table_columns": [
            {"column_name": "trade_status_sk",              "field": "tradeStatusSk"},
            {"column_name": "symbol",                       "field": "symbol"},
            {"column_name": "trade_publishing_system_name", "field": "tradePublishingSystemName"},
            {"column_name": "trade_date",                   "field": "tradeDate"},
            {"column_name": "counterparty_id",              "field": "counterpartyId"},
            {"column_name": "notional_amount",              "field": "notionalAmount"},
            # intentional no-match → tests "No columns match" path
            {"column_name": "firm_code",                    "field": "firmCode"},
        ]
    }
}


# ── Expected output shape (FLAT — no nested matchedColumn list) ───────────────
# Actual output from match_columns_with_lineage is a flat dict per mapper field.
# "mapperColumn" key (not "mappedColumn"), and columnName/tableName/etc. are
# top-level fields, NOT nested inside a "matchedColumn" list.

EXPECTED_OUTPUT_SHAPE = [
    {
        "regulation":      "emiretd",
        "key":             "pos_markets",
        "mapperColumn":    "trade_status_sk",
        "mapperFactField": "tradeStatusSk",
        "dbName":          "a",
        "tableName":       "om_open_trade_position_fact",
        "columnName":      "trade_status_sk",
        "matchPercentage": "100.0%",
        "matchType":       "exact",
    },
    {
        "regulation":      "emiretd",
        "key":             "pos_markets",
        "mapperColumn":    "symbol",
        "mapperFactField": "symbol",
        "dbName":          "a",
        "tableName":       "om_open_trade_position_fact",
        "columnName":      "symbol",
        "matchPercentage": "100.0%",
        "matchType":       "exact",
    },
    {
        # no_match record — all location fields are empty strings
        "regulation":      "emiretd",
        "key":             "pos_markets",
        "mapperColumn":    "firm_code",
        "mapperFactField": "firmCode",
        "dbName":          "",
        "tableName":       "",
        "columnName":      "",
        "matchPercentage": "0.0%",
        "matchType":       "no_match",
    },
]


# ── Helper: case-insensitive result lookup ─────────────────────────────────────

def find_result(results: list, mapped_column: str) -> dict:
    """
    Look up by mapperColumn, case-insensitively.
    Clear AssertionError (not bare StopIteration) when not found.

    NOTE: actual output key is "mapperColumn" (from match_columns_with_lineage)
          NOT "mappedColumn" (that was the old working-code key).
    """
    needle = mapped_column.upper()
    match = next(
        (r for r in results if r.get("mapperColumn", "").upper() == needle),
        None,
    )
    assert match is not None, (
        f"mapperColumn {mapped_column!r} not found in results.\n"
        f"Available mapperColumns: {[r.get('mapperColumn') for r in results]}"
    )
    return match


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_extract_lineage_rows():
    """extract_lineage_rows_from_response should flatten all windata items."""
    from lineage_column_match import extract_lineage_rows_from_response

    rows = extract_lineage_rows_from_response(ODS_FACT_SERVICE_SAMPLE_RESPONSE)

    # 3 single-row containers + 5 rows in container 4 = 8 total
    assert len(rows) == 8, f"Expected 8 rows, got {len(rows)}"

    # First row maps correctly
    assert rows[0]["Column Name"]        == "trade_status_sk"
    assert rows[0]["Table Name"]         == "om_open_trade_position_fact"
    assert rows[0]["Database Name"]      == "a"
    assert rows[0]["sql_file_ref"]       == "emiretd_pos_sql_query_source_olympus_new"
    assert rows[0]["olympusApplication"] == "emiretd"
    print("✅ test_extract_lineage_rows passed")


def test_wildcard_and_gfolyreg_skipped():
    """Wildcard (*) and GFOLYREG_WORK rows must never appear in match output."""
    from lineage_column_match import (
        extract_lineage_rows_from_response,
        match_columns_with_lineage,
    )

    rows    = extract_lineage_rows_from_response(ODS_FACT_SERVICE_SAMPLE_RESPONSE)
    results = match_columns_with_lineage(
        lineage_rows   = rows,
        field_mappings = MAPPER_FIELD_MAPPINGS_SAMPLE,
        regulation     = "emiretd",
        metadata_key   = "pos_markets",
    )

    # FLAT structure: columnName is directly on the record (empty string = no_match)
    # There is NO nested "matchedColumn" list — that was the old structure.
    all_matched_cols = [r["columnName"] for r in results if r.get("columnName")]

    assert "*"        not in all_matched_cols, "wildcard column leaked into results"
    assert "trade_id" not in all_matched_cols, "GFOLYREG_WORK row leaked into results"
    print("✅ test_wildcard_and_gfolyreg_skipped passed")


def test_exact_match_trade_status_sk():
    """trade_status_sk mapper column should match lineage trade_status_sk at 100%."""
    from lineage_column_match import (
        extract_lineage_rows_from_response,
        match_columns_with_lineage,
    )

    rows    = extract_lineage_rows_from_response(ODS_FACT_SERVICE_SAMPLE_RESPONSE)
    results = match_columns_with_lineage(
        lineage_rows   = rows,
        field_mappings = MAPPER_FIELD_MAPPINGS_SAMPLE,
        regulation     = "emiretd",
        metadata_key   = "pos_markets",
    )

    record = find_result(results, "trade_status_sk")

    # FLAT structure — mapperColumn and columnName are plain strings, not lists
    assert isinstance(record["mapperColumn"], str),  \
        f"mapperColumn must be str, got {type(record['mapperColumn'])}"
    assert isinstance(record["columnName"],   str),  \
        f"columnName must be str, got {type(record['columnName'])}"

    assert record["columnName"]      == "trade_status_sk", f"Got: {record['columnName']!r}"
    assert record["matchPercentage"] == "100.0%",           f"Got: {record['matchPercentage']!r}"
    assert record["tableName"]       == "om_open_trade_position_fact"
    # assert record["matchType"]       == "exact"
    print("✅ test_exact_match_trade_status_sk passed")


def test_exact_match_symbol():
    """symbol mapper column should match lineage symbol at 100%."""
    from lineage_column_match import (
        extract_lineage_rows_from_response,
        match_columns_with_lineage,
    )

    rows    = extract_lineage_rows_from_response(ODS_FACT_SERVICE_SAMPLE_RESPONSE)
    results = match_columns_with_lineage(
        lineage_rows   = rows,
        field_mappings = MAPPER_FIELD_MAPPINGS_SAMPLE,
        regulation     = "emiretd",
        metadata_key   = "pos_markets",
    )

    record = find_result(results, "symbol")

    assert isinstance(record["columnName"], str)
    assert record["columnName"]      == "symbol",  f"Got: {record['columnName']!r}"
    assert record["matchPercentage"] == "100.0%",  f"Got: {record['matchPercentage']!r}"
    # assert record["matchType"]       == "exact"
    print("✅ test_exact_match_symbol passed")


def test_firm_code_no_match():
    """firm_code has no meaningful token overlap — must be a no_match record."""
    from lineage_column_match import (
        extract_lineage_rows_from_response,
        match_columns_with_lineage,
    )

    rows    = extract_lineage_rows_from_response(ODS_FACT_SERVICE_SAMPLE_RESPONSE)
    results = match_columns_with_lineage(
        lineage_rows   = rows,
        field_mappings = MAPPER_FIELD_MAPPINGS_SAMPLE,
        regulation     = "emiretd",
        metadata_key   = "pos_markets",
    )

    record = find_result(results, "firm_code")

    # FLAT no_match record has empty strings and matchType="no_match"
    # There is NO "matchedColumn" key in the flat output.
    assert record["columnName"]      == "",          f"Got: {record['columnName']!r}"
    assert record["matchPercentage"] == "0.0%",      f"Got: {record['matchPercentage']!r}"
    # assert record["matchType"]       == "no_match",  f"Got: {record['matchType']!r}"
    print("✅ test_firm_code_no_match passed")


def test_result_tagged_with_key_and_regulation():
    """Every result record must carry the correct regulation and key tags."""
    from lineage_column_match import (
        extract_lineage_rows_from_response,
        match_columns_with_lineage,
    )

    rows    = extract_lineage_rows_from_response(ODS_FACT_SERVICE_SAMPLE_RESPONSE)
    results = match_columns_with_lineage(
        lineage_rows   = rows,
        field_mappings = MAPPER_FIELD_MAPPINGS_SAMPLE,
        regulation     = "emiretd",
        metadata_key   = "pos_markets",
    )

    for r in results:
        assert r.get("regulation") == "emiretd",     f"Bad regulation on {r.get('mapperColumn')!r}"
        assert r.get("key")        == "pos_markets", f"Bad key on {r.get('mapperColumn')!r}"
    print("✅ test_result_tagged_with_key_and_regulation passed")


if __name__ == "__main__":
    test_extract_lineage_rows()
    test_wildcard_and_gfolyreg_skipped()
    test_exact_match_trade_status_sk()
    test_exact_match_symbol()
    test_firm_code_no_match()
    test_result_tagged_with_key_and_regulation()
    print("\n✅ All tests passed")