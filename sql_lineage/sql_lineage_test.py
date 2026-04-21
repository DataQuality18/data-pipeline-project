import os
from typing import Any, Dict, List

import pytest
import requests


BASE_URL = os.getenv("SQL_LINEAGE_BASE_URL", "http://localhost:8000")
ENDPOINT = os.getenv("SQL_LINEAGE_ENDPOINT", "/sql-lineage")


def call_lineage_api(
    sql: str,
    regulation: str = "test_reg",
    metadatakey: str = "test_key",
    view_name: str = "test_view",
) -> Dict[str, Any]:
    """
    Calls the SQL Lineage API using a hardcoded SQL payload.

    Adjust payload keys here if your FastAPI endpoint expects a different schema.
    """
    url = f"{BASE_URL.rstrip('/')}{ENDPOINT}"

    payload = {
        "sql": sql,
        "regulation": regulation,
        "metadatakey": metadatakey,
        "view_name": view_name,
    }

    response = requests.post(url, json=payload, timeout=30)

    assert response.status_code == 200, (
        f"Expected 200 but got {response.status_code}. "
        f"Response text: {response.text}"
    )

    body = response.json()
    assert isinstance(body, dict), f"Expected dict response but got: {type(body)}"

    # Adjust this if your response uses a different key like 'data' or 'results'
    assert "lineage" in body, f"Response missing 'lineage' key. Body: {body}"
    assert isinstance(body["lineage"], list), f"'lineage' should be a list. Body: {body}"

    return body


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize row values for stable comparisons.
    """
    return {
        "databaseName": str(row.get("databaseName", "") or "").lower(),
        "tableName": str(row.get("tableName", "") or "").lower(),
        "tableAliasName": str(row.get("tableAliasName", "") or "").lower(),
        "columnName": str(row.get("columnName", "") or "").lower(),
        "aliasName": str(row.get("aliasName", "") or "").lower(),
        "remarks": [str(x).lower() for x in row.get("remarks", [])],
    }


def assert_contains_row(
    lineage_rows: List[Dict[str, Any]],
    expected_partial: Dict[str, Any],
) -> None:
    """
    Assert at least one lineage row matches the expected partial fields.
    """
    normalized_rows = [normalize_row(r) for r in lineage_rows]
    expected_partial_normalized = {
        k: ([str(x).lower() for x in v] if isinstance(v, list) else str(v).lower())
        for k, v in expected_partial.items()
    }

    for row in normalized_rows:
        matched = True
        for key, expected_value in expected_partial_normalized.items():
            actual_value = row.get(key)

            if key == "remarks":
                # For remarks, expected items must all be present
                if not all(item in actual_value for item in expected_value):
                    matched = False
                    break
            else:
                if actual_value != expected_value:
                    matched = False
                    break

        if matched:
            return

    raise AssertionError(
        f"Expected row not found.\nExpected partial: {expected_partial_normalized}\n"
        f"Actual rows: {normalized_rows}"
    )


def assert_forbidden_remarks_absent(
    lineage_rows: List[Dict[str, Any]],
    forbidden_remarks: List[str],
) -> None:
    """
    Assert that none of the forbidden remarks appear in any lineage row.
    """
    forbidden = {item.lower() for item in forbidden_remarks}

    for row in lineage_rows:
        remarks = {str(x).lower() for x in row.get("remarks", [])}
        overlap = remarks.intersection(forbidden)
        assert not overlap, f"Forbidden remarks found {overlap} in row: {row}"


TEST_CASES = [
    {
        "name": "simple_select",
        "sql": """
            SELECT id, name
            FROM users
        """,
        # Update after first real API run if needed
        "expected_count": 2,
        "expected_rows": [
            {"tableName": "users", "columnName": "id"},
            {"tableName": "users", "columnName": "name"},
        ],
        "forbidden_remarks": ["invalid_table_alias", "tech_failure"],
    },
    {
        "name": "join_with_aliases",
        "sql": """
            SELECT a.id, b.name
            FROM table_a a
            JOIN table_b b
              ON a.id = b.id
        """,
        # Expected rows may include:
        # - selected a.id
        # - selected b.name
        # - join_on a.id
        # - join_on b.id
        # Deduplication/final API behavior may affect final count.
        "expected_count": 4,
        "expected_rows": [
            {"tableName": "table_a", "columnName": "id"},
            {"tableName": "table_b", "columnName": "name"},
            {"tableName": "table_b", "columnName": "id", "remarks": ["join_on_clause_column"]},
        ],
        "forbidden_remarks": ["invalid_table_alias", "tech_failure"],
    },
    {
        "name": "derived_and_case",
        "sql": """
            SELECT
                emp_id,
                salary * 12 AS annual_salary,
                CASE WHEN salary > 5000 THEN 'HIGH' ELSE 'LOW' END AS salary_band
            FROM employees
        """,
        # Likely rows:
        # - emp_id selected
        # - salary from derived expression
        # - salary from CASE expression
        # Deduplication may or may not merge depending on remarks.
        "expected_count": 3,
        "expected_rows": [
            {"tableName": "employees", "columnName": "emp_id"},
            {"tableName": "employees", "columnName": "salary", "aliasName": "annual_salary"},
            {
                "tableName": "employees",
                "columnName": "salary",
                "aliasName": "salary_band",
                "remarks": ["derived_expression", "case_expression"],
            },
        ],
        "forbidden_remarks": ["invalid_table_alias", "tech_failure"],
    },
    {
        "name": "basic_cte",
        "sql": """
            WITH cte_users AS (
                SELECT id, name, dept_id
                FROM users
            )
            SELECT id, name
            FROM cte_users
        """,
        # Based on current scope logic, expected rows should resolve stably.
        # Freeze exact count after first verified run if needed.
        "expected_count": 2,
        "expected_rows": [
            {"columnName": "id"},
            {"columnName": "name"},
        ],
        "forbidden_remarks": ["invalid_table_alias", "tech_failure"],
    },
    {
        "name": "cte_plus_join",
        "sql": """
            WITH orders_cte AS (
                SELECT customer_id, order_id
                FROM orders
            ),
            customers_cte AS (
                SELECT id, name
                FROM customers
            )
            SELECT o.order_id, c.name
            FROM orders_cte o
            JOIN customers_cte c
              ON o.customer_id = c.id
        """,
        "expected_count": 4,
        "expected_rows": [
            {"columnName": "order_id"},
            {"columnName": "name"},
            {"columnName": "customer_id", "remarks": ["join_on_clause_column"]},
            {"columnName": "id", "remarks": ["join_on_clause_column"]},
        ],
        "forbidden_remarks": ["invalid_table_alias", "tech_failure"],
    },
]


@pytest.mark.parametrize(
    "case",
    TEST_CASES,
    ids=[case["name"] for case in TEST_CASES],
)
def test_sql_lineage_regression_cases(case: Dict[str, Any]) -> None:
    """
    Regression tests for stable SQL lineage behavior using frozen SQL inputs.
    """
    response_body = call_lineage_api(case["sql"])
    lineage_rows = response_body["lineage"]

    assert len(lineage_rows) == case["expected_count"], (
        f"Unexpected lineage count for case '{case['name']}'.\n"
        f"Expected: {case['expected_count']}\n"
        f"Actual: {len(lineage_rows)}\n"
        f"Rows: {lineage_rows}"
    )

    for expected_row in case.get("expected_rows", []):
        assert_contains_row(lineage_rows, expected_row)

    assert_forbidden_remarks_absent(
        lineage_rows,
        case.get("forbidden_remarks", []),
    )