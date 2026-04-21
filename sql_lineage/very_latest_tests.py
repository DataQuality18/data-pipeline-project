"""
Comprehensive test suite for SQL lineage extraction.
Tests all possible SQL patterns and validates output.
"""

import json
import base64
import pytest

from test_sql import (
    extract_lineage_rows,
    parse_metadata_and_extract_lineage,
    REMARKS
)

# ============================================================================
# CONSTANTS
# ============================================================================

REG = "TEST_REG"
KEY = "TEST_KEY"
VIEW = "TEST_VIEW"

# ============================================================================
# HELPERS
# ============================================================================

def lineage(sql, reg=REG, key=KEY, view=VIEW):
    """Run the extractor and return the result rows."""
    return extract_lineage_rows(sql, reg, key, view)


def has_column(results, column_name, **kwargs):
    """Check if column exists with filters."""
    for r in results:
        if r.get("columnName", "").lower() != column_name.lower():
            continue

        if "db" in kwargs and r.get("databaseName", "").lower() != kwargs["db"].lower():
            continue

        if "table" in kwargs and r.get("tableName", "").lower() != kwargs["table"].lower():
            continue

        if "table_alias" in kwargs and r.get("tableAliasName", "").lower() != kwargs["table_alias"].lower():
            continue

        if "has_remark" in kwargs:
            remarks = " ".join(r.get("remarks", [])).lower()
            if kwargs["has_remark"].lower() not in remarks:
                continue

        return True

    return False


def assert_no_forbidden_remarks(results):
    """Ensure no forbidden remarks exist."""
    forbidden = ["error", "unknown", "invalid"]
    for r in results:
        remarks = " ".join(r.get("remarks", [])).lower()
        for f in forbidden:
            assert f not in remarks, f"Forbidden remark '{f}' found in {r}"


# ============================================================================
# TEST CASES (CORE)
# ============================================================================

def test_basic_select():
    sql = "SELECT id, name FROM users"
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "id", table="users")
    assert has_column(results, "name", table="users")

    assert_no_forbidden_remarks(results)


def test_select_with_database():
    sql = "SELECT id, name FROM sales.users"
    results = lineage(sql)

    assert has_column(results, "id", db="sales", table="users")
    assert has_column(results, "name", db="sales", table="users")


def test_select_with_alias():
    sql = "SELECT u.id, u.name FROM users u"
    results = lineage(sql)

    assert has_column(results, "id", table="users", table_alias="u")
    assert has_column(results, "name", table="users", table_alias="u")


def test_select_star():
    sql = "SELECT * FROM products"
    results = lineage(sql)

    assert has_column(results, "*", table="products", has_remark="all_columns_selected")


def test_where_clause():
    sql = "SELECT id FROM users WHERE status = 'active' AND age > 18"
    results = lineage(sql)

    assert has_column(results, "status", table="users", has_remark="where_clause_column")
    assert has_column(results, "age", table="users", has_remark="where_clause_column")


def test_inner_join():
    sql = """
    SELECT u.id, o.order_id
    FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    """
    results = lineage(sql)

    assert has_column(results, "id", table="users", table_alias="u")
    assert has_column(results, "order_id", table="orders", table_alias="o")
    assert has_column(results, "user_id", has_remark="join_on_clause_column")


def test_cte():
    sql = """
    WITH active_users AS (
        SELECT id, name FROM users WHERE status = 'active'
    )
    SELECT id FROM active_users
    """
    results = lineage(sql)

    assert has_column(results, "id", table="users")
    assert has_column(results, "status", table="users", has_remark="where_clause_column")


def test_case_expression():
    sql = """
    SELECT id,
           CASE WHEN status='active' THEN 'A' ELSE 'I' END as status_code
    FROM users
    """
    results = lineage(sql)

    assert has_column(results, "status", table="users", has_remark="case_expression")


def test_union():
    sql = """
    SELECT id FROM users
    UNION
    SELECT id FROM customers
    """
    results = lineage(sql)

    assert has_column(results, "id")


def test_metadata_api():
    sql = "SELECT id FROM users"

    metadata = json.dumps({
        "sql_query": base64.b64encode(sql.encode()).decode()
    })

    results = parse_metadata_and_extract_lineage(metadata, REG, KEY, VIEW)

    assert any(r["columnName"] == "id" for r in results)


# ============================================================================
# EXACT OUTPUT VALIDATION
# ============================================================================

def test_exact_output_structure():
    sql = "SELECT id FROM users"
    results = lineage(sql)

    required_fields = [
        "databaseName", "tableName", "tableAliasName",
        "columnName", "aliasName", "remarks"
    ]

    for r in results:
        for f in required_fields:
            assert f in r, f"Missing field {f}"