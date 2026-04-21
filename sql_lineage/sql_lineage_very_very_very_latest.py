
"""
Comprehensive test suite for SQL lineage extraction.
Tests all possible SQL patterns and validates output.
"""
import json
import base64
import pytest
from test_sql import extract_lineage_rows, parse_metadata_and_extract_lineage, REMARKS

# ============================================================================
# CONSTANTS
# ============================================================================

REG  = "TEST_REG"
KEY  = "TEST_KEY"
VIEW = "TEST_VIEW"


# ============================================================================
# HELPERS
# ============================================================================

def lineage(sql, reg=REG, key=KEY, view=VIEW):
    """Run the extractor and return the result rows."""
    return extract_lineage_rows(sql, reg, key, view)


def has_column(results, column_name, **kwargs):
    """Return True if any row matches column_name plus the given filters.

    Keyword filters
    ---------------
    db          : databaseName (case-insensitive)
    table       : tableName    (case-insensitive)
    table_alias : tableAliasName (case-insensitive)
    has_remark  : substring that must appear in the joined remarks string
    """
    for r in results:
        if r["columnName"].lower() != column_name.lower():
            continue
        if "db" in kwargs and r["databaseName"].lower() != kwargs["db"].lower():
            continue
        if "table" in kwargs and r["tableName"].lower() != kwargs["table"].lower():
            continue
        if "table_alias" in kwargs and r["tableAliasName"].lower() != kwargs["table_alias"].lower():
            continue
        if "has_remark" in kwargs:
            remarks_str = " ".join(r["remarks"]).lower()
            if kwargs["has_remark"].lower() not in remarks_str:
                continue
        return True
    return False


# ============================================================================
# TEST CASES
# ============================================================================

def test_basic_select():
    """Test 1: Basic SELECT from single table."""
    sql = "SELECT id, name FROM users"
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "id",   table="users")
    assert has_column(results, "name", table="users")


def test_select_with_database():
    """Test 2: SELECT with database-qualified table."""
    sql = "SELECT id, name FROM sales.users"
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "id",   db="sales", table="users")
    assert has_column(results, "name", db="sales", table="users")


def test_select_with_alias():
    """Test 3: SELECT with table alias."""
    sql = "SELECT u.id, u.name FROM users u"
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "id",   table="users", table_alias="u")
    assert has_column(results, "name", table="users", table_alias="u")


def test_select_star():
    """Test 4: SELECT *."""
    sql = "SELECT * FROM products"
    results = lineage(sql)

    assert len(results) >= 1
    assert has_column(results, "*", table="products", has_remark="all_columns_selected")


def test_select_star_with_alias():
    """Test 5: SELECT * with table alias."""
    sql = "SELECT p.* FROM products p"
    results = lineage(sql)

    assert len(results) >= 1
    assert has_column(results, "*", table="products", table_alias="p", has_remark="all_columns_selected")


def test_where_clause():
    """Test 6: WHERE clause columns are captured with correct remark."""
    sql = "SELECT id FROM users WHERE status = 'active' AND age > 18"
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "id",     table="users")
    assert has_column(results, "status", table="users", has_remark="where_clause_column")
    assert has_column(results, "age",    table="users", has_remark="where_clause_column")


def test_group_by():
    """Test 7: GROUP BY column carries correct remark."""
    sql = "SELECT category, COUNT(*) FROM products GROUP BY category"
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "category", table="products", has_remark="group_by_column")


def test_having():
    """Test 8: HAVING clause."""
    sql = "SELECT category FROM products GROUP BY category HAVING COUNT(*) > 10"
    results = lineage(sql)

    assert len(results) >= 1
    assert has_column(results, "category", table="products")


def test_inner_join():
    """Test 9: INNER JOIN — selected and ON columns all resolved."""
    sql = """
    SELECT u.id, o.order_id
    FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    """
    results = lineage(sql)

    assert len(results) >= 4
    assert has_column(results, "id",       table="users",  table_alias="u")
    assert has_column(results, "order_id", table="orders", table_alias="o")
    assert has_column(results, "user_id",  has_remark="join_on_clause_column")


def test_left_join():
    """Test 10: LEFT JOIN."""
    sql = """
    SELECT u.name, p.product_name
    FROM users u
    LEFT JOIN purchases p ON u.id = p.user_id
    """
    results = lineage(sql)

    assert len(results) >= 4
    assert has_column(results, "name",         table="users",     table_alias="u")
    assert has_column(results, "product_name", table="purchases", table_alias="p")


def test_multiple_joins():
    """Test 11: Multiple JOINs."""
    sql = """
    SELECT u.name, o.order_id, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "name",         table="users",    table_alias="u")
    assert has_column(results, "order_id",     table="orders",   table_alias="o")
    assert has_column(results, "product_name", table="products", table_alias="p")


def test_cte():
    """Test 12: Common Table Expression (CTE)."""
    sql = """
    WITH active_users AS (
        SELECT id, name
        FROM users
        WHERE status = 'active'
    )
    SELECT id, name FROM active_users
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "id",     table="users")
    assert has_column(results, "name",   table="users")
    assert has_column(results, "status", table="users", has_remark="where_clause_column")


def test_cte_with_database():
    """Test 13: CTE with database-qualified table."""
    sql = """
    WITH active_orders AS (
        SELECT *
        FROM sales.orders
        WHERE status = 'ACTIVE'
    )
    SELECT id, amount FROM active_orders
    """
    results = lineage(sql)

    assert len(results) >= 4
    assert has_column(results, "id",     db="sales", table="orders")
    assert has_column(results, "amount", db="sales", table="orders")
    assert has_column(results, "*",      db="sales", table="orders", has_remark="all_columns_selected")
    assert has_column(results, "status", db="sales", table="orders", has_remark="where_clause_column")


def test_subquery():
    """Test 14: Subquery in FROM."""
    sql = """
    SELECT sub.id, sub.total
    FROM (
        SELECT user_id as id, SUM(amount) as total
        FROM orders
        GROUP BY user_id
    ) sub
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "id",      table="orders")
    assert has_column(results, "user_id", table="orders", has_remark="group_by_column")


def test_case_expression():
    """Test 15: CASE expression."""
    sql = """
    SELECT
        id,
        CASE
            WHEN status = 'active'   THEN 'A'
            WHEN status = 'inactive' THEN 'I'
            ELSE 'U'
        END as status_code
    FROM users
    """
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "id",     table="users")
    assert has_column(results, "status", table="users", has_remark="case_expression")


def test_derived_columns():
    """Test 16: Derived / calculated columns."""
    sql = """
    SELECT
        id,
        price * quantity as total,
        CONCAT(first_name, ' ', last_name) as full_name
    FROM orders
    """
    results = lineage(sql)

    assert len(results) >= 4
    assert has_column(results, "id",       table="orders")
    assert has_column(results, "price",    table="orders", has_remark="derived_expression")
    assert has_column(results, "quantity", table="orders", has_remark="derived_expression")


def test_union():
    """Test 17: UNION query."""
    sql = """
    SELECT id, name FROM users
    UNION
    SELECT id, name FROM customers
    """
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "id")
    assert has_column(results, "name")


def test_nested_subquery():
    """Test 18: Nested subquery in WHERE IN."""
    sql = """
    SELECT u.name
    FROM users u
    WHERE u.id IN (
        SELECT user_id
        FROM orders
        WHERE amount > 1000
    )
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "name",    table="users",  table_alias="u")
    assert has_column(results, "id",      table="users",  table_alias="u", has_remark="where_clause_column")
    assert has_column(results, "amount",  table="orders", has_remark="where_clause_column")


def test_join_with_subquery():
    """Test 19: JOIN with derived-table subquery."""
    sql = """
    SELECT u.name, sub.total
    FROM users u
    JOIN (
        SELECT user_id, SUM(amount) as total
        FROM orders
        GROUP BY user_id
    ) sub ON u.id = sub.user_id
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "name",    table="users",  table_alias="u")
    assert has_column(results, "user_id", table="orders", has_remark="join_on_clause_column")


def test_complex_query():
    """Test 20: Complex query — CTE + JOIN + CASE + WHERE + ORDER BY."""
    sql = """
    WITH monthly_sales AS (
        SELECT
            DATE_TRUNC('month', order_date) as month,
            product_id,
            SUM(quantity * price) as revenue
        FROM sales.orders
        WHERE order_date >= '2024-01-01'
        GROUP BY DATE_TRUNC('month', order_date), product_id
        HAVING SUM(quantity * price) > 10000
    )
    SELECT
        ms.month,
        p.product_name,
        ms.revenue,
        CASE
            WHEN ms.revenue > 50000 THEN 'High'
            WHEN ms.revenue > 20000 THEN 'Medium'
            ELSE 'Low'
        END as revenue_category
    FROM monthly_sales ms
    JOIN products p ON ms.product_id = p.id
    WHERE p.category = 'Electronics'
    ORDER BY ms.revenue DESC
    """
    results = lineage(sql)

    assert len(results) >= 5
    assert has_column(results, "product_name", table="products", table_alias="p")
    assert has_column(results, "order_date",   db="sales", table="orders", has_remark="where_clause_column")
    assert has_column(results, "category",     table="products", table_alias="p", has_remark="where_clause_column")


def test_function_only():
    """Test 21: Function-only expressions (no source table)."""
    sql = "SELECT NOW() as current_time, RAND() as random_value"
    results = lineage(sql)

    assert len(results) >= 2
    assert (
        has_column(results, "now()",  has_remark="function_expression") or
        has_column(results, "rand()", has_remark="function_expression")
    )


def test_ambiguous_column():
    """Test 22: Unqualified column when multiple tables are in scope."""
    sql = """
    SELECT id
    FROM users u
    CROSS JOIN orders o
    """
    results = lineage(sql)

    assert len(results) >= 1
    assert has_column(results, "id", has_remark="table_name_ambiguous")


def test_qualified_ambiguous():
    """Test 23: Qualified columns resolve ambiguity in CROSS JOIN."""
    sql = """
    SELECT u.id, o.id
    FROM users u
    CROSS JOIN orders o
    """
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "id", table="users",  table_alias="u")
    assert has_column(results, "id", table="orders", table_alias="o")


def test_metadata_api():
    """Test 24: parse_metadata_and_extract_lineage API (base64-encoded SQL)."""
    sql = "SELECT id, name FROM users"
    metadata = json.dumps({
        "sql_query": base64.b64encode(sql.encode()).decode()
    })

    results = parse_metadata_and_extract_lineage(metadata, REG, KEY, VIEW)

    assert len(results) >= 2
    assert any(r["columnName"] == "id"   and r["tableName"] == "users" for r in results)
    assert any(r["columnName"] == "name" and r["tableName"] == "users" for r in results)


def test_right_join():
    """Test 25: RIGHT JOIN."""
    sql = """
    SELECT e.employee_id, d.department_name
    FROM employees e
    RIGHT JOIN departments d ON e.department_id = d.department_id
    """
    results = lineage(sql)

    assert len(results) >= 4
    assert has_column(results, "employee_id",     table="employees",  table_alias="e")
    assert has_column(results, "department_name", table="departments", table_alias="d")
    assert has_column(results, "department_id",   has_remark="join_on_clause_column")


def test_full_outer_join():
    """Test 26: FULL OUTER JOIN."""
    sql = """
    SELECT a.account_id, b.balance
    FROM accounts a
    FULL OUTER JOIN balances b ON a.account_id = b.account_id
    """
    results = lineage(sql)

    assert len(results) >= 4
    assert has_column(results, "account_id", table="accounts", table_alias="a")
    assert has_column(results, "balance",    table="balances", table_alias="b")
    assert has_column(results, "account_id", has_remark="join_on_clause_column")


def test_self_join():
    """Test 27: Self JOIN (same table with two aliases)."""
    sql = """
    SELECT e.employee_id, e.name, m.name AS manager_name
    FROM employees e
    JOIN employees m ON e.manager_id = m.employee_id
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "employee_id", table="employees")
    assert has_column(results, "name",        table="employees")
    assert has_column(results, "manager_id",  has_remark="join_on_clause_column")


def test_order_by():
    """Test 28: ORDER BY clause columns are present."""
    sql = """
    SELECT id, name, created_at
    FROM users
    ORDER BY created_at DESC, name ASC
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "id",         table="users")
    assert has_column(results, "name",       table="users")
    assert has_column(results, "created_at", table="users")


def test_union_all():
    """Test 29: UNION ALL query."""
    sql = """
    SELECT id, email FROM users
    UNION ALL
    SELECT id, email FROM archived_users
    """
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "id")
    assert has_column(results, "email")


def test_intersect():
    """Test 30: INTERSECT query."""
    sql = """
    SELECT product_id FROM orders
    INTERSECT
    SELECT product_id FROM returns
    """
    results = lineage(sql)

    assert len(results) >= 1
    assert has_column(results, "product_id")


def test_except():
    """Test 31: EXCEPT (set difference) query."""
    sql = """
    SELECT user_id FROM subscribers
    EXCEPT
    SELECT user_id FROM unsubscribed
    """
    results = lineage(sql)

    assert len(results) >= 1
    assert has_column(results, "user_id")


def test_multiple_ctes():
    """Test 32: Multiple CTEs."""
    sql = """
    WITH active_users AS (
        SELECT id, name FROM users WHERE status = 'active'
    ),
    recent_orders AS (
        SELECT user_id, order_id, amount
        FROM orders
        WHERE order_date >= '2024-01-01'
    )
    SELECT au.name, ro.order_id, ro.amount
    FROM active_users au
    JOIN recent_orders ro ON au.id = ro.user_id
    """
    results = lineage(sql)

    assert len(results) >= 5
    assert has_column(results, "name",       table="users")
    assert has_column(results, "order_id",   table="orders")
    assert has_column(results, "amount",     table="orders")
    assert has_column(results, "status",     table="users",  has_remark="where_clause_column")
    assert has_column(results, "order_date", table="orders", has_remark="where_clause_column")


def test_recursive_cte():
    """Test 33: Recursive CTE."""
    sql = """
    WITH RECURSIVE org_hierarchy AS (
        SELECT employee_id, name, manager_id, 1 AS level
        FROM employees
        WHERE manager_id IS NULL
        UNION ALL
        SELECT e.employee_id, e.name, e.manager_id, oh.level + 1
        FROM employees e
        JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
    )
    SELECT employee_id, name, level FROM org_hierarchy
    """
    results = lineage(sql)

    assert len(results) >= 4
    assert has_column(results, "employee_id", table="employees")
    assert has_column(results, "name",        table="employees")
    assert has_column(results, "manager_id",  table="employees")


def test_window_function():
    """Test 34: Window function (OVER / PARTITION BY)."""
    sql = """
    SELECT
        employee_id,
        department_id,
        salary,
        RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank,
        SUM(salary) OVER (PARTITION BY department_id) AS dept_total_salary
    FROM employees
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "employee_id",  table="employees")
    assert has_column(results, "salary",       table="employees")
    assert has_column(results, "department_id", table="employees")


def test_subquery_in_select():
    """Test 35: Correlated subquery in SELECT list."""
    sql = """
    SELECT
        d.department_name,
        (SELECT COUNT(*) FROM employees e WHERE e.department_id = d.department_id) AS headcount
    FROM departments d
    """
    results = lineage(sql)

    assert len(results) >= 2
    assert has_column(results, "department_name", table="departments", table_alias="d")
    assert has_column(results, "department_id",   table="departments")


def test_subquery_in_where():
    """Test 36: Subquery in WHERE with EXISTS."""
    sql = """
    SELECT product_id, product_name
    FROM products p
    WHERE EXISTS (
        SELECT 1 FROM inventory i
        WHERE i.product_id = p.product_id AND i.stock > 0
    )
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "product_id",   table="products",  table_alias="p")
    assert has_column(results, "product_name", table="products",  table_alias="p")
    assert has_column(results, "stock",        table="inventory", has_remark="where_clause_column")


def test_multi_level_subquery():
    """Test 37: Multi-level nested subquery."""
    sql = """
    SELECT outer_query.region, outer_query.total_revenue
    FROM (
        SELECT mid.region, SUM(mid.revenue) AS total_revenue
        FROM (
            SELECT r.region_name AS region, o.amount AS revenue
            FROM regions r
            JOIN orders o ON r.region_id = o.region_id
        ) mid
        GROUP BY mid.region
    ) outer_query
    WHERE outer_query.total_revenue > 100000
    """
    results = lineage(sql)

    assert len(results) >= 4
    assert has_column(results, "region_name", table="regions")
    assert has_column(results, "amount",      table="orders")
    assert has_column(results, "region_id",   has_remark="join_on_clause_column")


def test_case_in_where():
    """Test 38: CASE expression used inside WHERE."""
    sql = """
    SELECT id, name, score
    FROM students
    WHERE CASE WHEN grade = 'A' THEN score > 90 ELSE score > 75 END
    """
    results = lineage(sql)

    assert len(results) >= 3
    assert has_column(results, "id",    table="students")
    assert has_column(results, "score", table="students")
    assert has_column(results, "grade", table="students", has_remark="where_clause_column")


def test_coalesce_and_nullif():
    """Test 39: COALESCE and NULLIF expressions."""
    sql = """
    SELECT
        id,
        COALESCE(phone, email, 'N/A') AS contact,
        NULLIF(discount, 0) AS effective_discount
    FROM customers
    """
    results = lineage(sql)

    assert len(results) >= 4
    assert has_column(results, "id",       table="customers")
    assert has_column(results, "phone",    table="customers", has_remark="derived_expression")
    assert has_column(results, "email",    table="customers", has_remark="derived_expression")
    assert has_column(results, "discount", table="customers", has_remark="derived_expression")


def test_multi_database_join():
    """Test 40: JOIN across multiple databases."""
    sql = """
    SELECT s.sale_id, p.product_name, c.customer_name
    FROM sales.transactions s
    JOIN inventory.products p ON s.product_id = p.product_id
    JOIN crm.customers c ON s.customer_id = c.customer_id
    """
    results = lineage(sql)

    assert len(results) >= 6
    assert has_column(results, "sale_id",       db="sales",     table="transactions", table_alias="s")
    assert has_column(results, "product_name",  db="inventory", table="products",     table_alias="p")
    assert has_column(results, "customer_name", db="crm",       table="customers",    table_alias="c")
    assert has_column(results, "product_id",    has_remark="join_on_clause_column")


# ============================================================================
# TEST 41 — exact field-by-field output assertion
# ============================================================================

COALESCE_SQL = """SELECT
    customer_id,
    COALESCE(mobile_phone, home_phone, work_phone, 'UNKNOWN')  AS best_phone,
    COALESCE(preferred_name, first_name)                        AS display_name,
    COALESCE(promo_price, sale_price, list_price)               AS effective_price
FROM  customers"""

COALESCE_EXPECTED = [
    {
        "databaseName": "", "tableName": "customers", "tableAliasName": "",
        "columnName": "customer_id", "aliasName": "",
        "regulation": "SEC", "metadatakey": "KEY123", "viewName": "VW_SAMPLE",
        "remarks": ["database_not_specified_in_query"],
    },
    {
        "databaseName": "", "tableName": "customers", "tableAliasName": "",
        "columnName": "mobile_phone", "aliasName": "best_phone",
        "regulation": "SEC", "metadatakey": "KEY123", "viewName": "VW_SAMPLE",
        "remarks": ["derived_expression"],
    },
    {
        "databaseName": "", "tableName": "customers", "tableAliasName": "",
        "columnName": "home_phone", "aliasName": "best_phone",
        "regulation": "SEC", "metadatakey": "KEY123", "viewName": "VW_SAMPLE",
        "remarks": ["derived_expression"],
    },
    {
        "databaseName": "", "tableName": "customers", "tableAliasName": "",
        "columnName": "work_phone", "aliasName": "best_phone",
        "regulation": "SEC", "metadatakey": "KEY123", "viewName": "VW_SAMPLE",
        "remarks": ["derived_expression"],
    },
    {
        "databaseName": "", "tableName": "customers", "tableAliasName": "",
        "columnName": "preferred_name", "aliasName": "display_name",
        "regulation": "SEC", "metadatakey": "KEY123", "viewName": "VW_SAMPLE",
        "remarks": ["derived_expression"],
    },
    {
        "databaseName": "", "tableName": "customers", "tableAliasName": "",
        "columnName": "first_name", "aliasName": "display_name",
        "regulation": "SEC", "metadatakey": "KEY123", "viewName": "VW_SAMPLE",
        "remarks": ["derived_expression"],
    },
    {
        "databaseName": "", "tableName": "customers", "tableAliasName": "",
        "columnName": "promo_price", "aliasName": "effective_price",
        "regulation": "SEC", "metadatakey": "KEY123", "viewName": "VW_SAMPLE",
        "remarks": ["derived_expression"],
    },
    {
        "databaseName": "", "tableName": "customers", "tableAliasName": "",
        "columnName": "sale_price", "aliasName": "effective_price",
        "regulation": "SEC", "metadatakey": "KEY123", "viewName": "VW_SAMPLE",
        "remarks": ["derived_expression"],
    },
    {
        "databaseName": "", "tableName": "customers", "tableAliasName": "",
        "columnName": "list_price", "aliasName": "effective_price",
        "regulation": "SEC", "metadatakey": "KEY123", "viewName": "VW_SAMPLE",
        "remarks": ["derived_expression"],
    },
]

FIELDS = [
    "databaseName", "tableName", "tableAliasName",
    "columnName", "aliasName", "regulation",
    "metadatakey", "viewName", "remarks",
]


def test_coalesce_exact_output():
    """Test 41: Exact field-by-field output assertion for COALESCE query."""
    actual = extract_lineage_rows(COALESCE_SQL, "SEC", "KEY123", "VW_SAMPLE")

    assert len(actual) == len(COALESCE_EXPECTED), (
        f"Row count mismatch: expected {len(COALESCE_EXPECTED)}, got {len(actual)}"
    )

    for idx, (exp_row, act_row) in enumerate(zip(COALESCE_EXPECTED, actual)):
        for field in FIELDS:
            exp_val = exp_row[field]
            act_val = act_row[field]

            # Sort remarks so order does not matter
            if field == "remarks":
                exp_val = sorted(exp_val)
                act_val = sorted(act_val)

            assert exp_val == act_val, (
                f"Row {idx} | field '{field}' mismatch:\n"
                f"  expected : {exp_val!r}\n"
                f"  actual   : {act_val!r}\n"
                f"  full row : {json.dumps(act_row, indent=2)}"
            )


# ============================================================================
# TEST 42 — FRY15 Derivative Aggregated (real-world query)
# ============================================================================

FRY15_SQL = """
SELECT * FROM (
    SELECT
        OM_FIN_RWA_AGGREGATOR_FACT_SK,
        DATA_CATEGORY,
        FDL_ACCOUNT_DESC,
        FDL_ACCOUNT,
        FYR_ACCOUNTING_PERIOD,
        LOWER(RUN_FREQUENCY_TYPE) AS RUN_FREQUENCY_TYPE,
        BATCH_SCOPE,
        ADJUSTMENT_FILE_ID,
        COMMENTS,
        CWM_TSA_DESC,
        CWM_TSA_ID,
        NETTING_AGREEMENT_ID,
        NETTING_FLAG,
        NETTING_SET_ID,
        SA_MARGIN_MODE_APPLIED,
        AA_RISK_WEIGHT,
        ACCRUAL_STATUS,
        COUNTRY_RISK_CLASSIFICATION,
        REGULATORY_SEGMENT_DESC,
        TRANSACTION_CURRENCY_CODE,
        REGULATORY_EXPOSURE_TYPE,
        RWA_EXPOSURE_DESC,
        RWA_EXPOSURE_TYPE,
        SA_RISK_WEIGHT,
        FFIEC_MAPPED_FLAG,
        AVC_ASSET_VALUE_CORRELTN_FLAG,
        DEFAULT_FUND_DF_FLAG,
        EAD_DEFAULT_FLAG,
        MATURITY_DEFAULT_FLAG,
        SWWR_SPECIFIC_WRNG_WAY_RSK_FLG,
        GOC,
        MANAGED_GEOGRAPHY,
        MANAGED_SEGMENT_CODE,
        LEGAL_VEHICLE_ID,
        RE_IS_CBNA,
        RE_IS_CG,
        AA_EAD_AMOUNT,
        AA_RWA_AMOUNT,
        SACCR_EAD_AMT,
        SA_EAD_AMOUNT,
        SA_RWA_AMOUNT,
        GFCID,
        CUST_INDUSTRY_CODE,
        CUST_CURRENCY_LOCAL,
        CUST_CORP_COUNTRY,
        CUST_CRC_COUNTRY_RISK_CLASSIFC,
        CUSTOMER_DOMICILE_CODE,
        GFPID,
        CUST_LOCAL,
        CUST_NATIONALITY_COUNTRY_CODE,
        CUST_OBLIGATION_TYPE,
        CUSTOMER_RISK_WEIGHT,
        PARTY_NAME,
        CUSTOMER_RELATIONSHIP_NAME,
        SA_BASEL_ASSET_CLASS,
        CUST_DOMICILE_COUNTRY_CODE,
        GEO_FRS_ID,
        GEO_SOURCE_COUNTRY_CODE,
        RISK_ASSET_CLASS,
        RISK_SUBASSET_CLASS,
        GEO_DESCRIPTION_LEVEL_1,
        GEO_DESCRIPTION_LEVEL_2,
        GEO_DESCRIPTION_LEVEL_3,
        GEO_DESCRIPTION_LEVEL_4,
        GEO_DESCRIPTION_LEVEL_5,
        GEO_DESCRIPTION_LEVEL_6,
        GEO_DESCRIPTION_LEVEL_10,
        GEO_DESCRIPTION_LEVEL_11,
        GEO_DESCRIPTION_LEVEL_12,
        GEO_DESCRIPTION_LEVEL_13,
        GEO_DESCRIPTION_LEVEL_14,
        GEO_DESCRIPTION_LEVEL_15,
        GEO_DESCRIPTION_LEVEL_7,
        GEO_DESCRIPTION_LEVEL_8,
        GEO_DESCRIPTION_LEVEL_9,
        SEGMENT_DESCRIPTION_LEVEL_1,
        SEGMENT_DESCRIPTION_LEVEL_10,
        SEGMENT_DESCRIPTION_LEVEL_11,
        SEGMENT_DESCRIPTION_LEVEL_12,
        SEGMENT_DESCRIPTION_LEVEL_13,
        SEGMENT_DESCRIPTION_LEVEL_14,
        SEGMENT_DESCRIPTION_LEVEL_15,
        SEGMENT_DESCRIPTION_LEVEL_2,
        SEGMENT_DESCRIPTION_LEVEL_3,
        SEGMENT_DESCRIPTION_LEVEL_4,
        SEGMENT_DESCRIPTION_LEVEL_5,
        SEGMENT_DESCRIPTION_LEVEL_6,
        SEGMENT_DESCRIPTION_LEVEL_7,
        SEGMENT_DESCRIPTION_LEVEL_8,
        SEGMENT_DESCRIPTION_LEVEL_9,
        PM_ACCOUNT_ID,
        PM_CUSTOMER_SEGMENT,
        PMF_ACCOUNT_ID,
        PMF_LVL4_DESC,
        PMF_LVL5_DESC,
        PMF_LVL6_DESC,
        PMF_LVL7_DESC,
        PMF_LVL8_DESC,
        SA_PM_ACCOUNT_ID,
        PROCESS_ID,
        REGULATORY_SEGMENT_CODE,
        PLUG_CATEGORY,
        SACCR_RWA_AMT,
        REPORTED_SA_EAD_AMT,
        REPORTED_SA_RWA_AMT,
        EXTERNAL_RATING,
        EXTERNAL_RATING_PROVIDERID,
        OBLIGOR_RATING_BAND,
        EXPOSURE_SUBTYPE,
        FFIEC_SCHEDULE,
        GAAP_BASE_CCY_AMOUNT,
        REPORT_PROCESSOR_OUT_FLAG,
        FDL_PMF_LEVEL4_DESC,
        FDL_PMF_LEVEL5_DESC,
        FDL_PMF_LEVEL6_DESC,
        FDL_PMF_LEVEL7_DESC,
        FDL_PMF_LEVEL8_DESC,
        IS_TSA_FINAL,
        BCM_CODE,
        BCM_DESC,
        AFFILIATE_CODE,
        BCM_CATEGORY,
        BCM_GROUP,
        FRS_BUSINESS_UNIT_CODE,
        FRS_BUSINESS_UNIT_DESCRIPTION,
        BASEL_WAREHOUSE_INDICATOR,
        IR_SUMMARY_KEY,
        IS_OPTIMA_FINAL,
        CAST(COLLATERAL_TYPE AS STRING) AS COLLATERAL_TYPE,
        CCF_DEFAULT_FLAG,
        CRM_APPLIED_FLAG,
        EXPOSURE_AMOUNT,
        FACILITY_SUPPORT_CODE,
        FACILITY_SUPPORT_TYPE,
        FACILITY_TYPE_CODE,
        GAAP_CATEGORY_CODE,
        GFRN_ID,
        INVESTMENT_GRADE,
        MATURITY_BAND,
        MITIGANT_RISK_WEIGHT,
        RWA_JTD_106MULTIPLIER_AMOUNT,
        RWA_METHOD,
        SA_CCF,
        SA_PRE_RELIEF_RISK_WEIGHT,
        SA_PRE_RELIEF_RWA_AMOUNT,
        WEIGHTED_MATURITY,
        GAAP_CATEGORY_DESCRIPTION,
        SA_PRE_CRM_RWA_AMOUNT,
        FRS_AFFILIATE_FLAG,
        GFPID_NAME,
        COB_DATE,
        BUSINESS_DAY_NO,
        SLR_COMPONENT_DESC,
        SLR_SUB_COMPONENT_DESC,
        SLR_AMT,
        NET_LONG_CBNA_AMT,
        NET_LONG_USCON_AMT,
        ERROR_FLAG,
        SLR_REPORTABLE_FLAG,
        FDL_EXPOSURE_TYPE,
        TRADE_TYPE,
        LEGAL_CERTAINTY_FLAG,
        AFFILIATE_INDICATOR,
        US_NPR_CCF,
        COLLATERAL_AMT,
        RWA_SOURCE,
        NOW() AS DWH_CREATED_TIME,
        DWH_UPDATED_TIME AS dwh_updated_time,
        sum(case when is_optima_final = 'Y' then 1 else 0 end)
            over (partition by run_frequency_type, cob_date order by business_day_no asc) as dwh_flag_reached
    FROM GFDLYRSK_MANAGED.OM_FIN_RWA_AGGREGATOR_FACT
    WHERE
        COB_DATE >= CAST(DATE_FORMAT(DATE_SUB(TO_DATE('##START_TIME##'), 30), 'yyyyMMdd') AS INT)
        AND RWA_EXPOSURE_TYPE = '04'
        AND BUSINESS_DAY_NO IS NOT NULL
        AND UPPER(RUN_FREQUENCY_TYPE) = 'MONTHLY'
) a
WHERE (a.dwh_flag_reached = 1 OR a.is_optima_final = 'Y')
AND a.DWH_UPDATED_TIME >= TO_TIMESTAMP('##START_TIME##', 'yyyy-MM-dd HH:mm:ss')
AND a.DWH_UPDATED_TIME <  TO_TIMESTAMP('##END_TIME##',   'yyyy-MM-dd HH:mm:ss')
"""

FRY15_DB    = "gfdlyrsk_managed"
FRY15_TABLE = "om_fin_rwa_aggregator_fact"


def test_fry15_derivative_aggregated_lineage():
    """Test 42: Full lineage test for APP_REGHUB_RHOO_FRY15_DERIVATIVE_AGGREGATED.

    Validates source table/db resolution, plain columns, LOWER/CAST/NOW()
    expressions, window function columns, and WHERE clause columns.
    """
    results = lineage(FRY15_SQL)

    assert len(results) >= 150, (
        f"Expected at least 150 lineage rows, got {len(results)}"
    )

    # Source table resolved correctly
    assert has_column(results, "om_fin_rwa_aggregator_fact_sk",
                      db=FRY15_DB, table=FRY15_TABLE)

    # Sample of plain columns
    for col in [
        "data_category", "fdl_account", "batch_scope", "netting_flag",
        "aa_risk_weight", "gfcid", "gfpid", "risk_asset_class",
        "cob_date", "business_day_no", "slr_amt", "rwa_source",
        "external_rating", "bcm_code", "exposure_amount", "rwa_method",
    ]:
        assert has_column(results, col, db=FRY15_DB, table=FRY15_TABLE), \
            f"Plain column '{col}' not traced to {FRY15_DB}.{FRY15_TABLE}"

    # LOWER() expression
    assert has_column(results, "run_frequency_type",
                      db=FRY15_DB, table=FRY15_TABLE,
                      has_remark="derived_expression"), \
        "RUN_FREQUENCY_TYPE (inside LOWER()) should have derived_expression remark"

    # CAST() expression
    assert has_column(results, "collateral_type",
                      db=FRY15_DB, table=FRY15_TABLE,
                      has_remark="derived_expression"), \
        "COLLATERAL_TYPE (inside CAST()) should have derived_expression remark"

    # NOW() function-only
    assert has_column(results, "now()", has_remark="function_expression"), \
        "NOW() should be captured as a function_expression"

    # Window function columns
    assert has_column(results, "is_optima_final", db=FRY15_DB, table=FRY15_TABLE)

    # WHERE clause columns
    for col in ["cob_date", "rwa_exposure_type", "business_day_no", "run_frequency_type"]:
        assert has_column(results, col,
                          db=FRY15_DB, table=FRY15_TABLE,
                          has_remark="where_clause_column"), \
            f"WHERE column '{col}' missing or lacks where_clause_column remark"

    # Outer WHERE
    assert has_column(results, "dwh_updated_time", has_remark="where_clause_column"), \
        "DWH_UPDATED_TIME used in outer WHERE should have where_clause_column remark"


# ============================================================================
# TEST 43 — LEI CAD Category ID Details (two CTEs + UNION)
# ============================================================================

LEI_CAD_SQL = """
WITH CONTROLS_LEI_BASE_DATA AS (
    SELECT
        *,
        sha2(COMMENTS, 512) AS CATEGORY_ID_HASH,
        UPPER(ITEM_SFIELD_7) AS BREAK_TYPE_NEW,
        DENSE_RANK() OVER (PARTITION BY A.RECON_ID ORDER BY DATE_1 DESC, A.RECON_LOAD_ID DESC) AS LOAD_RANK
    FROM GFOLYGRU_MANAGED.OM_LEDGER_ITEM_AUDIT_FACT_QRK A
    WHERE
        DWH_BUSINESS_DATE >= 20250701
        AND DWH_BUSINESS_DATE < 20250812
        AND UPPER(INSTANCE_NAME) IN ('RNC1PRD', 'RCN1UAT')
        AND RECON_ID IN ('9959754', '9963291')
        AND UPPER(BREAK_STATUS) IN ('OPEN')
        AND UPPER(ITEM_SFIELD_7) IN ('DTCC', 'AMC')
),

CONTROLS_LEI_DATA AS (
    SELECT
        CONCAT('rhoo_controls_lei_', CAST(DATE_1 AS STRING),
            CONCAT(NVL(ITEM_CURRENCY, ''), NVL(ITEM_CURRENCY_19, ''))) AS 'ckeys:id',
        CONCAT('rhoo_category_', CATEGORY_ID_HASH) AS 'cstate:categoryId',
        RECON_ID AS 'ckeys:reconId',
        RECON_LOAD_ID AS 'ckeys:reconLoadId',
        ITEM_CURRENCY_10 AS 'ckeys:uitid',
        CONCAT(NVL(ITEM_CURRENCY_19, '')) AS 'csrc:asset',
        CONCAT(NVL(ITEM_CURRENCY, '')) AS 'ckeys:positionId',
        RECON_TYPE AS 'csrc:reconType',
        BREAK_STATUS AS 'cstate:breakStatus',
        BREAK_TYPE_NEW AS 'cstate:breakType',
        CASE
            WHEN '##RECON_ID##' IN ('9963291') THEN 'MASKING'
            WHEN '##RECON_ID##' IN ('9959754') THEN 'NON_MASKING'
        END AS 'cstate:reportingType',
        ROW_NUMBER() OVER (
            PARTITION BY CONCAT(NVL(ITEM_CURRENCY, ''), NVL(ITEM_CURRENCY_19, '')), ITEM_SFIELD_7
            ORDER BY REC_INSERT_DATETIME DESC, DWH_CREATE_TIMESTAMP DESC
        ) AS LTO
    FROM CONTROLS_LEI_BASE_DATA
    WHERE
        LOAD_RANK = 1
        AND RECON_ID = '9959754'
        AND CATEGORY_ID_HASH IS NOT NULL
),

CONTROLS_LEI_MASKING_DATA AS (
    SELECT
        CONCAT('rhoo_controls_lei_', CAST(DATE_1 AS STRING),
            CONCAT(NVL(ITEM_CURRENCY_24, ''), NVL(ITEM_CURRENCY_14, ''))) AS 'ckeys:id',
        CONCAT('rhoo_category_', CATEGORY_ID_HASH) AS 'cstate:categoryId',
        RECON_ID AS 'ckeys:reconId',
        RECON_LOAD_ID AS 'ckeys:reconLoadId',
        ITEM_CURRENCY_17 AS 'ckeys:uitid',
        CONCAT(NVL(ITEM_CURRENCY_14, '')) AS 'csrc:asset',
        CONCAT(NVL(ITEM_CURRENCY_24, '')) AS 'ckeys:positionId',
        RECON_TYPE AS 'csrc:reconType',
        BREAK_STATUS AS 'cstate:breakStatus',
        BREAK_TYPE_NEW AS 'cstate:breakType',
        CASE
            WHEN '##RECON_ID##' IN ('9963291') THEN 'MASKING'
            WHEN '##RECON_ID##' IN ('9959754') THEN 'NON_MASKING'
        END AS 'cstate:reportingType',
        ROW_NUMBER() OVER (
            PARTITION BY CONCAT(NVL(ITEM_CURRENCY_24, ''), NVL(ITEM_CURRENCY_14, '')), ITEM_SFIELD_7
            ORDER BY REC_INSERT_DATETIME DESC, DWH_CREATE_TIMESTAMP DESC
        ) AS LTO
    FROM CONTROLS_LEI_BASE_DATA
    WHERE
        LOAD_RANK = 1
        AND RECON_ID = '9963291'
        AND CATEGORY_ID_HASH IS NOT NULL
)

SELECT * FROM CONTROLS_LEI_DATA WHERE LTO = 1
UNION
SELECT * FROM CONTROLS_LEI_MASKING_DATA WHERE LTO = 1
"""

LEI_DB    = "gfolygru_managed"
LEI_TABLE = "om_ledger_item_audit_fact_qrk"


def test_lei_cad_category_id_details_lineage():
    """Test 43: Combined lineage test for APP_REGHUB_RHOO_CONTROLS_LELCAD_CATEGORY_ID_DETAILS.

    Covers base CTE (source table, WHERE, SHA2/UPPER/DENSE_RANK derived columns),
    CONTROLS_LEI_DATA (CONCAT/NVL, CASE, ROW_NUMBER),
    CONTROLS_LEI_MASKING_DATA (masking-specific columns),
    and the final UNION of both CTEs.
    """
    results = lineage(LEI_CAD_SQL)

    assert len(results) >= 10, (
        f"Expected at least 10 lineage rows, got {len(results)}"
    )

    # Base CTE: SELECT * traced correctly
    assert has_column(results, "*", db=LEI_DB, table=LEI_TABLE,
                      has_remark="all_columns_selected")

    # Base CTE: SHA2 derived column
    assert has_column(results, "comments", db=LEI_DB, table=LEI_TABLE,
                      has_remark="derived_expression"), \
        "COMMENTS (inside SHA2) should have derived_expression remark"

    # Base CTE: UPPER / DENSE_RANK columns
    for col in ["item_sfield_7", "recon_id", "recon_load_id", "date_1"]:
        assert has_column(results, col, db=LEI_DB, table=LEI_TABLE), \
            f"Base CTE column '{col}' not found"

    # Base CTE: WHERE clause columns
    for col in ["dwh_business_date", "instance_name", "break_status"]:
        assert has_column(results, col, db=LEI_DB, table=LEI_TABLE,
                          has_remark="where_clause_column"), \
            f"WHERE column '{col}' missing or lacks where_clause_column remark"

    # CONTROLS_LEI_DATA: CONCAT/NVL expression columns
    for col in ["item_currency", "item_currency_19"]:
        assert has_column(results, col, db=LEI_DB, table=LEI_TABLE,
                          has_remark="derived_expression"), \
            f"'{col}' (LEI_DATA CONCAT/NVL) should have derived_expression remark"

    assert has_column(results, "item_currency_10", db=LEI_DB, table=LEI_TABLE)
    assert has_column(results, "recon_type",       db=LEI_DB, table=LEI_TABLE)

    assert has_column(results, "category_id_hash", has_remark="where_clause_column"), \
        "CATEGORY_ID_HASH (WHERE IS NOT NULL) should have where_clause_column remark"

    # CONTROLS_LEI_DATA: ROW_NUMBER ORDER BY columns
    assert has_column(results, "rec_insert_datetime", db=LEI_DB, table=LEI_TABLE)
    assert has_column(results, "dwh_create_timestamp", db=LEI_DB, table=LEI_TABLE)

    # CONTROLS_LEI_MASKING_DATA: masking-specific columns
    for col in ["item_currency_24", "item_currency_14"]:
        assert has_column(results, col, db=LEI_DB, table=LEI_TABLE,
                          has_remark="derived_expression"), \
            f"'{col}' (MASKING CTE) should have derived_expression remark"

    assert has_column(results, "item_currency_17", db=LEI_DB, table=LEI_TABLE)

    # UNION: shared columns present from both CTEs
    assert has_column(results, "break_status",  db=LEI_DB, table=LEI_TABLE)
    assert has_column(results, "break_type_new", db=LEI_DB, table=LEI_TABLE)
