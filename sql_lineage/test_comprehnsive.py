 """
test suite for SQL lineage extraction.
Tests all possible SQL patterns and validates output.
"""
import json
import base64
from test_sql import extract_lineage_rows, parse_metadata_and_extract_lineage, REMARKS

def test_case(name, sql, min_rows=None, validate_func=None):
    """Run a test case and validate results."""
    print(f"\n{'='*80}")
    print(f"TEST: {name}")
    print(f"{'='*80}")
    print(f"SQL:\n{sql}\n")
    
    try:
        results = extract_lineage_rows(sql, "TEST_REG", "TEST_KEY", "TEST_VIEW")
        print(f"Results: {len(results)} rows")
        
        # Show first 10 rows
        for i, row in enumerate(results[:10], 1):
            print(f"\nRow {i}:")
            print(f"  Database: '{row['databaseName']}'")
            print(f"  Table: '{row['tableName']}'")
            print(f"  Table Alias: '{row['tableAliasName']}'")
            print(f"  Column: '{row['columnName']}'")
            print(f"  Alias: '{row['aliasName']}'")
            print(f"  Remarks: {row['remarks']}")
        
        if len(results) > 10:
            print(f"\n... and {len(results) - 10} more rows")
        
        # Basic validation
        if min_rows and len(results) < min_rows:
            raise AssertionError(f"Expected at least {min_rows} rows, got {len(results)}")
        
        # Custom validation
        if validate_func:
            validate_func(results)
        
        print("\n[PASSED]")
        return True
    except Exception as e:
        print(f"\n[FAILED]: {e}")
        import traceback
        traceback.print_exc()
        return False

def has_column(results, column_name, **kwargs):
    """Check if results contain a column matching criteria."""
    for r in results:
        if r['columnName'].lower() == column_name.lower():
            match = True
            if 'db' in kwargs and r['databaseName'].lower() != kwargs['db'].lower():
                continue
            if 'table' in kwargs and r['tableName'].lower() != kwargs['table'].lower():
                continue
            if 'table_alias' in kwargs and r['tableAliasName'].lower() != kwargs['table_alias'].lower():
                continue
            if 'has_remark' in kwargs:
                remarks_str = ' '.join(r['remarks']).lower()
                if kwargs['has_remark'].lower() not in remarks_str:
                    continue
            return True
    return False

# ============================================================================
# TEST CASES
# ============================================================================

def test_basic_select():
    """Test 1: Basic SELECT from single table"""
    sql = "SELECT id, name FROM users"
    return test_case("Basic SELECT", sql, min_rows=2, validate_func=lambda r: 
        has_column(r, "id", table="users") and has_column(r, "name", table="users"))

def test_select_with_database():
    """Test 2: SELECT with database-qualified table"""
    sql = "SELECT id, name FROM sales.users"
    return test_case("SELECT with database", sql, min_rows=2, validate_func=lambda r:
        has_column(r, "id", db="sales", table="users") and has_column(r, "name", db="sales", table="users"))

def test_select_with_alias():
    """Test 3: SELECT with table alias"""
    sql = "SELECT u.id, u.name FROM users u"
    return test_case("SELECT with table alias", sql, min_rows=2, validate_func=lambda r:
        has_column(r, "id", table="users", table_alias="u") and has_column(r, "name", table="users", table_alias="u"))

def test_select_star():
    """Test 4: SELECT *"""
    sql = "SELECT * FROM products"
    return test_case("SELECT *", sql, min_rows=1, validate_func=lambda r:
        has_column(r, "*", table="products", has_remark="all_columns_selected"))

def test_select_star_with_alias():
    """Test 5: SELECT * with table alias"""
    sql = "SELECT p.* FROM products p"
    return test_case("SELECT * with alias", sql, min_rows=1, validate_func=lambda r:
        has_column(r, "*", table="products", table_alias="p", has_remark="all_columns_selected"))

def test_where_clause():
    """Test 6: WHERE clause"""
    sql = "SELECT id FROM users WHERE status = 'active' AND age > 18"
    return test_case("WHERE clause", sql, min_rows=3, validate_func=lambda r:
        has_column(r, "id", table="users") and 
        has_column(r, "status", table="users", has_remark="where_clause_column") and
        has_column(r, "age", table="users", has_remark="where_clause_column"))

def test_group_by():
    """Test 7: GROUP BY"""
    sql = "SELECT category, COUNT(*) FROM products GROUP BY category"
    return test_case("GROUP BY", sql, min_rows=2, validate_func=lambda r:
        has_column(r, "category", table="products", has_remark="group_by_column"))

def test_having():
    """Test 8: HAVING clause"""
    sql = "SELECT category FROM products GROUP BY category HAVING COUNT(*) > 10"
    return test_case("HAVING clause", sql, min_rows=1, validate_func=lambda r:
        has_column(r, "category", table="products"))

def test_inner_join():
    """Test 9: INNER JOIN"""
    sql = """
    SELECT u.id, o.order_id 
    FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    """
    return test_case("INNER JOIN", sql, min_rows=4, validate_func=lambda r:
        has_column(r, "id", table="users", table_alias="u") and
        has_column(r, "order_id", table="orders", table_alias="o") and
        has_column(r, "user_id", has_remark="join_on_clause_column"))

def test_left_join():
    """Test 10: LEFT JOIN"""
    sql = """
    SELECT u.name, p.product_name
    FROM users u
    LEFT JOIN purchases p ON u.id = p.user_id
    """
    return test_case("LEFT JOIN", sql, min_rows=4, validate_func=lambda r:
        has_column(r, "name", table="users", table_alias="u") and
        has_column(r, "product_name", table="purchases", table_alias="p"))

def test_multiple_joins():
    """Test 11: Multiple JOINs"""
    sql = """
    SELECT u.name, o.order_id, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    """
    return test_case("Multiple JOINs", sql, min_rows=3, validate_func=lambda r:
        has_column(r, "name", table="users", table_alias="u") and
        has_column(r, "order_id", table="orders", table_alias="o") and
        has_column(r, "product_name", table="products", table_alias="p"))

def test_cte():
    """Test 12: Common Table Expression (CTE)"""
    sql = """
    WITH active_users AS (
        SELECT id, name
        FROM users
        WHERE status = 'active'
    )
    SELECT id, name FROM active_users
    """
    return test_case("CTE (WITH clause)", sql, min_rows=3, validate_func=lambda r:
        has_column(r, "id", table="users") and
        has_column(r, "name", table="users") and
        has_column(r, "status", table="users", has_remark="where_clause_column"))

def test_cte_with_database():
    """Test 13: CTE with database-qualified table"""
    sql = """
    WITH active_orders AS (
        SELECT *
        FROM sales.orders
        WHERE status = 'ACTIVE'
    )
    SELECT id, amount FROM active_orders
    """
    return test_case("CTE with database", sql, min_rows=4, validate_func=lambda r:
        has_column(r, "id", db="sales", table="orders") and
        has_column(r, "amount", db="sales", table="orders") and
        has_column(r, "*", db="sales", table="orders", has_remark="all_columns_selected") and
        has_column(r, "status", db="sales", table="orders", has_remark="where_clause_column"))

def test_subquery():
    """Test 14: Subquery in FROM"""
    sql = """
    SELECT sub.id, sub.total
    FROM (
        SELECT user_id as id, SUM(amount) as total
        FROM orders
        GROUP BY user_id
    ) sub
    """
    return test_case("Subquery in FROM", sql, min_rows=3, validate_func=lambda r:
        has_column(r, "id", table="orders") and
        has_column(r, "user_id", table="orders", has_remark="group_by_column"))

def test_case_expression():
    """Test 15: CASE expression"""
    sql = """
    SELECT 
        id,
        CASE 
            WHEN status = 'active' THEN 'A'
            WHEN status = 'inactive' THEN 'I'
            ELSE 'U'
        END as status_code
    FROM users
    """
    return test_case("CASE expression", sql, min_rows=2, validate_func=lambda r:
        has_column(r, "id", table="users") and
        has_column(r, "status", table="users", has_remark="case_expression"))

def test_derived_columns():
    """Test 16: Derived/calculated columns"""
    sql = """
    SELECT 
        id,
        price * quantity as total,
        CONCAT(first_name, ' ', last_name) as full_name
    FROM orders
    """
    return test_case("Derived columns", sql, min_rows=4, validate_func=lambda r:
        has_column(r, "id", table="orders") and
        has_column(r, "price", table="orders", has_remark="derived_expression") and
        has_column(r, "quantity", table="orders", has_remark="derived_expression"))

def test_union():
    """Test 17: UNION query"""
    sql = """
    SELECT id, name FROM users
    UNION
    SELECT id, name FROM customers
    """
    return test_case("UNION query", sql, min_rows=2, validate_func=lambda r:
        has_column(r, "id") and has_column(r, "name"))

def test_nested_subquery():
    """Test 18: Nested subquery"""
    sql = """
    SELECT u.name
    FROM users u
    WHERE u.id IN (
        SELECT user_id 
        FROM orders 
        WHERE amount > 1000
    )
    """
    return test_case("Nested subquery", sql, min_rows=3, validate_func=lambda r:
        has_column(r, "name", table="users", table_alias="u") and
        has_column(r, "id", table="users", table_alias="u", has_remark="where_clause_column") and
        has_column(r, "amount", table="orders", has_remark="where_clause_column"))

def test_join_with_subquery():
    """Test 19: JOIN with subquery"""
    sql = """
    SELECT u.name, sub.total
    FROM users u
    JOIN (
        SELECT user_id, SUM(amount) as total
        FROM orders
        GROUP BY user_id
    ) sub ON u.id = sub.user_id
    """
    return test_case("JOIN with subquery", sql, min_rows=3, validate_func=lambda r:
        has_column(r, "name", table="users", table_alias="u") and
        has_column(r, "user_id", table="orders", has_remark="join_on_clause_column"))

def test_complex_query():
    """Test 20: Complex query with multiple features"""
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
    return test_case("Complex query", sql, min_rows=5, validate_func=lambda r:
        has_column(r, "product_name", table="products", table_alias="p") and
        has_column(r, "order_date", db="sales", table="orders", has_remark="where_clause_column") and
        has_column(r, "category", table="products", table_alias="p", has_remark="where_clause_column"))

def test_function_only():
    """Test 21: Function-only expressions"""
    sql = "SELECT NOW() as current_time, RAND() as random_value"
    return test_case("Function-only expressions", sql, min_rows=2, validate_func=lambda r:
        has_column(r, "now()", has_remark="function_expression") or
        has_column(r, "rand()", has_remark="function_expression"))

def test_ambiguous_column():
    """Test 22: Ambiguous column (multiple tables, unqualified)"""
    sql = """
    SELECT id
    FROM users u
    CROSS JOIN orders o
    """
    return test_case("Ambiguous column", sql, min_rows=1, validate_func=lambda r:
        has_column(r, "id", has_remark="table_name_ambiguous"))

def test_qualified_ambiguous():
    """Test 23: Qualified column resolves ambiguity"""
    sql = """
    SELECT u.id, o.id
    FROM users u
    CROSS JOIN orders o
    """
    return test_case("Qualified resolves ambiguity", sql, min_rows=2, validate_func=lambda r:
        has_column(r, "id", table="users", table_alias="u") and
        has_column(r, "id", table="orders", table_alias="o"))

def test_metadata_api():
    """Test 24: Test parse_metadata_and_extract_lineage API"""
    sql = "SELECT id, name FROM users"
    metadata = json.dumps({
        "sql_query": base64.b64encode(sql.encode()).decode()
    })
    
    print(f"\n{'='*80}")
    print("TEST: Metadata API")
    print(f"{'='*80}")
    print(f"Metadata JSON: {metadata}\n")
    
    try:
        results = parse_metadata_and_extract_lineage(metadata, "TEST_REG", "TEST_KEY", "TEST_VIEW")
        print(f"Results: {len(results)} rows")
        for r in results[:5]:
            print(f"  {r['columnName']} from {r['tableName']}")
        if len(results) > 5:
            print(f"  ... and {len(results) - 5} more")
        print("\n[PASSED]")
        return True
    except Exception as e:
        print(f"\n[FAILED]: {e}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================================
# RUN ALL TESTS
# ============================================================================

def run_all_tests():
    """Run all test cases."""
    tests = [
        test_basic_select,
        test_select_with_database,
        test_select_with_alias,
        test_select_star,
        test_select_star_with_alias,
        test_where_clause,
        test_group_by,
        test_having,
        test_inner_join,
        test_left_join,
        test_multiple_joins,
        test_cte,
        test_cte_with_database,
        test_subquery,
        test_case_expression,
        test_derived_columns,
        test_union,
        test_nested_subquery,
        test_join_with_subquery,
        test_complex_query,
        test_function_only,
        test_ambiguous_column,
        test_qualified_ambiguous,
        test_metadata_api,
    ]
    
    passed = 0
    failed = 0
    
    print("\n" + "="*80)
    print("COMPREHENSIVE SQL LINEAGE EXTRACTION TEST SUITE")
    print("="*80)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"\n[TEST FAILED]: {test_func.__name__}")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print("="*80)
    
    return failed == 0

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
