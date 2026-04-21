import pytest
from lineage import extract_lineage_rows


# ----------------------------------------
# Helper Functions
# ----------------------------------------

def has_column(results, column_name, **kwargs):
    for r in results:
        if r['columnName'].lower() != column_name.lower():
            continue

        if 'db' in kwargs and r['databaseName'].lower() != kwargs['db'].lower():
            continue

        if 'table' in kwargs and r['tableName'].lower() != kwargs['table'].lower():
            continue

        if 'table_alias' in kwargs and r.get('tableAliasName', '').lower() != kwargs['table_alias'].lower():
            continue

        if 'has_remark' in kwargs:
            remarks_str = ' '.join(r.get('remarks', [])).lower()
            if kwargs['has_remark'].lower() not in remarks_str:
                continue

        return True

    return False


def assert_forbidden_remark_absent(results, forbidden_remark):
    for r in results:
        remarks = ' '.join(r.get('remarks', [])).lower()
        assert forbidden_remark.lower() not in remarks, \
            f"Forbidden remark '{forbidden_remark}' found in {r}"


def extract_results(sql):
    return extract_lineage_rows(sql, "TEST_REG", "TEST_KEY", "TEST_VIEW")


# ----------------------------------------
# TEST CASES (24 TOTAL)
# ----------------------------------------

test_cases = [

    ("simple_select",
     "SELECT id FROM customers",
     1,
     lambda r: has_column(r, "id"),
     "where"),

    ("multiple_columns",
     "SELECT id, name FROM customers",
     2,
     lambda r: has_column(r, "id") and has_column(r, "name"),
     "join"),

    ("table_alias",
     "SELECT c.id FROM customers c",
     1,
     lambda r: has_column(r, "id", table_alias="c"),
     None),

    ("join_query",
     """
     SELECT c.id, o.order_id
     FROM customers c
     JOIN orders o ON c.id = o.customer_id
     """,
     2,
     lambda r: has_column(r, "id") and has_column(r, "order_id"),
     None),

    ("left_join",
     """
     SELECT c.id, o.order_id
     FROM customers c
     LEFT JOIN orders o ON c.id = o.customer_id
     """,
     2,
     lambda r: has_column(r, "order_id"),
     None),

    ("where_clause",
     "SELECT id FROM customers WHERE age > 30",
     2,
     lambda r: has_column(r, "age", has_remark="where"),
     None),

    ("case_expression",
     """
     SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END
     FROM users
     """,
     1,
     lambda r: has_column(r, "age"),
     None),

    ("aggregation",
     "SELECT COUNT(id) FROM customers",
     1,
     lambda r: has_column(r, "id"),
     None),

    ("group_by",
     "SELECT department, COUNT(*) FROM employees GROUP BY department",
     1,
     lambda r: has_column(r, "department"),
     None),

    ("having",
     """
     SELECT department, COUNT(*) 
     FROM employees 
     GROUP BY department 
     HAVING COUNT(*) > 5
     """,
     1,
     lambda r: has_column(r, "department"),
     None),

    ("order_by",
     "SELECT id FROM customers ORDER BY name",
     2,
     lambda r: has_column(r, "name"),
     None),

    ("subquery",
     """
     SELECT id FROM (
        SELECT id FROM customers
     ) t
     """,
     1,
     lambda r: has_column(r, "id"),
     None),

    ("nested_subquery",
     """
     SELECT id FROM (
        SELECT id FROM (
            SELECT id FROM customers
        ) x
     ) y
     """,
     1,
     lambda r: has_column(r, "id"),
     None),

    ("cte",
     """
     WITH temp AS (
         SELECT id FROM customers
     )
     SELECT id FROM temp
     """,
     1,
     lambda r: has_column(r, "id"),
     None),

    ("cte_join",
     """
     WITH temp AS (
         SELECT id FROM customers
     )
     SELECT t.id, o.order_id
     FROM temp t
     JOIN orders o ON t.id = o.customer_id
     """,
     2,
     lambda r: has_column(r, "order_id"),
     None),

    ("column_alias",
     "SELECT id AS customer_id FROM customers",
     1,
     lambda r: has_column(r, "id"),
     None),

    ("function_usage",
     "SELECT UPPER(name) FROM customers",
     1,
     lambda r: has_column(r, "name"),
     None),

    ("distinct",
     "SELECT DISTINCT name FROM customers",
     1,
     lambda r: has_column(r, "name"),
     None),

    ("union",
     """
     SELECT id FROM customers
     UNION
     SELECT id FROM orders
     """,
     1,
     lambda r: has_column(r, "id"),
     None),

    ("insert_select",
     """
     INSERT INTO target_table
     SELECT id FROM customers
     """,
     1,
     lambda r: has_column(r, "id"),
     None),

    ("update_query",
     "UPDATE customers SET name = 'abc' WHERE id = 1",
     2,
     lambda r: has_column(r, "name") and has_column(r, "id"),
     None),

    ("delete_query",
     "DELETE FROM customers WHERE id = 1",
     1,
     lambda r: has_column(r, "id"),
     None),

    ("no_where_remark",
     "SELECT id FROM customers",
     1,
     lambda r: has_column(r, "id"),
     "where"),

    ("invalid_sql",
     "SELECT FROM",
     None,
     None,
     None),
]


# ----------------------------------------
# MAIN TEST FUNCTION
# ----------------------------------------

@pytest.mark.parametrize("name, sql, expected_count, validate_func, forbidden_remark", test_cases)
def test_sql_lineage(name, sql, expected_count, validate_func, forbidden_remark):

    if name == "invalid_sql":
        with pytest.raises(Exception):
            extract_results(sql)
        return

    results = extract_results(sql)

    # ✅ Exact row count validation
    if expected_count is not None:
        assert len(results) == expected_count, \
            f"{name}: Expected {expected_count}, got {len(results)}"

    # ✅ Logical validation
    if validate_func:
        assert validate_func(results), \
            f"{name}: Validation failed"

    # ✅ Forbidden remarks validation
    if forbidden_remark:
        assert_forbidden_remark_absent(results, forbidden_remark)