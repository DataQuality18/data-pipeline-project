"""
unittest test suite for extract_columns_from_expression.
Run with:  python -m unittest test_extract_columns_unittest.py -v
"""

import unittest
import logging
import types
import sys
from unittest.mock import MagicMock

# ── Minimal stubs so the module works without sqlglot/sqlalchemy ─────────────

exp_stub = types.ModuleType("exp")


class _Column:
    """Minimal Column node stub."""
    def __init__(self, name=None, table=None):
        self._name  = name
        self._table = table

    @property
    def name(self):
        return self._name

    @property
    def table(self):
        return self._table


exp_stub.Column = _Column
sys.modules.setdefault("exp", exp_stub)

# ── Helpers assumed to exist in the real module ───────────────────────────────

logger = logging.getLogger("test_logger")


def safe_name(name):
    return str(name) if name else "unknown"


# ── Function under test ───────────────────────────────────────────────────────

def extract_columns_from_expression(expr) -> list:
    """Extract all Column nodes from an expression."""
    if expr is None:
        logger.debug("Expression is None - no columns to extract")
        return []

    if not hasattr(expr, "find_all"):
        logger.error(f"Invalid expr type: {type(expr)} | value:{expr}")
        return []

    try:
        columns = list(expr.find_all(exp_stub.Column))
        logger.debug(f"Extracted {len(columns)} column references from expression")

        for col in columns:
            col_name  = safe_name(col.name)  if hasattr(col, "name")  else "unknown"
            col_table = safe_name(col.table) if hasattr(col, "table") and col.table else None
            logger.debug(
                f"Found column: {col_table}.{col_name}" if col_table
                else f"Found column: {col_name}"
            )

        return columns

    except (AttributeError, TypeError) as error:
        logger.warning(f"Invalid expression type:{error}")
        return []

    except Exception as error:
        logger.error(f"unexpected error in column extraction:{error}")
        raise


# ═════════════════════════════════════════════════════════════════════════════
# Test Suite
# ═════════════════════════════════════════════════════════════════════════════

class TestNoneExpression(unittest.TestCase):
    """Scenario 1 — expr is None."""

    def test_returns_empty_list(self):
        result = extract_columns_from_expression(None)
        self.assertEqual(result, [])

    def test_return_type_is_list(self):
        result = extract_columns_from_expression(None)
        self.assertIsInstance(result, list)


class TestInvalidExprType(unittest.TestCase):
    """Scenario 2 — expr has no find_all (invalid types)."""

    def test_string_expr(self):
        result = extract_columns_from_expression("SELECT id FROM users")
        self.assertEqual(result, [])

    def test_integer_expr(self):
        result = extract_columns_from_expression(42)
        self.assertEqual(result, [])

    def test_float_expr(self):
        result = extract_columns_from_expression(3.14)
        self.assertEqual(result, [])

    def test_list_expr(self):
        result = extract_columns_from_expression([1, 2, 3])
        self.assertEqual(result, [])

    def test_dict_expr(self):
        result = extract_columns_from_expression({"col": "id"})
        self.assertEqual(result, [])

    def test_bool_expr(self):
        result = extract_columns_from_expression(True)
        self.assertEqual(result, [])

    def test_return_type_is_list_for_invalid(self):
        result = extract_columns_from_expression("bad")
        self.assertIsInstance(result, list)


class TestNoColumnsFound(unittest.TestCase):
    """Scenario 3 — valid expr but find_all yields nothing."""

    def test_empty_result(self):
        expr = MagicMock()
        expr.find_all.return_value = iter([])
        result = extract_columns_from_expression(expr)
        self.assertEqual(result, [])

    def test_find_all_called_with_column_type(self):
        expr = MagicMock()
        expr.find_all.return_value = iter([])
        extract_columns_from_expression(expr)
        expr.find_all.assert_called_once_with(exp_stub.Column)


class TestSingleColumnNoTable(unittest.TestCase):
    """Scenario 4 — single column, no table qualifier."""

    def setUp(self):
        self.col  = _Column(name="id", table=None)
        self.expr = MagicMock()
        self.expr.find_all.return_value = iter([self.col])

    def test_returns_one_column(self):
        result = extract_columns_from_expression(self.expr)
        self.assertEqual(len(result), 1)

    def test_column_name_is_correct(self):
        result = extract_columns_from_expression(self.expr)
        self.assertEqual(result[0].name, "id")

    def test_column_table_is_none(self):
        result = extract_columns_from_expression(self.expr)
        self.assertIsNone(result[0].table)


class TestSingleColumnWithTable(unittest.TestCase):
    """Scenario 5 — single column with table qualifier."""

    def setUp(self):
        self.col  = _Column(name="email", table="users")
        self.expr = MagicMock()
        self.expr.find_all.return_value = iter([self.col])

    def test_returns_one_column(self):
        result = extract_columns_from_expression(self.expr)
        self.assertEqual(len(result), 1)

    def test_column_name(self):
        result = extract_columns_from_expression(self.expr)
        self.assertEqual(result[0].name, "email")

    def test_column_table(self):
        result = extract_columns_from_expression(self.expr)
        self.assertEqual(result[0].table, "users")


class TestMultipleColumns(unittest.TestCase):
    """Scenario 6 — multiple columns, mixed table qualifiers."""

    def setUp(self):
        self.cols = [
            _Column(name="id",    table="orders"),
            _Column(name="email", table=None),
            _Column(name="total", table="orders"),
        ]
        self.expr = MagicMock()
        self.expr.find_all.return_value = iter(self.cols)

    def test_returns_correct_count(self):
        result = extract_columns_from_expression(self.expr)
        self.assertEqual(len(result), 3)

    def test_first_column_has_table(self):
        result = extract_columns_from_expression(self.expr)
        self.assertEqual(result[0].table, "orders")

    def test_second_column_no_table(self):
        result = extract_columns_from_expression(self.expr)
        self.assertIsNone(result[1].table)

    def test_column_names_preserved(self):
        result = extract_columns_from_expression(self.expr)
        names = [c.name for c in result]
        self.assertEqual(names, ["id", "email", "total"])


class TestColumnMissingNameAttr(unittest.TestCase):
    """Scenario 7 — column has no 'name' attribute at all."""

    def test_column_without_name_attr_still_returned(self):
        """hasattr returns False → 'unknown' fallback used; column is still in result."""
        col = MagicMock(spec=[])          # no attributes
        expr = MagicMock()
        expr.find_all.return_value = iter([col])

        result = extract_columns_from_expression(expr)
        self.assertEqual(len(result), 1)

    def test_multiple_columns_one_missing_name(self):
        good = _Column(name="price")
        bad  = MagicMock(spec=[])
        expr = MagicMock()
        expr.find_all.return_value = iter([good, bad])

        result = extract_columns_from_expression(expr)
        self.assertEqual(len(result), 2)


class TestFindAllRaisesAttributeError(unittest.TestCase):
    """Scenario 8 — find_all raises AttributeError."""

    def test_returns_empty_list(self):
        expr = MagicMock()
        expr.find_all.side_effect = AttributeError("no inner node")
        result = extract_columns_from_expression(expr)
        self.assertEqual(result, [])

    def test_return_type_is_list(self):
        expr = MagicMock()
        expr.find_all.side_effect = AttributeError("no inner node")
        result = extract_columns_from_expression(expr)
        self.assertIsInstance(result, list)


class TestFindAllRaisesTypeError(unittest.TestCase):
    """Scenario 9 — find_all raises TypeError."""

    def test_returns_empty_list(self):
        expr = MagicMock()
        expr.find_all.side_effect = TypeError("unhashable type")
        result = extract_columns_from_expression(expr)
        self.assertEqual(result, [])

    def test_return_type_is_list(self):
        expr = MagicMock()
        expr.find_all.side_effect = TypeError("unhashable type")
        result = extract_columns_from_expression(expr)
        self.assertIsInstance(result, list)


class TestUnexpectedExceptionReRaised(unittest.TestCase):
    """Scenario 10 — unexpected exception must bubble up."""

    def test_runtime_error_is_reraised(self):
        expr = MagicMock()
        expr.find_all.side_effect = RuntimeError("DB connection lost")
        with self.assertRaises(RuntimeError):
            extract_columns_from_expression(expr)

    def test_value_error_is_reraised(self):
        expr = MagicMock()
        expr.find_all.side_effect = ValueError("unexpected value")
        with self.assertRaises(ValueError):
            extract_columns_from_expression(expr)

    def test_exception_message_preserved(self):
        expr = MagicMock()
        expr.find_all.side_effect = RuntimeError("original message")
        with self.assertRaises(RuntimeError) as ctx:
            extract_columns_from_expression(expr)
        self.assertIn("original message", str(ctx.exception))


class TestColumnNamePropertyRaises(unittest.TestCase):
    """Scenario 11 — col.name property raises AttributeError mid-loop."""

    def test_hasattr_catches_raises_uses_fallback(self):
        """
        Python's hasattr() swallows AttributeError → False.
        Function takes the else-branch ('unknown') and still returns the column.
        """
        class BrokenColumn:
            @property
            def name(self):
                raise AttributeError("broken name")
            @property
            def table(self):
                return None

        expr = MagicMock()
        expr.find_all.return_value = iter([BrokenColumn()])
        result = extract_columns_from_expression(expr)
        self.assertEqual(len(result), 1)   # column is still returned


class TestColumnNameIsNone(unittest.TestCase):
    """Scenario 12 — col.name is None."""

    def test_no_crash_when_name_is_none(self):
        col  = _Column(name=None, table=None)
        expr = MagicMock()
        expr.find_all.return_value = iter([col])
        result = extract_columns_from_expression(expr)
        self.assertEqual(len(result), 1)

    def test_column_with_none_name_and_table(self):
        col  = _Column(name=None, table=None)
        expr = MagicMock()
        expr.find_all.return_value = iter([col])
        result = extract_columns_from_expression(expr)
        self.assertIsNone(result[0].name)


class TestManyColumns(unittest.TestCase):
    """Scenario 13 — stress test with large number of columns."""

    def test_thousand_columns(self):
        cols = [_Column(name=f"col_{i}", table="tbl") for i in range(1000)]
        expr = MagicMock()
        expr.find_all.return_value = iter(cols)
        result = extract_columns_from_expression(expr)
        self.assertEqual(len(result), 1000)

    def test_column_names_intact_at_scale(self):
        cols = [_Column(name=f"col_{i}") for i in range(100)]
        expr = MagicMock()
        expr.find_all.return_value = iter(cols)
        result = extract_columns_from_expression(expr)
        self.assertEqual(result[0].name,  "col_0")
        self.assertEqual(result[99].name, "col_99")


class TestLoggingBehaviour(unittest.TestCase):
    """Scenario 14 — verify logger is called at the right points."""

    def test_debug_logged_on_none(self):
        with self.assertLogs("test_logger", level="DEBUG") as cm:
            extract_columns_from_expression(None)
        self.assertTrue(any("None" in msg for msg in cm.output))

    def test_error_logged_on_invalid_type(self):
        with self.assertLogs("test_logger", level="ERROR") as cm:
            extract_columns_from_expression("bad input")
        self.assertTrue(any("Invalid expr type" in msg for msg in cm.output))

    def test_warning_logged_on_attribute_error(self):
        expr = MagicMock()
        expr.find_all.side_effect = AttributeError("oops")
        with self.assertLogs("test_logger", level="WARNING") as cm:
            extract_columns_from_expression(expr)
        self.assertTrue(any("Invalid expression type" in msg for msg in cm.output))

    def test_debug_logged_on_successful_extraction(self):
        col  = _Column(name="id")
        expr = MagicMock()
        expr.find_all.return_value = iter([col])
        with self.assertLogs("test_logger", level="DEBUG") as cm:
            extract_columns_from_expression(expr)
        self.assertTrue(any("Extracted" in msg for msg in cm.output))


class TestReturnType(unittest.TestCase):
    """Scenario 15 — return type is always list."""

    def test_returns_list_on_none(self):
        self.assertIsInstance(extract_columns_from_expression(None), list)

    def test_returns_list_on_invalid(self):
        self.assertIsInstance(extract_columns_from_expression(99), list)

    def test_returns_list_on_attribute_error(self):
        expr = MagicMock()
        expr.find_all.side_effect = AttributeError()
        self.assertIsInstance(extract_columns_from_expression(expr), list)

    def test_returns_list_on_success(self):
        expr = MagicMock()
        expr.find_all.return_value = iter([_Column("x")])
        self.assertIsInstance(extract_columns_from_expression(expr), list)


# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    unittest.main(verbosity=2)
