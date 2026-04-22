"""
Core SQL lineage extraction.
"""
import re
from typing import Dict, List

import sqlglot
from sqlglot import exp

from sql_lineage.constants import REMARKS, OUTPUT_KEYS
from sql_lineage.utils import ensure_list, extract_columns_from_expression, safe_name
from sql_lineage.scope import build_from_scope_map, build_select_scope_map, _pick_base_table_from_subquery
from sql_lineage.helpers import (
    extract_select_list,
    _nearest_subquery_alias,
    get_outer_derived_table,
    is_function_only_expression,
)
from sql_lineage.emit import emit_dataset_lineage, resolve_star, _emit_column_lineage


# ---------------------------------------------------------------------------
# Bug Fix 1 — Pre-processing: normalise "TABLE alias1 alias2" double-alias syntax
#
# Some SQL dialects (Hive/Spark) allow omitting AS for aliases, and some
# hand-written SQL contains a spurious second token after the real alias, e.g.:
#
#   FROM GOLDEYRU_MANAGED.OM_LEDGER_ITEM_AUDIT_FACT_QR K A
#
# sqlglot (Spark dialect) picks up "K" as the alias and then interprets the
# lone "A" as the start of a new identifier, which corrupts the entire parse
# tree — all subsequent CTEs and the UNION body are silently dropped.
#
# Fix: rewrite "FROM <table> <alias1> <alias2>" -> "FROM <table> AS <alias1>"
# when BOTH alias1 AND alias2 are non-keyword plain identifiers.
# ---------------------------------------------------------------------------
_SQL_KEYWORDS: frozenset = frozenset({
    'WHERE', 'AND', 'OR', 'ON', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
    'CROSS', 'FULL', 'GROUP', 'ORDER', 'BY', 'HAVING', 'LIMIT', 'UNION',
    'INTERSECT', 'EXCEPT', 'WITH', 'SELECT', 'FROM', 'AS', 'NOT', 'NULL',
    'IN', 'LIKE', 'BETWEEN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'DISTINCT', 'ALL', 'INTO', 'SET', 'VALUES', 'INSERT', 'UPDATE', 'DELETE',
    'CREATE', 'DROP', 'ALTER', 'PARTITION', 'OVER',
})

_DOUBLE_ALIAS_RE = re.compile(
    r'\bFROM\s+((?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*)\s+([A-Za-z_]\w*)\s+([A-Za-z_]\w*)\b',
    re.IGNORECASE,
)


def _fix_double_alias(sql_text: str) -> str:
    """Rewrite  FROM tbl X Y  ->  FROM tbl AS X  when X and Y are both non-keyword tokens.

    This handles the anti-pattern where a table reference has two consecutive
    unquoted alias tokens (e.g. the literal ``K A`` in the source SQL).  The
    first token (X) is kept as the canonical alias; the second (Y) is dropped.
    The rewrite is intentionally narrow: it only fires when *neither* token is
    a SQL keyword, so ``FROM tbl WHERE col`` and ``FROM tbl AS alias`` are left
    untouched.
    """
    def _replacer(m: re.Match) -> str:
        table_ref, alias1, alias2 = m.group(1), m.group(2), m.group(3)
        # "FROM tbl AS alias" — alias1 is the literal word AS; skip
        if alias1.upper() == 'AS':
            return m.group(0)
        # Either token is a keyword (WHERE, JOIN, ON, …) — this is not a double-alias
        if alias1.upper() in _SQL_KEYWORDS or alias2.upper() in _SQL_KEYWORDS:
            return m.group(0)
        # Both tokens are plain identifiers — alias2 is spurious; normalise to AS
        return f'FROM {table_ref} AS {alias1}'

    return _DOUBLE_ALIAS_RE.sub(_replacer, sql_text)

def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    """Parse SQL and extract all column/table lineage rows; returns normalized list of dicts."""
    # Bug Fix 1: normalise spurious double-alias patterns (e.g. "TABLE K A") before parsing
    sql = _fix_double_alias(sql)

    try:
        ast = sqlglot.parse_one(sql, dialect="spark", error_level="ignore")
    except Exception as err:
        print("Testing fallback with dialect='spark' due to parse error:", err)
        ast = None

    try:
        if ast is None:
            ast = sqlglot.parse_one(sql)
    except Exception as err:
        print("Final parse failure:", err)
        return [{
            "databaseName": "",
            "tableName": "",
            "tableAliasName": "",
            "columnName": "",
            "aliasName": "",
            "regulation": regulation,
            "metadatakey": metadatakey,
            "viewName": view_name,
            "remarks": [REMARKS["TECH_FAILURE"]],
        }]

    results: List[Dict] = []
    results.extend(emit_dataset_lineage(ast, regulation, metadatakey, view_name))

    # Build root-level scope ONCE from the full AST so CTE chains resolve correctly.
    # Individual Union sub-nodes (e.g. Union.this) do NOT carry the WITH clause,
    # so calling build_from_scope_map on them loses all CTE-to-base-table mappings.
    root_scope = build_from_scope_map(ast)

    def process_node(node, _root_ast=None):
        """Process UNION nodes recursively, or process SELECT nodes.

        Bug fixes applied here:
          1. CTE body selects were never visited — fixed by iterating with_ CTEs
             before recursing into the UNION arms.
          2. build_from_scope_map was called on Union sub-nodes (bare Select nodes
             that have no WITH clause attached), losing all CTE scope — fixed by
             always using the pre-built root_scope instead of rebuilding per sub-node.
          3. CTE-to-base-table chain resolution (e.g. CONTROLS_LEI_DATA ->
             CONTROLS_LEI_BASE_DATA -> OM_LEDGER_ITEM_AUDIT_FACT_QR) only works
             correctly with the full-AST scope — ensured by using root_scope for
             every call into process_bau.
        """
        if isinstance(node, exp.Union):
            # --- Fix 1: process every CTE body select FIRST ---
            # The WITH node is attached to the top-level Union (ast.args["with_"]).
            # We extract it once here so nested unions also benefit if they ever
            # carry their own CTEs.
            with_node = node.args.get("with_") if hasattr(node, "args") else None
            if with_node and getattr(with_node, "expressions", None):
                for cte in with_node.expressions:
                    cte_body = cte.this  # Select (or nested Union) inside the CTE
                    # Collect all Select nodes inside this CTE body.
                    # Use root_scope (Fix 2 & 3) so CTE-chain resolution works.
                    cte_selects = list(cte_body.find_all(exp.Select))
                    process_bau(cte_selects, root_scope)

            # Recurse into UNION arms — pass root_ast so nested unions can still
            # reach the top-level WITH node if needed.
            process_node(node.this)
            process_node(node.expression)
        else:
            # node is a plain Select (leaf arm of a UNION, or the whole query when
            # there is no UNION).  Always use root_scope (Fix 2 & 3).
            selects_local = list(node.find_all(exp.Select))
            process_bau(selects_local, root_scope)

    def process_bau(selects_in, from_scope):
        """Process SELECT statements: extract lineage from SELECT list, WHERE, GROUP BY, HAVING, and JOINs."""
        for select in selects_in:
            # local scope for this select only (FROM+JOIN)
            local_scope = build_select_scope_map(select)
            enclosing_alias = _nearest_subquery_alias(select)

            # ------------------------------------------------
            # 1) SELECT list
            # ------------------------------------------------
            for col_text, col_alias, col_node in extract_select_list(select):

                # STAR
                if isinstance(col_node, exp.Star):
                    results.extend(
                        resolve_star(
                            col_node,
                            local_scope,
                            from_scope,
                            enclosing_alias,
                            regulation,
                            metadatakey,
                            view_name,
                        )
                    )
                    continue

                # Direct column
                if isinstance(col_node, exp.Column):
                    qualifier = col_node.table
                    column_name = col_node.name
                    db = ""
                    table = ""
                    table_alias = ""
                    remarks = []

                    if qualifier and qualifier in from_scope:
                        _, db, table, table_alias = from_scope[qualifier]
                        remarks.append(
                            REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"]
                        )
                    elif len(from_scope) == 1:
                        _, db, table, table_alias = next(iter(from_scope.values()))
                        remarks.append(
                            REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"]
                        )
                    elif local_scope and len(local_scope) == 1:
                        # Single source in this SELECT (e.g. FROM cte_name); resolve via global scope
                        key = next(iter(local_scope.keys()))
                        if key in from_scope:
                            _, db, table, table_alias = from_scope[key]
                            remarks.append(
                                REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"]
                            )
                        else:
                            base_tables = [
                                v for v in from_scope.values()
                                if not isinstance(v[0], exp.Subquery)
                            ]
                            if len(base_tables) > 1:
                                remarks.append(REMARKS["TABLE_AMBIGUOUS"])
                    else:
                        if qualifier:
                            # Qualified column but alias not found -> NOT ambiguous
                            remarks.append(REMARKS["INVALID_TABLE_ALIAS"])
                            table = ""
                            table_alias = qualifier
                            db = ""
                        else:
                            # Unqualified with multiple base tables -> ambiguous
                            base_tables = [
                                v for v in from_scope.values()
                                if not isinstance(v[0], exp.Subquery)
                            ]
                            if len(base_tables) > 1:
                                remarks.append(REMARKS["TABLE_AMBIGUOUS"])

                    # Emit using helper (will capture invalid alias / derived)
                    if table == table_alias:
                        table_alias = ""
                    if column_name == "*":
                        remarks.append(REMARKS["ALL_COLUMNS"])
                    results.append({
                        "databaseName": str(db or "").lower(),
                        "tableName": str(table or "").lower(),
                        "tableAliasName": str(table_alias or "").lower(),
                        "columnName": str(column_name or "").lower(),
                        "aliasName": str(col_alias or "").lower(),
                        "regulation": regulation,
                        "metadatakey": metadatakey,
                        "viewName": view_name,
                        "remarks": remarks,
                    })
                    continue

                # Derived / CASE expressions
                derived_columns = extract_columns_from_expression(col_node)
                if derived_columns:
                    for dcol in derived_columns:
                        qualifier = dcol.table
                        column_name = dcol.name
                        db = ""
                        table = ""
                        table_alias = ""
                        remarks = [REMARKS["DERIVED_EXPR"]]

                        # Normal qualified resolution
                        if qualifier and qualifier in from_scope:
                            _, db, table, table_alias = from_scope[qualifier]
                        elif len(from_scope) == 1:
                            _, db, table, table_alias = next(iter(from_scope.values()))
                        elif local_scope and len(local_scope) == 1:
                            key = next(iter(local_scope.keys()))
                            if key in from_scope:
                                _, db, table, table_alias = from_scope[key]
                        elif qualifier:
                            remarks.append(REMARKS["INVALID_TABLE_ALIAS"])
                            table = ""
                            table_alias = qualifier
                        else:
                            # NEW: outermost derived SELECT fallback (CASE in outermost select)
                            parent = select.parent
                            while parent:
                                if isinstance(parent, exp.Select):
                                    break
                                parent = parent.parent
                            else:
                                # outermost SELECT
                                table, table_alias = get_outer_derived_table(select)
                                

                        if isinstance(col_node, exp.Case):
                            remarks.append(REMARKS["CASE_EXPR"])
                            if table:
                                remarks.append("table name Derived")

                        if table == table_alias:
                            table_alias = ""

                        results.append({
                            "databaseName": str(db or "").lower(),
                            "tableName": str(table or "").lower(),
                            "tableAliasName": str(table_alias or "").lower(),
                            "columnName": str(column_name or "").lower(),
                            "aliasName": str(col_alias or "").lower(),
                            "regulation": regulation,
                            "metadatakey": metadatakey,
                            "viewName": view_name,
                            "remarks": remarks,
                        })
                else:
                    # Function-only expression
                    if is_function_only_expression(col_node):
                        # Fix: use col_node.sql() instead of undefined col_sql
                        results.append({
                            "databaseName": "",
                            "tableName": "",
                            "tableAliasName": "",
                            "columnName": str(col_node.sql()).lower(),
                            "aliasName": str(col_alias or "").lower(),
                            "regulation": regulation,
                            "metadatakey": metadatakey,
                            "viewName": view_name,
                            "remarks": [
                                REMARKS["DERIVED_EXPR"],
                                REMARKS.get("FUNCTION_EXPR"),
                            ],
                        })

            # ------------------------------------------------
            # 2) WHERE / GROUP BY / HAVING lineage
            #     (skip expressions under JOIN nodes)
            # ------------------------------------------------
            def process_clause(expr_node, remark_key):
                if not expr_node:
                    return

                parent = select.parent
                while parent:
                    if isinstance(parent, exp.Join):
                        break
                    parent = parent.parent
                else:
                    for wcol in extract_columns_from_expression(expr_node):
                        _emit_column_lineage(
                            results=results,
                            qualifier=wcol.table,
                            column_name=wcol.name,
                            from_scope=from_scope,
                            regulation=regulation,
                            metadatakey=metadatakey,
                            view_name=view_name,
                            remark_list=[remark_key],
                            fallback_alias=enclosing_alias,
                            local_scope=local_scope,
                        )

            process_clause(select.args.get("where"), REMARKS["WHERE_COLUMN"])

            if select.args.get("group"):
                for g in select.args["group"].expressions:
                    process_clause(g, REMARKS["GROUP_BY_COLUMN"])

            process_clause(select.args.get("having"), REMARKS["HAVING_COLUMN"])

            # ------------------------------------------------
            # 3) JOIN lineage
            #    (inner select may carry joins even when the outer FROM is a subquery)
            # ------------------------------------------------
            joins = select.args.get("joins") or []
            for j in joins:
                # join type
                kind = j.args.get("kind") or "INNER"
                join_type_tag = f"{REMARKS['JOIN_TYPE']}:{str(kind).upper()}"

                # ON clause and right-side node alias (table or subquery alias)
                on_expr = j.args.get("on")
                right_node = j.this
                right_alias = None

                if isinstance(right_node, (exp.Table, exp.Subquery, exp.Alias)):
                    right_alias = getattr(right_node, "alias_or_name", None)
                    if not right_alias and isinstance(right_node, exp.Table):
                        right_alias = safe_name(right_node.this)

                # 3a) ON columns
                if on_expr:
                    for c in extract_columns_from_expression(on_expr):
                        _emit_column_lineage(
                            results=results,
                            qualifier=c.table,
                            column_name=c.name,
                            from_scope=from_scope,
                            regulation=regulation,
                            metadatakey=metadatakey,
                            view_name=view_name,
                            remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                            fallback_alias=right_alias,  # use JOIN alias if column is unqualified
                        )

                    # 3b) Equality pairs a.col = b.col
                    for eq in on_expr.find_all(exp.EQ):
                        left = getattr(eq, "this", None)
                        right = getattr(eq, "expression", None)

                        left_sql = left.sql() if isinstance(left, exp.Expression) else str(left)
                        right_sql = right.sql() if isinstance(right, exp.Expression) else str(right)

                        # per-side lineage (both sides)
                        if isinstance(left, exp.Column):
                            _emit_column_lineage(
                                results=results,
                                qualifier=left.table,
                                column_name=left.name,
                                from_scope=from_scope,
                                regulation=regulation,
                                metadatakey=metadatakey,
                                view_name=view_name,
                                remark_list=[
                                    REMARKS["JOIN_ON_COLUMN"],
                                    join_type_tag,
                                    # f"{REMARKS['JOIN_EQ_PAIR']}:{left_sql}={right_sql}",
                                ],
                                fallback_alias=right_alias,
                            )
                        if isinstance(right, exp.Column):
                            _emit_column_lineage(
                                results=results,
                                qualifier=right.table,
                                column_name=right.name,
                                from_scope=from_scope,
                                regulation=regulation,
                                metadatakey=metadatakey,
                                view_name=view_name,
                                remark_list=[
                                    REMARKS["JOIN_ON_COLUMN"],
                                    join_type_tag,
                                    # f"{REMARKS['JOIN_EQ_PAIR']}:{left_sql}={right_sql}",
                                ],
                                fallback_alias=right_alias,
                            )

                # 3c) NEW: subquery WHERE inside JOIN (e.g., ROW_NUM)
                if isinstance(right_node, exp.Subquery):
                    # The subquery usually wraps a Select in .this
                    sub_select = right_node.this if isinstance(right_node.this, exp.Select) else right_node.find(exp.Select)
                    if sub_select:
                        sub_where = sub_select.args.get("where")
                        if sub_where:
                            # Resolve base table of the subquery for accurate tableName
                            sub_db, sub_base_table = _pick_base_table_from_subquery(right_node)
                            for c in extract_columns_from_expression(sub_where):
                                _emit_column_lineage(
                                    results=results,
                                    qualifier=c.table,  # may be None
                                    column_name=c.name,
                                    from_scope=from_scope,
                                    regulation=regulation,
                                    metadatakey=metadatakey,
                                    view_name=view_name,
                                    remark_list=[REMARKS["JOIN_SUBQUERY_WHERE_COLUMN"], join_type_tag],
                                    fallback_alias=right_alias,
                                    explicit_table_name=sub_base_table,
                                    explicit_table_alias=right_alias,
                                )

    process_node(ast)

    # 4) Normalize output rows
    normalized: List[Dict] = []
    for r in results:
        row = {}
        for k in OUTPUT_KEYS:
            if k == "remarks":
                row[k] = ensure_list(r.get(k))
            else:
                row[k] = str(r.get(k, "")) if r.get(k) is not None else ""
        normalized.append(row)

    return normalized
