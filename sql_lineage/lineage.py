"""
Core SQL lineage extraction.
"""
from typing import Dict, List

import sqlglot
from sqlglot import exp

from constants import REMARKS, OUTPUT_KEYS
from utils import ensure_list, extract_columns_from_expression, safe_name
from scope import build_from_scope_map, build_select_scope_map, _pick_base_table_from_subquery
from helpers import (
    extract_select_list,
    _nearest_subquery_alias,
    get_outer_derived_table,
    is_function_only_expression,
)
from emit import emit_dataset_lineage, resolve_star, _emit_column_lineage


def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    """Parse SQL and extract all column/table lineage rows; returns normalized list of dicts."""
    # Parse; fallback to Spark dialect if default parser fails
    try:
        ast = sqlglot.parse_one(sql)
    except Exception:
        ast = sqlglot.parse_one(sql, dialect="spark", error_level="ignore")

    results: List[Dict] = []
    # Emit lineage for outermost SELECT * FROM (subquery) alias
    results.extend(emit_dataset_lineage(ast, regulation, metadatakey, view_name))
    # Global scope: all tables, aliases, subqueries, CTEs
    from_scope = build_from_scope_map(ast)
    selects = list(ast.find_all(exp.Select))

    for select in selects:
        # Per-SELECT local scope (this query’s FROM + JOINs only)
        local_scope = build_select_scope_map(select)
        # Alias of enclosing subquery if this Select is inside one
        enclosing_alias = _nearest_subquery_alias(select)

        # --- 1) SELECT list ---
        for col_text, col_alias, col_node in extract_select_list(select):
            # SELECT *
            if isinstance(col_node, exp.Star):
                results.extend(
                    resolve_star(col_node, local_scope, from_scope, enclosing_alias, regulation, metadatakey, view_name)
                )
                continue

            # Direct column (e.g. a.x or x)
            if isinstance(col_node, exp.Column):
                qualifier = col_node.table
                column_name = col_node.name
                db = ""
                table = ""
                table_alias = ""
                remarks = []

                if qualifier and qualifier in from_scope:
                    _, db, table, table_alias = from_scope[qualifier]
                    remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"])
                elif len(from_scope) == 1:
                    _, db, table, table_alias = next(iter(from_scope.values()))
                    remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"])
                # Single source in this SELECT (e.g. FROM cte_name); resolve via global scope
                elif local_scope and len(local_scope) == 1:
                    key = next(iter(local_scope.keys()))
                    if key in from_scope:
                        _, db, table, table_alias = from_scope[key]
                        remarks.append(REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"])
                    else:
                        base_tables = [
                            v for v in from_scope.values()
                            if not isinstance(v[0], exp.Subquery)
                        ]
                        if len(base_tables) > 1:
                            remarks.append(REMARKS["TABLE_AMBIGUOUS"])
                else:
                    if qualifier:
                        remarks.append(REMARKS["INVALID_TABLE_ALIAS"])
                        table = ""
                        table_alias = qualifier
                        db = ""
                    else:
                        base_tables = [
                            v for v in from_scope.values()
                            if not isinstance(v[0], exp.Subquery)
                        ]
                        if len(base_tables) > 1:
                            remarks.append(REMARKS["TABLE_AMBIGUOUS"])
                # Avoid redundant table_alias when it equals table name
                if table == table_alias:
                    table_alias = ""
                results.append({
                    "Database Name": db or "",
                    "Table Name": table or "",
                    "Table Alias Name": table_alias or "",
                    "Column Name": column_name,
                    "Alias Name": col_alias or "",
                    "Regulation": regulation,
                    "Metadatakey": metadatakey,
                    "View Name": view_name,
                    "Remarks": remarks,
                })
                continue

            # Derived / CASE: expand columns inside the expression
            derived_columns = extract_columns_from_expression(col_node)
            if derived_columns:
                for dcol in derived_columns:
                    qualifier = dcol.table
                    column_name = dcol.name
                    db = ""
                    table = ""
                    table_alias = ""
                    remarks = [REMARKS["DERIVED_EXPR"]]

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
                        # Outermost SELECT with derived table
                        parent = select.parent
                        while parent:
                            if isinstance(parent, exp.Select):
                                break
                            parent = parent.parent
                        else:
                            table, table_alias = get_outer_derived_table(select)

                    if isinstance(col_node, exp.Case):
                        remarks.append(REMARKS["CASE_EXPR"])
                        if table:
                            remarks.append("table name Derived")
                    if table == table_alias:
                        table_alias = ""
                    results.append({
                        "Database Name": db or "",
                        "Table Name": table or "",
                        "Table Alias Name": table_alias or "",
                        "Column Name": column_name,
                        "Alias Name": col_alias or "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": remarks,
                    })
            else:
                # No columns in expression: e.g. pure function (COUNT(*), RAND())
                if is_function_only_expression(col_node):
                    results.append({
                        "Database Name": "",
                        "Table Name": "",
                        "Table Alias Name": "",
                        "Column Name": col_node.sql(),
                        "Alias Name": col_alias or "",
                        "Regulation": regulation,
                        "Metadatakey": metadatakey,
                        "View Name": view_name,
                        "Remarks": [
                            REMARKS["DERIVED_EXPR"],
                            REMARKS.get("FUNCTION_EXPR"),
                        ],
                    })

        # --- 2) WHERE / GROUP BY / HAVING lineage (only if not inside a JOIN’s ON) ---
        def process_clause(expr_node, remark_key):
            if not expr_node:
                return
            # Skip columns that belong to a JOIN ON clause (handled in JOIN section)
            parent = select.parent
            while parent:
                if isinstance(parent, exp.Join):
                    break
                parent = parent.parent
            else:
                for wcol in extract_columns_from_expression(expr_node):
                    _emit_column_lineage(
                        results,
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

        # --- 3) JOIN lineage: ON columns, equality pairs, and subquery WHERE columns ---
        joins = select.args.get("joins") or []
        for j in joins:
            kind = j.args.get("kind") or "INNER"
            join_type_tag = f"{REMARKS['JOIN_TYPE']}={str(kind).upper()}"

            on_expr = j.args.get("on")
            right_node = j.this
            right_alias = None
            if isinstance(right_node, (exp.Table, exp.Subquery, exp.Alias)):
                right_alias = getattr(right_node, "alias_or_name", None)
                if not right_alias and isinstance(right_node, exp.Table):
                    right_alias = safe_name(right_node.this)

            if on_expr:
                # Columns in ON clause
                for c in extract_columns_from_expression(on_expr):
                    _emit_column_lineage(
                        results,
                        qualifier=c.table,
                        column_name=c.name,
                        from_scope=from_scope,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                        fallback_alias=right_alias,
                    )
                # Equality pairs (a.col = b.col) — emit both sides
                for eq in on_expr.find_all(exp.EQ):
                    left = getattr(eq, "this", None)
                    right = getattr(eq, "expression", None)
                    if isinstance(left, exp.Column):
                        _emit_column_lineage(
                            results,
                            qualifier=left.table,
                            column_name=left.name,
                            from_scope=from_scope,
                            regulation=regulation,
                            metadatakey=metadatakey,
                            view_name=view_name,
                            remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                            fallback_alias=right_alias,
                        )
                    if isinstance(right, exp.Column):
                        _emit_column_lineage(
                            results,
                            qualifier=right.table,
                            column_name=right.name,
                            from_scope=from_scope,
                            regulation=regulation,
                            metadatakey=metadatakey,
                            view_name=view_name,
                            remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                            fallback_alias=right_alias,
                        )

            # Subquery on right: also emit columns from its WHERE (e.g. ROW_NUM filter)
            if isinstance(right_node, exp.Subquery):
                sub_select = right_node.this if isinstance(right_node.this, exp.Select) else right_node.find(exp.Select)
                if sub_select:
                    sub_where = sub_select.args.get("where")
                    if sub_where:
                        sub_db, sub_base_table = _pick_base_table_from_subquery(right_node)
                        for c in extract_columns_from_expression(sub_where):
                            _emit_column_lineage(
                                results,
                                qualifier=c.table,
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

    # --- 4) Normalize output: fixed keys, Remarks as list, all values stringified ---
    normalized: List[Dict] = []
    for r in results:
        row = {}
        for k in OUTPUT_KEYS:
            if k == "Remarks":
                row[k] = ensure_list(r.get(k))
            else:
                row[k] = str(r.get(k, "")) if r.get(k) is not None else ""
        normalized.append(row)
    return normalized
