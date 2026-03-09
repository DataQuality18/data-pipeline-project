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
from app.logging_config import get_logger

logger = get_logger(__name__)

# ── Tech-failure sentinel row ─────────────────────────────────────────────────

def _tech_failure_row(regulation: str, metadatakey: str, view_name: str) -> Dict:
    return {
        "databaseName": "",
        "tableName": "",
        "tableAliasName": "",
        "columnName": "",
        "aliasName": "",
        "regulation": regulation,
        "metadatakey": metadatakey,
        "viewName": view_name,
        "remarks": [REMARKS["TECH_FAILURE"]],
    }


# ── Public entry point ────────────────────────────────────────────────────────

def extract_lineage_rows(sql: str, regulation: str, metadatakey: str, view_name: str) -> List[Dict]:
    """
    Parse SQL and extract all column/table lineage rows.
    Returns a normalized list of dicts keyed by OUTPUT_KEYS.

    On total parse failure, returns a single row with TECH_FAILURE remark.
    Never raises.
    """
    if not sql or not sql.strip():
        logger.warning(
            "extract_lineage_rows: empty SQL received",
            regulation=regulation,
            metadatakey=metadatakey,
            view_name=view_name,
        )
        return [_tech_failure_row(regulation, metadatakey, view_name)]

    logger.info(
        "Starting lineage extraction",
        sql_len=len(sql),
        regulation=regulation,
        metadatakey=metadatakey,
        view_name=view_name,
        sql_preview=sql[:120].replace("\n", " "),
    )

    # ── Parse: try Spark dialect first, then default ──────────────────────────
    ast = None
    try:
        ast = sqlglot.parse_one(sql, dialect="spark", error_level="ignore")
        logger.debug("SQL parsed successfully with Spark dialect")
    except Exception as exc:
        logger.warning(
            "Spark-dialect parse failed; will retry with default dialect",
            exc=exc,
            sql_preview=sql[:120].replace("\n", " "),
        )

    if ast is None:
        try:
            ast = sqlglot.parse_one(sql)
            logger.debug("SQL parsed successfully with default dialect")
        except Exception as exc:
            logger.error(
                "All parse attempts failed; returning TECH_FAILURE row",
                exc=exc,
                sql_preview=sql[:120].replace("\n", " "),
            )
            return [_tech_failure_row(regulation, metadatakey, view_name)]

    if ast is None:
        logger.error("AST is None after parsing; returning TECH_FAILURE row")
        return [_tech_failure_row(regulation, metadatakey, view_name)]

    # ── Collect results ───────────────────────────────────────────────────────
    results: List[Dict] = []

    try:
        results.extend(emit_dataset_lineage(ast, regulation, metadatakey, view_name))
    except Exception as exc:
        logger.error("emit_dataset_lineage raised unexpectedly", exc=exc)

    # Global scope for all selects/aliases in the query
    try:
        from_scope = build_from_scope_map(ast)
        logger.debug("Global from-scope built", scope_keys=list(from_scope.keys()))
    except Exception as exc:
        logger.error("build_from_scope_map failed; using empty scope", exc=exc)
        from_scope = {}

    # ── UNION / SELECT processing ─────────────────────────────────────────────

    def process_node(node) -> None:
        """Recursively process UNION nodes; delegate SELECT nodes to process_bau."""
        if node is None:
            logger.debug("process_node: received None node; skipping")
            return
        try:
            if isinstance(node, exp.Union):
                logger.debug("Processing UNION node")
                process_node(node.this)        # left branch
                process_node(node.expression)  # right branch
            else:
                local_from_scope = build_from_scope_map(node)
                selects_local = list(node.find_all(exp.Select))
                logger.debug(
                    "Processing SELECT branch",
                    select_count=len(selects_local),
                )
                process_bau(selects_local, local_from_scope)
        except Exception as exc:
            logger.error("process_node: error processing node", exc=exc, node_type=type(node).__name__)

    def process_bau(selects_in: list, current_from_scope: dict) -> None:
        """
        Process SELECT statements: extract lineage from SELECT list,
        WHERE, GROUP BY, HAVING, and JOINs.
        """
        for select in selects_in:
            try:
                _process_single_select(select, current_from_scope)
            except Exception as exc:
                logger.error(
                    "process_bau: unhandled error processing SELECT; skipping",
                    exc=exc,
                )

    def _process_single_select(select: exp.Select, current_from_scope: dict) -> None:
        """Process one SELECT node: projections, WHERE, GROUP BY, HAVING, JOINs."""
        local_scope = build_select_scope_map(select)
        enclosing_alias = _nearest_subquery_alias(select)

        logger.debug(
            "Processing SELECT",
            enclosing_alias=enclosing_alias,
            local_scope_keys=list(local_scope.keys()),
        )

        # ── 1) SELECT list ────────────────────────────────────────────────────
        for col_text, col_alias, col_node in extract_select_list(select):
            try:
                _process_projection(
                    col_text, col_alias, col_node,
                    select, local_scope, current_from_scope, enclosing_alias,
                )
            except Exception as exc:
                logger.error(
                    "Error processing SELECT projection; skipping",
                    exc=exc,
                    col_text=col_text,
                    col_alias=col_alias,
                )

        # ── 2) WHERE / GROUP BY / HAVING ─────────────────────────────────────
        def process_clause(expr_node, remark_key: str) -> None:
            if not expr_node:
                return
            # Skip expressions that live under a JOIN node
            parent = select.parent
            while parent:
                if isinstance(parent, exp.Join):
                    logger.debug("Skipping clause under JOIN", remark_key=remark_key)
                    return
                parent = parent.parent
            try:
                for wcol in extract_columns_from_expression(expr_node):
                    _emit_column_lineage(
                        results=results,
                        qualifier=wcol.table,
                        column_name=wcol.name,
                        from_scope=current_from_scope,
                        regulation=regulation,
                        metadatakey=metadatakey,
                        view_name=view_name,
                        remark_list=[remark_key],
                        fallback_alias=enclosing_alias,
                        local_scope=local_scope,
                    )
            except Exception as exc:
                logger.error(
                    "process_clause: error emitting clause columns",
                    exc=exc,
                    remark_key=remark_key,
                )

        process_clause(select.args.get("where"), REMARKS["WHERE_COLUMN"])

        try:
            if select.args.get("group"):
                for g in select.args["group"].expressions:
                    process_clause(g, REMARKS["GROUP_BY_COLUMN"])
        except Exception as exc:
            logger.error("Error processing GROUP BY", exc=exc)

        process_clause(select.args.get("having"), REMARKS["HAVING_COLUMN"])

        # ── 3) JOIN lineage ───────────────────────────────────────────────────
        try:
            joins = select.args.get("joins") or []
            for j in joins:
                _process_join(j, select, local_scope, current_from_scope)
        except Exception as exc:
            logger.error("Error processing JOINs", exc=exc)

    def _process_projection(
        col_text, col_alias, col_node,
        select, local_scope, current_from_scope, enclosing_alias,
    ) -> None:
        """Emit lineage for a single SELECT-list projection."""

        # STAR
        if isinstance(col_node, exp.Star):
            rows = resolve_star(
                col_node, local_scope, current_from_scope,
                enclosing_alias, regulation, metadatakey, view_name,
            )
            results.extend(rows)
            logger.debug("STAR projection emitted", row_count=len(rows))
            return

        # Direct column reference
        if isinstance(col_node, exp.Column):
            qualifier = col_node.table
            column_name = col_node.name
            db = table = table_alias = ""
            remarks = []

            if qualifier and qualifier in current_from_scope:
                _, db, table, table_alias = current_from_scope[qualifier]
                remarks.append(
                    REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"]
                )
            elif len(current_from_scope) == 1:
                _, db, table, table_alias = next(iter(current_from_scope.values()))
                remarks.append(
                    REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"]
                )
            elif local_scope and len(local_scope) == 1:
                key = next(iter(local_scope.keys()))
                if key in current_from_scope:
                    _, db, table, table_alias = current_from_scope[key]
                    remarks.append(
                        REMARKS["COLUMN_SELECTED_WITH_DB"] if db else REMARKS["DATABASE_NOT_SPECIFIED"]
                    )
                else:
                    base_tables = [v for v in current_from_scope.values() if not isinstance(v[0], exp.Subquery)]
                    if len(base_tables) > 1:
                        remarks.append(REMARKS["TABLE_AMBIGUOUS"])
                        logger.debug("Ambiguous table for column", column=column_name)
            else:
                if qualifier:
                    remarks.append(REMARKS["INVALID_TABLE_ALIAS"])
                    table = ""
                    table_alias = qualifier
                    db = ""
                    logger.debug("Invalid table alias for direct column", qualifier=qualifier, column=column_name)
                else:
                    base_tables = [v for v in current_from_scope.values() if not isinstance(v[0], exp.Subquery)]
                    if len(base_tables) > 1:
                        remarks.append(REMARKS["TABLE_AMBIGUOUS"])
                        logger.debug("Unqualified column with multiple base tables (ambiguous)", column=column_name)

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
            logger.debug("Direct column lineage emitted", table=table, column=column_name)
            return

        # Derived / CASE expression
        derived_columns = extract_columns_from_expression(col_node)
        if derived_columns:
            for dcol in derived_columns:
                qualifier = dcol.table
                column_name = dcol.name
                db = table = table_alias = ""
                remarks = [REMARKS["DERIVED_EXPR"]]

                if qualifier and qualifier in current_from_scope:
                    _, db, table, table_alias = current_from_scope[qualifier]
                elif len(current_from_scope) == 1:
                    _, db, table, table_alias = next(iter(current_from_scope.values()))
                elif local_scope and len(local_scope) == 1:
                    key = next(iter(local_scope.keys()))
                    if key in current_from_scope:
                        _, db, table, table_alias = current_from_scope[key]
                elif qualifier:
                    remarks.append(REMARKS["INVALID_TABLE_ALIAS"])
                    table = ""
                    table_alias = qualifier
                    logger.debug("Derived expr invalid alias", qualifier=qualifier, column=column_name)
                else:
                    # Outermost derived SELECT fallback
                    parent = select.parent
                    while parent:
                        if isinstance(parent, exp.Select):
                            break
                        parent = parent.parent
                    else:
                        table, table_alias = get_outer_derived_table(select)
                        logger.debug("Derived expr resolved via outer derived table", table=table)

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
            return

        # Function-only expression (no column refs)
        if is_function_only_expression(col_node):
            try:
                col_sql = str(col_node.sql()).lower()
            except Exception as exc:
                logger.warning("Could not get sql() from function node", exc=exc)
                col_sql = ""
            results.append({
                "databaseName": "",
                "tableName": "",
                "tableAliasName": "",
                "columnName": col_sql,
                "aliasName": str(col_alias or "").lower(),
                "regulation": regulation,
                "metadatakey": metadatakey,
                "viewName": view_name,
                "remarks": [
                    REMARKS["DERIVED_EXPR"],
                    REMARKS.get("FUNCTION_EXPR", "function_expression"),
                ],
            })
            logger.debug("Function-only expression emitted", col_sql=col_sql)

    def _process_join(j, select, local_scope, current_from_scope) -> None:
        """Emit lineage rows for a single JOIN clause."""
        try:
            kind = j.args.get("kind") or "INNER"
            join_type_tag = f"{REMARKS['JOIN_TYPE']}:{str(kind).upper()}"

            on_expr = j.args.get("on")
            right_node = j.this
            right_alias = None

            if isinstance(right_node, (exp.Table, exp.Subquery, exp.Alias)):
                right_alias = getattr(right_node, "alias_or_name", None)
                if not right_alias and isinstance(right_node, exp.Table):
                    right_alias = safe_name(right_node.this)

            logger.debug("Processing JOIN", join_type=str(kind), right_alias=right_alias)

            # 3a) ON columns
            if on_expr:
                try:
                    for c in extract_columns_from_expression(on_expr):
                        _emit_column_lineage(
                            results=results,
                            qualifier=c.table,
                            column_name=c.name,
                            from_scope=current_from_scope,
                            regulation=regulation,
                            metadatakey=metadatakey,
                            view_name=view_name,
                            remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                            fallback_alias=right_alias,
                        )
                except Exception as exc:
                    logger.error("Error emitting JOIN ON columns", exc=exc)

                # 3b) Equality pairs a.col = b.col
                try:
                    for eq in on_expr.find_all(exp.EQ):
                        left = getattr(eq, "this", None)
                        right = getattr(eq, "expression", None)

                        if isinstance(left, exp.Column):
                            _emit_column_lineage(
                                results=results,
                                qualifier=left.table,
                                column_name=left.name,
                                from_scope=current_from_scope,
                                regulation=regulation,
                                metadatakey=metadatakey,
                                view_name=view_name,
                                remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                                fallback_alias=right_alias,
                            )
                        if isinstance(right, exp.Column):
                            _emit_column_lineage(
                                results=results,
                                qualifier=right.table,
                                column_name=right.name,
                                from_scope=current_from_scope,
                                regulation=regulation,
                                metadatakey=metadatakey,
                                view_name=view_name,
                                remark_list=[REMARKS["JOIN_ON_COLUMN"], join_type_tag],
                                fallback_alias=right_alias,
                            )
                except Exception as exc:
                    logger.error("Error emitting JOIN equality pairs", exc=exc)

            # 3c) Subquery WHERE inside JOIN (e.g., ROW_NUM filter)
            if isinstance(right_node, exp.Subquery):
                try:
                    sub_select = (
                        right_node.this
                        if isinstance(right_node.this, exp.Select)
                        else right_node.find(exp.Select)
                    )
                    if sub_select:
                        sub_where = sub_select.args.get("where")
                        if sub_where:
                            sub_db, sub_base_table = _pick_base_table_from_subquery(right_node)
                            logger.debug(
                                "Emitting JOIN subquery WHERE columns",
                                sub_base_table=sub_base_table,
                                right_alias=right_alias,
                            )
                            for c in extract_columns_from_expression(sub_where):
                                _emit_column_lineage(
                                    results=results,
                                    qualifier=c.table,
                                    column_name=c.name,
                                    from_scope=current_from_scope,
                                    regulation=regulation,
                                    metadatakey=metadatakey,
                                    view_name=view_name,
                                    remark_list=[REMARKS["JOIN_SUBQUERY_WHERE_COLUMN"], join_type_tag],
                                    fallback_alias=right_alias,
                                    explicit_table_name=sub_base_table,
                                    explicit_table_alias=right_alias,
                                )
                except Exception as exc:
                    logger.error("Error emitting JOIN subquery WHERE lineage", exc=exc)

        except Exception as exc:
            logger.error("_process_join: unhandled error", exc=exc)

    # ── Kick off processing ───────────────────────────────────────────────────
    try:
        process_node(ast)
    except Exception as exc:
        logger.error("process_node raised unexpectedly; partial results may be returned", exc=exc)

    # ── 4) Normalize output rows ──────────────────────────────────────────────
    normalized: List[Dict] = []
    for idx, r in enumerate(results):
        try:
            row = {}
            for k in OUTPUT_KEYS:
                if k == "remarks":
                    row[k] = ensure_list(r.get(k))
                else:
                    row[k] = str(r.get(k, "")) if r.get(k) is not None else ""
            normalized.append(row)
        except Exception as exc:
            logger.error(
                "Normalization error on result row; skipping",
                exc=exc,
                row_index=idx,
                row_preview=str(r)[:200],
            )

    logger.info(
        "Lineage extraction complete",
        raw_row_count=len(results),
        normalized_row_count=len(normalized),
        view_name=view_name,
    )
    return normalized
