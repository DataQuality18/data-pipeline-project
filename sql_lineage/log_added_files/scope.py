"""
FROM-scope mapping and resolution for SQL lineage.
"""
from typing import Dict, Tuple

from sqlglot import exp

from utils import safe_name
from app.logging_config import get_logger

logger = get_logger("utils")


def _pick_base_table_from_query(node) -> Tuple[str, str]:
    """
    Resolve the single base table from any query node (Select, Subquery, etc.).
    Returns: (db, table_name). If zero or multiple tables exist, returns ("", "").
    Never raises.
    """
    if node is None:
        logger.debug("_pick_base_table_from_query: received None node")
        return "", ""

    try:
        tables = list(node.find_all(exp.Table))
        uniq = []
        seen: set = set()
        for t in tables:
            t_name = safe_name(t.this) or ""
            t_db = safe_name(t.db) or ""
            key = (t_db, t_name)
            if t_name and key not in seen:
                seen.add(key)
                uniq.append(key)

        if len(uniq) == 1:
            db, table = uniq[0]
            logger.debug("Base table resolved", db=db, table=table)
            return db, table

        if len(uniq) > 1:
            logger.debug(
                "Multiple base tables found; cannot resolve single base",
                candidate_count=len(uniq),
                candidates=uniq,
            )
        return "", ""

    except Exception as exc:
        logger.error(
            "_pick_base_table_from_query: error resolving base table",
            exc=exc,
            node_type=type(node).__name__,
        )
        return "", ""


def _pick_base_table_from_subquery(subq: exp.Subquery) -> Tuple[str, str]:
    """
    Resolve the base table inside a subquery.
    Delegates to _pick_base_table_from_query.
    """
    if not isinstance(subq, exp.Subquery):
        logger.warning(
            "_pick_base_table_from_subquery: expected exp.Subquery",
            received_type=type(subq).__name__,
        )
        return "", ""
    return _pick_base_table_from_query(subq)


def build_from_scope_map(ast_root) -> Dict[str, Tuple]:
    """
    Build global scope: alias_key -> (node, db, table_name, table_alias).
    Covers plain tables, aliased tables, subqueries, and CTEs.

    Returns an empty dict on any fatal error (never raises).
    """
    if ast_root is None:
        logger.error("build_from_scope_map: received None AST root")
        return {}

    from_map: Dict[str, Tuple] = {}

    # 1) All plain tables in the query
    try:
        for tbl in ast_root.find_all(exp.Table):
            try:
                table_name = safe_name(tbl.this)
                db = safe_name(tbl.db)
                table_alias = tbl.alias_or_name if tbl.alias else None
                key = table_alias or table_name
                if key and key not in from_map:
                    from_map[key] = (tbl, db, table_name, table_alias)
            except Exception as exc:
                logger.warning("build_from_scope_map: error processing Table node", exc=exc)
    except Exception as exc:
        logger.error("build_from_scope_map: error iterating tables", exc=exc)

    # 2) Aliased expressions
    try:
        for alias in ast_root.find_all(exp.Alias):
            try:
                key = alias.alias_or_name
                if not key:
                    continue

                if isinstance(alias.this, exp.Table):
                    tbl = alias.this
                    from_map[key] = (alias, safe_name(tbl.db), safe_name(tbl.this), key)

                elif isinstance(alias.this, exp.Subquery):
                    db, base_table = _pick_base_table_from_subquery(alias.this)
                    from_map[key] = (alias.this, db, base_table, key)
            except Exception as exc:
                logger.warning("build_from_scope_map: error processing Alias node", exc=exc)
    except Exception as exc:
        logger.error("build_from_scope_map: error iterating aliases", exc=exc)

    # 3) Subquery aliases (JOIN and FROM subqueries)
    try:
        for subq in ast_root.find_all(exp.Subquery):
            try:
                subq_alias = subq.alias_or_name if subq.args.get("alias") else None
                if not subq_alias:
                    continue
                db, base_table = _pick_base_table_from_subquery(subq)
                from_map[subq_alias] = (subq, db, base_table, subq_alias)
            except Exception as exc:
                logger.warning("build_from_scope_map: error processing Subquery alias", exc=exc)
    except Exception as exc:
        logger.error("build_from_scope_map: error iterating subqueries", exc=exc)

    # 4) CTEs  (WITH cte_name AS (SELECT ... FROM db.table))
    try:
        with_node = ast_root.args.get("with_") if hasattr(ast_root, "args") else None
        if with_node and getattr(with_node, "expressions", None):
            for cte in with_node.expressions:
                try:
                    if not isinstance(cte, exp.CTE):
                        continue
                    cte_alias = cte.alias_or_name
                    if not cte_alias:
                        alias_arg = cte.args.get("alias")
                        cte_alias = safe_name(alias_arg) if alias_arg else None
                    if not cte_alias:
                        logger.debug("CTE has no resolvable alias; skipping")
                        continue
                    cte_query = cte.this
                    db, base_table = _pick_base_table_from_query(cte_query)
                    from_map[cte_alias] = (cte_query, db or "", base_table or cte_alias, cte_alias)
                    logger.debug("CTE registered in scope", cte_alias=cte_alias, db=db, base_table=base_table)
                except Exception as exc:
                    logger.warning("build_from_scope_map: error processing CTE", exc=exc)
    except Exception as exc:
        logger.error("build_from_scope_map: error iterating CTEs", exc=exc)

    logger.debug("From-scope map built", entry_count=len(from_map), keys=list(from_map.keys()))
    return from_map


def build_select_scope_map(select_exp: exp.Select) -> Dict[str, Tuple]:
    """
    Build LOCAL scope for one SELECT: only the FROM + JOIN sources of *this* SELECT.
    Returns an empty dict on any fatal error (never raises).
    """
    if not isinstance(select_exp, exp.Select):
        logger.warning(
            "build_select_scope_map: expected exp.Select",
            received_type=type(select_exp).__name__,
        )
        return {}

    from_map: Dict[str, Tuple] = {}

    def _add_source(src) -> None:
        if src is None:
            return
        try:
            if isinstance(src, exp.Alias):
                key = src.alias_or_name
                if isinstance(src.this, exp.Table):
                    tbl = src.this
                    from_map[key] = (src, safe_name(tbl.db), safe_name(tbl.this), key)
                    return
                if isinstance(src.this, exp.Subquery):
                    db, base_table = _pick_base_table_from_subquery(src.this)
                    from_map[key] = (src.this, db, base_table, key)
                    return

            if isinstance(src, exp.Table):
                table_name = safe_name(src.this)
                db = safe_name(src.db)
                table_alias = src.alias_or_name if src.alias else None
                key = table_alias or table_name
                if key:
                    from_map[key] = (src, db, table_name, table_alias)
                return

            if isinstance(src, exp.Subquery):
                subq_alias = src.alias_or_name if src.args.get("alias") else None
                if subq_alias:
                    db, base_table = _pick_base_table_from_subquery(src)
                    from_map[subq_alias] = (src, db, base_table, subq_alias)
                return

        except Exception as exc:
            logger.warning(
                "build_select_scope_map: error adding source",
                exc=exc,
                src_type=type(src).__name__,
            )

    # FROM clause (sqlglot uses "from_" or "from" depending on version)
    try:
        from_clause = select_exp.args.get("from_") or select_exp.args.get("from")
        if from_clause:
            sources = []
            if hasattr(from_clause, "expressions") and from_clause.expressions:
                sources = list(from_clause.expressions)
            elif getattr(from_clause, "this", None) is not None:
                sources = [from_clause.this]

            for src in sources:
                _add_source(src)
    except Exception as exc:
        logger.error("build_select_scope_map: error processing FROM clause", exc=exc)

    # JOINs
    try:
        for j in (select_exp.args.get("joins") or []):
            _add_source(getattr(j, "this", None))
    except Exception as exc:
        logger.error("build_select_scope_map: error processing JOIN sources", exc=exc)

    logger.debug(
        "Local SELECT scope built",
        entry_count=len(from_map),
        keys=list(from_map.keys()),
    )
    return from_map


def _resolve_source(
    qualifier: str,
    local_scope: Dict[str, Tuple],
    global_scope: Dict[str, Tuple],
) -> Tuple[str, str, str]:
    """
    Resolve (db, table, table_alias) using qualifier, falling back to single-source scopes.
    Never raises; returns ("", "", "") when resolution is impossible.
    """
    try:
        if qualifier:
            if qualifier in local_scope:
                _, db, table, table_alias = local_scope[qualifier]
                logger.debug("Qualifier resolved via local scope", qualifier=qualifier, table=table)
                return db or "", table or "", table_alias or ""
            if qualifier in global_scope:
                _, db, table, table_alias = global_scope[qualifier]
                logger.debug("Qualifier resolved via global scope", qualifier=qualifier, table=table)
                return db or "", table or "", table_alias or ""
            logger.debug("Qualifier not found in any scope", qualifier=qualifier)
            return "", "", ""

        if len(local_scope) == 1:
            _, db, table, table_alias = next(iter(local_scope.values()))
            return db or "", table or "", table_alias or ""

        if len(global_scope) == 1:
            _, db, table, table_alias = next(iter(global_scope.values()))
            return db or "", table or "", table_alias or ""

    except Exception as exc:
        logger.error(
            "_resolve_source: unexpected error during resolution",
            exc=exc,
            qualifier=qualifier,
        )

    return "", "", ""


def _attach_enclosing_alias_if_missing(
    db: str,
    table: str,
    table_alias: str,
    enclosing_alias: str,
    global_scope: Dict[str, Tuple],
) -> Tuple[str, str, str]:
    """
    Attach enclosing subquery alias to table_alias when missing.
    If table is also missing, resolves it via global_scope using the enclosing_alias.
    Never raises.
    """
    try:
        if enclosing_alias and not table_alias:
            table_alias = enclosing_alias

        if enclosing_alias and not table:
            if enclosing_alias in global_scope:
                _, db2, base_table, _ = global_scope[enclosing_alias]
                if base_table and base_table not in ("__SUBQUERY__", "__DERIVED__"):
                    if not db:
                        db = db2 or ""
                    table = base_table
                    logger.debug(
                        "Enclosing alias resolved to base table",
                        enclosing_alias=enclosing_alias,
                        resolved_table=table,
                    )
                else:
                    table = "__DERIVED__"
                    logger.debug(
                        "Enclosing alias points to derived/subquery; using __DERIVED__",
                        enclosing_alias=enclosing_alias,
                    )
            else:
                logger.debug(
                    "Enclosing alias not found in global scope; using __DERIVED__",
                    enclosing_alias=enclosing_alias,
                )
                table = "__DERIVED__"

    except Exception as exc:
        logger.error(
            "_attach_enclosing_alias_if_missing: unexpected error",
            exc=exc,
            enclosing_alias=enclosing_alias,
        )

    return db, table, table_alias
