"""
FROM-scope mapping and resolution for SQL lineage.
"""
from typing import Dict, Tuple

from sqlglot import exp

from utils import safe_name


def _pick_base_table_from_query(node) -> Tuple[str, str]:
    """
    Resolve the single base table from any query node (Select, Subquery, etc.).
    Returns: (db, table_name). If zero or multiple tables exist, returns ("", "").
    """
    if node is None:
        return "", ""
    # Collect all Table nodes under this node (e.g. FROM / JOINs)
    tables = list(node.find_all(exp.Table))
    uniq = []
    seen = set()
    for t in tables:
        t_name = safe_name(t.this) or ""
        t_db = safe_name(t.db) or ""
        key = (t_db, t_name)
        if t_name and key not in seen:
            seen.add(key)
            uniq.append(key)
    # Only return a single (db, table); ambiguous or empty -> ("", "")
    if len(uniq) == 1:
        return uniq[0][0], uniq[0][1]
    return "", ""


def _pick_base_table_from_subquery(subq: exp.Subquery) -> Tuple[str, str]:
    """Resolve the base table inside a subquery. Delegates to _pick_base_table_from_query."""
    return _pick_base_table_from_query(subq)


def build_from_scope_map(ast_root) -> Dict[str, Tuple]:
    """
    Build global scope: alias_key -> (node, db, table_name, table_alias).
    Covers plain tables, aliased tables, subqueries, and CTEs.
    """
    from_map = {}

    # 1) All physical tables in the query (db.table or table [alias])
    for tbl in ast_root.find_all(exp.Table):
        table_name = safe_name(tbl.this)
        db = safe_name(tbl.db)
        table_alias = tbl.alias_or_name if tbl.alias else None
        key = table_alias or table_name
        if key and key not in from_map:
            from_map[key] = (tbl, db, table_name, table_alias)

    # 2) Explicit aliases (e.g. FROM t1 AS a, or FROM (subq) AS x)
    for alias in ast_root.find_all(exp.Alias):
        key = alias.alias_or_name
        if not key:
            continue
        if isinstance(alias.this, exp.Table):
            tbl = alias.this
            table_name = safe_name(tbl.this)
            db = safe_name(tbl.db)
            from_map[key] = (alias, db, table_name, key)
        elif isinstance(alias.this, exp.Subquery):
            db, base_table = _pick_base_table_from_subquery(alias.this)
            from_map[key] = (alias.this, db, base_table, key)

    # 3) Subquery aliases (e.g. FROM (SELECT ...) subq_alias)
    for subq in ast_root.find_all(exp.Subquery):
        subq_alias = subq.alias_or_name if subq.args.get("alias") else None
        if not subq_alias:
            continue
        db, base_table = _pick_base_table_from_subquery(subq)
        from_map[subq_alias] = (subq, db, base_table, subq_alias)

    # 4) CTEs (WITH cte_name AS (SELECT ... FROM db.table)): map cte_name -> (db, table)
    with_node = ast_root.args.get("with_") if hasattr(ast_root, "args") else None
    if with_node and getattr(with_node, "expressions", None):
        for cte in with_node.expressions:
            if not isinstance(cte, exp.CTE):
                continue
            cte_alias = cte.alias_or_name
            if not cte_alias:
                alias_arg = cte.args.get("alias")
                cte_alias = safe_name(alias_arg) if alias_arg else None
            if not cte_alias:
                continue
            cte_query = cte.this
            db, base_table = _pick_base_table_from_query(cte_query)
            from_map[cte_alias] = (cte_query, db or "", base_table or cte_alias, cte_alias)

    return from_map


def build_select_scope_map(select_exp: exp.Select) -> Dict[str, Tuple]:
    """Build LOCAL scope for one SELECT: only FROM + JOIN sources of this SELECT."""
    from_map: Dict[str, Tuple] = {}

    def _add_source(src):
        if src is None:
            return
        # Alias: e.g. table AS x or (subquery) AS x
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
        # Bare table (with optional alias)
        if isinstance(src, exp.Table):
            table_name = safe_name(src.this)
            db = safe_name(src.db)
            table_alias = src.alias_or_name if src.alias else None
            key = table_alias or table_name
            if key:
                from_map[key] = (src, db, table_name, table_alias)
            return
        # Bare subquery with alias
        if isinstance(src, exp.Subquery):
            subq_alias = src.alias_or_name if src.args.get("alias") else None
            if subq_alias:
                db, base_table = _pick_base_table_from_subquery(src)
                from_map[subq_alias] = (src, db, base_table, subq_alias)
            return

    # FROM clause: sqlglot uses "from_" (and sometimes "from")
    from_clause = select_exp.args.get("from_") or select_exp.args.get("from")
    if from_clause:
        sources = []
        if hasattr(from_clause, "expressions") and from_clause.expressions:
            sources = list(from_clause.expressions)
        elif getattr(from_clause, "this", None) is not None:
            sources = [from_clause.this]
        for src in sources:
            _add_source(src)

    # JOINs: add each joined table/subquery
    for j in (select_exp.args.get("joins") or []):
        _add_source(getattr(j, "this", None))

    return from_map


def _resolve_source(
    qualifier: str, local_scope: Dict[str, Tuple], global_scope: Dict[str, Tuple]
) -> Tuple[str, str, str]:
    """Resolve (db, table, table_alias): qualifier in local then global; else single source if only one."""
    if qualifier:
        if qualifier in local_scope:
            _, db, table, table_alias = local_scope[qualifier]
            return db or "", table or "", table_alias or ""
        if qualifier in global_scope:
            _, db, table, table_alias = global_scope[qualifier]
            return db or "", table or "", table_alias or ""
        return "", "", ""

    if len(local_scope) == 1:
        _, db, table, table_alias = next(iter(local_scope.values()))
        return db or "", table or "", table_alias or ""

    if len(global_scope) == 1:
        _, db, table, table_alias = next(iter(global_scope.values()))
        return db or "", table or "", table_alias or ""

    return "", "", ""


def _attach_enclosing_alias_if_missing(
    db: str,
    table: str,
    table_alias: str,
    enclosing_alias: str,
    global_scope: Dict[str, Tuple],
) -> Tuple[str, str, str]:
    """When inside a subquery/CTE, fill missing table/table_alias from enclosing alias's base table."""
    if enclosing_alias and not table_alias:
        table_alias = enclosing_alias

    if enclosing_alias and not table:
        if enclosing_alias in global_scope:
            _, db2, base_table, _ = global_scope[enclosing_alias]
            if base_table and base_table not in ("__SUBQUERY__", "__DERIVED__"):
                if not db:
                    db = db2 or ""
                table = base_table
            else:
                table = "__DERIVED__"
        else:
            table = "__DERIVED__"

    return db, table, table_alias
