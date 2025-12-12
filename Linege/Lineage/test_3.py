# ============================================================
#                LINEAGE ENGINE (SQL + MONGO)
# ============================================================
import json
import sqlglot
from sqlglot import exp
from typing import Any, Dict, List, Optional, Set
from test_sql import parse_metadata_and_extract_lineage
import base64
# ============================================================
#                COMMON RECORD FORMAT
# ============================================================

def _make_row(db, table, column, alias, regulation, metadatakey, view_name, remarks):
    return {
        "Database Name": db or "",
        "Table Name": table or "",
        "Column Name": column or "",
        "Alias Name": alias or "",
        "Regulation": regulation or "",
        "Metadatakey": metadatakey or "",
        "View Name": view_name or "",
        "Remarks": remarks or "",
    }


def _safe(node):
    if node is None:
        return ""
    if hasattr(node, "name"):
        return node.name
    try:
        return node.sql()
    except:
        return str(node)


def _get_alias_name(node):
    try:
        if hasattr(node, "alias_or_name") and node.alias_or_name:
            return node.alias_or_name
        alias_expr = node.args.get("alias")
        if alias_expr:
            return _safe(alias_expr)
    except:
        pass
    return ""


# ============================================================
#                  SQL: UNWRAP TOP-LEVEL SELECT
# ============================================================

def unwrap_select(node):
    if isinstance(node, exp.Select):
        return node
    if isinstance(node, exp.Subquery):
        if isinstance(node.this, exp.Select):
            return node.this
        return unwrap_select(node.this)
    if isinstance(node, exp.Paren):
        return unwrap_select(node.this)
    return node.find(exp.Select)


# ============================================================
#      SQL: REGISTER TABLES, SUBQUERIES, CTE SOURCES
# ============================================================

def _register_sources_recursive(node: exp.Expression, alias_map: dict):
    # CTEs
    for cte in node.find_all(exp.CTE):
        alias = getattr(cte, "alias_or_name", None) or _safe(cte.args.get("alias"))
        inner = cte.this
        inner_sel = inner.this if isinstance(inner, exp.Subquery) else inner
        if not isinstance(inner_sel, exp.Select):
            inner_sel = inner_sel.find(exp.Select)
        alias_map[alias] = {"type": "cte", "select": inner_sel, "database": "", "schema": "", "table": ""}

    # Tables
    for t in node.find_all(exp.Table):
        alias = getattr(t, "alias_or_name", None)
        if not alias:
            alias = _safe(t.args.get("alias")) or _safe(t.this)
        catalog = t.args.get("catalog")
        db_expr = t.args.get("db")
        database_name = _safe(catalog) if catalog else (_safe(db_expr) if db_expr else "")
        schema_name = _safe(db_expr) if db_expr else ""
        alias_map[alias] = {
            "type": "table",
            "database": database_name,
            "schema": schema_name,
            "table": _safe(t.this),
            "select": None
        }
    # Subqueries in FROM/JOIN
    for sub in node.find_all(exp.Subquery):
        alias = getattr(sub, "alias_or_name", None)
        if not alias:
            alias = _safe(sub.args.get("alias"))
        inner = sub.this
        inner_sel = inner if isinstance(inner, exp.Select) else inner.find(exp.Select)
        if alias:
            alias_map[alias] = {"type": "subquery", "database": "", "schema": "", "table": "", "select": inner_sel}


# ============================================================
#           SQL: PROJECTIONS (WITH RECURSION)
# ============================================================
# -------------------------
# MAP() extraction helper
# -------------------------
def _extract_map_pairs(map_expr: exp.Expression, alias_map: Dict[str, Dict[str, Any]],
                       global_ctes: Dict[str, exp.Select], regulation: str, metadatakey: str, view_name: str) -> List[Dict[str,str]]:
    """
    Given an exp.Map expression (MAP(key1, val1, key2, val2, ...)),
    return lineage rows for pairs where value is a Column.
    alias_map: current select-level mapping of aliases -> source info.
    """
    
    rows = []
    args = getattr(map_expr, "expressions", []) or []
    # iterate in pairs
    remarks = "spark complex SQL - map function"
    for i in range(0, len(args), 2):
        key_node = args[i]
        val_node = args[i+1] if i+1 < len(args) else None

        # extract key name: can be a Literal or Identifier
        key_name = ""
        if key_node is None:
            key_name = ""
        elif isinstance(key_node, exp.Literal):
            # remove surrounding quotes if present
            key_name = str(key_node.this).strip("'\"")
        else:
            key_name = _safe(key_node)

        if val_node is None:
            # no value for key — skip or emit remark
            rows.append(_make_row("", "", "", key_name, regulation, metadatakey, view_name, remarks))
            continue

        # If value is a column, resolve it
        if isinstance(val_node, exp.Column):
            col_name = val_node.name
            qualifier = val_node.table or ""
            # resolve qualifier to alias_map
            if qualifier and qualifier in alias_map:
                src = alias_map[qualifier]
                if src["type"] == "table":
                    rows.append(_make_row(src["database"], src["table"], col_name, key_name, regulation, metadatakey, view_name, remarks))
                else:
                    # qualifier points to subquery/cte: mark inner alias layer and attempt to dive into subquery if present
                    rows.append(_make_row("", "", col_name, key_name, regulation, metadatakey, view_name, remarks))
                    if src.get("select"):
                        # try to resolve inner mapping to physical table(s)
                        child_alias_map = _register_sources_for_select(src["select"], global_ctes)
                        # find if any child projection contains this column as a column -> map to its physical table
                        for child_proj in src["select"].expressions or []:
                            for c2 in child_proj.find_all(exp.Column):
                                if c2.name == col_name:
                                    c2_qual = c2.table or ""
                                    if c2_qual and c2_qual in child_alias_map:
                                        c_src = child_alias_map[c2_qual]
                                        if c_src["type"] == "table":
                                            rows.append(_make_row(c_src["database"], c_src["table"], c2.name, key_name, regulation, metadatakey, view_name, remarks))
            else:
                # qualifier absent or unknown; if no qualifier but exactly one physical table in scope, attribute to that
                phys = [v for v in alias_map.values() if v["type"] == "table"]
                if not qualifier and len(phys) == 1:
                    p = phys[0]
                    rows.append(_make_row(p["database"], p["table"], col_name, key_name, regulation, metadatakey, view_name, remarks))
                else:
                    # unknown mapping
                    rows.append(_make_row("", qualifier or "", col_name, key_name, regulation, metadatakey, view_name, remarks))
        else:
            # value is expression (not plain column) - attempt to find nested columns inside expression
            nested_cols = list(val_node.find_all(exp.Column))
            if nested_cols:
                for nc in nested_cols:
                    nc_name = nc.name
                    nc_qual = nc.table or ""
                    if nc_qual and nc_qual in alias_map:
                        src = alias_map[nc_qual]
                        if src["type"] == "table":
                            rows.append(_make_row(src["database"], src["table"], nc_name, key_name, regulation, metadatakey, view_name, remarks))
                        else:
                            rows.append(_make_row("", "", nc_name, key_name, regulation, metadatakey, view_name, remarks))
                    else:
                        rows.append(_make_row("", nc_qual or "", nc_name, key_name, regulation, metadatakey, view_name, remarks))
            else:
                # no columns referenced, treat as computed literal
                rows.append(_make_row("", "", "", key_name, regulation, metadatakey, view_name, remarks))
    print("row:", rows)
    return rows


def _extract_lineage_from_select(select: exp.Select, regulation: str, metadatakey: str,
                                 view_name: str, parent_alias: Optional[str], seen: Set):

    rows = []
    alias_map = {}
    _register_sources_recursive(select, alias_map)

    physical_tables = [v for v in alias_map.values() if v["type"] == "table"]

    for proj in select.expressions or []:
        # MAP() handling
        if isinstance(proj, exp.Map):
            # generate lines for each map key/value
            map_rows = _extract_map_pairs(proj, alias_map, global_ctes, regulation, metadatakey, view_name)
            # tag alias if projection has alias
            proj_alias = _get_proj_alias(proj)
            # If rows were emitted, prefer to set Alias Name to projection alias (CASHFLOW_MAP)
            for r in map_rows:
                if proj_alias:
                    r["Alias Name"] = proj_alias
                rows.append(r)
            continue
        # ★ STAR handling
        if isinstance(proj, exp.Star):
            qual = _safe(proj.args.get("this"))
            alias_label = parent_alias or (qual + ".*" if qual else "*")

            # qualified star (alias.*)
            if qual:
                entry = alias_map.get(qual)
                if entry:
                    if entry["type"] == "table":
                        key = (entry["database"], entry["table"], "*", alias_label, "all_columns_selected")
                        if key not in seen:
                            seen.add(key)
                            rows.append(_make_row(entry["database"], entry["table"], "*", alias_label,
                                                  regulation, metadatakey, view_name, "all_columns_selected"))
                    else:
                        # subquery or CTE
                        key1 = ("*", qual, "*", alias_label, "all_columns_selected")
                        key2 = ("*", qual, "*", alias_label, "Inner Query Alias Layer")
                        if key1 not in seen:
                            seen.add(key1)
                            rows.append(_make_row("", qual, "*", alias_label, regulation, metadatakey, view_name,
                                                  "all_columns_selected"))
                        if key2 not in seen:
                            seen.add(key2)
                            rows.append(_make_row("", "", "*", alias_label, regulation, metadatakey, view_name,
                                                  "Inner Query Alias Layer"))

                        if entry["select"]:
                            rows.extend(_extract_lineage_from_select(entry["select"],
                                                                     regulation, metadatakey, view_name,
                                                                     alias_label, seen))
                continue
            
            # unqualified *
            for name, entry in alias_map.items():
                if entry["type"] == "table":
                    alias_label2 = parent_alias or f"{name}.*"
                    key = (entry["database"], entry["table"], "*", alias_label2, "all_columns_selected")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row(entry["database"], entry["table"], "*", alias_label2,
                                              regulation, metadatakey, view_name, "all_columns_selected"))
                elif entry["type"] in ("subquery", "cte"):
                    alias_label2 = parent_alias or f"{name}.*"
                    # emit layer rows
                    k1 = ("*", name, "*", alias_label2, "all_columns_selected")
                    k2 = ("*", name, "*", alias_label2, "Inner Query Alias Layer")
                    if k1 not in seen:
                        seen.add(k1)
                        rows.append(_make_row("", name, "*", alias_label2, regulation, metadatakey, view_name,
                                              "all_columns_selected"))
                    if k2 not in seen:
                        seen.add(k2)
                        rows.append(_make_row("", "", "*", alias_label2, regulation, metadatakey, view_name,
                                              "Inner Query Alias Layer"))
                    if entry["select"]:
                        rows.extend(_extract_lineage_from_select(entry["select"],
                                                                 regulation, metadatakey, view_name,
                                                                 alias_label2, seen))
            
            continue
        
        # Non-star columns
        for col in proj.find_all(exp.Column):
            col_name = col.name
            qual = col.table or ""
            alias_name = _get_alias_name(proj) or ""

            if qual:
                entry = alias_map.get(qual)
                if entry and entry["type"] == "table":
                    key = (entry["database"], entry["table"], col_name, alias_name, "")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row(entry["database"], entry["table"],
                                              col_name, alias_name, regulation, metadatakey, view_name, ""))
                elif entry and entry["type"] in ("subquery", "cte"):
                    key = ("", "", col_name, alias_name, "Inner Query Alias Layer")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row("", "", col_name, alias_name,
                                              regulation, metadatakey, view_name, "Inner Query Alias Layer"))
                else:
                    key = ("", qual, col_name, alias_name, "database_not_specified_in_query")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row("", qual, col_name, alias_name,
                                              regulation, metadatakey, view_name,
                                              "database_not_specified_in_query"))
            else:
                # no qualifier
                if len(physical_tables) == 1:
                    entry = physical_tables[0]
                    key = (entry["database"], entry["table"], col_name, alias_name, "")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row(entry["database"], entry["table"],
                                              col_name, alias_name, regulation, metadatakey, view_name, ""))
                else:
                    key = ("", "", col_name, alias_name, "database_not_specified_in_query")
                    if key not in seen:
                        seen.add(key)
                        rows.append(_make_row("", "", col_name, alias_name,
                                              regulation, metadatakey, view_name, "database_not_specified_in_query"))

    return rows
import re
from typing import Optional, List, Dict

# Heuristics for Spark / PySpark SQL
_SPARK_INDICATORS = [
    r"\bpyspark\b",
    r"\bspark\.sql\s*\(",
    r"\bMAP\s*\(",
    r"\bEXPLODE\s*\(",
    r"\bLATERAL\s+VIEW\b",
    r"\bTRANSFORM\s*\(",
    r"\bSTRUCT\s*\(",
    r"`[^`]+`",               # backtick identifiers (Hive/Spark style)
    r"\bARRAY\(",             # ARRAY(...) common in Spark
    r"\bMAP\(",               # explicit MAP function
]

_SPARK_RE = re.compile("|".join(f"({p})" for p in _SPARK_INDICATORS), re.I)


def _pick_sql_dialect(source_text: Optional[str], explicit_dialect: Optional[str]) -> Optional[str]:
    """
    Decide which dialect to use.
    - If an explicit_dialect is provided, return it (respect caller).
    - If heuristics detect Spark-like constructs in source_text, return "spark".
    - Otherwise return explicit_dialect (which may be None) so sqlglot can decide.
    """
    if explicit_dialect:
        return explicit_dialect

    if not source_text:
        return None

    if _SPARK_RE.search(source_text):
        return "spark"

    return None

def _generate_lineage_sql(sql: str, regulation: str, metadatakey: str,
                           view_name: str, dialect: Optional[str]) -> List[Dict[str, str]]:
    chosen_dialect = _pick_sql_dialect(sql, dialect)
    if chosen_dialect != 'spark':
        metadata = {"sql_query": base64.b64encode(sql.encode()).decode()}
        rows = parse_metadata_and_extract_lineage(json.dumps(metadata), regulation, metadatakey, view_name)
        return rows
    try:
        tree = sqlglot.parse_one(sql, read=chosen_dialect)
    except:
        return []
    select = unwrap_select(tree)
    if not select:
        return []
    seen = set()
    return _extract_lineage_from_select(select, regulation, metadatakey, view_name, None, seen)


# ============================================================
#                      MONGO LINEAGE
# ============================================================

def _mongo_row(db, coll, col, alias, regulation, metadatakey, view_name, remarks):
    return _make_row(db, coll, col, alias, regulation, metadatakey, view_name, remarks)


def parse_mongo_find(collection, flt, proj, regulation, metadatakey, view_name):
    rows = []
    if not proj:
        rows.append(_mongo_row("", collection, "*", "", regulation, metadatakey, view_name, "Mongo query"))
        return rows
    for k, v in proj.items():
        if isinstance(v, int) and v == 1:
            rows.append(_mongo_row("", collection, k, "", regulation, metadatakey, view_name, "Mongo Query"))
        else:
            rows.append(_mongo_row("", collection, k, k, regulation, metadatakey, view_name, "Mongo Query"))
    return rows


def parse_mongo_aggregate(collection, pipeline, regulation, metadatakey, view_name):
    rows = []
    for stage in pipeline:
        if "$project" in stage:
            for k, v in stage["$project"].items():
                if isinstance(v, int) and v == 1:
                    rows.append(_mongo_row("", collection, k, "", regulation, metadatakey, view_name, "project_included"))
                else:
                    rows.append(_mongo_row("", collection, k, k, regulation, metadatakey, view_name, "project_computed"))
            continue
        if "$lookup" in stage:
            lk = stage["$lookup"]
            from_coll = lk.get("from")
            as_field = lk.get("as", from_coll)
            rows.append(_mongo_row("", collection, f"$lookup->{from_coll}", as_field, regulation, metadatakey, view_name, "lookup_join"))
            continue
    return rows


def parse_mongo_operation(op, regulation, metadatakey, view_name):
    if op.get("op") == "find":
        return parse_mongo_find(op.get("collection"), op.get("filter"), op.get("projection"),
                                regulation, metadatakey, view_name)
    if op.get("op") == "aggregate":
        return parse_mongo_aggregate(op.get("collection"), op.get("pipeline"),
                                     regulation, metadatakey, view_name)
    return []

# ---- Elastic parser integration ----
from typing import Dict, Any, List

def _mk_elastic_row(db, table, column, alias, regulation, metadatakey, view_name, remarks):
    # same final keys as SQL/Mongo rows
    return {
        "Database Name": db or "",
        "Table Name": table or "",
        "Column Name": column or "",
        "Alias Name": alias or "",
        "Regulation": regulation or "",
        "Metadatakey": metadatakey or "",
        "View Name": view_name or "",
        "Remarks": remarks or "",
    }

def _extract_fields_from_bool_clause(clause) -> List[Dict[str, Any]]:
    """
    Given a single clause (match/range/exists/etc) return list of dicts: {field, type, value}
    Works on simple forms; best-effort for nested structures.
    """
    results = []

    if not isinstance(clause, dict):
        return results

    # common cases (match, match_phrase, term)
    for key in ("match", "match_phrase", "term"):
        if key in clause:
            body = clause[key]
            if isinstance(body, dict):
                for f, v in body.items():
                    results.append({"field": f, "type": key, "value": v})
            else:
                # sometimes shorthand
                results.append({"field": str(body), "type": key, "value": ""})
            return results

    # range
    if "range" in clause:
        body = clause["range"]
        if isinstance(body, dict):
            for f, v in body.items():
                results.append({"field": f, "type": "range", "value": v})
        return results

    # exists
    if "exists" in clause:
        body = clause["exists"]
        # body could be {"field":"x"} or {"field":"x", ...}
        if isinstance(body, dict):
            fld = body.get("field") or body.get("name") or ""
            results.append({"field": fld, "type": "exists", "value": ""})
        return results

    # prefix, wildcard, query_string, simple_query_string
    if "prefix" in clause:
        for f, v in clause["prefix"].items():
            results.append({"field": f, "type": "prefix", "value": v})
        return results

    if "wildcard" in clause:
        for f, v in clause["wildcard"].items():
            results.append({"field": f, "type": "wildcard", "value": v})
        return results

    # fallback: scan top-level keys to see direct field usages
    for k, v in clause.items():
        if isinstance(v, dict) and len(v) == 1:
            inner_k = list(v.keys())[0]
            # heuristics
            if inner_k in ("query", "value", "match"):
                results.append({"field": k, "type": "unknown", "value": v.get(inner_k)})
    return results

def parse_elastic_query(payload: Dict[str, Any], regulation: str, metadatakey: str, view_name: str) -> List[Dict[str, str]]:
    """
    Best-effort extractor for Elastic DSL query objects.
    - payload: dict containing "query": { "bool": { "must": [...], ... } } OR inner bool stages
    - returns list of uniform lineage rows
    """
    rows = []

    def walk_bool(b):
        # b is a dict that may contain "must"/"should"/"filter"/"must_not"
        clauses = []
        if not isinstance(b, dict):
            return []
        for key in ("must", "should", "filter", "must_not"):
            if key in b:
                part = b[key]
                if isinstance(part, list):
                    clauses.extend(part)
                elif isinstance(part, dict):
                    clauses.append(part)
        return clauses

    # entry point: payload might be {"query": {...}} or the bool itself
    root = payload.get("query", payload)

    # If root contains bool
    bool_body = root.get("bool") if isinstance(root, dict) else None
    clauses = []
    if bool_body:
        clauses = walk_bool(bool_body)
    else:
        # single clause (match / range / exists at top)
        clauses = [root]

    for clause in clauses:
        # clause may be {'range': {...}} or {'match_phrase': {...}} etc.
        extracted = _extract_fields_from_bool_clause(clause)
        if not extracted:
            # nested bool inside clause? e.g., {"bool": {...}} inside must
            if isinstance(clause, dict) and "bool" in clause:
                nested_clauses = walk_bool(clause["bool"])
                for nc in nested_clauses:
                    extracted += _extract_fields_from_bool_clause(nc)
        for ex in extracted:
            field = ex.get("field") or ""
            typ = ex.get("type") or ""
            value = ex.get("value")
            remarks = "Mongo query"
            # for range we can include from/to details in remarks
            if typ == "range" and isinstance(value, dict):
                # normalize range representation
                rparts = []
                for k in ("from", "gte", "gt", "to", "lte", "lt"):
                    if k in value:
                        rparts.append(f"{k}={value[k]}")
                if rparts:
                    remarks = "range:" + ",".join(rparts)
            # produce a row; DB/Table unknown unless caller supplies index/db
            rows.append(_mk_elastic_row("", "", field, "", regulation, metadatakey, view_name, "Mongo query"))

    return rows

# ============================================================
#                UNIFIED PUBLIC FUNCTION
# ============================================================

def parse_sql_lineage(source: Any, regulation: str,
                             metadatakey: str, view_name: str,
                             dialect: Optional[str] = None) -> List[Dict[str, str]]:

    if isinstance(source, str):
        source_str = source.strip()
        # Try JSON decode if it looks like JSON
        if (source_str.startswith("{") and source_str.endswith("}")) or \
           (source_str.startswith("[") and source_str.endswith("]")):
            try:
                source_json = json.loads(source_str)
                source = source_json
            except Exception:
                pass   # keep as SQL string if JSON parse fails
    # Mongo branch
    if isinstance(source, dict) and source.get("op"):
        return parse_mongo_operation(source, regulation, metadatakey, view_name)
    if isinstance(source, dict) and ("query" in source or any(k in source for k in ("bool","match","match_phrase","range","exists"))):
        return parse_elastic_query(source, regulation, metadatakey, view_name)
    # SQL branch
    if isinstance(source, str):
        return _generate_lineage_sql(source, regulation, metadatakey, view_name, dialect)

    return []



def extract_map_key_values(expr, source_table_info, regulation, metadatakey, view_name):
    """
    Extract lineage for Spark MAP('key', value, ...)
    expr: exp.Map(...)
    """
    rows = []
    
    # sqlglot stores MAP arguments as list: ['PAYMENT_TYPE', PAYMENT_TYPE, 'PAYMENT_AMOUNT', PAYMENT_AMOUNT, ...]
    args = expr.expressions

    # iterate in pairs: (key_literal, value_expr)
    for i in range(0, len(args), 2):
        key_literal = args[i]
        value_expr = args[i+1] if i+1 < len(args) else None

        key_name = key_literal.name if hasattr(key_literal, "name") else key_literal.sql().strip("'")

        # If right-side is a Column → standard lineage
        if isinstance(value_expr, exp.Column):
            col_name = value_expr.name
            table_alias = value_expr.table or source_table_info["alias"]
            db = source_table_info["db"]
            tbl = source_table_info["table"]

            rows.append({
                "Database Name": db,
                "Table Name": tbl,
                "Column Name": col_name,
                "Alias Name": key_name,
                "Regulation": regulation,
                "Metadatakey": metadatakey,
                "View Name": view_name,
                "Remarks": "Spark complex SQL - MAP function"
            })

    return rows



if __name__ == "__main__":
    mongo_query = {
        "op": "find",
        "db": "crm",
        "collection": "customers",
        "filter": {"country": "IN"},
        "projection": {"name": 1, "email": 1}
    }
    print("-------------------mongo query op----------------------------------")
    rows_mongo = parse_sql_lineage(mongo_query, "GDPR", "MONGO1", "VW_CUSTOMERS")
    print(json.dumps(rows_mongo, indent=2))
    
    mongo_query2 =""" {
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "elasticUpdatedTs": {
              "from": "##ELASTIC_START_TIME##",
              "to": "##ELASTIC_END_TIME##",
              "include_lower": "true",
              "include_upper": "false",
              "boost": 1
            }
          }
        },
        {
          "match_phrase": {
            "gkeys.olympusRegulation": "##OLYMPUS_REG##"
          }
        },
        {
          "match_phrase": {
            "gstate.activeFlag": "*"
          }
        },
        {
          "match_phrase": {
            "gkeys.factor": "DO"
          }
        },
        {
          "exists": {
            "field": "gkeys.rolloverKey",
            "adjust_pure_negative": "true",
            "boost": 1
          }
        }
      ]
    }
  }
}"""
    res =  parse_sql_lineage(mongo_query2, "GDPR", "MONGO1", "VW_CUSTOMERS")
    print("--------------------------mongo db elastic query-----------------------------------------")
    print(json.dumps(res, indent=2))
    print("*"*40)
    sql_txt = """

SELECT *
FROM (
    SELECT
        T.TRADE_SK,
        T.DWH_MESSAGE_HASHCODE,
        T.TRADE_EVENT_TIMESTAMP,
        T.PARTY_EXECUTION_TIMESTAMP,
        T.UTI,
        T.UTI_NAMESPACE,
        T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY,
        T.FIRM_ACCOUNT_MNEMONIC,
        T.FIRM_PARTY_GFCID,
        T.COUNTER_PARTY_MNEMONIC,
        T.COUNTER_PARTY_GFCID,
        T.USI,
        T.USI_NAMESPACE,
        T.PRIMARY_ASSET_CLASS,
        T.TRADE_UTI_ID,
        T.FIRM_PARTY_LEI,
        T.ACTUAL_TERMINATION_DATE,
        T.BUSINESS_DATE,
        T.DWH_CREATE_TIMESTAMP,
        T.TRADE_PUBLISHING_SYSTEM_NAME,
        T.TRADE_DATE,
        T.COUNTER_PARTY_LEI,
        T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY AS SB_SUMMARY,
        ROW_NUMBER() OVER (
            PARTITION BY T.TRADE_UT_ID
            ORDER BY CASE
                WHEN NVL(T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY, '') IN ('', 'NULL', 'none', 'NONE')
                    THEN 3
                WHEN NVL(T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY, '') NOT IN ('', 'NULL', 'none', 'NONE')
                    THEN 2
                ELSE 1
            END DESC,
            T.TRADE_EVENT_TIMESTAMP DESC
        ) AS ROWNUMBERBANK_1,
        T.TRADE_CLEARING_STATUS,
        T.CLEARING_HOUSE_ID,
        T.UPI,
        T.CLEARING_TRADE_ID,
        T.DWH_UPDATED_TIME,
        GFOLYNSD_STANDARIZATION.TRADE_FACT_DATA
    FROM
        GFOLYRE_MANAGED.APP_REGHUB_RHOO_TRADE T
        LEFT JOIN GFOLYNSD_STANDARIZATION.TRADE_FACT_DATA_L ON TRADE_UT_ID = T.TRADE_UT_ID
    WHERE
        T.TRADE_STATUS = 'ACTIVE'
        AND T.ACTUAL_TERMINATION_DATE >= TO_TIMESTAMP(
            DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), 'yyyyMMdd'),
            'yyyyMMdd'
        )
        AND T.ACTUAL_TERMINATION_DATE >= TO_TIMESTAMP(
            DATE_FORMAT(
                DATE_SUB(
                    TO_TIMESTAMP(TRADE_EVENT_TIMESTAMP, 'America/New_York'),
                    (CASE WHEN '#DAY_OF_WEEK#' = 'MONDAY'
                         THEN 3
                         WHEN '#DAY_OF_WEEK#' = 'MONDAY'
                         THEN 3 END)
                ),
                'yyyyMMdd'
            ),
            'yyyyMMdd'
        )
        AND T.DWH_BUSINESS_DATE <= CAST(
            DATE_FORMAT(
                DATE_SUB(
                    DATE(T.DWH_EVENT_TIMESTAMP),
                    3
                ),
                'yyyy-MM-dd'
            ) AS TIMESTAMP
        )
        AND T.DWH_UPDATED_TIME >= TO_TIMESTAMP(
            DATE_FORMAT(
                DATE_SUB(TO_TIMESTAMP(SYSTIMESTAMP), 5),
                'yyyy-MM-dd HH:mm:ss.SSS'
            )
        )
        AND T.DWH_UPDATED_TIME < TO_TIMESTAMP(
            DATE_FORMAT(
                DATE_SUB(TO_TIMESTAMP(SYSTIMESTAMP), 6),
                'yyyy-MM-dd HH:mm:ss.SSS'
            )
        )
)
WHERE ROWNUMBERBANK_1 = 1
    OR (
        IF EXISTS (
            SELECT T.SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY
            WHERE REPLACE(NVL(LATEST_VERSION, 'N'), 'Y', 'N') = 'Y'
        ) THEN LATEST_VERSION
        ELSE EXISTING_VALUE
    )

"""
    print("==="*30)
    # print(extract_query_details(sql_txt))
    rows = parse_sql_lineage(
        sql_txt,
        regulation="SEC",
        metadatakey="KEY123",
        view_name="VW_SAMPLE"
    )

    import json
    print(json.dumps(rows, indent=2))
    
    map_query = """SELECT 
    P.UTID,
    P.DB_KEY,
    MAP(
        'PAYMENT_TYPE', PAYMENT_TYPE,
        'PAYMENT_AMOUNT', PAYMENT_AMOUNT,
        'PAYMENT_CURRENCY', PAYMENT_CURRENCY,
        'PAYMENT_DATE', PAYMENT_DATE,
        'FIRM_ACCOUNT_MNEMONIC', FIRM_ACCOUNT_MNEMONIC,
        'COUNTER_PARTY_MNEMONIC', COUNTER_PARTY_MNEMONIC,
        'LEG_TYPE', LEG_TYPE
    ) AS CASHFLOW_MAP
FROM 
    GFG_WORK.RHOO_REF_TRADE_CASHFLOW_DATA P
WHERE 
    P.GEMFIRE_ENV = '##GEMFIRE_CONNECTING_ENV##';
 """
    rows = parse_sql_lineage(
        map_query,
        regulation="SEC",
        metadatakey="KEY123",
        view_name="VW_SAMPLE"
    )

    import json
    print("=====================this map query ========================================")
    print(json.dumps(rows, indent=2))