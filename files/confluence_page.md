# SQL Lineage & Column Match Service

> **Type:** FastAPI Microservice · **Engine:** sqlglot · **Language:** Python 3  
> **Regulation:** RHOO · **Team:** RegHub Platform

---

## Overview

The **SQL Lineage & Column Match Service** is a FastAPI-based microservice that provides two tightly coupled capabilities:

1. **SQL Lineage Extraction** — Parses SQL view/query definitions and produces structured column-level lineage records. For each column referenced in a SQL statement, the service resolves its originating physical table, database, and alias, and annotates the record with semantic tags that describe how the column was used (projection, JOIN condition, WHERE filter, derived expression, CASE, etc.).

2. **Column Matching** — Takes lineage rows produced by the extractor and matches them against field definitions fetched from a mapper service, using a three-tier exact → fuzzy → weak scoring algorithm to identify which source columns correspond to which mapped fields.

The service integrates with two external systems:

- **Metadata Service** — supplies JSON payloads containing base64-encoded SQL, view names, class names, and metadatakeys for batch lineage extraction.
- **ODS Fact Service** — supplies pre-computed lineage records (windata) used in column matching.

All heavy I/O (mapper lookups, ODS queries) is performed concurrently via `asyncio.gather`. The system is fault-tolerant throughout — individual key failures never abort batch processing.

---

## Key Features

| Feature | Detail |
|---|---|
| **Column-level lineage** | Resolves every SELECT projection to its physical source table and database |
| **Full SQL clause coverage** | Captures lineage from SELECT, WHERE, GROUP BY, HAVING, JOIN ON, and subquery WHERE filters |
| **JOIN lineage** | Records ON-clause columns, equality pairs, join type, and WHERE inside JOIN subqueries |
| **UNION support** | Recursively splits UNION/UNION ALL trees; each branch processed independently |
| **CTE & nested subquery resolution** | Follows CTEs, aliased subqueries, and derived tables to their base physical table |
| **Spark MAP SQL detection** | Detects Spark `MAP()` / `CREATE_MAP()` expressions and short-circuits with a dedicated remark instead of mishandling unsupported syntax |
| **Elastic/Mongo query bypass** | Automatically identifies and skips Elastic `[...]` and Mongo `{...}` query strings with an `ignored_*` remark |
| **Dual-dialect parsing** | Spark dialect attempted first; falls back to standard SQL on failure |
| **URL-based batch extraction** | Fetches SQL payloads from an external metadata URL, filters by view_name/class_name/metadatakey, and processes in a single call |
| **Direct SQL endpoint** | Accepts base64-encoded SQL via POST body for ad-hoc lineage queries |
| **Three-tier column matching** | Exact (100%), fuzzy (≥40%), and weak (<40%) match types with a no_match sentinel |
| **Concurrent key processing** | Multiple metadata keys processed in parallel via `asyncio.gather` |
| **Stop-word-aware token matching** | Column name tokenization filters noise words (ID, CODE, KEY, SK, etc.) before scoring |
| **Fault-tolerant** | Per-key, per-row, and per-column error isolation; failures are logged and skipped rather than propagating |
| **Structured logging** | Key context (regulation, metadatakey, view_name, sql_preview) logged at every major step |


## How It Works

### Flow 1 — URL-Based Lineage Extraction (`/parse_lineage_from_url`)

```
POST /parse_lineage_from_url
  { url, regulation, metadatakey?, class_name?, view_names?, headers? }
        │
        ▼
  Fetch JSON payload from metadata URL (HTTP GET)
        │
        ▼
  _extract_entries_with_context(payload)
        │  Recursively walk JSON tree
        │  Capture: name → metadatakey, classname, view_sql_objects
        │  Find dicts containing BOTH "view_name" AND "sql_query"
        │
        ▼
  Apply filters: metadatakey / view_name / class_name
        │
        ▼
  For each matching entry:
        │
        ├─ Decode base64 SQL  (_safe_b64decode_to_text)
        │    handles padding issues + urlsafe fallback
        │
        └─ parse_sql_lineage(decoded_sql, regulation, metadatakey, view_name)
                │
                ├─ Elastic query [...]  → ignored_elastic_query remark
                ├─ Mongo query   {...}  → ignored_mongo_query remark
                ├─ Spark MAP SQL        → MAP function remark (short-circuit)
                └─ Standard SQL         → extract_lineage_rows() → deduplicate()
        │
        ▼
  Aggregate all rows → return { success, total_records, lineage_data }
```

### Flow 2 — Direct SQL Lineage (`/parse_lineage_from_sql`)

```
POST /parse_lineage_from_sql
  { sql_text: "<base64 SQL>" }
        │
        ▼
  _safe_b64decode_to_text(sql_text)
        │
        ▼
  parse_sql_lineage(decoded_sql)
        │
        ▼
  { success, message, total_records, lineage_data }
```

### Flow 3 — Column Matching (`/match-lineage`)

```
POST /match-lineage
  { base_url, regulation_meta, regulation, current_branch }
        │
        ▼
  GET metadata URL → { value: { key1: { mapper, sql_files }, key2: ... } }
        │
        ▼
  For each key (concurrent via asyncio.gather):
        │
        ├─ GET mapper field definitions  (build_metadata_url → call_gateway)
        │       { name, value: { table_columns: [{ column_name, field, column_type }] } }
        │
        ├─ POST ODS Fact Service  (filterCriteria: currBranch, windowType, sql_files)
        │       → { factContainers: [{ winkeys, windata: [{ a1..a7, lal }] }] }
        │
        ├─ extract_lineage_rows_from_response()  → flat List[Dict]
        │
        └─ match_columns_with_lineage()
                │
                For each mapper field:
                ├─ Tokenize + filter stop-words
                ├─ Score each lineage row (exact / fuzzy)
                ├─ Case 1: score ≥ 40%  → emit ALL qualifying rows (exact/fuzzy)
                ├─ Case 2: 0 < score < 40% → emit single best weak row
                └─ Case 3: no score    → emit no_match sentinel
        │
        ▼
  Aggregate all key results
  Return { success, matchSummary, keySummary, lineage_data }
```
---

## Output Schema

### Lineage Row

Each row returned by the lineage endpoints:

| Field | Type | Description |
|---|---|---|
| `databaseName` | `str` | Source database (lowercase). Empty if unresolvable. |
| `tableName` | `str` | Physical base table. `__DERIVED__` for anonymous subqueries. |
| `tableAliasName` | `str` | Alias as written in the query. |
| `columnName` | `str` | Column name; `*` for wildcards; function SQL for function-only expressions. |
| `aliasName` | `str` | `AS` alias from the SELECT list. |
| `regulation` | `str` | Passed through from caller. |
| `metadatakey` | `str` | Passed through from caller. |
| `viewName` | `str` | Target view/object name. |
| `remarks` | `List[str]` | Semantic tags (see Remarks Vocabulary). |

### Column Match Row

Each row returned by `/match-lineage`:

| Field | Type | Description |
|---|---|---|
| `regulation` | `str` | Regulation tag (e.g. `rhoo`) |
| `key` | `str` | Metadata key this match belongs to |
| `mapperColumn` | `str` | Field name from the mapper definition |
| `mapperFactField` | `str` | Fact field path from the mapper (e.g. `src:tradeId`) |
| `dbName` | `str` | Matched source database |
| `tableName` | `str` | Matched source table |
| `columnName` | `str` | Matched column name from lineage |
| `columnAliasName` | `str` | Matched column alias from lineage |
| `matchedOn` | `str` | `"alias"` or `"columnName"` — which field was used for scoring |
| `matchPercentage` | `str` | Score as formatted string e.g. `"87.5%"` |
| `matchType` | `str` | `"exact"`, `"fuzzy"`, `"weak"`, or `"no_match"` |

### Remarks Vocabulary

| Remark | When applied |
|---|---|
| `COLUMN_SELECTED_WITH_DB` | Column resolved with a database qualifier present |
| `DATABASE_NOT_SPECIFIED` | Column resolved to a table but no database qualifier |
| `TABLE_AMBIGUOUS` | Multiple base tables in scope; source cannot be determined |
| `INVALID_TABLE_ALIAS` | Qualifier present in SQL but not found in any scope |
| `DERIVED_EXPR` | Column originates from a derived or function expression |
| `DERIVED_TABLE` | Source is an anonymous subquery |
| `CASE_EXPR` | Column is inside a CASE expression |
| `ALL_COLUMNS` | SELECT * wildcard |
| `JOIN_ON_COLUMN` | Column appears in a JOIN ON condition |
| `JOIN_TYPE:<kind>` | Join type — e.g. `JOIN_TYPE:LEFT`, `JOIN_TYPE:INNER` |
| `JOIN_SUBQUERY_WHERE_COLUMN` | Column is in a WHERE clause inside a JOIN subquery |
| `WHERE_COLUMN` | Column appears in a WHERE filter |
| `GROUP_BY_COLUMN` | Column appears in GROUP BY |
| `HAVING_COLUMN` | Column appears in HAVING |
| `TECH_FAILURE` | SQL could not be parsed; sentinel row with empty identity fields |
| `Spark complex SQL -MAP function` | SQL contains Spark MAP/CREATE_MAP; not parsed further |
| `ignored_elastic_query` | Source is an Elastic `[...]` query string; skipped |
| `ignored_mongo_query` | Source is a Mongo `{...}` query string; skipped |

---

**Sample Response:**
```json
{
  "success": true,
  "regulation": "rhoo",
  "regulationMeta": "rhoo_emiretd_metadata_batch_sourcing",
  "currBranch": "1.26.3.2",
  "keysProcessed": ["key1", "key2"],
  "totalMatchRecords": 134,
  "matchSummary": { "exact": 98, "fuzzy": 22, "no_match": 14 },
  "keySummary": { "key1": { "total": 80, "matched": 70, "no_match": 10 } },
  "lineage_data": [ ... ]
}
```
---

## Error Handling Summary

| Layer | Failure | Behaviour |
|---|---|---|
| `/parse_lineage_from_url` | Missing url or regulation | HTTP 400 |
| `/parse_lineage_from_url` | Metadata fetch fails | HTTP 500 |
| `/match-lineage` | Metadata fetch fails | HTTP 502 |
| `/match-lineage` | Bad metadata response type | HTTP 502 |
| `/match-lineage` | Empty metadata value | HTTP 404 |
| `/match-lineage` | No valid keys | HTTP 422 |
| `_process_metadata_key` | Any key-level error | Key skipped, returns `[]`, logged |
| `match_columns_with_lineage` | Per-row scoring error | Row skipped, logged |
| `extract_lineage_rows` | Empty SQL | `TECH_FAILURE` row |
| `extract_lineage_rows` | All parse attempts fail | `TECH_FAILURE` row |
| `_process_single_select` | Per-SELECT error | SELECT skipped, rest processed |
| `_emit_column_lineage` | Per-column error | Column skipped, rest processed |

---