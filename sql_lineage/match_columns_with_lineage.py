def has_meaningful_token_overlap(a: str, b: str) -> bool:
    STOP_WORDS = {
        "ID", "IDENTIFIER", "CODE", "KEY", "SK", "NO", "NUM"
    }
    #  PRODUCT_ID_TOXNOMY  PRODUCT
    tokens_a = tokenize(a) - STOP_WORDS
    tokens_b = tokenize(b) - STOP_WORDS

    return bool(tokens_a & tokens_b)


def tokenize(col: str) -> set:
    if not col:
        return set()
    # 1. Normalize separators
    col = col.upper().replace("_", " ")
    # 2. Split alpha-numeric boundaries: COUNTERPARTY1 → COUNTERPARTY 1
    col = re.sub(r"([A-Z]+)(\d+)", r"\1 \2", col)
    # 3. Tokenize
    tokens = set(col.split())
    # 4. Remove pure numbers
    tokens = {t for t in tokens if not t.isdigit()}
    return tokens


def match_columns_with_lineage(
    lineage_rows: List[Dict[str, Any]],
    field_mappings: Dict[str, Any],
    *,
    metadata_key: str = "",   # e.g. "pos_markets" - tag injected by caller
    regulation:   str = "",
) -> List[Dict[str, Any]]:

    mapper_fields = _extract_mapper_fields(field_mappings)
    results: List[Dict[str, Any]] = []
    print(f"[{metadata_key}] mapper_fields_count = {len(mapper_fields)}")
    for mf in mapper_fields:
        mapper_col  = mf["column_name"]
        mapper_fact = mf["field"]
        norm_mapper = normalize_col(mapper_col)

        target_col_raw = mapper_col
        target_col     = normalize_col(mapper_col)
        best_score: float          = -1.0
        best_row:   Optional[Dict] = None
        best_type:  str            = "no_match"

        skipped_db    = 0
        skipped_wild  = 0
        skipped_token = 0
        tried         = 0

        for row in lineage_rows:
            # skip invalid database entries
            print(f"linease row:{row}")
            dbName = row.get("Database Name")
            if dbName is None or len(dbName) == 0 or "gfolyreg_work" in dbName:
                skipped_db += 1
                continue

            lineage_col_row = row.get("Column Alias Name", "") or ""

            # if alias is empty, fall back to Column Name
            if not lineage_col_row.strip():
                lineage_col_row = row.get("Column Name", "") or ""

            # Skip wildcard columns
            if lineage_col_row.strip() == "*":
                skipped_wild += 1
                continue

            lineage_col = normalize_col(lineage_col_row)
            score       = fuzzy_score(target_col, lineage_col)

            if not has_meaningful_token_overlap(target_col_raw, lineage_col_row):

                lineage_col_row = row.get("Column Name")

                if lineage_col_row.strip() == "*" or lineage_col_row is None:
                    continue

                lineage_col = normalize_col(lineage_col_row)
                score       = fuzzy_score(target_col, lineage_col)

            if not has_meaningful_token_overlap(target_col_raw, lineage_col_row):
                skipped_token += 1
                continue

            if score > best_score:
                best_score, best_row = score, row
            elif score == best_score and best_row is not None:
                current_col = best_row.get("Column Name", "")
                new_col     = lineage_col_row
                if len(new_col) > len(current_col):
                    best_row = row
            results.append({
                "regulation":      regulation or best_row.get("olympusApplication", ""),
                "key":             metadata_key,
                "mapperColumn":    mapper_col,
                # "mapperFactField": mapper_fact,
                "dbName":          best_row.get("Database Name", ""),
                "tableName":       best_row.get("Table Name", ""),
                "columnName":      best_row.get("Column Name", ""),
                "columnAliasName": best_row.get("Column Alias Name", ""),
                "matchedOn":       "alias" if best_row.get("Column Alias Name", "").strip() else "columnName",
                "matchPercentage": f"{best_score:.1f}%",
            })

    return results