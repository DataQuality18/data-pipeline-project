"""
Deduplication of lineage records (v2).
"""


def deduplicate_records(records: list) -> list:
    """Return records with duplicates removed; drop junk all-empty rows."""
    seen = set()
    unique = []

    for record in records:
        # Drop junk completely-empty rows
        if (
            record.get("databaseName", "") == ""
            and record.get("tableName", "") == ""
            and record.get("tableAliasName", "") == ""
            and record.get("columnName", "") == ""
            and record.get("aliasName", "") == ""
        ):
            continue

        # Convert dict -> immutable canonical form for deduplication
        key = tuple(
            (k, tuple(v) if isinstance(v, list) else v)
            for k, v in sorted(record.items())
        )

        if key not in seen:
            seen.add(key)
            unique.append(record)

    return unique
