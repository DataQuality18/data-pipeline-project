"""
Deduplication of lineage records.
"""


def deduplicate_records(records: list) -> list:
    """Return records with duplicates removed; drop junk all-empty STAR rows."""
    seen = set()
    unique = []

    for record in records:
        # Drop junk STAR rows: all key fields empty and Column Name is *
        if (
            record.get("Database Name", "") == ""
            and record.get("Table Name", "") == ""
            and record.get("Table Alias Name", "") == ""
            and record.get("Column Name") == "*"
            and record.get("Alias Name", "") == ""
        ):
            continue

        # Build immutable key for dedup (lists -> tuples for hashability)
        key = tuple(
            (k, tuple(v) if isinstance(v, list) else v)
            for k, v in sorted(record.items())
        )

        if key not in seen:
            seen.add(key)
            unique.append(record)

    return unique
