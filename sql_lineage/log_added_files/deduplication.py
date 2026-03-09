"""
Deduplication of lineage records (v2).
"""
from app.logging_config import get_logger

logger = get_logger(__name__)

def deduplicate_records(records: list) -> list:
    """
    Return records with exact duplicates removed; drop junk all-empty rows.

    Raises:
        TypeError: if records is not a list.
    """
    if not isinstance(records, list):
        logger.error(
            "deduplicate_records expects a list",
            received_type=type(records).__name__,
        )
        raise TypeError(f"records must be a list, got {type(records).__name__}")

    logger.debug("Starting deduplication", total_input=len(records))

    seen: set = set()
    unique: list = []
    junk_dropped = 0
    duplicate_dropped = 0

    for idx, record in enumerate(records):
        if not isinstance(record, dict):
            logger.warning(
                "Skipping non-dict record during deduplication",
                record_index=idx,
                record_type=type(record).__name__,
            )
            continue

        # Drop completely-empty (junk) rows
        if (
            record.get("databaseName", "") == ""
            and record.get("tableName", "") == ""
            and record.get("tableAliasName", "") == ""
            and record.get("columnName", "") == ""
            and record.get("aliasName", "") == ""
        ):
            junk_dropped += 1
            logger.debug("Dropping junk empty record", record_index=idx)
            continue

        # Build immutable canonical key for deduplication
        try:
            key = tuple(
                (k, tuple(v) if isinstance(v, list) else v)
                for k, v in sorted(record.items())
            )
        except Exception as exc:
            logger.error(
                "Failed to build dedup key for record; skipping",
                exc=exc,
                record_index=idx,
                record_preview=str(record)[:200],
            )
            continue

        if key not in seen:
            seen.add(key)
            unique.append(record)
        else:
            duplicate_dropped += 1
            logger.debug("Duplicate record removed", record_index=idx)

    logger.info(
        "Deduplication complete",
        input_count=len(records),
        output_count=len(unique),
        junk_dropped=junk_dropped,
        duplicate_dropped=duplicate_dropped,
    )
    return unique
