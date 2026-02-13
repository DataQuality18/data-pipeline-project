"""
Constants for SQL lineage extraction
"""

# Taxonomy keys for "Remarks" in lineage output; values are human-readable labels.
REMARKS = {
    "ALL_COLUMNS": "all_columns_selected",
    "COLUMN_SELECTED": "column_selected",
    "COLUMN_SELECTED_WITH_DB": "column_selected_with_database",
    "COLUMN_SELECTED_NO_DB": "column_selected_database_not_specified",
    "TABLE_AMBIGUOUS": "table_name_ambiguous",
    "DATABASE_NOT_SPECIFIED": "database_not_specified_in_query",
    "INNER_ALIAS": "inner_query_alias_layer",
    "SUBQUERY_LAYER": "subquery_layer",
    "DERIVED_EXPR": "derived_expression",
    "CASE_EXPR": "case_expression",
    "WHERE_COLUMN": "where_clause_column",
    "GROUP_BY_COLUMN": "group_by_column",
    "HAVING_COLUMN": "having_clause_column",
    "JOIN_ON_COLUMN": "join_on_clause_column",
    "JOIN_EQ_PAIR": "join_equality_pair",
    "JOIN_TYPE": "join_type",
    "JOIN_SUBQUERY_WHERE_COLUMN": "join_subquery_where_column",
    "FUNCTION_EXPR": "function_expression",
    "INVALID_TABLE_ALIAS": "invalid_table_alias",
    "DERIVED_TABLE": "table_name_derived",
    "TECH_FAILURE": "tech_failure",
}

# Ordered keys for each lineage output row (dict); used to normalize and serialize results.
# Note: Uses camelCase format (databaseName, tableName, etc.)
OUTPUT_KEYS = [
    "databaseName",
    "tableName",
    "tableAliasName",
    "columnName",
    "aliasName",
    "regulation",
    "metadatakey",
    "viewName",
    "remarks",
]
