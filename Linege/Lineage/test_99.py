import sqlglot

tree = sqlglot.parse_one("SELECT a + b FROM x WHERE id = 10")
print(tree)


sql = sqlglot.transpile(
    "select a,b from x where a>10 order by b",
    pretty=True
)[0]

print(sql)
print("------------------------table name column name ---------------------------------")
import sqlglot

tree = sqlglot.parse_one("SELECT a + b AS x FROM test WHERE c > 10")

print(tree.find_all(sqlglot.exp.Column))
print(tree.find_all(sqlglot.exp.Table))




print("------------------------find all ---------------------------------")
import sqlglot
from sqlglot.expressions import Column, Table

def extract_query_details(sql):
    parsed = sqlglot.parse_one(sql)

    # Tables info
    tables = []
    for t in parsed.find_all(Table):
        tables.append({
            "db_name": t.catalog,
            "schema_name": t.db,
            "table_name": t.name
        })

    # Columns info (with alias if present)
    columns = []
    for c in parsed.find_all(Column):
        col_info = {
            "column_name": c.name,
            "alias": c.alias_or_name,
            "table": c.table,
            "schema": c.db,
            "database": c.catalog
        }
        columns.append(col_info)

    return {"tables": tables, "columns": columns}


# ---- Test with your CREATE TABLE ----
sql = """
 select platform_level AS INTERFACE_PLATFORM, count(*) from IRIS_PRD.AEM.T_FACT_PLAYER_SPEND_BY_PLATFORM_DAILY where title_id = 356 and date_id <= '20251210' AND is_trial_base_spender_today='TRUE' group by 1 order by 1;
"""

print(extract_query_details(sql))

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
print(extract_query_details(sql_txt))
"""
{
  "tables": [
    {
      "db_name": "",
      "schema_name": "GFOLYRE_MANAGED",
      "table_name": "APP_REGHUB_RHOO_TRADE"
    },
    {
      "db_name": "",
      "schema_name": "GFOLYNSD_STANDARIZATION",
      "table_name": "TRADE_FACT_DATA_L"
    }
  ],
  "columns": [
    {
      "column_name": "TRADE_SK",
      "alias": "TRADE_SK",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "DWH_MESSAGE_HASHCODE",
      "alias": "DWH_MESSAGE_HASHCODE",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_EVENT_TIMESTAMP",
      "alias": "TRADE_EVENT_TIMESTAMP",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "PARTY_EXECUTION_TIMESTAMP",
      "alias": "PARTY_EXECUTION_TIMESTAMP",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "UTI",
      "alias": "UTI",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "UTI_NAMESPACE",
      "alias": "UTI_NAMESPACE",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "alias": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "FIRM_ACCOUNT_MNEMONIC",
      "alias": "FIRM_ACCOUNT_MNEMONIC",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "FIRM_PARTY_GFCID",
      "alias": "FIRM_PARTY_GFCID",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "COUNTER_PARTY_MNEMONIC",
      "alias": "COUNTER_PARTY_MNEMONIC",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "COUNTER_PARTY_GFCID",
      "alias": "COUNTER_PARTY_GFCID",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "USI",
      "alias": "USI",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "USI_NAMESPACE",
      "alias": "USI_NAMESPACE",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "PRIMARY_ASSET_CLASS",
      "alias": "PRIMARY_ASSET_CLASS",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_UTI_ID",
      "alias": "TRADE_UTI_ID",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "FIRM_PARTY_LEI",
      "alias": "FIRM_PARTY_LEI",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "ACTUAL_TERMINATION_DATE",
      "alias": "ACTUAL_TERMINATION_DATE",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "BUSINESS_DATE",
      "alias": "BUSINESS_DATE",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "DWH_CREATE_TIMESTAMP",
      "alias": "DWH_CREATE_TIMESTAMP",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_PUBLISHING_SYSTEM_NAME",
      "alias": "TRADE_PUBLISHING_SYSTEM_NAME",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_DATE",
      "alias": "TRADE_DATE",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "COUNTER_PARTY_LEI",
      "alias": "COUNTER_PARTY_LEI",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_CLEARING_STATUS",
      "alias": "TRADE_CLEARING_STATUS",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "CLEARING_HOUSE_ID",
      "alias": "CLEARING_HOUSE_ID",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "UPI",
      "alias": "UPI",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "CLEARING_TRADE_ID",
      "alias": "CLEARING_TRADE_ID",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "DWH_UPDATED_TIME",
      "alias": "DWH_UPDATED_TIME",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_FACT_DATA",
      "alias": "TRADE_FACT_DATA",
      "table": "GFOLYNSD_STANDARIZATION",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "ROWNUMBERBANK_1",
      "alias": "ROWNUMBERBANK_1",
      "table": "",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "alias": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "LATEST_VERSION",
      "alias": "LATEST_VERSION",
      "table": "",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "EXISTING_VALUE",
      "alias": "EXISTING_VALUE",
      "table": "",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_UT_ID",
      "alias": "TRADE_UT_ID",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_UT_ID",
      "alias": "TRADE_UT_ID",
      "table": "",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_UT_ID",
      "alias": "TRADE_UT_ID",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "DWH_UPDATED_TIME",
      "alias": "DWH_UPDATED_TIME",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "alias": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_EVENT_TIMESTAMP",
      "alias": "TRADE_EVENT_TIMESTAMP",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "DWH_UPDATED_TIME",
      "alias": "DWH_UPDATED_TIME",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "DWH_BUSINESS_DATE",
      "alias": "DWH_BUSINESS_DATE",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "ACTUAL_TERMINATION_DATE",
      "alias": "ACTUAL_TERMINATION_DATE",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_STATUS",
      "alias": "TRADE_STATUS",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "ACTUAL_TERMINATION_DATE",
      "alias": "ACTUAL_TERMINATION_DATE",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "SYSTIMESTAMP",
      "alias": "SYSTIMESTAMP",
      "table": "",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "LATEST_VERSION",
      "alias": "LATEST_VERSION",
      "table": "",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "alias": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "SYSTIMESTAMP",
      "alias": "SYSTIMESTAMP",
      "table": "",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "alias": "SUPERVISORY_BODY_SUMMARY_REPORTING_ONLY",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "DWH_EVENT_TIMESTAMP",
      "alias": "DWH_EVENT_TIMESTAMP",
      "table": "T",
      "schema": "",
      "database": ""
    },
    {
      "column_name": "TRADE_EVENT_TIMESTAMP",
      "alias": "TRADE_EVENT_TIMESTAMP",
      "table": "",
      "schema": "",
      "database": ""
    }
  ]
}
"""