# ============================================================
#                LINEAGE ENGINE (SQL + MONGO)
# ============================================================
import json
import sqlglot
from sqlglot import exp
from typing import Any, Dict, List, Optional, Set
from test_sql import parse_metadata_and_extract_lineage
import base64
import re




SPARK_MAP_REGEX = re.compile(
    r"\b(map|create_map)\s*\(",
    re.IGNORECASE
)


def is_spark_map_sql(sql: str) -> bool:
    """
    Identify whether given SQL string is Spark MAP SQL.
    
    Returns:
        True  -> Spark MAP SQL
        False -> Normal SQL
    """

    if not sql or not sql.strip():
        return False

    if not SPARK_MAP_REGEX.search(sql):
        return False
    return True

def _generate_lineage_sql(sql: str, regulation: str, metadatakey: str,
                           view_name: str, dialect: Optional[str]) -> List[Dict[str, str]]:
        if is_spark_map_sql(sql):
            res = [{
                "Database Name": "",
                "Table Name": "",
                "Column Name": "",
                "Alias Name": "",
                "Regulation": regulation,
                "Metadatakey": metadatakey,
                "View Name": view_name,
                "Remarks": "Spark complex SQL -MAP function"
            }]
            return  res
        else:
            metadata = {"sql_query": base64.b64encode(sql.encode()).decode()}
            rows = parse_metadata_and_extract_lineage(json.dumps(metadata), regulation, metadatakey, view_name)
            return rows
        


# ============================================================
#                UNIFIED PUBLIC FUNCTION
# ============================================================

def parse_sql_lineage(source: Any, regulation: str,
                             metadatakey: str, view_name: str,
                             dialect: Optional[str] = None) -> List[Dict[str, str]]:

    source_str = source.strip()
    if isinstance(source, str) and (source_str.startswith("{") and source_str.endswith("}")):
        res = [{
            "Database Name": "",
            "Table Name": "",
            "Column Name": "",
            "Alias Name": "",
            "Regulation": regulation,
            "Metadatakey": metadatakey,
            "View Name": view_name,
            "Remarks": "ignored_elastic_query"
        }]
        return res
        
    elif isinstance(source, str) and  (source_str.startswith("[") and source_str.endswith("]")):
        res = [{
                "Database Name": "",
                "Table Name": "",
                "Column Name": "",
                "Alias Name": "",
                "Regulation": regulation,
                "Metadatakey": metadatakey,
                "View Name": view_name,
                "Remarks": "ignored_mongo_query"
            }]
        return res
    # SQL branch
    elif isinstance(source, str):
        return _generate_lineage_sql(source, regulation, metadatakey, view_name, dialect)

    return []

if __name__ == "__main__":
    mongo_query = """[{
        "op": "find",
        "db": "crm",
        "collection": "customers",
        "filter": {"country": "IN"},
        "projection": {"name": 1, "email": 1}
    }]"""
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
    
    normal_sql = """select c1,c2,c3,c4 as col_c4 from abc.test where c1='123'"""
    rows = parse_sql_lineage(
        normal_sql,
        regulation="SEC",
        metadatakey="KEY123",
        view_name="VW_SAMPLE"
    )

    import json
    print("=====================this mnormal sql query ========================================")
    print(json.dumps(rows, indent=2))