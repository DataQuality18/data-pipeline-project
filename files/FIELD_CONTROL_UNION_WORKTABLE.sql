-- =============================================================================
-- FIELD CONTROL RECONCILIATION — TEST WORKTABLE
-- All 6 break-classification scenarios combined via UNION ALL
-- RECON_ID = 9999999 (test only — do not use 9774339 production id)
--
-- New columns added for Field Control logic:
--   GRU.version       — Olympus trade version (increments per amendment)
--   GRU.tsr_msg_id    — DTCC TSR Msg ID; strip last 10 chars → REGHUB_ID in REG
--   REG.version       — Version of the trade this REG submission covers
--
-- TSR linkage rule:  SUBSTR(tsr_msg_id, 1, LENGTH(tsr_msg_id) - 10) = reghub_id
-- =============================================================================

-- ─── STEP A: Populate GRU test rows ─────────────────────────────────────────
-- Each row = one open break position from Olympus (BREAK_TYPE=OLYMPUS/DTCC)

INSERT INTO GFOLYREG_WORK.APP_REGHUB_RHOO_CONTROLS_POSITION_GRU

-- SCENARIO 1 | value_mismatch_reghub_error
-- Olympus v2 == DTCC-reflected v2 (same version) but value (age/quantity) still differs.
-- TSR_MSG_ID → cftc_fc_mock_SC01_V2_... = latest REG record for this UITID.
-- Expected classification: value_mismatch_reghub_error
SELECT
    'cftc'                                                              AS olympus_reg,
    '999001:SC01_VALUE_MISMATCH'                                        AS uitid,
    'FCTEST_SC01_UTI_VALUE_MISMATCH_V2'                                 AS uti,
    ''                                                                  AS usi,
    ''                                                                  AS taxonomy,
    TIMESTAMP('2026-08-17 00:00:00')                                    AS cob_date,
    'COMMODITIES'                                                       AS asset,
    'TEST_CP1_FC_MOCK_LEI001'                                           AS counterparty1,
    'TEST_CP2_FC_MOCK_LEI002'                                           AS counterparty2,
    150.00                                                              AS age,
    TIMESTAMP('2026-01-15 00:00:00')                                    AS age_date,
    '91-180'                                                            AS age_bucket,
    'OLYMPUS-vs-DTCC-Position-SDR-CFTC-Commodities-Trade-Recon'        AS name,
    'POSITION'                                                          AS recon_type,
    'RNC1PRD'                                                           AS instance_name,
    'OPEN'                                                              AS break_status,
    'OLYMPUS'                                                           AS break_type,
    90000001                                                            AS recon_load_id,
    20260817                                                            AS dwh_business_date,
    ''                                                                  AS province_id,
    'TEST_CP1_FC_MOCK_LEI001TEST_CP2_FC_MOCK_LEI002FCTEST_SC01_UTI_VALUE_MISMATCH_V2999001:SC01_VALUE_MISMATCH' AS position_id,
    1                                                                   AS gru_latest,
    1                                                                   AS load_rank,
    9999999                                                             AS recon_id,
    2                                                                   AS version,
    'cftc_fc_mock_SC01_V2_17800020000000000000_TSRSUFFIX'              AS tsr_msg_id

UNION ALL

-- SCENARIO 2 | false_break_timing_issue
-- Olympus v3 > DTCC-reflected v2.  RegHub also submitted v3 and got ACK —
-- DTCC just hasn't reflected v3 yet (timing lag).  Not a real break.
-- TSR_MSG_ID → V2; REG_RANKING=1 → V3 with ACK.
-- Expected classification: false_break_timing_issue
SELECT
    'cftc', '999002:SC02_FALSE_BREAK_TIMING', 'FCTEST_SC02_UTI_FALSE_BREAK_V3',
    '', '', TIMESTAMP('2026-08-17 00:00:00'), 'COMMODITIES',
    'TEST_CP1_FC_MOCK_LEI001', 'TEST_CP2_FC_MOCK_LEI002',
    45.00, TIMESTAMP('2026-07-03 00:00:00'), '31-90',
    'OLYMPUS-vs-DTCC-Position-SDR-CFTC-Commodities-Trade-Recon',
    'POSITION', 'RNC1PRD', 'OPEN', 'OLYMPUS', 90000001, 20260817, '',
    'TEST_CP1_FC_MOCK_LEI001TEST_CP2_FC_MOCK_LEI002FCTEST_SC02_UTI_FALSE_BREAK_V3999002:SC02_FALSE_BREAK_TIMING',
    1, 1, 9999999,
    3,                                                  -- olympus_version = 3
    'cftc_fc_mock_SC02_V2_17800020000000000000_TSRSUFFIX'  -- links to v2 in REG

UNION ALL

-- SCENARIO 3 | pending_nack_resolution
-- Olympus v3 > DTCC-reflected v2.  RegHub submitted v3 but DTCC returned NACK
-- (rejected).  Needs resubmission before the break resolves.
-- TSR_MSG_ID → V2; REG_RANKING=1 → V3 with NACK.
-- Expected classification: pending_nack_resolution
SELECT
    'cftc', '999003:SC03_PENDING_NACK', 'FCTEST_SC03_UTI_NACK_RESOLUTION_V3',
    '', '', TIMESTAMP('2026-08-17 00:00:00'), 'COMMODITIES',
    'TEST_CP1_FC_MOCK_LEI001', 'TEST_CP2_FC_MOCK_LEI002',
    210.00, TIMESTAMP('2026-01-19 00:00:00'), '>180',
    'OLYMPUS-vs-DTCC-Position-SDR-CFTC-Commodities-Trade-Recon',
    'POSITION', 'RNC1PRD', 'OPEN', 'OLYMPUS', 90000001, 20260817, '',
    'TEST_CP1_FC_MOCK_LEI001TEST_CP2_FC_MOCK_LEI002FCTEST_SC03_UTI_NACK_RESOLUTION_V3999003:SC03_PENDING_NACK',
    1, 1, 9999999,
    3,
    'cftc_fc_mock_SC03_V2_17800020000000000000_TSRSUFFIX'

UNION ALL

-- SCENARIO 4 | non_reportable_review
-- Olympus v3 > DTCC-reflected v2.  RegHub classified v3 as NON_REPORTABLE
-- (excluded from regulatory submission).  Needs review of exclusion reason.
-- TSR_MSG_ID → V2; REG_RANKING=1 → V3 with NON_REPORTABLE.
-- Expected classification: non_reportable_review
SELECT
    'cftc', '999004:SC04_NON_REPORTABLE', 'FCTEST_SC04_UTI_NON_REPORTABLE_V3',
    '', '', TIMESTAMP('2026-08-17 00:00:00'), 'COMMODITIES',
    'TEST_CP1_FC_MOCK_LEI001', 'TEST_CP2_FC_MOCK_LEI002',
    75.00, TIMESTAMP('2026-05-30 00:00:00'), '31-90',
    'OLYMPUS-vs-DTCC-Position-SDR-CFTC-Commodities-Trade-Recon',
    'POSITION', 'RNC1PRD', 'OPEN', 'OLYMPUS', 90000001, 20260817, '',
    'TEST_CP1_FC_MOCK_LEI001TEST_CP2_FC_MOCK_LEI002FCTEST_SC04_UTI_NON_REPORTABLE_V3999004:SC04_NON_REPORTABLE',
    1, 1, 9999999,
    3,
    'cftc_fc_mock_SC04_V2_17800020000000000000_TSRSUFFIX'

UNION ALL

-- SCENARIO 5 | missing_from_source
-- Olympus v3 > DTCC-reflected v2.  RegHub also only has v2 — v3 never arrived
-- in RegHub at all.  Genuine data gap between Olympus and RegHub.
-- TSR_MSG_ID → V2 = REG_RANKING=1 (no newer version in REG at all).
-- Expected classification: missing_from_source
SELECT
    'cftc', '999005:SC05_MISSING_FROM_SRC', 'FCTEST_SC05_UTI_MISSING_V3',
    '', '', TIMESTAMP('2026-08-17 00:00:00'), 'COMMODITIES',
    'TEST_CP1_FC_MOCK_LEI001', 'TEST_CP2_FC_MOCK_LEI002',
    310.00, TIMESTAMP('2025-10-10 00:00:00'), '>180',
    'OLYMPUS-vs-DTCC-Position-SDR-CFTC-Commodities-Trade-Recon',
    'POSITION', 'RNC1PRD', 'OPEN', 'OLYMPUS', 90000001, 20260817, '',
    'TEST_CP1_FC_MOCK_LEI001TEST_CP2_FC_MOCK_LEI002FCTEST_SC05_UTI_MISSING_V3999005:SC05_MISSING_FROM_SRC',
    1, 1, 9999999,
    3,
    'cftc_fc_mock_SC05_V2_17800020000000000000_TSRSUFFIX'  -- V2 IS the latest in REG

UNION ALL

-- SCENARIO 6 | olympus_data_issue
-- BREAK_TYPE=DTCC (over-reporting).  Olympus only at v2, but DTCC's TSR_MSG_ID
-- points to RegHub v3 (which RegHub submitted and DTCC ACK'd).
-- Olympus should have produced v3 but did not — problem is on Olympus's side.
-- TSR_MSG_ID → V3; GRU.version=2 < linked_reg_version=3 → reverse case.
-- Expected classification: olympus_data_issue
SELECT
    'cftc', '999006:SC06_OLYMPUS_ISSUE', 'FCTEST_SC06_UTI_OLYMPUS_BEHIND_V3',
    '', '', TIMESTAMP('2026-08-17 00:00:00'), 'COMMODITIES',
    'TEST_CP1_FC_MOCK_LEI001', 'TEST_CP2_FC_MOCK_LEI002',
    12.00, TIMESTAMP('2026-08-05 00:00:00'), '0-30',
    'OLYMPUS-vs-DTCC-Position-SDR-CFTC-Commodities-Trade-Recon',
    'POSITION', 'RNC1PRD', 'OPEN', 'DTCC', 90000001, 20260817, '',
    'TEST_CP1_FC_MOCK_LEI001TEST_CP2_FC_MOCK_LEI002FCTEST_SC06_UTI_OLYMPUS_BEHIND_V3999006:SC06_OLYMPUS_ISSUE',
    1, 1, 9999999,
    2,                                                  -- olympus_version = 2 (behind)
    'cftc_fc_mock_SC06_V3_17800030000000000000_TSRSUFFIX'  -- links to v3 in REG!
;


-- ─── STEP B: Populate REG test rows ─────────────────────────────────────────
-- Note: reghub_id pattern = cftc_fc_mock_{SCID}_{VERSION}_{TIMESTAMP}
-- TSR linkage: SUBSTR(tsr_msg_id, 1, LENGTH(tsr_msg_id)-10) = reghub_id

INSERT INTO GFOLYREG_WORK.APP_REGHUB_RHOO_CONTROLS_POSITION_REG

-- ── SC01 REG rows ────────────────────────────────────────────────────────────
SELECT 'cftc_fc_mock_SC01_V1_17800010000000000000','NEWT','NEWT','ACK','DEAD_TRANS',
       TIMESTAMP('2026-06-01 10:00:00.000000'),TIMESTAMP('2026-06-01 09:55:00'),
       '999001:SC01_VALUE_MISMATCH',TIMESTAMP('2026-06-01 10:05:00.000000'),'',2,1
UNION ALL
SELECT 'cftc_fc_mock_SC01_V2_17800020000000000000','MODI','MODI','ACK','DEAD_TRANS',
       TIMESTAMP('2026-07-15 14:00:00.000000'),TIMESTAMP('2026-07-15 13:55:00'),
       '999001:SC01_VALUE_MISMATCH',TIMESTAMP('2026-07-15 14:05:00.000000'),'',1,2
-- ^ v2 is REG_RANKING=1 (most recent). TSR also points here. Same version as Olympus.

UNION ALL
-- ── SC02 REG rows ────────────────────────────────────────────────────────────
SELECT 'cftc_fc_mock_SC02_V1_17800010000000000000','NEWT','NEWT','ACK','DEAD_TRANS',
       TIMESTAMP('2026-05-01 10:00:00.000000'),TIMESTAMP('2026-05-01 09:55:00'),
       '999002:SC02_FALSE_BREAK_TIMING',TIMESTAMP('2026-05-01 10:05:00.000000'),'',3,1
UNION ALL
SELECT 'cftc_fc_mock_SC02_V2_17800020000000000000','MODI','MODI','ACK','DEAD_TRANS',
       TIMESTAMP('2026-06-15 14:00:00.000000'),TIMESTAMP('2026-06-15 13:55:00'),
       '999002:SC02_FALSE_BREAK_TIMING',TIMESTAMP('2026-06-15 14:05:00.000000'),'',2,2
-- ^ TSR points here (v2). But v3 below exists with ACK → false break.
UNION ALL
SELECT 'cftc_fc_mock_SC02_V3_17800030000000000000','MODI','MODI','ACK','DEAD_TRANS',
       TIMESTAMP('2026-08-10 09:00:00.000000'),TIMESTAMP('2026-08-10 08:55:00'),
       '999002:SC02_FALSE_BREAK_TIMING',TIMESTAMP('2026-08-10 09:05:00.000000'),'',1,3
-- ^ REG_RANKING=1. v3 ACK'd. Olympus is at v3. → false_break_timing_issue

UNION ALL
-- ── SC03 REG rows ────────────────────────────────────────────────────────────
SELECT 'cftc_fc_mock_SC03_V1_17800010000000000000','NEWT','NEWT','ACK','DEAD_TRANS',
       TIMESTAMP('2026-05-01 10:00:00.000000'),TIMESTAMP('2026-05-01 09:55:00'),
       '999003:SC03_PENDING_NACK',TIMESTAMP('2026-05-01 10:05:00.000000'),'',3,1
UNION ALL
SELECT 'cftc_fc_mock_SC03_V2_17800020000000000000','MODI','MODI','ACK','DEAD_TRANS',
       TIMESTAMP('2026-06-15 14:00:00.000000'),TIMESTAMP('2026-06-15 13:55:00'),
       '999003:SC03_PENDING_NACK',TIMESTAMP('2026-06-15 14:05:00.000000'),'',2,2
UNION ALL
SELECT 'cftc_fc_mock_SC03_V3_17800030000000000000','MODI','MODI','NACK','OPEN',
       TIMESTAMP('2026-08-10 09:00:00.000000'),TIMESTAMP('2026-08-10 08:55:00'),
       '999003:SC03_PENDING_NACK',TIMESTAMP('2026-08-10 09:05:00.000000'),
       'NOAR-AT-SFUL-0001-modi',1,3
-- ^ REG_RANKING=1. v3 NACK'd by DTCC → pending_nack_resolution

UNION ALL
-- ── SC04 REG rows ────────────────────────────────────────────────────────────
SELECT 'cftc_fc_mock_SC04_V1_17800010000000000000','NEWT','NEWT','ACK','DEAD_TRANS',
       TIMESTAMP('2026-05-01 10:00:00.000000'),TIMESTAMP('2026-05-01 09:55:00'),
       '999004:SC04_NON_REPORTABLE',TIMESTAMP('2026-05-01 10:05:00.000000'),'',3,1
UNION ALL
SELECT 'cftc_fc_mock_SC04_V2_17800020000000000000','MODI','MODI','ACK','DEAD_TRANS',
       TIMESTAMP('2026-06-15 14:00:00.000000'),TIMESTAMP('2026-06-15 13:55:00'),
       '999004:SC04_NON_REPORTABLE',TIMESTAMP('2026-06-15 14:05:00.000000'),'',2,2
UNION ALL
SELECT 'cftc_fc_mock_SC04_V3_17800030000000000000','MODI','','NON_REPORTABLE','OPEN',
       NULL,TIMESTAMP('2026-08-10 08:55:00'),
       '999004:SC04_NON_REPORTABLE',TIMESTAMP('2026-08-10 09:05:00.000000'),
       'cftc_ts_code_invalid_rptgPty',1,3
-- ^ REG_RANKING=1. v3 marked NON_REPORTABLE → non_reportable_review

UNION ALL
-- ── SC05 REG rows ────────────────────────────────────────────────────────────
SELECT 'cftc_fc_mock_SC05_V1_17800010000000000000','NEWT','NEWT','ACK','DEAD_TRANS',
       TIMESTAMP('2026-05-01 10:00:00.000000'),TIMESTAMP('2026-05-01 09:55:00'),
       '999005:SC05_MISSING_FROM_SRC',TIMESTAMP('2026-05-01 10:05:00.000000'),'',2,1
UNION ALL
SELECT 'cftc_fc_mock_SC05_V2_17800020000000000000','MODI','MODI','ACK','DEAD_TRANS',
       TIMESTAMP('2026-06-15 14:00:00.000000'),TIMESTAMP('2026-06-15 13:55:00'),
       '999005:SC05_MISSING_FROM_SRC',TIMESTAMP('2026-06-15 14:05:00.000000'),'',1,2
-- ^ REG_RANKING=1 = v2 (same as TSR pointer).  No v3 exists → missing_from_source

UNION ALL
-- ── SC06 REG rows ────────────────────────────────────────────────────────────
SELECT 'cftc_fc_mock_SC06_V1_17800010000000000000','NEWT','NEWT','ACK','DEAD_TRANS',
       TIMESTAMP('2026-05-01 10:00:00.000000'),TIMESTAMP('2026-05-01 09:55:00'),
       '999006:SC06_OLYMPUS_ISSUE',TIMESTAMP('2026-05-01 10:05:00.000000'),'',3,1
UNION ALL
SELECT 'cftc_fc_mock_SC06_V2_17800020000000000000','MODI','MODI','ACK','DEAD_TRANS',
       TIMESTAMP('2026-06-15 14:00:00.000000'),TIMESTAMP('2026-06-15 13:55:00'),
       '999006:SC06_OLYMPUS_ISSUE',TIMESTAMP('2026-06-15 14:05:00.000000'),'',2,2
UNION ALL
SELECT 'cftc_fc_mock_SC06_V3_17800030000000000000','MODI','MODI','ACK','DEAD_TRANS',
       TIMESTAMP('2026-08-10 09:00:00.000000'),TIMESTAMP('2026-08-10 08:55:00'),
       '999006:SC06_OLYMPUS_ISSUE',TIMESTAMP('2026-08-10 09:05:00.000000'),'',1,3
-- ^ REG_RANKING=1 = v3 ACK'd. TSR also points to v3. But GRU.version=2 < 3 → olympus_data_issue
;


-- =============================================================================
-- STEP C: Field Control classification query skeleton (for Drashti's SQL task)
-- Joins GRU + REG, strips TSR suffix, looks up the linked REG record,
-- and classifies into one of the six break reasons.
-- =============================================================================

WITH
-- Derive the REGHUB_ID from TSR_MSG_ID by stripping the 10-char timestamp suffix
gru_with_linked_rhid AS (
    SELECT
        g.*,
        SUBSTR(g.tsr_msg_id, 1, LENGTH(g.tsr_msg_id) - 10) AS linked_reghub_id
    FROM GFOLYREG_WORK.APP_REGHUB_RHOO_CONTROLS_POSITION_GRU g
    WHERE g.recon_id = '##RECON_ID##'
      AND UPPER(g.break_status) = 'OPEN'
),

-- Look up the version that DTCC's TSR message actually reflects
linked_reg AS (
    SELECT r.reghub_id, r.keys_uitid, r.version AS linked_version, r.state_status
    FROM GFOLYREG_WORK.APP_REGHUB_RHOO_CONTROLS_POSITION_REG r
),

-- Fetch the latest RegHub record per UITID (REG_RANKING = 1)
latest_reg AS (
    SELECT r.keys_uitid, r.reghub_id AS latest_reghub_id,
           r.version AS latest_version, r.state_status AS latest_status
    FROM GFOLYREG_WORK.APP_REGHUB_RHOO_CONTROLS_POSITION_REG r
    WHERE r.reg_ranking = 1
)

SELECT
    g.uitid,
    g.version                       AS olympus_version,
    lr.linked_version               AS dtcc_reflected_version,
    ltr.latest_version              AS reghub_latest_version,
    ltr.latest_status               AS reghub_latest_status,
    g.break_type,

    CASE
        -- Same version, value still mismatches → genuine RegHub error
        WHEN g.version = lr.linked_version
         AND g.linked_reghub_id = ltr.latest_reghub_id
            THEN 'value_mismatch_reghub_error'

        -- Olympus ahead, RegHub also has newer version with ACK → timing lag, false break
        WHEN g.version > lr.linked_version
         AND g.linked_reghub_id <> ltr.latest_reghub_id
         AND ltr.latest_status = 'ACK'
            THEN 'false_break_timing_issue'

        -- Olympus ahead, RegHub newer version got NACK'd → needs resubmission
        WHEN g.version > lr.linked_version
         AND g.linked_reghub_id <> ltr.latest_reghub_id
         AND ltr.latest_status = 'NACK'
            THEN 'pending_nack_resolution'

        -- Olympus ahead, RegHub newer version is NON_REPORTABLE → review exclusion
        WHEN g.version > lr.linked_version
         AND g.linked_reghub_id <> ltr.latest_reghub_id
         AND ltr.latest_status = 'NON_REPORTABLE'
            THEN 'non_reportable_review'

        -- Olympus ahead but TSR points to REG's latest (no newer version in RegHub at all)
        WHEN g.version > lr.linked_version
         AND g.linked_reghub_id = ltr.latest_reghub_id
            THEN 'missing_from_source'

        -- RegHub/DTCC is ahead of Olympus → problem on Olympus side
        WHEN g.version < lr.linked_version
            THEN 'olympus_data_issue'

        ELSE 'cannot_be_determined'
    END                             AS field_control_break_reason

FROM gru_with_linked_rhid g
LEFT JOIN linked_reg  lr  ON lr.reghub_id  = g.linked_reghub_id
                          AND lr.keys_uitid = g.uitid
LEFT JOIN latest_reg  ltr ON ltr.keys_uitid = g.uitid
ORDER BY g.uitid;
