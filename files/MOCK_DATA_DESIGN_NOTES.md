# Field Control Mock Data — Design Notes
**File**: `MOCK_DATA_DESIGN_NOTES.md`  
**Author**: Data Engineering  
**Date**: 2026-08-17  
**RECON_ID (test)**: `9999999`  (production: `9774339` — kept separate so test rows are never confused with real data)

---

## 1. Purpose

This document explains the design decisions behind the two mock data files:
- `APP_REGHUB_RHOO_CONTROLS_POSITION_GRU_mock.csv` — 6 rows (one per scenario)
- `APP_REGHUB_RHOO_CONTROLS_POSITION_REG_mock.csv` — 16 rows (1–3 per scenario)

The mock data is intended to drive development and unit-testing of the Field Control break-classification SQL. Each row is engineered so that the classification logic produces one and only one correct `field_control_break_reason` label per scenario.

---

## 2. Schema Additions (New Columns)

The existing GRU and REG table schemas do not carry the version or TSR linkage information that Field Control classification requires. Two columns have been added to the mock tables that will need to be sourced or derived when the SQL is productionised.

### GRU table — two new columns appended

| Column | Type | Notes |
|--------|------|-------|
| `version` | INTEGER | Unity Version of the trade as Olympus currently sees it. Increments each time the trade is amended (NEWT=1, first MODI=2, second MODI=3, …). In production this will be sourced from the Olympus ledger item audit field. |
| `tsr_msg_id` | VARCHAR | DTCC-side message identifier. Pattern: `{reghub_id}_TSRSUFFIX` (the last 10 characters are always the DTCC-appended suffix). Stripping the suffix reveals the `reghub_id` that reported this version to DTCC. In production this comes from the TSR feed. |

### REG table — one new column appended

| Column | Type | Notes |
|--------|------|-------|
| `version` | INTEGER | Unity Version this REG row was submitted for. Matches the GRU `version` when the two systems are in sync. |

---

## 3. TSR_MSG_ID Linkage Mechanics

The central join logic for Field Control is:

```
linked_reghub_id  =  SUBSTR(tsr_msg_id, 1, LENGTH(tsr_msg_id) - 10)
```

This strips the last 10 characters (`_TSRSUFFIX` in mock data) to recover the `reghub_id` that reported the DTCC-reflected version. That `reghub_id` can then be looked up in the REG table to find **which version** DTCC is aware of, and compared against:
- `gru.version` — what Olympus currently believes
- `reg.version WHERE reg_ranking = 1` — what RegHub most recently submitted

Example (SC02):
```
tsr_msg_id  =  cftc_fc_mock_SC02_V2_17800020000000000000_TSRSUFFIX
linked_reghub_id  =  cftc_fc_mock_SC02_V2_17800020000000000000   ← V2
latest reg (reg_ranking=1).reghub_id  =  cftc_fc_mock_SC02_V3_17800030000000000000  ← V3
→  linked_reghub_id ≠ latest → Olympus is ahead of DTCC
→  latest.state_status = ACK → false_break_timing_issue
```

---

## 4. Classification Decision Tree

```
                         START
                           │
              ┌────────────▼────────────┐
              │  gru.version == version │
              │  of TSR-linked REG row? │
              └────────────┬────────────┘
                   YES     │     NO
                   │       └────────────────────────────┐
                   ▼                                    ▼
    ┌──────────────────────────┐       ┌───────────────────────────┐
    │  TSR → REG_RANKING=1?   │       │  gru.version > linked?    │
    └────────────┬─────────────┘       └──────────┬────────────────┘
          YES    │                        YES      │      NO
          │      │                        │        └──────► olympus_data_issue
          ▼      │                        ▼
  value_mismatch │              ┌──────────────────────────┐
  _reghub_error  │              │  linked_reghub_id ==     │
                 │ (no branch)  │  REG_RANKING=1 reghub_id?│
                 │              └──────────┬───────────────┘
                 │                  YES    │      NO
                 │                  │      │
                 │                  ▼      ▼
                 │    missing_  Check latest_reg.state_status
                 │    from_     ├── ACK          → false_break_timing_issue
                 │    source    ├── NACK         → pending_nack_resolution
                 │              └── NON_REPORTABLE→ non_reportable_review
```

---

## 5. Scenario-by-Scenario Breakdown

---

### SC01 — `value_mismatch_reghub_error`

**Narrative**: Olympus and RegHub are on the same version (V2). DTCC's TSR message also points back to V2. Yet DTCC still shows a break — meaning the *value* of a reportable field differs between what RegHub submitted and what DTCC recorded. This is a data quality issue in the V2 submission itself.

**GRU row** (`uitid = 999001:SC01_VALUE_MISMATCH`):
| Field | Value | Reason |
|-------|-------|--------|
| `break_type` | `OLYMPUS` | Under-reporting direction |
| `version` | `2` | Olympus is at V2 |
| `tsr_msg_id` | `…SC01_V2…_TSRSUFFIX` | DTCC's message was submitted at V2 |

**REG rows** (2 rows for uitid `999001:SC01_VALUE_MISMATCH`):
| reghub_id | version | state_status | reg_ranking |
|-----------|---------|--------------|-------------|
| `…SC01_V2…` | 2 | ACK | 1 (latest) |
| `…SC01_V1…` | 1 | ACK | 2 |

**Classification logic**:
- `linked_reghub_id` = `…SC01_V2…` (strip suffix → V2)
- `gru.version` (2) == `linked_reg.version` (2) → versions match ✓
- `linked_reghub_id` == `reg_ranking=1 reghub_id` (V2 is also the latest) ✓
- → **`value_mismatch_reghub_error`** ✓

---

### SC02 — `false_break_timing_issue`

**Narrative**: Olympus has processed a second amendment, bumping to V3. RegHub has already submitted V3 and it is ACK'd. But DTCC's TSR message still references V2 — DTCC hasn't yet processed RegHub's V3 message. This is a timing lag; the break will self-heal once DTCC processes the V3 message. No action needed.

**GRU row** (`uitid = 999002:SC02_FALSE_BREAK_TIMING`):
| Field | Value | Reason |
|-------|-------|--------|
| `break_type` | `OLYMPUS` | Under-reporting direction |
| `version` | `3` | Olympus is at V3 |
| `tsr_msg_id` | `…SC02_V2…_TSRSUFFIX` | DTCC's last-known message was V2 |

**REG rows** (3 rows for uitid `999002:SC02_FALSE_BREAK_TIMING`):
| reghub_id | version | state_status | reg_ranking |
|-----------|---------|--------------|-------------|
| `…SC02_V3…` | 3 | ACK | 1 (latest) |
| `…SC02_V2…` | 2 | ACK | 2 |
| `…SC02_V1…` | 1 | ACK | 3 |

**Classification logic**:
- `linked_reghub_id` = `…SC02_V2…` (V2)
- `gru.version` (3) > `linked_reg.version` (2) → Olympus is ahead ✓
- `linked_reghub_id` (V2) ≠ `reg_ranking=1 reghub_id` (V3) → RegHub has moved on ✓
- `latest_reg.state_status` = ACK → **`false_break_timing_issue`** ✓

---

### SC03 — `pending_nack_resolution`

**Narrative**: Same timing situation as SC02 — Olympus at V3, DTCC reflects V2. RegHub submitted V3 but it came back NACK'd. The break is not a timing lag; action is required to resolve the NACK before DTCC will clear.

**GRU row** (`uitid = 999003:SC03_PENDING_NACK`):
| Field | Value | Reason |
|-------|-------|--------|
| `break_type` | `OLYMPUS` | Under-reporting direction |
| `version` | `3` | Olympus is at V3 |
| `tsr_msg_id` | `…SC03_V2…_TSRSUFFIX` | DTCC's last message was V2 |

**REG rows** (3 rows for uitid `999003:SC03_PENDING_NACK`):
| reghub_id | version | state_status | reason_codes_str | reg_ranking |
|-----------|---------|--------------|-----------------|-------------|
| `…SC03_V3…` | 3 | NACK | `NOAR-AT-SFUL-0001-modi` | 1 (latest) |
| `…SC03_V2…` | 2 | ACK | | 2 |
| `…SC03_V1…` | 1 | ACK | | 3 |

**Classification logic**:
- `linked_reghub_id` = `…SC03_V2…` (V2)
- `gru.version` (3) > `linked_reg.version` (2) → Olympus is ahead ✓
- `linked_reghub_id` (V2) ≠ `reg_ranking=1 reghub_id` (V3) → RegHub has moved on ✓
- `latest_reg.state_status` = NACK → **`pending_nack_resolution`** ✓

---

### SC04 — `non_reportable_review`

**Narrative**: Olympus at V3, DTCC reflects V2. RegHub tried to submit V3 but the submission was flagged NON_REPORTABLE (e.g., invalid reporting party code). Someone needs to review whether the trade is actually reportable or whether the NON_REPORTABLE flag itself is incorrect.

**GRU row** (`uitid = 999004:SC04_NON_REPORTABLE`):
| Field | Value | Reason |
|-------|-------|--------|
| `break_type` | `OLYMPUS` | Under-reporting direction |
| `version` | `3` | Olympus is at V3 |
| `tsr_msg_id` | `…SC04_V2…_TSRSUFFIX` | DTCC's last message was V2 |

**REG rows** (3 rows for uitid `999004:SC04_NON_REPORTABLE`):
| reghub_id | version | state_status | reason_codes_str | reg_ranking |
|-----------|---------|--------------|-----------------|-------------|
| `…SC04_V3…` | 3 | NON_REPORTABLE | `cftc_ts_code_invalid_rptgPty` | 1 (latest) |
| `…SC04_V2…` | 2 | ACK | | 2 |
| `…SC04_V1…` | 1 | ACK | | 3 |

**Classification logic**:
- `linked_reghub_id` = `…SC04_V2…` (V2)
- `gru.version` (3) > `linked_reg.version` (2) → Olympus is ahead ✓
- `linked_reghub_id` (V2) ≠ `reg_ranking=1 reghub_id` (V3) → RegHub has moved on ✓
- `latest_reg.state_status` = NON_REPORTABLE → **`non_reportable_review`** ✓

---

### SC05 — `missing_from_source`

**Narrative**: Olympus is at V3, but RegHub has only ever seen V2. The V2 TSR message is also RegHub's latest (REG_RANKING=1). RegHub has no knowledge of V3 at all — Olympus data never fed through. This is a sourcing gap and requires investigation of the feed between Olympus and RegHub.

**GRU row** (`uitid = 999005:SC05_MISSING_FROM_SRC`):
| Field | Value | Reason |
|-------|-------|--------|
| `break_type` | `OLYMPUS` | Under-reporting direction |
| `version` | `3` | Olympus is at V3 |
| `tsr_msg_id` | `…SC05_V2…_TSRSUFFIX` | DTCC's last message was V2 |

**REG rows** (2 rows — **no V3 row exists**):
| reghub_id | version | state_status | reg_ranking |
|-----------|---------|--------------|-------------|
| `…SC05_V2…` | 2 | ACK | 1 (latest) |
| `…SC05_V1…` | 1 | ACK | 2 |

**Classification logic**:
- `linked_reghub_id` = `…SC05_V2…` (V2)
- `gru.version` (3) > `linked_reg.version` (2) → Olympus is ahead ✓
- `linked_reghub_id` (V2) **==** `reg_ranking=1 reghub_id` (V2 is still the latest) ✓
- → **`missing_from_source`** ✓

**Key distinction vs SC02–SC04**: In SC02–SC04, `reg_ranking=1` is V3 (RegHub submitted V3). In SC05, `reg_ranking=1` is still V2 — RegHub never received or submitted V3.

---

### SC06 — `olympus_data_issue`

**Narrative**: Olympus is at V2, but the TSR message from DTCC references V3 — meaning DTCC has a version *higher* than what Olympus holds. This is the reverse direction: Olympus is *behind*, not DTCC. RegHub submitted V3 (ACK'd), but Olympus never recorded that amendment. Olympus data is stale or missing an event. BREAK_TYPE is `DTCC` (over-reporting).

**GRU row** (`uitid = 999006:SC06_OLYMPUS_ISSUE`):
| Field | Value | Reason |
|-------|-------|--------|
| `break_type` | `DTCC` | Over-reporting direction (DTCC has more than Olympus) |
| `version` | `2` | Olympus is only at V2 |
| `tsr_msg_id` | `…SC06_V3…_TSRSUFFIX` | DTCC's message was for V3 |

**REG rows** (3 rows for uitid `999006:SC06_OLYMPUS_ISSUE`):
| reghub_id | version | state_status | reg_ranking |
|-----------|---------|--------------|-------------|
| `…SC06_V3…` | 3 | ACK | 1 (latest) |
| `…SC06_V2…` | 2 | ACK | 2 |
| `…SC06_V1…` | 1 | ACK | 3 |

**Classification logic**:
- `linked_reghub_id` = `…SC06_V3…` (V3)
- `gru.version` (2) **<** `linked_reg.version` (3) → DTCC is ahead of Olympus ✓
- → **`olympus_data_issue`** ✓

---

## 6. REGHUB_ID Naming Convention

All mock `reghub_id` values follow this pattern:

```
cftc_fc_mock_{SCENARIO}_{VERSION}_{SEQUENCE}
                                  └── 20-digit zero-padded sequence
                                      (distinct per version, monotonically increasing per scenario)
```

Examples:
```
cftc_fc_mock_SC01_V1_17800010000000000000  ← SC01, V1
cftc_fc_mock_SC01_V2_17800020000000000000  ← SC01, V2
cftc_fc_mock_SC02_V3_17800030000000000000  ← SC02, V3
```

`TSR_MSG_ID = reghub_id + "_TSRSUFFIX"` where `_TSRSUFFIX` is exactly 10 characters.

Strip: `SUBSTR(tsr_msg_id, 1, LENGTH(tsr_msg_id) - 10)` → recovers `reghub_id`.

---

## 7. Verification Results

The following Python-based classification was run against the mock CSV data and all 6 scenarios passed:

```
SC01: version(2)==linked_version(2), linked_reghub_id==reg_rank1  → value_mismatch_reghub_error    ✓
SC02: version(3)>linked(2), linked≠reg_rank1, latest=ACK          → false_break_timing_issue        ✓
SC03: version(3)>linked(2), linked≠reg_rank1, latest=NACK         → pending_nack_resolution         ✓
SC04: version(3)>linked(2), linked≠reg_rank1, latest=NON_REPORTABLE→ non_reportable_review          ✓
SC05: version(3)>linked(2), linked==reg_rank1                     → missing_from_source             ✓
SC06: version(2)<linked(3)                                         → olympus_data_issue              ✓
```

---

## 8. Files Reference

| File | Description |
|------|-------------|
| `APP_REGHUB_RHOO_CONTROLS_POSITION_GRU_mock.csv` | 6 GRU rows, one per scenario |
| `APP_REGHUB_RHOO_CONTROLS_POSITION_REG_mock.csv` | 16 REG rows, 1–3 per scenario |
| `FIELD_CONTROL_UNION_WORKTABLE.sql` | STEP A: GRU inserts, STEP B: REG inserts, STEP C: classification SQL skeleton |
| `MOCK_DATA_DESIGN_NOTES.md` | This document |

---

## 9. Implementation Notes for SQL Developer

When porting the classification logic to production SQL:

1. **`version` column in GRU**: source from `ITEM_CURRENCY_XX` (TBD — confirm which Olympus field carries Unity Version for CFTC Commodities).
2. **`tsr_msg_id` column in GRU**: source from TSR feed join — the field currently missing per §4 of the requirement document.
3. **`version` column in REG**: derive from `KEYS_UITID` suffix or join back to Olympus ledger on `REGHUB_ID`.
4. The 10-character suffix strip (`LENGTH - 10`) must match the actual TSR suffix length in production — confirm with the TSR feed spec.
5. The `FIELD_CONTROL_UNION_WORKTABLE.sql` STEP C CTE skeleton can be dropped directly into the STEP3/STEP4 branch of the existing pipeline as an additional classification layer after `BREAK_RANKING` de-duplication.
