# Healthcare RCM: Recoverable Revenue & CPT Denial Intelligence Engine
**Technical Lead:** Melinda Corbett, CPC, CPPM, CPB  
**Target Impact:** Recoverable Revenue — CPT Denial Intelligence Engine

## 🚀 Project Overview

SQL-driven denial resolution engine for healthcare Revenue Cycle Management (RCM). Ingests EDI 835 remittance files, loads claims into a SQLite database with CARC/RARC code intelligence, and produces detailed and rollup denial analytics.

> **Note:** This group combines the **Recoverable-Revenue** and **CPT_Denial_Intelligence** repositories into a single unified workflow.

## Repository Structure

Recoverable_Revenue/
├── run_denials_rcm.py            # Main entry point — runs end-to-end denial analysis
├── denials_db_loader.py          # SQLite loader: creates/populates denials_engine.db
├── cpt_denial_intelligence.sql   # CPT-level denial pattern queries
├── enterprise_rcm_recovery_engine.sql  # Enterprise recovery analytics queries
└── generators/
    ├── generate_835.py           # EDI 835 test data generator (remittance files)
    ├── generate_835_categorization.py  # Denial-categorized 835 test data
    ├── test_data_commons.py      # Shared data catalog (practice types, CPT codes, payers)
    └── __init__.py
```

## What It Does

1. **Database Initialization** — `denials_db_loader.py` creates a SQLite database (`denials_engine.db`) with tables for claims, CARC codes, RARC codes, CPT intelligence, and payer-specific denial patterns.
2. **835 Ingestion** — Parses EDI 835 remittance files across 16 practice types, extracting claim-level denial data including CARC/RARC codes, billed vs. paid amounts, and adjustment reasons.
3. **Denial Analysis** — `run_denials_rcm.py` joins Claims_Denials with CARC and CPT tables to produce:
   - `detailed_denials_<timestamp>.csv` — Every denied line item with CARC description, recovery potential, and modifier flags
   - `rollup_denials_<timestamp>.csv` — Aggregated denial metrics by practice type, payer, and CARC code

## Prerequisites

- Python 3.12+
- sqlite3 (standard library)
- No additional pip packages required

## Usage

```bash
# Generate test 835 data (if needed)
python generators/generate_835.py
python generators/generate_835_categorization.py

# Run the full denial analysis pipeline
python run_denials_rcm.py --dirs test_data/835_denials test_data/835_denial_categorization

# Specify a custom database path
python run_denials_rcm.py --dirs test_data/835_denials --db-path denials_engine.db
```

## Output

Results are written to `Results/Denials_RCM/`:
- `detailed_denials_<timestamp>.csv`
- `rollup_denials_<timestamp>.csv`

## Cross-Dependencies

- The **837 Validator** group uses `denials_engine.db` (produced by this pipeline) for CPT denial-risk lookups.

This repository contains a SQL-driven framework designed to optimize the Healthcare Revenue Cycle by identifying and routing "Actionable" denials. Unlike standard reporting, this engine utilizes ANSI X12 standards (CARC/RARC) to filter non-recoverable adjustments and route claims to specific operational "fix" departments—such as Credentialing, Coding Review, or RPA Automation.

## 🛠 Technical Architecture
The core logic relies on a hierarchical assessment of claim data:
1. **Exclusion Layer:** Filters out patient responsibility (PR 1-3) and standard contractual adjustments (CO-45) to focus on net recoverable revenue.
2. **CARC/RARC Mapping:** Evaluates the relationship between Claim Adjustment Reason Codes and Remittance Advice Remark Codes to define the "last mile" of the fix.
3. **Operational Routing:**
    - [cite_start]**CARC 185:** Routed to **Credentialing/Enrollment** to address provider eligibility gaps[cite: 112, 159].
    - [cite_start]**CARC 16/97 + RARC M-Series:** Routed to **Coding Review** for clinical correction[cite: 68, 205].
    - **CARC OA-18:** Routed to **Duplicate Review** to prevent redundant processing.

## 📂 Files
- `enterprise_rcm_recovery_engine.sql`: The primary orchestration script with CPT-enriched prioritization logic.
- `cpt_denial_intelligence.sql`: CPT code denial intelligence reference — CARC/RARC master tables, 30+ high-denial CPT codes with root causes, prevention strategies, and recovery routing.
- [cite_start]`/docs/Product_Vision.md`: Strategic roadmap for scaling this engine to support **300% platform growth**[cite: 18, 150].

## 📊 CPT Denial Intelligence Module
The `cpt_denial_intelligence.sql` module provides:
1. **CARC Denial Master** — 22 denial codes classified by type (SOFT/HARD), recovery rate, rework cost, priority tier, and department routing.
2. **RARC Supplemental Codes** — 11 remark codes with action-required guidance and common CARC pairings.
3. **CPT Denial Intelligence** — 30+ high-denial-risk CPT codes across E/M, TCM, CCM, RPM, AWV, Surgery, Radiology, Lab, PT, Behavioral Health, Cardiology, and Dermatology with denial rates, root causes, and prevention strategies.
4. **Recovery Analytics Views** — `vw_CPT_Denial_Recovery_Intelligence` and `vw_High_Value_Recovery_Targets` for operational dashboards.

## 🎓 About the Author
[cite_start]Melinda Corbett is an Executive Transformation Leader with 12+ years of experience in healthcare operations and AI-driven optimization[cite: 5, 50]. [cite_start]She specializes in translating complex aggregate platform data into board-level narratives[cite: 16, 61].
