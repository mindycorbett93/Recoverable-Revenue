# Healthcare RCM: Recoverable Revenue & Denial Orchestration Engine
**Technical Lead:** Melinda Corbett, CPC, CPPM, CPB  
**Target Impact:** Cash Flow Acceleration & A/R Optimization

## 🚀 Project Overview
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

**Data Sources:** OrbDoc Bill Analyzer research (168 CPT codes), ANSI X12 CARC/RARC standards, CMS LCD/NCD guidelines, industry denial pattern analysis.

## 🎓 About the Author
[cite_start]Melinda Corbett is an Executive Transformation Leader with 12+ years of experience in healthcare operations and AI-driven optimization[cite: 5, 50]. [cite_start]She specializes in translating complex aggregate platform data into board-level narratives[cite: 16, 61].
