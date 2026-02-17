-- ============================================================================
-- CPT Code Denial Intelligence & Recovery Reference
-- Source: OrbDoc research (168 commonly billed CPT codes analyzed),
--         ANSI X12 CARC/RARC standards, CMS/payer denial pattern data
-- Purpose: Map high-denial CPT codes to root causes, CARC/RARC patterns,
--          and actionable recovery workflows
-- ============================================================================

-- ============================================================================
-- 1. REFERENCE TABLE: CARC Denial Code Master with Recovery Classification
-- ============================================================================
-- Group Codes: CO = Contractual Obligation, PR = Patient Responsibility,
--              OA = Other Adjustment, PI = Payer Initiated
-- Denial Type: HARD = typically non-recoverable, SOFT = correctable/resubmittable
-- ============================================================================

CREATE TABLE CARC_Denial_Master (
    CARC_Code           VARCHAR(10)     PRIMARY KEY,
    CARC_Description    VARCHAR(500)    NOT NULL,
    Group_Code          VARCHAR(5),             -- CO, PR, OA, PI
    Denial_Type         VARCHAR(10)     NOT NULL, -- HARD, SOFT
    Denial_Category     VARCHAR(50)     NOT NULL,
    Recovery_Action     VARCHAR(200)    NOT NULL,
    Route_To_Department VARCHAR(100)    NOT NULL,
    Avg_Recovery_Rate   DECIMAL(5,2),           -- Percentage recovered on appeal
    Rework_Cost_USD     DECIMAL(10,2),          -- Average cost to rework per claim
    Priority_Tier       INT             NOT NULL  -- 1=Highest ROI, 2=Medium, 3=Low
);

INSERT INTO CARC_Denial_Master VALUES
-- === CODING & MODIFIER ERRORS (Soft Denials - High Recovery) ===
('4',   'Procedure code inconsistent with modifier or missing required modifier',
        'CO', 'SOFT', 'Coding Error - Modifier',
        'Review modifier usage; correct and resubmit within 48 hours',
        'Coding Review', 78.50, 25.00, 1),

('11',  'Diagnosis inconsistent with procedure code',
        'CO', 'SOFT', 'Coding Error - Dx Mismatch',
        'Verify ICD-10/CPT pairing against LCD/NCD; correct and resubmit',
        'Coding Review', 72.00, 35.00, 1),

('97',  'Payment included in allowance for another service (bundled)',
        'CO', 'SOFT', 'Coding Error - Bundling',
        'Review CCI edits; apply appropriate modifier 59/XE/XS/XP/XU or appeal with documentation',
        'Coding Review', 65.00, 40.00, 1),

('167', 'Diagnosis does not meet medical necessity criteria for CPT code billed',
        'CO', 'SOFT', 'Medical Necessity - Dx/CPT Mismatch',
        'Review LCD/NCD; obtain supporting documentation from provider; appeal with clinical rationale',
        'Coding Review', 60.00, 55.00, 1),

-- === BILLING & INFORMATION ERRORS (Soft Denials - High Recovery) ===
('16',  'Missing or invalid claim information (NPI, dates, demographics)',
        'CO', 'SOFT', 'Billing Error - Missing Info',
        'Identify missing field from RARC; correct and resubmit within 48 hours',
        'Billing/AR Team', 85.00, 18.00, 1),

('B15', 'Payment adjusted because this service was submitted after the coverage ended',
        'CO', 'SOFT', 'Billing Error - Coverage',
        'Verify coverage dates; resubmit with correct dates or appeal with eligibility proof',
        'Billing/AR Team', 55.00, 30.00, 2),

-- === DUPLICATE CLAIMS ===
('18',  'Duplicate claim/service',
        'OA', 'SOFT', 'Duplicate Review',
        'Verify against remit history; if unique service, appeal with documentation of distinct encounter',
        'Duplicate Review', 45.00, 22.00, 2),

-- === ELIGIBILITY & COVERAGE ===
('22',  'Coordination of benefits - another payer is primary',
        'CO', 'SOFT', 'COB/Eligibility',
        'Verify primary/secondary insurance order; resubmit to correct payer',
        'Eligibility/COB Team', 70.00, 28.00, 1),

('27',  'Expenses incurred after coverage terminated',
        'CO', 'HARD', 'Eligibility - Coverage Ended',
        'Verify coverage dates; if error, appeal with eligibility documentation',
        'Eligibility/COB Team', 30.00, 35.00, 3),

('31',  'Patient cannot be identified as our insured',
        'CO', 'SOFT', 'Eligibility - Member ID',
        'Verify demographics and member ID; resubmit with corrected information',
        'Eligibility/COB Team', 65.00, 20.00, 2),

-- === TIMELY FILING ===
('29',  'Time limit for filing has expired',
        'CO', 'HARD', 'Timely Filing',
        'If proof of timely submission exists, appeal immediately; otherwise write-off',
        'Appeals Team', 15.00, 45.00, 3),

-- === MEDICAL NECESSITY ===
('50',  'Non-covered service - not deemed medically necessary',
        'CO', 'SOFT', 'Medical Necessity',
        'Obtain clinical documentation; appeal with peer-reviewed literature and provider attestation',
        'Clinical Appeals', 48.00, 65.00, 2),

('96',  'Non-covered charge - service excluded from benefit plan',
        'CO', 'HARD', 'Non-Covered Service',
        'Verify benefit plan; if coding error, correct and resubmit; otherwise ABN/write-off',
        'Billing/AR Team', 20.00, 30.00, 3),

-- === CREDENTIALING & ENROLLMENT ===
('185', 'Provider not certified/eligible to be paid for this procedure',
        'CO', 'SOFT', 'Credentialing/Enrollment',
        'Verify provider enrollment status; submit enrollment application; retroactive appeal if applicable',
        'Credentialing/Enrollment', 58.00, 75.00, 1),

('109', 'Claim not covered by this payer - submit to correct payer',
        'CO', 'SOFT', 'Provider Enrollment',
        'Verify payer assignment and provider network status; redirect claim',
        'Credentialing/Enrollment', 62.00, 30.00, 1),

-- === AUTHORIZATION ===
('197', 'Precertification/authorization/notification absent',
        'CO', 'SOFT', 'Prior Authorization',
        'Obtain retro-authorization if within payer window; appeal with medical necessity documentation',
        'Authorization Team', 42.00, 55.00, 2),

('222', 'Exceeds plan/contract limit for number of services in allowed period',
        'CO', 'HARD', 'Plan Limits Exceeded',
        'Review benefit limits; appeal with medical necessity if additional services were clinically required',
        'Clinical Appeals', 25.00, 50.00, 3),

-- === PATIENT RESPONSIBILITY ===
('1',   'Deductible amount',
        'PR', 'HARD', 'Patient Responsibility',
        'Route to patient billing; verify deductible accumulator accuracy',
        'Patient Accounts', 5.00, 15.00, 3),

('2',   'Coinsurance amount',
        'PR', 'HARD', 'Patient Responsibility',
        'Route to patient billing; verify coinsurance percentage against contract',
        'Patient Accounts', 5.00, 15.00, 3),

('3',   'Co-payment amount',
        'PR', 'HARD', 'Patient Responsibility',
        'Route to patient billing; collect at point of service',
        'Patient Accounts', 5.00, 10.00, 3),

-- === 2025-2026 NEW CODES ===
('307', 'Adjustment for Medicare Drug Price Negotiation Program',
        'CO', 'HARD', 'Medicare Drug Pricing',
        'Verify pricing under Inflation Reduction Act provisions; apply negotiated rate',
        'Pharmacy/Drug Pricing', 10.00, 20.00, 3),

('308', 'Adjustment based on contracted payment agreement',
        'CO', 'HARD', 'Contracted Rate',
        'Verify contracted rate against fee schedule; dispute if incorrect',
        'Contract Management', 35.00, 25.00, 2);


-- ============================================================================
-- 2. REFERENCE TABLE: RARC Supplemental Codes
-- ============================================================================

CREATE TABLE RARC_Supplemental_Codes (
    RARC_Code           VARCHAR(10)     PRIMARY KEY,
    RARC_Description    VARCHAR(500)    NOT NULL,
    RARC_Type           VARCHAR(20)     NOT NULL, -- Alert, Supplemental
    Action_Required     VARCHAR(200)    NOT NULL,
    Common_CARC_Pairing VARCHAR(50)              -- CARC codes commonly paired with
);

INSERT INTO RARC_Supplemental_Codes VALUES
('M20',  'Missing/incomplete/invalid HCPCS code',
         'Supplemental', 'Review and correct HCPCS/CPT code; resubmit', '4, 16'),
('M27',  'Provider liable for waived charges - services not medically necessary',
         'Alert', 'Verify ABN was obtained; if not, write-off provider liability', '50, 96'),
('M31',  'Missing radiology report',
         'Supplemental', 'Obtain and submit radiology report with corrected claim', '16, 167'),
('M127', 'Medical record missing - submit complete record',
         'Supplemental', 'Request medical record from provider; resubmit with documentation', '16, 50'),
('N130', 'Service excluded per plan benefit documents',
         'Alert', 'Review plan benefits; inform patient of non-covered status', '96, 222'),
('N517', 'Provider did not follow contractual obligations - resubmit with requested info',
         'Supplemental', 'Review contract requirements; resubmit with all required elements', '16, 185'),
('N871', 'No Surprises Act - payment calculated based on state law',
         'Alert', 'Review state surprise billing law; initiate IDR process if applicable', '45, 308'),
('N862', 'Cost-sharing calculated per No Surprises Act provisions',
         'Alert', 'Verify QPA calculation accuracy; initiate IDR if underpaid', '45'),
('N864', 'Claim processed under No Surprises Act emergency services provision',
         'Alert', 'Verify emergency classification; review payment against QPA', '45'),
('MA18', 'Claim requires attachment - submit medical records',
         'Supplemental', 'Submit required attachments per payer specifications', '16, 252'),
('N657', 'Provider must submit supporting documentation per LCD/NCD',
         'Supplemental', 'Obtain and submit LCD/NCD-specific documentation', '50, 167');


-- ============================================================================
-- 3. CPT CODES: High-Denial Risk Procedures with Intelligence
-- Source: OrbDoc Bill Analyzer research across 168 commonly billed CPT codes
-- ============================================================================

CREATE TABLE CPT_Denial_Intelligence (
    CPT_Code            VARCHAR(10)     NOT NULL,
    CPT_Description     VARCHAR(300)    NOT NULL,
    Specialty_Category  VARCHAR(100)    NOT NULL,
    Denial_Risk_Level   VARCHAR(10)     NOT NULL, -- HIGH, MEDIUM, LOW
    Top_CARC_Codes      VARCHAR(50)     NOT NULL, -- Most common denial reasons
    Top_RARC_Codes      VARCHAR(50),
    Denial_Rate_Pct     DECIMAL(5,2),             -- Estimated denial rate
    Common_Root_Cause   VARCHAR(300)    NOT NULL,
    Prevention_Strategy VARCHAR(500)    NOT NULL,
    Recovery_Potential   VARCHAR(10)    NOT NULL,  -- HIGH, MEDIUM, LOW
    PRIMARY KEY (CPT_Code)
);

INSERT INTO CPT_Denial_Intelligence VALUES
-- === EVALUATION & MANAGEMENT (E/M) ===
('99213', 'Office visit - Established patient, low complexity',
         'E/M - Office Visit', 'MEDIUM', '11, 97', 'M20',
         8.50, 'Upcoding from 99212 or underdocumented for 99214 level',
         'Ensure documentation supports MDM complexity level; use time-based billing when applicable',
         'HIGH'),

('99214', 'Office visit - Established patient, moderate complexity',
         'E/M - Office Visit', 'HIGH', '11, 97, 167', 'M20, M127',
         14.20, 'Insufficient documentation of MDM elements; bundling with procedures',
         'Document 2 of 3 MDM elements; separate E/M with modifier 25 when billed with procedure',
         'HIGH'),

('99215', 'Office visit - Established patient, high complexity',
         'E/M - Office Visit', 'HIGH', '11, 50, 167', 'M127, N657',
         18.50, 'Medical necessity challenged; documentation gaps in high-complexity MDM',
         'Document all 3 MDM elements; include clinical rationale for high-complexity decision-making',
         'HIGH'),

('99203', 'Office visit - New patient, low complexity',
         'E/M - Office Visit', 'LOW', '16, 31', 'M20',
         5.00, 'Missing patient demographics or incorrect member ID',
         'Verify eligibility and demographics at registration; ensure new patient status is accurate',
         'HIGH'),

('99204', 'Office visit - New patient, moderate complexity',
         'E/M - Office Visit', 'MEDIUM', '11, 167', 'M20, N657',
         10.00, 'New patient status disputed; diagnosis does not support MDM level',
         'Confirm no visits within 3 years; document comprehensive history and exam',
         'HIGH'),

('99205', 'Office visit - New patient, high complexity',
         'E/M - Office Visit', 'HIGH', '11, 50, 167', 'M127',
         16.80, 'High-complexity MDM not substantiated; medical necessity questioned',
         'Thorough documentation of data reviewed, diagnoses addressed, and risk of management',
         'HIGH'),

-- === TRANSITIONAL CARE & CHRONIC CARE MANAGEMENT ===
('99495', 'Transitional Care Management - moderate complexity (within 14 days)',
         'TCM', 'HIGH', '16, 97, 185', 'N517',
         22.00, 'Missing discharge documentation; contact not made within required timeframe',
         'Document discharge date, contact within 2 business days, face-to-face within 14 days',
         'HIGH'),

('99496', 'Transitional Care Management - high complexity (within 7 days)',
         'TCM', 'HIGH', '16, 97, 185', 'N517, M127',
         25.00, 'Face-to-face visit not within 7 days; incomplete discharge coordination documentation',
         'Document contact within 2 business days, face-to-face within 7 days, medication reconciliation',
         'HIGH'),

('99490', 'Chronic Care Management - 20 minutes clinical staff time',
         'CCM', 'HIGH', '16, 97, 185', 'N517',
         20.00, 'Time not documented; consent not obtained; overlapping with TCM billing period',
         'Document patient consent, ensure 20+ min clinical staff time, no overlap with TCM 30-day window',
         'MEDIUM'),

('99491', 'Chronic Care Management - 30 minutes physician time',
         'CCM', 'HIGH', '16, 97', 'N517',
         18.00, 'Physician time not documented separately from clinical staff time',
         'Document physician-directed time separately; maintain detailed time logs',
         'MEDIUM'),

-- === REMOTE PATIENT MONITORING ===
('99453', 'Remote Patient Monitoring - initial device setup and patient education',
         'RPM', 'HIGH', '16, 185, 96', 'N517',
         28.00, 'Device setup not documented; patient education not substantiated',
         'Document device type, setup process, and patient education session; verify payer coverage',
         'MEDIUM'),

('99454', 'Remote Patient Monitoring - device supply with daily recordings',
         'RPM', 'HIGH', '16, 96, 222', 'N517, N130',
         30.00, 'Insufficient daily readings (requires 16+ days per 30-day period)',
         'Ensure minimum 16 days of data transmission per billing period; document device compliance',
         'MEDIUM'),

('99457', 'Remote Patient Monitoring - clinical staff interactive communication (20 min)',
         'RPM', 'HIGH', '16, 97, 96', 'N517',
         26.00, 'Time threshold not met; interactive communication not documented',
         'Document 20+ minutes of interactive communication; distinguish from non-interactive review',
         'MEDIUM'),

-- === ANNUAL WELLNESS VISITS ===
('G0438', 'Annual Wellness Visit - initial (Welcome to Medicare)',
         'Preventive', 'MEDIUM', '18, 97, 22', NULL,
         12.00, 'Duplicate billing; patient had prior AWV; billed with problem-oriented E/M without modifier',
         'Verify no prior AWV; use modifier 25 if billing with problem-oriented E/M same day',
         'HIGH'),

('G0439', 'Annual Wellness Visit - subsequent',
         'Preventive', 'MEDIUM', '18, 97, 22', NULL,
         11.00, 'Billed within 11 months of prior AWV; documentation missing required elements',
         'Ensure 12-month interval; document health risk assessment, care plan, cognitive screening',
         'HIGH'),

-- === SURGICAL PROCEDURES (High-Value Recovery) ===
('27447', 'Total knee arthroplasty',
         'Orthopedic Surgery', 'HIGH', '197, 50, 167', 'N657, M127',
         15.00, 'Prior authorization not obtained; medical necessity not established per LCD',
         'Obtain prior auth; document failed conservative treatment; include BMI, functional scores',
         'HIGH'),

('29881', 'Arthroscopy, knee, surgical - meniscectomy',
         'Orthopedic Surgery', 'HIGH', '50, 167, 97', 'N657',
         13.00, 'Medical necessity denied; conservative treatment not documented',
         'Document failed conservative treatment (PT, NSAIDs, injections); include MRI findings',
         'HIGH'),

('20610', 'Arthrocentesis/injection - major joint',
         'Orthopedic/Pain Mgmt', 'MEDIUM', '4, 97, 167', 'M20',
         9.00, 'Missing laterality modifier; bundled with E/M without modifier 25',
         'Include laterality modifier (RT/LT); use modifier 25 on E/M; document medical necessity',
         'HIGH'),

-- === DIAGNOSTIC IMAGING ===
('71046', 'Chest X-ray, 2 views',
         'Radiology', 'LOW', '18, 11', 'M31',
         4.00, 'Duplicate order; diagnosis does not support imaging',
         'Verify no duplicate orders; ensure ICD-10 supports chest imaging indication',
         'HIGH'),

('72148', 'MRI lumbar spine without contrast',
         'Radiology', 'HIGH', '197, 50, 167', 'N657, M31',
         16.00, 'Prior auth required; insufficient conservative treatment documented',
         'Obtain prior auth; document 6+ weeks conservative treatment; include red flag symptoms if urgent',
         'HIGH'),

('70553', 'MRI brain with and without contrast',
         'Radiology', 'HIGH', '197, 50, 167', 'N657',
         14.00, 'Prior auth not obtained; diagnosis does not meet LCD criteria',
         'Obtain prior auth; ensure ICD-10 maps to LCD-approved indications; document clinical need',
         'HIGH'),

('73721', 'MRI joint lower extremity without contrast',
         'Radiology', 'HIGH', '197, 167, 4', 'N657',
         13.50, 'Prior auth required; incorrect laterality modifier',
         'Obtain prior auth; specify joint and laterality; document failed conservative treatment',
         'HIGH'),

-- === LABORATORY ===
('80053', 'Comprehensive metabolic panel',
         'Laboratory', 'LOW', '11, 97', 'M20',
         3.00, 'Diagnosis does not support panel; bundled with individual tests',
         'Use diagnosis that supports comprehensive panel; do not bill individual components separately',
         'HIGH'),

('85025', 'Complete blood count (CBC) with differential',
         'Laboratory', 'LOW', '11, 97', 'M20',
         2.50, 'Diagnosis does not support CBC; frequency exceeded',
         'Ensure ICD-10 supports lab order; check frequency limits per payer',
         'HIGH'),

('81001', 'Urinalysis with microscopy',
         'Laboratory', 'LOW', '97, 11', 'M20',
         3.50, 'Bundled with office visit; microscopy not separately reportable',
         'Bill only when medically indicated and documented; verify CCI bundling edits',
         'HIGH'),

-- === PHYSICAL THERAPY / REHABILITATION ===
('97110', 'Therapeutic exercises - 15 min',
         'Physical Therapy', 'MEDIUM', '4, 97, 222', 'M20, N130',
         11.00, 'Units exceed plan limits; bundled with manual therapy; missing GP/GO modifier',
         'Apply therapy cap tracking; use appropriate modifier (GP/GO/GN); document medical necessity per unit',
         'HIGH'),

('97140', 'Manual therapy - 15 min',
         'Physical Therapy', 'MEDIUM', '97, 4, 222', 'M20',
         12.00, 'Bundled with therapeutic exercise; exceeds therapy cap',
         'Verify CCI edits for same-day billing; track therapy cap accumulator; use modifier 59 when appropriate',
         'HIGH'),

('97530', 'Therapeutic activities - 15 min',
         'Physical Therapy', 'MEDIUM', '97, 50, 167', 'M20, N657',
         10.00, 'Medical necessity not established; overlaps with 97110 documentation',
         'Document distinct therapeutic activities separate from exercises; establish functional goals',
         'HIGH'),

-- === BEHAVIORAL HEALTH ===
('90837', 'Psychotherapy - 60 minutes',
         'Behavioral Health', 'MEDIUM', '11, 185, 167', 'N517',
         10.50, 'Provider not credentialed for behavioral health; time not documented',
         'Verify provider credentials with payer; document start/stop times; use correct place of service',
         'HIGH'),

('90834', 'Psychotherapy - 45 minutes',
         'Behavioral Health', 'MEDIUM', '11, 185', 'N517',
         9.00, 'Incorrect time code selected; provider enrollment issue',
         'Document 38-52 minute session for 90834; verify provider behavioral health credentialing',
         'HIGH'),

('90791', 'Psychiatric diagnostic evaluation',
         'Behavioral Health', 'MEDIUM', '18, 185, 11', 'N517',
         11.00, 'Duplicate evaluation billed; provider not credentialed',
         'Verify no prior diagnostic eval on file; confirm provider psychiatric credentials with payer',
         'HIGH'),

-- === CARDIOLOGY ===
('93000', 'Electrocardiogram (ECG) - complete',
         'Cardiology', 'LOW', '97, 18', 'M20',
         5.00, 'Bundled with E/M; duplicate test same day',
         'Use modifier 59 when medically distinct; verify no duplicate orders',
         'HIGH'),

('93306', 'Echocardiography - complete with Doppler',
         'Cardiology', 'MEDIUM', '197, 50, 167', 'N657',
         13.00, 'Prior auth required; medical necessity not established for repeat echo',
         'Obtain prior auth; document change in clinical status; ensure ICD-10 supports echo indication',
         'HIGH'),

-- === DERMATOLOGY ===
('11102', 'Tangential biopsy of skin - first lesion',
         'Dermatology', 'MEDIUM', '4, 11, 97', 'M20',
         8.00, 'Wrong biopsy technique code selected; bundled with destruction/excision',
         'Match biopsy code to technique (tangential vs punch vs incisional); modifier 59 if distinct lesion',
         'HIGH'),

('17000', 'Destruction of premalignant lesion - first lesion',
         'Dermatology', 'LOW', '11, 4', 'M20',
         5.50, 'Diagnosis does not support premalignant destruction; modifier missing for additional lesions',
         'Use ICD-10 for actinic keratosis or premalignant condition; 17003 for 2nd-14th lesions',
         'HIGH');


-- ============================================================================
-- 4. DENIAL PATTERN ANALYTICS VIEW
-- Cross-references CPT denial intelligence with CARC master for operational routing
-- ============================================================================

CREATE VIEW vw_CPT_Denial_Recovery_Intelligence AS
SELECT
    cpt.CPT_Code,
    cpt.CPT_Description,
    cpt.Specialty_Category,
    cpt.Denial_Risk_Level,
    cpt.Denial_Rate_Pct,
    cpt.Common_Root_Cause,
    cpt.Prevention_Strategy,
    cpt.Recovery_Potential,
    carc.CARC_Code,
    carc.CARC_Description,
    carc.Denial_Type,
    carc.Denial_Category,
    carc.Recovery_Action,
    carc.Route_To_Department,
    carc.Avg_Recovery_Rate,
    carc.Rework_Cost_USD,
    carc.Priority_Tier
FROM CPT_Denial_Intelligence cpt
CROSS APPLY STRING_SPLIT(cpt.Top_CARC_Codes, ',') AS split
INNER JOIN CARC_Denial_Master carc
    ON LTRIM(RTRIM(split.value)) = carc.CARC_Code;


-- ============================================================================
-- 5. HIGH-VALUE RECOVERY OPPORTUNITIES VIEW
-- Identifies CPT codes with highest recovery ROI (high denial + high recovery rate)
-- ============================================================================

CREATE VIEW vw_High_Value_Recovery_Targets AS
SELECT
    cpt.CPT_Code,
    cpt.CPT_Description,
    cpt.Specialty_Category,
    cpt.Denial_Rate_Pct,
    cpt.Recovery_Potential,
    carc.CARC_Code,
    carc.Denial_Category,
    carc.Avg_Recovery_Rate,
    carc.Rework_Cost_USD,
    carc.Route_To_Department,
    -- ROI Score: higher denial rate * higher recovery rate = better target
    CAST((cpt.Denial_Rate_Pct * carc.Avg_Recovery_Rate / 100.0) AS DECIMAL(5,2)) AS Recovery_ROI_Score,
    carc.Recovery_Action
FROM CPT_Denial_Intelligence cpt
CROSS APPLY STRING_SPLIT(cpt.Top_CARC_Codes, ',') AS split
INNER JOIN CARC_Denial_Master carc
    ON LTRIM(RTRIM(split.value)) = carc.CARC_Code
WHERE cpt.Recovery_Potential IN ('HIGH', 'MEDIUM')
  AND carc.Denial_Type = 'SOFT'
  AND carc.Priority_Tier <= 2;
