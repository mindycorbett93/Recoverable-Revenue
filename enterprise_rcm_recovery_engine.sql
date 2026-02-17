-- ============================================================================
-- Enterprise RCM Recovery Engine - Advanced Prioritization Logic
-- Enhanced with CPT Denial Intelligence (see: cpt_denial_intelligence.sql)
-- ============================================================================

WITH DenialFactTable AS (
    SELECT
        d.Claim_ID,
        d.Payer_ID,
        d.CPT_Code,
        d.Group_Code, -- CO, PR, PI, OA
        d.CARC_Code,
        d.RARC_Code,
        d.Balance_Amount,
        p.Appeal_Deadline_Days,
        (d.Balance_Amount * p.Payer_Yield_Rate) AS Expected_Recovery_Value,
        -- CARC-driven denial categories from CARC_Denial_Master
        COALESCE(cm.Denial_Category,
            CASE
                WHEN d.CARC_Code IN ('16', 'B15') THEN 'Billing Error'
                WHEN d.CARC_Code IN ('22', '109') THEN 'Provider Enrollment'
                WHEN d.CARC_Code IN ('1', '2', '3') THEN 'Patient Responsibility'
                WHEN d.CARC_Code IN ('50', '96') THEN 'Non-Covered'
                WHEN d.CARC_Code IN ('4', '11') THEN 'Coding'
                ELSE 'Credentialing'
            END
        ) AS Denial_Category,
        -- Enrichment from CARC master
        cm.Denial_Type,
        cm.Recovery_Action,
        cm.Route_To_Department,
        cm.Avg_Recovery_Rate,
        cm.Rework_Cost_USD,
        cm.Priority_Tier,
        -- CPT-level denial intelligence
        cpt.Denial_Risk_Level       AS CPT_Denial_Risk,
        cpt.Denial_Rate_Pct         AS CPT_Denial_Rate,
        cpt.Common_Root_Cause       AS CPT_Root_Cause,
        cpt.Prevention_Strategy     AS CPT_Prevention,
        cpt.Recovery_Potential      AS CPT_Recovery_Potential
    FROM Claims_Denials d
    JOIN Payer_Rules p ON d.Payer_ID = p.Payer_ID
    LEFT JOIN CARC_Denial_Master cm ON d.CARC_Code = cm.CARC_Code
    LEFT JOIN CPT_Denial_Intelligence cpt ON d.CPT_Code = cpt.CPT_Code
    WHERE d.Status = 'Denied'
      AND d.Denial_Date >= DATEADD(day, -p.Appeal_Deadline_Days, GETDATE())
),
PriorityMatrix AS (
    SELECT *,
        RANK() OVER (ORDER BY Expected_Recovery_Value DESC) AS Financial_Priority,
        CASE
            WHEN DATEDIFF(day, GETDATE(), DATEADD(day, Appeal_Deadline_Days, GETDATE())) < 10 THEN 'CRITICAL'
            ELSE 'STANDARD'
        END AS Time_Sensitivity,
        -- Recovery ROI: balance * recovery probability, minus rework cost
        CAST(
            (Expected_Recovery_Value * ISNULL(Avg_Recovery_Rate, 50) / 100.0)
            - ISNULL(Rework_Cost_USD, 30)
        AS DECIMAL(12,2)) AS Net_Recovery_Value,
        -- Soft denials are actionable; hard denials need special handling
        CASE
            WHEN Denial_Type = 'SOFT' THEN 'ACTIONABLE'
            WHEN Denial_Type = 'HARD' AND Avg_Recovery_Rate > 25 THEN 'CONDITIONAL'
            WHEN Denial_Type = 'HARD' THEN 'WRITE-OFF CANDIDATE'
            ELSE 'REVIEW'
        END AS Action_Classification
    FROM DenialFactTable
)
-- Actionable Work Queue for managers and offshore teams
-- Now enriched with CPT intelligence, recovery actions, and department routing
SELECT
    Claim_ID,
    Payer_ID,
    CPT_Code,
    CARC_Code,
    RARC_Code,
    Group_Code,
    Balance_Amount,
    Expected_Recovery_Value,
    Net_Recovery_Value,
    Denial_Category,
    Denial_Type,
    Action_Classification,
    Route_To_Department,
    Recovery_Action,
    CPT_Denial_Risk,
    CPT_Root_Cause,
    CPT_Prevention,
    Financial_Priority,
    Time_Sensitivity,
    Priority_Tier,
    Avg_Recovery_Rate,
    Rework_Cost_USD
FROM PriorityMatrix
WHERE (Financial_Priority <= 100 OR Time_Sensitivity = 'CRITICAL')
  AND Action_Classification IN ('ACTIONABLE', 'CONDITIONAL', 'REVIEW')
ORDER BY
    Time_Sensitivity DESC,
    Action_Classification ASC,
    Net_Recovery_Value DESC;
