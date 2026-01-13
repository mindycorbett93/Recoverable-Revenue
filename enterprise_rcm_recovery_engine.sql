/* PROJECT: Enterprise Revenue Recovery & Orchestration 
   AUTHOR: Melinda Corbett, CPC, CPPM, CPB
   PURPOSE: Comprehensive denial routing engine. 
            - Excludes non-recoverable codes (PR 1-3, CO-45, OA-23)
            - Routes OA-18 to Duplicate Review
            - Routes Provider Eligibility (CARC 185) to Credentialing
            - Prioritizes Coding Review for RARC 'M' series
*/

WITH Denial_Base AS (
    SELECT 
        c.claim_id,
        c.payer_id,
        c.carc_code, 
        c.rarc_code,
        c.claim_amount,
        -- Logic: Routing for specific operational fixes
        CASE 
            WHEN c.carc_code = '185' THEN 'Credentialing / Enrollment Review'
            WHEN c.carc_code = 'OA-18' THEN 'Duplicate Review'
            WHEN c.carc_code IN ('16', '22', '97') OR c.rarc_code LIKE 'M%' 
                 THEN 'Coding Review'
            WHEN c.carc_code IN ('19', '20', '21') THEN 'Eligibility / COB'
            ELSE 'Technical / Administrative'
        END AS resolution_path
    FROM Claims_Master AS c
    WHERE c.status = 'Denied'
      -- EXCLUSION: Patient Share, Contractual Adjustments, and Secondary Adjudication
      AND c.carc_code NOT IN ('PR-1', 'PR-2', 'PR-3', 'CO-45', 'OA-23')
      AND c.denial_date >= DATEADD(month, -6, CURRENT_DATE())
)
SELECT 
    resolution_path,
    COUNT(claim_id) AS claim_volume,
    SUM(claim_amount) AS potential_recovery_value,
    -- Provides context for 'last-mile' operational change
    CASE 
        WHEN resolution_path = 'Credentialing / Enrollment Review' THEN 'Fix Enrollment Gap'
        WHEN resolution_path = 'Coding Review' THEN 'Provider Education/Correction'
        ELSE 'Process Automation'
    END AS operational_lever
FROM Denial_Base
GROUP BY 1
ORDER BY potential_recovery_value DESC;
