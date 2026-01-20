-- Advanced Prioritization Logic (Not just a scraper) [Image 2]
WITH DenialFactTable AS (
    SELECT 
        d.Claim_ID,
        d.Payer_ID,
        d.Group_Code, -- CO, PR, PI, OA
        d.CARC_Code,
        d.Balance_Amount,
        p.Appeal_Deadline_Days,
        (d.Balance_Amount * p.Payer_Yield_Rate) AS Expected_Recovery_Value,
        -- Targeted Denial Categories
        CASE 
            WHEN d.CARC_Code IN ('16', 'B15') THEN 'Billing Error'
            WHEN d.CARC_Code IN ('22', '109') THEN 'Provider Enrollment'
            WHEN d.CARC_Code IN ('1', '2', '3') THEN 'Patient Responsibility'
            WHEN d.CARC_Code IN ('50', '96') THEN 'Non-Covered'
            WHEN d.CARC_Code IN ('4', '11') THEN 'Coding'
            ELSE 'Credentialing'
        END AS Denial_Category
    FROM Claims_Denials d
    JOIN Payer_Rules p ON d.Payer_ID = p.Payer_ID
    WHERE d.Status = 'Denied'
      AND d.Denial_Date >= DATEADD(day, -p.Appeal_Deadline_Days, GETDATE())
),
PriorityMatrix AS (
    SELECT *,
        RANK() OVER (ORDER BY Expected_Recovery_Value DESC) as Financial_Priority,
        CASE 
            WHEN DATEDIFF(day, GETDATE(), DATEADD(day, Appeal_Deadline_Days, GETDATE())) < 10 THEN 'CRITICAL'
            ELSE 'STANDARD'
        END AS Time_Sensitivity
    FROM DenialFactTable
)
-- Actionable Work Queue for managers and offshore teams [3]
SELECT * FROM PriorityMatrix 
WHERE Financial_Priority <= 100 OR Time_Sensitivity = 'CRITICAL'
ORDER BY Time_Sensitivity DESC, Financial_Priority ASC;
