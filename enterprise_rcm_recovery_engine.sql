-- Advanced Prioritization Logic (Not just a scraper)
WITH DenialFactTable AS (
    SELECT 
        d.Claim_ID,
        d.Payer_ID,
        d.Group_Code, -- CO, PR, PI, OA
        d.CARC_Code,
        d.Balance_Amount,
        p.Appeal_Deadline_Days,
        (d.Balance_Amount * p.Payer_Yield_Rate) AS Expected_Recovery_Value
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
-- Actionable Work Queue for offshore billers (Image 1 logic)
SELECT * FROM PriorityMatrix 
WHERE Financial_Priority <= 100 OR Time_Sensitivity = 'CRITICAL'
ORDER BY Time_Sensitivity DESC, Financial_Priority ASC;
