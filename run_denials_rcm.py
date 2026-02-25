#!/usr/bin/env python3
"""Run Denials RCM analysis and export detailed + rollup CSVs.

This script reuses the loader helpers to ensure CPT intelligence is loaded
and then ingests 835 files from the provided directories. It then joins
Claims_Denials with CARC and CPT tables and writes two CSVs:
 - detailed_denials_<ts>.csv
 - rollup_denials_<ts>.csv

Usage:
    python scripts/run_denials_rcm.py --dirs <dir1> <dir2> --db-path scripts/denials_engine.db
"""
import argparse
import os
import csv
import sys
from datetime import datetime
from collections import defaultdict

# ensure local `scripts` sibling imports work when the file is executed directly
# by adding this script's directory to sys.path and importing the loader module
from pathlib import Path
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))
import denials_db_loader as loader


def parse_date_guess(s):
    if not s:
        return None
    s = str(s).strip()
    # common formats: YYYYMMDD, YYYY-MM-DD, YYYY/MM/DD
    try:
        if len(s) == 8 and s.isdigit():
            return datetime.strptime(s, "%Y%m%d")
        if "-" in s:
            return datetime.fromisoformat(s)
        if "/" in s:
            return datetime.strptime(s, "%Y/%m/%d")
    except Exception:
        pass
    # fallback: try ISO parse
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def analyze(conn, outdir):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT d.Claim_ID, d.Payer_ID, d.CPT_Code, d.Group_Code, d.CARC_Code,
               d.RARC_Code, d.Balance_Amount, d.Status, d.Denial_Date, d.Practice_Type,
               cm.Denial_Type, cm.Denial_Category, cm.Avg_Recovery_Rate, cm.Rework_Cost_USD,
               cm.Priority_Tier, cpt.Denial_Risk_Level, cpt.Denial_Rate_Pct, cpt.Recovery_Potential,
               cpt.Top_CARC_Codes, cpt.Top_RARC_Codes, cm.CARC_Description
        FROM Claims_Denials d
        LEFT JOIN CARC_Denial_Master cm ON d.CARC_Code = cm.CARC_Code
        LEFT JOIN CPT_Denial_Intelligence cpt ON d.CPT_Code = cpt.CPT_Code
        WHERE 1=1
        """
    )

    rows = cur.fetchall()

    # ensure Payer_Rules exist; if missing, create defaults for observed payers
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Payer_Rules'")
        if not cur.fetchone():
            cur.execute("CREATE TABLE Payer_Rules (Payer_ID TEXT PRIMARY KEY, Appeal_Deadline_Days INT, Payer_Yield_Rate REAL)")
            # populate defaults from observed payer ids
            payers = sorted({r[1] for r in rows if r[1]})
            for p in payers:
                cur.execute("INSERT OR REPLACE INTO Payer_Rules (Payer_ID, Appeal_Deadline_Days, Payer_Yield_Rate) VALUES (?, ?, ?)", (p, 120, 0.5))
            conn.commit()
    except Exception:
        # ignore if unable to create
        pass

    # load payer rules into memory
    payer_rules = {}
    try:
        for pr in conn.execute("SELECT Payer_ID, Appeal_Deadline_Days, Payer_Yield_Rate FROM Payer_Rules").fetchall():
            payer_rules[pr[0]] = {"appeal_days": int(pr[1] or 120), "yield": float(pr[2] or 0.5)}
    except Exception:
        payer_rules = {}

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(outdir, exist_ok=True)
    detailed_path = os.path.join(outdir, f"detailed_denials_{ts}.csv")
    rollup_path = os.path.join(outdir, f"rollup_denials_{ts}.csv")

    detailed_rows = []
    rollup = defaultdict(lambda: {"count":0,"total_balance":0.0,"expected":0.0,"net":0.0,"sum_rate":0.0,"high_risk":0})

    for r in rows:
        (claim_id,payer,cpt,group,carc,rarc,balance,status,denial_date,practice,
         denial_type,denial_cat,avg_rec_rate,rework_usd,priority_tier,risk_level,denial_rate_pct,recovery_potential, top_carc_codes, top_rarc_codes, carc_description) = r

        balance = float(balance or 0.0)

        # payer rule defaults
        pr = payer_rules.get(payer, {"appeal_days":120,"yield":0.5})

        # expected recovery value based on payer yield
        expected_by_payer = balance * pr.get("yield", 0.5)

        # recovery rate to apply (prefer CARC avg_recovery then CPT denial rate)
        rec_rate = None
        if avg_rec_rate is not None:
            rec_rate = avg_rec_rate
        elif denial_rate_pct is not None:
            rec_rate = denial_rate_pct
        else:
            rec_rate = 50.0

        recovery_value = expected_by_payer * (float(rec_rate) / 100.0)
        rework = float(rework_usd) if rework_usd is not None else 30.0
        net = recovery_value - rework

        # days since denial
        dd = parse_date_guess(denial_date)
        days_since = None
        if dd:
            days_since = (datetime.now() - dd).days

        den_date_iso = dd.isoformat() if dd else ""

        # time sensitivity: critical if within 10 days of appeal deadline
        time_sensitivity = "STANDARD"
        try:
            if days_since is not None and pr and (pr.get("appeal_days") - days_since) <= 10:
                time_sensitivity = "CRITICAL"
        except Exception:
            pass

        # action classification similar to original
        if denial_type == 'SOFT':
            action = 'ACTIONABLE'
        elif denial_type == 'HARD' and (avg_rec_rate or 0) > 25:
            action = 'CONDITIONAL'
        elif denial_type == 'HARD':
            action = 'WRITE-OFF CANDIDATE'
        else:
            action = 'REVIEW'

        detailed_rows.append({
            "Claim_ID": claim_id,
            "Payer_ID": payer,
            "CPT_Code": cpt,
            "Group_Code": group,
            "CARC_Code": carc,
            "RARC_Code": rarc,
            "Balance_Amount": balance,
            "Status": status,
            "Denial_Date": denial_date,
            "Denial_Date_ISO": den_date_iso,
            "Practice_Type": practice,
            "Denial_Type": denial_type,
            "Denial_Category": denial_cat,
            "Avg_Recovery_Rate": avg_rec_rate,
            "Rework_Cost_USD": rework_usd,
            "Priority_Tier": priority_tier,
            "Denial_Risk_Level": risk_level,
            "Denial_Rate_Pct": denial_rate_pct,
            "Recovery_Potential": recovery_potential,
            "Top_CARC_Codes": top_carc_codes,
            "Top_RARC_Codes": top_rarc_codes,
            "CARC_Description": carc_description,
            "Expected_By_Payer": expected_by_payer,
            "Recovery_Value": recovery_value,
            "Net_Recovery_Value": net,
            "Days_Since_Denial": days_since,
            "Time_Sensitivity": time_sensitivity,
            "Action_Classification": action,
            "Payer_Appeal_Days": pr.get("appeal_days"),
        })

        key = cpt or "<unknown>"
        rec = rollup[key]
        rec["count"] += 1
        rec["total_balance"] += balance
        rec["expected"] += recovery_value
        rec["net"] += net
        rec["sum_rate"] += float(rec_rate or 0)
        if risk_level == 'HIGH':
            rec["high_risk"] += 1

    # compute financial priority (rank by expected recovery value)
    # instead of ranking in Python, persist the analysis rows into a temporary
    # table and use SQLite window functions to compute financial priority and
    # per-payer ranks/percentiles. This translates more of the enterprise
    # RCM query into a portable SQLite form.
    cur.executescript("""
    DROP TABLE IF EXISTS analysis_temp;
    CREATE TEMP TABLE analysis_temp (
        Claim_ID TEXT, Payer_ID TEXT, CPT_Code TEXT, Group_Code TEXT, CARC_Code TEXT,
        RARC_Code TEXT, Balance_Amount REAL, Status TEXT, Denial_Date TEXT, Denial_Date_ISO TEXT,
        Practice_Type TEXT, Denial_Type TEXT, Denial_Category TEXT, Avg_Recovery_Rate REAL,
        Rework_Cost_USD REAL, Priority_Tier TEXT, Denial_Risk_Level TEXT, Denial_Rate_Pct REAL,
        Recovery_Potential REAL, Top_CARC_Codes TEXT, Top_RARC_Codes TEXT, CARC_Description TEXT,
        Expected_By_Payer REAL, Recovery_Value REAL, Net_Recovery_Value REAL, Days_Since_Denial INTEGER,
        Time_Sensitivity TEXT, Action_Classification TEXT, Payer_Appeal_Days INTEGER
    );
    """)

    insert_sql = ("INSERT INTO analysis_temp (Claim_ID,Payer_ID,CPT_Code,Group_Code,CARC_Code,RARC_Code,"
                  "Balance_Amount,Status,Denial_Date,Denial_Date_ISO,Practice_Type,Denial_Type,Denial_Category,"
                  "Avg_Recovery_Rate,Rework_Cost_USD,Priority_Tier,Denial_Risk_Level,Denial_Rate_Pct,Recovery_Potential,"
                  "Top_CARC_Codes,Top_RARC_Codes,CARC_Description,Expected_By_Payer,Recovery_Value,Net_Recovery_Value,"
                  "Days_Since_Denial,Time_Sensitivity,Action_Classification,Payer_Appeal_Days) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

    for row in detailed_rows:
        cur.execute(insert_sql, (
            row.get("Claim_ID"), row.get("Payer_ID"), row.get("CPT_Code"), row.get("Group_Code"), row.get("CARC_Code"), row.get("RARC_Code"),
            row.get("Balance_Amount"), row.get("Status"), row.get("Denial_Date"), row.get("Denial_Date_ISO"), row.get("Practice_Type"), row.get("Denial_Type"), row.get("Denial_Category"),
            row.get("Avg_Recovery_Rate"), row.get("Rework_Cost_USD"), row.get("Priority_Tier"), row.get("Denial_Risk_Level"), row.get("Denial_Rate_Pct"), row.get("Recovery_Potential"),
            row.get("Top_CARC_Codes"), row.get("Top_RARC_Codes"), row.get("CARC_Description"), row.get("Expected_By_Payer"), row.get("Recovery_Value"), row.get("Net_Recovery_Value"),
            row.get("Days_Since_Denial"), row.get("Time_Sensitivity"), row.get("Action_Classification"), row.get("Payer_Appeal_Days")
        ))
    conn.commit()

    # windowed selection: overall rank and per-payer rank and percentile
    window_sql = """
    SELECT *,
      ROW_NUMBER() OVER (ORDER BY Expected_By_Payer DESC) AS Financial_Priority,
      ROW_NUMBER() OVER (PARTITION BY COALESCE(Payer_ID,'<unknown>') ORDER BY Expected_By_Payer DESC) AS Rank_In_Payer,
      COUNT(*) OVER (PARTITION BY COALESCE(Payer_ID,'<unknown>')) AS Payer_Count,
      CASE WHEN COUNT(*) OVER (PARTITION BY COALESCE(Payer_ID,'<unknown>'))>1
           THEN (ROW_NUMBER() OVER (PARTITION BY COALESCE(Payer_ID,'<unknown>') ORDER BY Expected_By_Payer DESC)-1)*1.0/
                (COUNT(*) OVER (PARTITION BY COALESCE(Payer_ID,'<unknown>'))-1)
           ELSE 0 END AS Payer_Percentile
    FROM analysis_temp
    ORDER BY Expected_By_Payer DESC
    """

    # execute window function query and extract results
    detailed_rows = []
    window_cur = conn.execute(window_sql)
    cols = [d[0] for d in window_cur.description]
    for r in window_cur.fetchall():
        rowd = dict(zip(cols, r))
        detailed_rows.append(rowd)

    # write detailed CSV
    # write detailed CSV using UTF-8 with BOM for Excel friendliness
    with open(detailed_path, "w", newline='', encoding='utf-8-sig') as fh:
        writer = csv.writer(fh)
        hdr = [
            "Claim_ID","Payer_ID","CPT_Code","Group_Code","CARC_Code","RARC_Code",
            "Balance_Amount","Status","Denial_Date","Denial_Date_ISO","Practice_Type","Denial_Type",
            "Denial_Category","Avg_Recovery_Rate","Rework_Cost_USD","Priority_Tier","Denial_Risk_Level",
            "Denial_Rate_Pct","Recovery_Potential","Top_CARC_Codes","Top_RARC_Codes","CARC_Description","Expected_By_Payer","Recovery_Value","Net_Recovery_Value",
            "Days_Since_Denial","Payer_Appeal_Days","Time_Sensitivity","Action_Classification","Financial_Priority","Rank_In_Payer","Payer_Count","Payer_Percentile"
        ]
        writer.writerow(hdr)
        for row in detailed_rows:
            writer.writerow([
                row["Claim_ID"], row["Payer_ID"], row["CPT_Code"], row["Group_Code"], row["CARC_Code"], row["RARC_Code"],
                f"{row['Balance_Amount']:.2f}", row["Status"], row["Denial_Date"], row.get("Denial_Date_ISO"), row["Practice_Type"], row["Denial_Type"],
                row["Denial_Category"], row["Avg_Recovery_Rate"], row["Rework_Cost_USD"], row.get("Priority_Tier"), row.get("Denial_Risk_Level"),
                row.get("Denial_Rate_Pct"), row.get("Recovery_Potential"), row.get("Top_CARC_Codes"), row.get("Top_RARC_Codes"), row.get("CARC_Description"), f"{row['Expected_By_Payer']:.2f}", f"{row['Recovery_Value']:.2f}", f"{row['Net_Recovery_Value']:.2f}",
                row.get("Days_Since_Denial"), row.get("Payer_Appeal_Days"), row.get("Time_Sensitivity"), row.get("Action_Classification"), row.get("Financial_Priority"), row.get("Rank_In_Payer"), row.get("Payer_Count"), f"{row.get('Payer_Percentile') or 0:.2f}"
            ])

    # write rollup by CPT
    with open(rollup_path, "w", newline='', encoding='utf-8-sig') as fh:
        writer = csv.writer(fh)
        writer.writerow(["CPT_Code","Count","Total_Balance","Expected_Recovery","Net_Recovery","Avg_Denial_Rate","High_Risk_Count"])
        for cpt, stats in sorted(rollup.items(), key=lambda kv: kv[1]["expected"], reverse=True):
            avg_rate = stats["sum_rate"]/stats["count"] if stats["count"] else 0
            writer.writerow([cpt, stats["count"], f"{stats['total_balance']:.2f}", f"{stats['expected']:.2f}", f"{stats['net']:.2f}", f"{avg_rate:.2f}", stats["high_risk"]])

    return detailed_path, rollup_path


def main():
    parser = argparse.ArgumentParser(description="Run Denials RCM engine and export CSVs")
    parser.add_argument("--dirs", nargs='+', default=[
        os.path.join(os.path.dirname(__file__), "..", "test_data", "835_denial_categorization"),
        os.path.join(os.path.dirname(__file__), "..", "test_data", "835_denials"),
    ], help="One or more directories containing 835 denial files")
    parser.add_argument("--db-path", default=os.path.join(os.path.dirname(__file__), "denials_engine.db"), help="SQLite DB path")
    parser.add_argument("--outdir", default=os.path.join(os.path.dirname(__file__), "..", "Results", "Denials_RCM"), help="Output directory for CSVs")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db_path)
    conn = loader.connect_db(db_path)

    # ensure claims schema exists
    conn.execute(loader.CLAIMS_SCHEMA)
    conn.commit()

    # load CPT intelligence from SQL
    sql_path = os.path.join(os.path.dirname(__file__), "cpt_denial_intelligence.sql")
    if os.path.exists(sql_path):
        loader.execute_file_sql(conn, sql_path)

    # ingest all provided directories
    for d in args.dirs:
        if os.path.isdir(d):
            print(f"Ingesting 835 files from {d}...")
            loader.ingest_835_directory(conn, d)
        else:
            print(f"Warning: directory not found: {d}")

    print("Running RCM analysis and exporting CSVs...")
    detailed, rollup = analyze(conn, args.outdir)
    print(f"Wrote detailed CSV: {detailed}")
    print(f"Wrote rollup CSV:   {rollup}")
    conn.close()


if __name__ == '__main__':
    main()
