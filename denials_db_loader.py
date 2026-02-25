#!/usr/bin/env python3
"""
Local SQLite loader for the denials engine.

Creates `denials_engine.db` in the workspace and populates it with:

* CARC_Denial_Master and CPT_Denial_Intelligence
  (loaded from `cpt_denial_intelligence.sql` with logic to avoid
   duplicate definitions and SQL Server-specific constructs)
* Claims_Denials                   (parsed from 835 denial files)

Running the loader multiple times will always refresh the CPT table
(fresh copy of the file), automatically skip the CARC section if the
master table already exists, trim off unsupported view definitions, and
report counts after each load.  This makes the database portable and
self-contained for downstream queries.

The loader is designed to be portable; point it at any folder of
subdirectories containing 835 files (e.g. test_data/835_denials) and it
will recursively parse every .edi file it finds.  Practice type is not
required by the database schema, but the loader will add it for
reference if present in the path.

Usage:
    python scripts/denials_db_loader.py [--input-dir DIR] [--db-path PATH]

The default input directory is `test_data/835_denials` relative to the
workspace root; the default database name is `denials_engine.db` in the
same folder as this script.

Note: the companion SQL file `enterprise_rcm_recovery_engine.sql` is a
reference query containing T-SQL constructs that are not directly
runnable in SQLite; it is not executed by the loader.  You can open it
in a text editor or translate parts of it manually if needed.
"""

import argparse
import os
import re
import sqlite3
from datetime import datetime

# ---------------------------------------------------------------------------
# schema definitions
# ---------------------------------------------------------------------------

CLAIMS_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS Claims_Denials (
    Claim_ID TEXT PRIMARY KEY,
    Payer_ID TEXT,
    CPT_Code TEXT,
    Group_Code TEXT,
    CARC_Code TEXT,
    RARC_Code TEXT,
    Balance_Amount REAL,
    Status TEXT,
    Denial_Date TEXT,
    Practice_Type TEXT
);
"""

# ---------------------------------------------------------------------------
# utility helpers
# ---------------------------------------------------------------------------

def connect_db(db_path):
    conn = sqlite3.connect(db_path)
    return conn


def execute_file_sql(conn, sql_path):
    """Execute all statements from a .sql file using executescript.

    This handles multiline INSERTs and comments correctly. Unsupported
    constructs (e.g. SQL Server-specific views) will still raise errors
    which are logged but do not stop the loader.

    For the CPT intelligence file we trim off any view definitions so that
    a later parse error (e.g. CROSS APPLY/STRING_SPLIT) does not rollback the
    earlier table/insert operations.  After execution we optionally report the
    row count for verification.
    """
    with open(sql_path, "r", encoding="utf-8") as fh:
        content = fh.read()

    # if we're loading the CPT intelligence script, we may need to strip out
    # the CARC master section (it's also defined by the enterprise RCM script)
    # and drop everything after the first CREATE VIEW (views aren't supported)
    base = os.path.basename(sql_path).lower()
    if base.startswith("cpt_"):
        # start each run fresh by removing any existing CPT table; the file is
        # authoritative and we don't want duplicate entries upon reloading.
        conn.execute("DROP TABLE IF EXISTS CPT_Denial_Intelligence")
        conn.commit()

        # if CARC table already exists, remove the leading portion up through the
        # CPT table definition so we don't attempt to recreate or re-insert it.
        try:
            has_carc = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='CARC_Denial_Master'"
            ).fetchone()
        except sqlite3.Error:
            has_carc = None
        if has_carc:
            idx2 = content.upper().find("CREATE TABLE CPT_DENIAL_INTELLIGENCE")
            if idx2 != -1:
                print(f"[INFO] skipping CARC section in {sql_path} because CARC_Denial_Master already exists")
                content = content[idx2:]
        # if the CPT table already exists we don't want a duplicate CREATE
        # statement to abort the script; convert to IF NOT EXISTS so the
        # following INSERTs will still run.  We apply this transformation
        # after we've potentially stripped the CARC portion above.
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='CPT_Denial_Intelligence'").fetchone():
            # case-insensitive replace of the first occurrence
            content = re.sub(r"(?i)CREATE\s+TABLE\s+CPT_Denial_Intelligence", 
                             "CREATE TABLE IF NOT EXISTS CPT_Denial_Intelligence", content, count=1)
            print(f"[INFO] modified CPT create to IF NOT EXISTS to avoid error")

        # drop view definitions as before
        idx = content.upper().find("CREATE VIEW")
        if idx != -1:
            print(f"[INFO] trimming content of {sql_path} at first CREATE VIEW")
            content = content[:idx]

    try:
        conn.executescript(content)
    except sqlite3.Error as e:
        # log and continue
        print(f"[WARN] error executing script {sql_path}: {e}")
    conn.commit()

    # report number of rows if table exists
    if base.startswith("cpt_"):
        try:
            cnt = conn.execute("SELECT count(*) FROM CPT_Denial_Intelligence").fetchone()[0]
            print(f"[INFO] CPT_Denial_Intelligence row count after load: {cnt}")
        except sqlite3.Error:
            pass


def parse_835_file(filepath, practice_type=None):
    """Parse an EDI 835 file and return a list of denial dicts."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        raw = fh.read().replace("\n", "")  # EDI may use tilde as terminator
    segments = raw.split("~")
    current = None
    payer = None
    results = []

    for seg in segments:
        parts = seg.split("*")
        if not parts or not parts[0]:
            continue
        tag = parts[0]
        if tag == "N1" and len(parts) > 2 and parts[1] == "PR":
            payer = parts[2]
        elif tag == "CLP":
            # flush previous
            if current:
                results.append(current)
            current = {
                "Claim_ID": parts[1] if len(parts) > 1 else "",
                "Status": parts[2] if len(parts) > 2 else "",
                "Balance_Amount": float(parts[3]) if len(parts) > 3 and parts[3] else 0.0,
                "Payer_ID": payer or "",
                "CPT_Code": "",
                "CARC_Code": "",
                "RARC_Code": "",
                "Denial_Date": "",
                "Practice_Type": practice_type or "",
            }
        elif tag == "CAS" and current:
            # CAS*<group>*<code>*<amt>*<qty>
            if len(parts) >= 3:
                code = parts[2]
                # assume first CAS is CARC; subsequent may be RARC
                if not current.get("CARC_Code"):
                    current["CARC_Code"] = code
                elif not current.get("RARC_Code"):
                    current["RARC_Code"] = code
        elif tag == "SVC" and current:
            # SVC*HC:<CPT>...
            if len(parts) > 1 and parts[1].startswith("HC:"):
                cpt = parts[1].split(":")[1]
                if not current.get("CPT_Code"):
                    current["CPT_Code"] = cpt
        elif tag == "DTM" and current:
            # record denial date from common qualifiers
            if len(parts) >= 3 and parts[1] in ("232", "233"):
                current["Denial_Date"] = parts[2]
    if current:
        results.append(current)
    return results


def ingest_835_directory(conn, root_dir):
    """Walk subdirectories and insert parsed claims into database."""
    cursor = conn.cursor()
    for subdir, dirs, files in os.walk(root_dir):
        practice_type = os.path.basename(subdir)
        for fname in files:
            if not fname.lower().endswith(".edi"):
                continue
            path = os.path.join(subdir, fname)
            denials = parse_835_file(path, practice_type)
            for d in denials:
                # insert or replace
                cols = ",".join(d.keys())
                placeholders = ",".join("?" for _ in d)
                vals = tuple(d.values())
                cursor.execute(
                    f"INSERT OR REPLACE INTO Claims_Denials ({cols}) VALUES ({placeholders})",
                    vals,
                )
    conn.commit()


# ============================================================================
# main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Load denials data into SQLite")
    parser.add_argument(
        "--input-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "test_data", "835_denials"),
        help="Root directory containing denominated 835 files (default: test_data/835_denials)",
    )
    parser.add_argument(
        "--db-path",
        default=os.path.join(os.path.dirname(__file__), "denials_engine.db"),
        help="SQLite database path (default: denials_engine.db in scripts directory)",
    )
    args = parser.parse_args()

    db_path = os.path.abspath(args.db_path)
    conn = connect_db(db_path)
    print(f"Using database: {db_path}")

    # create schema
    conn.execute(CLAIMS_SCHEMA)
    conn.commit()

    # load CPT intelligence
    sql_path = os.path.join(os.path.dirname(__file__), "cpt_denial_intelligence.sql")
    if os.path.exists(sql_path):
        print("Loading CPT Denial Intelligence schema/data...")
        execute_file_sql(conn, sql_path)
    else:
        print(f"WARNING: cannot find {sql_path}")

    # ingest 835 files
    if os.path.isdir(args.input_dir):
        print(f"Parsing 835 files under {args.input_dir}...")
        ingest_835_directory(conn, args.input_dir)
        print("Done ingesting 835 data.")
    else:
        print(f"Input directory not found: {args.input_dir}")

    conn.close()


if __name__ == "__main__":
    main()
