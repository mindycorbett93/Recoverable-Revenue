#!/usr/bin/env python3
"""
eob_to_835.py - Convert text-based EOB (Explanation of Benefits) files into EDI 835 format.

Reads structured EOB text files, parses their content, maps non-standard CARC codes
to standard CARC codes, and generates compliant EDI 835 (005010X221A1) output files
along with conversion logs and summary reports.

Usage:
    python scripts/eob_to_835.py
    python scripts/eob_to_835.py --input-dir test_data/eob_pdf --output-dir test_data/eob_pdf
    python scripts/eob_to_835.py --practice-type cardiology --verbose
    python scripts/eob_to_835.py --mapping-file custom_mappings.csv
"""

import argparse
import csv
import os
import re
import sys
from collections import OrderedDict
from datetime import datetime


# ---------------------------------------------------------------------------
# Built-in non-standard CARC code mapping table
# ---------------------------------------------------------------------------
DEFAULT_CARC_MAPPING = {
    "N001": {"standard_code": "167", "description": "Medical necessity"},
    "N002": {"standard_code": "185", "description": "Provider not eligible"},
    "N003": {"standard_code": "197", "description": "Authorization"},
    "N004": {"standard_code": "29",  "description": "Timely filing"},
    "N005": {"standard_code": "27",  "description": "Coverage terminated"},
    "N006": {"standard_code": "18",  "description": "Duplicate"},
    "N007": {"standard_code": "4",   "description": "Modifier error"},
    "N008": {"standard_code": "97",  "description": "Bundling"},
    "N009": {"standard_code": "1",   "description": "Deductible"},
    "N010": {"standard_code": "222", "description": "Benefit limits"},
}

# Standard CARC descriptions for reporting
STANDARD_CARC_DESCRIPTIONS = {
    "1":   "Deductible amount",
    "2":   "Coinsurance amount",
    "3":   "Copay amount",
    "4":   "The procedure code is inconsistent with the modifier used",
    "16":  "Claim/service lacks information or has submission/billing error(s)",
    "18":  "Exact duplicate claim/service",
    "27":  "Expenses incurred after coverage terminated",
    "29":  "The time limit for filing has expired",
    "45":  "Charge exceeds fee schedule/maximum allowable",
    "50":  "These are non-covered services",
    "97":  "The benefit for this service is included in payment for another service",
    "167": "This (these) diagnosis(es) is (are) not covered",
    "185": "The rendering provider is not eligible to perform the service billed",
    "197": "Precertification/authorization/notification absent",
    "222": "Exceeds the facility's maximum length of stay or benefit limits",
}


# ---------------------------------------------------------------------------
# Data classes (plain dicts for stdlib-only)
# ---------------------------------------------------------------------------

def make_eob():
    """Return an empty EOB data structure."""
    return {
        "payer_name": "",
        "payer_id": "",
        "plan_type": "",
        "date": "",
        "eob_number": "",
        "tax_id": "",
        "patient_name": "",
        "patient_dob": "",
        "member_id": "",
        "group": "",
        "patient_account": "",
        "relationship": "",
        "provider_name": "",
        "npi": "",
        "provider_address_line": "",
        "provider_city": "",
        "provider_state": "",
        "provider_zip": "",
        "claim_number": "",
        "received_date": "",
        "service_from": "",
        "service_to": "",
        "place_of_service": "",
        "claim_status": "",
        "service_lines": [],
        "total_billed": 0.0,
        "total_allowed": 0.0,
        "total_deductible": 0.0,
        "total_copay": 0.0,
        "total_adjustment": 0.0,
        "total_paid": 0.0,
        "patient_responsibility": 0.0,
        "adjustment_reason_codes": OrderedDict(),
        "remarks": OrderedDict(),
        "source_file": "",
    }


def make_service_line():
    """Return an empty service line data structure."""
    return {
        "line_number": "",
        "cpt": "",
        "description": "",
        "billed": 0.0,
        "allowed": 0.0,
        "deductible": 0.0,
        "copay": 0.0,
        "adj_codes": [],  # list of raw adjustment code strings
        "adj_amount": 0.0,
        "paid": 0.0,
    }


# ---------------------------------------------------------------------------
# CARC mapping
# ---------------------------------------------------------------------------

def load_carc_mapping(mapping_file=None):
    """
    Load CARC mapping.  Start with the built-in defaults, then overlay
    any custom mappings from a CSV file if provided.

    Custom CSV must have columns: nonstandard_code, standard_code, description
    """
    mapping = dict(DEFAULT_CARC_MAPPING)

    if mapping_file and os.path.isfile(mapping_file):
        with open(mapping_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ns_code = row.get("nonstandard_code", "").strip()
                std_code = row.get("standard_code", "").strip()
                desc = row.get("description", "").strip()
                if ns_code and std_code:
                    mapping[ns_code] = {
                        "standard_code": std_code,
                        "description": desc,
                    }

    return mapping


def is_nonstandard_carc(code):
    """Return True if the code looks like a non-standard CARC (N followed by digits)."""
    return bool(re.match(r"^N\d{3,4}$", code))


def map_carc_code(code, mapping):
    """
    Map a single CARC code.  If it is a known non-standard code, return
    the mapped standard code.  Otherwise return the code unchanged.
    """
    code = code.strip()
    if code in mapping:
        return mapping[code]["standard_code"]
    return code


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_dollar(value_str):
    """Parse a dollar amount string like '$ 145.00' or '$145.00' into a float."""
    cleaned = value_str.replace("$", "").replace(",", "").strip()
    if not cleaned:
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_date_to_edi(date_str):
    """Convert MM/DD/YYYY to YYYYMMDD for EDI."""
    date_str = date_str.strip()
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        return dt.strftime("%Y%m%d")
    except ValueError:
        return date_str.replace("/", "")


def parse_eob_text(filepath):
    """
    Parse a structured EOB text file and return an EOB data dict.

    The file is expected to have sections separated by === and --- lines:
      PAYER / PATIENT INFORMATION / PROVIDER INFORMATION / CLAIM INFORMATION /
      SERVICE DETAILS / TOTALS / ADJUSTMENT REASON CODES / REMARKS
    """
    eob = make_eob()
    eob["source_file"] = filepath

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.splitlines()

    # Identify section boundaries
    section = None
    section_lines = {}
    current_lines = []

    for line in lines:
        stripped = line.strip()
        # Detect section headers
        if stripped.startswith("===="):
            if section and current_lines:
                section_lines.setdefault(section, []).extend(current_lines)
            current_lines = []
            continue
        if stripped.startswith("----"):
            continue

        # Detect named section headers
        upper = stripped.upper()
        if upper == "EXPLANATION OF BENEFITS":
            section = "HEADER"
            current_lines = []
            continue
        elif upper == "PATIENT INFORMATION":
            if section and current_lines:
                section_lines.setdefault(section, []).extend(current_lines)
            section = "PATIENT"
            current_lines = []
            continue
        elif upper == "PROVIDER INFORMATION":
            if section and current_lines:
                section_lines.setdefault(section, []).extend(current_lines)
            section = "PROVIDER"
            current_lines = []
            continue
        elif upper == "CLAIM INFORMATION":
            if section and current_lines:
                section_lines.setdefault(section, []).extend(current_lines)
            section = "CLAIM"
            current_lines = []
            continue
        elif upper == "SERVICE DETAILS":
            if section and current_lines:
                section_lines.setdefault(section, []).extend(current_lines)
            section = "SERVICE"
            current_lines = []
            continue
        elif upper == "TOTALS":
            if section and current_lines:
                section_lines.setdefault(section, []).extend(current_lines)
            section = "TOTALS"
            current_lines = []
            continue
        elif upper == "ADJUSTMENT REASON CODES":
            if section and current_lines:
                section_lines.setdefault(section, []).extend(current_lines)
            section = "ADJUSTMENT"
            current_lines = []
            continue
        elif upper == "REMARKS":
            if section and current_lines:
                section_lines.setdefault(section, []).extend(current_lines)
            section = "REMARKS"
            current_lines = []
            continue

        # First PAYER line comes right after header ====
        if section is None and "PAYER:" in stripped:
            section = "PAYER"
            current_lines = [stripped]
            continue

        if stripped:
            current_lines.append(stripped)

    # Capture last section
    if section and current_lines:
        section_lines.setdefault(section, []).extend(current_lines)

    # --- Parse PAYER section ---
    for pline in section_lines.get("PAYER", []):
        _parse_kv_line(pline, eob, {
            "PAYER": "payer_name",
            "PAYER ID": "payer_id",
            "PLAN TYPE": "plan_type",
            "DATE": "date",
            "EOB NUMBER": "eob_number",
            "TAX ID": "tax_id",
        })

    # --- Parse PATIENT section ---
    for pline in section_lines.get("PATIENT", []):
        _parse_kv_line(pline, eob, {
            "Patient Name": "patient_name",
            "Date of Birth": "patient_dob",
            "Member ID": "member_id",
            "Group": "group",
            "Patient Account": "patient_account",
            "Relationship": "relationship",
        })

    # --- Parse PROVIDER section ---
    prov_lines = section_lines.get("PROVIDER", [])
    for i, pline in enumerate(prov_lines):
        if "Provider Name:" in pline:
            # Provider Name and NPI are on the same line
            m_name = re.search(r"Provider Name:\s*(.+?)(?:\s{2,}|NPI:)", pline)
            if m_name:
                eob["provider_name"] = m_name.group(1).strip()
            m_npi = re.search(r"NPI:\s*(\S+)", pline)
            if m_npi:
                eob["npi"] = m_npi.group(1).strip()
        elif "Provider Address:" in pline:
            m_addr = re.search(r"Provider Address:\s*(.+)", pline)
            if m_addr:
                eob["provider_address_line"] = m_addr.group(1).strip()
        elif not pline.startswith("Provider") and eob["provider_address_line"] and not eob["provider_city"]:
            # City, State ZIP line
            m_csz = re.match(r"(.+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)", pline)
            if m_csz:
                eob["provider_city"] = m_csz.group(1).strip()
                eob["provider_state"] = m_csz.group(2).strip()
                eob["provider_zip"] = m_csz.group(3).strip()

    # --- Parse CLAIM section ---
    for pline in section_lines.get("CLAIM", []):
        _parse_kv_line(pline, eob, {
            "Claim Number": "claim_number",
            "Received Date": "received_date",
            "Service From": "service_from",
            "Service To": "service_to",
            "Claim Status": "claim_status",
        })
        if "Place of Service:" in pline:
            m = re.search(r"Place of Service:\s*(.+?)(?:\s{2,}|$)", pline)
            if m:
                eob["place_of_service"] = m.group(1).strip()

    # --- Parse SERVICE DETAILS ---
    svc_lines = section_lines.get("SERVICE", [])
    for sline in svc_lines:
        # Skip header/separator lines
        if sline.startswith("Line") or sline.startswith("----"):
            continue
        svc = _parse_service_line(sline)
        if svc:
            eob["service_lines"].append(svc)

    # --- Parse TOTALS ---
    for tline in section_lines.get("TOTALS", []):
        if "Total Billed:" in tline:
            eob["total_billed"] = _extract_dollar(tline, "Total Billed:")
        elif "Total Allowed:" in tline:
            eob["total_allowed"] = _extract_dollar(tline, "Total Allowed:")
        elif "Total Deductible:" in tline:
            eob["total_deductible"] = _extract_dollar(tline, "Total Deductible:")
        elif "Total Copay:" in tline:
            eob["total_copay"] = _extract_dollar(tline, "Total Copay:")
        elif "Total Adjustment:" in tline:
            eob["total_adjustment"] = _extract_dollar(tline, "Total Adjustment:")
        elif "Total Paid:" in tline:
            eob["total_paid"] = _extract_dollar(tline, "Total Paid:")
        elif "Patient Responsibility:" in tline:
            eob["patient_responsibility"] = _extract_dollar(tline, "Patient Responsibility:")

    # --- Parse ADJUSTMENT REASON CODES ---
    for aline in section_lines.get("ADJUSTMENT", []):
        m = re.match(r"(\S+?):\s*(.+)", aline)
        if m:
            eob["adjustment_reason_codes"][m.group(1).strip()] = m.group(2).strip()

    # --- Parse REMARKS ---
    for rline in section_lines.get("REMARKS", []):
        m = re.match(r"(\S+?):\s*(.+)", rline)
        if m:
            eob["remarks"][m.group(1).strip()] = m.group(2).strip()

    return eob


def _parse_kv_line(line, target_dict, key_map):
    """
    Parse a line that may contain multiple key: value pairs separated by
    whitespace.  The key_map maps display keys to target_dict field names.
    """
    for display_key, field_name in key_map.items():
        pattern = re.escape(display_key) + r":\s*(.+?)(?:\s{2,}|$)"
        m = re.search(pattern, line)
        if m:
            target_dict[field_name] = m.group(1).strip()


def _extract_dollar(line, label):
    """Extract a dollar amount following the given label in the line."""
    m = re.search(re.escape(label) + r"\s*\$\s*([\d,]+\.\d{2})", line)
    if m:
        return float(m.group(1).replace(",", ""))
    return 0.0


def _parse_service_line(line):
    """
    Parse a single service detail line.  The format is fixed-width-ish:
    Line  CPT     Description                   Billed   Allowed   Deduct   Copay  Adj Code   Adj Amt      Paid

    The Adj Code field can contain comma-separated codes like "N007,50" or "N006,1,3".
    """
    # Use a regex that captures the known fields based on the dollar sign pattern
    # Example line:
    # 1     99214   Office visit, establishe  $  145.00  $ 132.31  $ 52.92  $ 0.00        97  $  29.97  $  79.39
    # 1     99214   Office visit, establishe  $  145.00  $ 129.86  $ 51.94  $38.96   N007,50  $  56.06  $  38.96

    # Strategy: find all dollar amounts, then extract the adj code between copay and adj_amt
    m = re.match(
        r"\s*(\d+)\s+"              # Line number
        r"(\S+)\s+"                 # CPT code
        r"(.+?)\s+"                 # Description (greedy up to dollar sign area)
        r"\$\s*([\d,]+\.\d{2})\s+"  # Billed
        r"\$\s*([\d,]+\.\d{2})\s+"  # Allowed
        r"\$\s*([\d,]+\.\d{2})\s+"  # Deductible
        r"\$\s*([\d,]+\.\d{2})\s+"  # Copay
        r"([\w,]+)\s+"              # Adj Code(s) - may be comma-separated
        r"\$\s*([\d,]+\.\d{2})\s+"  # Adj Amount
        r"\$\s*([\d,]+\.\d{2})",    # Paid
        line,
    )
    if not m:
        return None

    svc = make_service_line()
    svc["line_number"] = m.group(1)
    svc["cpt"] = m.group(2)
    svc["description"] = m.group(3).strip()
    svc["billed"] = float(m.group(4).replace(",", ""))
    svc["allowed"] = float(m.group(5).replace(",", ""))
    svc["deductible"] = float(m.group(6).replace(",", ""))
    svc["copay"] = float(m.group(7).replace(",", ""))
    svc["adj_codes"] = [c.strip() for c in m.group(8).split(",") if c.strip()]
    svc["adj_amount"] = float(m.group(9).replace(",", ""))
    svc["paid"] = float(m.group(10).replace(",", ""))
    return svc


# ---------------------------------------------------------------------------
# EDI 835 Generation
# ---------------------------------------------------------------------------

def format_amount(amount):
    """Format a float as an EDI amount string (no leading $, two decimal places)."""
    return "{:.2f}".format(amount)


def determine_claim_status_code(eob):
    """
    Determine CLP status code:
      1 = Processed as Primary
      2 = Processed as Secondary
      4 = Denied
      22 = Reversed (not used here)
    """
    plan = eob.get("plan_type", "").upper()
    status = eob.get("claim_status", "").upper()

    if status == "DENIED" or eob["total_paid"] == 0.0:
        return "4"
    # Simple heuristic: if plan mentions secondary
    if "SECONDARY" in plan:
        return "2"
    return "1"


def generate_835(eob, carc_mapping, control_number, group_control, verbose=False):
    """
    Generate a complete EDI 835 transaction for one EOB.

    Returns a string containing the full 835 interchange.
    """
    segments = []

    prod_date = parse_date_to_edi(eob["date"])
    service_date = parse_date_to_edi(eob["service_from"])

    # Clean tax ID (remove hyphens)
    payer_tax_id = eob.get("tax_id", "000000000").replace("-", "")

    # Pad/truncate IDs as needed for ISA (fixed width fields)
    sender_id = (eob.get("payer_id", "") + " " * 15)[:15]
    receiver_id = (eob.get("npi", "") + " " * 15)[:15]

    # --- ISA Segment ---
    segments.append(
        "ISA*00*" + " " * 10 +
        "*00*" + " " * 10 +
        "*ZZ*" + sender_id +
        "*ZZ*" + receiver_id +
        "*" + prod_date[2:8] +     # YYMMDD (6 chars)
        "*" + datetime.now().strftime("%H%M") +
        "*^*00501*" +
        str(control_number).zfill(9) +
        "*0*P*:~"
    )

    # --- GS Segment ---
    segments.append(
        "GS*HP*" +
        eob.get("payer_id", "").strip() +
        "*" + eob.get("npi", "").strip() +
        "*" + prod_date +
        "*" + datetime.now().strftime("%H%M") +
        "*" + str(group_control) +
        "*X*005010X221A1~"
    )

    # --- ST Segment ---
    st_control = str(control_number).zfill(4)
    segments.append("ST*835*" + st_control + "*005010X221A1~")

    seg_count = 1  # ST counts as first

    # --- BPR Segment ---
    paid = format_amount(eob["total_paid"])
    bpr_method = "I" if eob["total_paid"] > 0 else "H"
    seg = ("BPR*" + bpr_method + "*" + paid +
           "*C*CHK*****" + eob.get("payer_id", "").strip() +
           "***01***" + prod_date + "~")
    segments.append(seg)
    seg_count += 1

    # --- TRN Segment ---
    seg = "TRN*1*" + eob.get("eob_number", "") + "*" + payer_tax_id + "~"
    segments.append(seg)
    seg_count += 1

    # --- DTM Segment (production date) ---
    seg = "DTM*405*" + prod_date + "~"
    segments.append(seg)
    seg_count += 1

    # --- Payer Identification (N1/N3/N4) ---
    seg = "N1*PR*" + eob.get("payer_name", "") + "~"
    segments.append(seg)
    seg_count += 1

    # Payer address (use generic placeholder since EOBs don't always have payer address)
    segments.append("N3*PO BOX 1000~")
    seg_count += 1
    segments.append("N4*DALLAS*TX*75201~")
    seg_count += 1

    # --- Payee Identification (N1/N3/N4) ---
    seg = "N1*PE*" + eob.get("provider_name", "") + "*XX*" + eob.get("npi", "") + "~"
    segments.append(seg)
    seg_count += 1

    if eob.get("provider_address_line"):
        seg = "N3*" + eob["provider_address_line"] + "~"
        segments.append(seg)
        seg_count += 1

        city = eob.get("provider_city", "")
        state = eob.get("provider_state", "")
        zipcode = eob.get("provider_zip", "")
        if city and state:
            seg = "N4*" + city + "*" + state + "*" + zipcode + "~"
            segments.append(seg)
            seg_count += 1

    # --- CLP Segment (Claim Payment) ---
    claim_status_code = determine_claim_status_code(eob)
    plan_type_code = _plan_type_to_code(eob.get("plan_type", ""))

    seg = ("CLP*" + eob.get("claim_number", "") +
           "*" + claim_status_code +
           "*" + format_amount(eob["total_billed"]) +
           "*" + format_amount(eob["total_paid"]) +
           "*" + format_amount(eob["patient_responsibility"]) +
           "*" + plan_type_code +
           "*" + eob.get("eob_number", "") + "~")
    segments.append(seg)
    seg_count += 1

    # --- NM1 for patient ---
    name_parts = eob.get("patient_name", ",").split(",")
    last_name = name_parts[0].strip() if len(name_parts) > 0 else ""
    first_name = name_parts[1].strip() if len(name_parts) > 1 else ""

    seg = ("NM1*QC*1*" + last_name + "*" + first_name +
           "***MI*" + eob.get("member_id", "") + "~")
    segments.append(seg)
    seg_count += 1

    # --- Service Lines ---
    for svc in eob["service_lines"]:
        # SVC segment
        seg = ("SVC*HC:" + svc["cpt"] +
               "*" + format_amount(svc["billed"]) +
               "*" + format_amount(svc["paid"]) +
               "**1~")  # units=1
        segments.append(seg)
        seg_count += 1

        # DTM*472 service date
        seg = "DTM*472*" + service_date + "~"
        segments.append(seg)
        seg_count += 1

        # --- CAS segments ---
        # First handle the CO (contractual obligation) adjustments from adj_codes
        co_adjustments = []
        pr_deductible = svc["deductible"]
        pr_copay = svc["copay"]

        # Calculate coinsurance: if allowed > 0 and paid < allowed,
        # the difference minus deductible and copay might be coinsurance
        coinsurance = 0.0
        if svc["allowed"] > 0:
            implied_patient = svc["allowed"] - svc["paid"]
            known_patient = svc["deductible"] + svc["copay"]
            if implied_patient > known_patient + 0.005:
                coinsurance = round(implied_patient - known_patient, 2)

        for raw_code in svc["adj_codes"]:
            mapped = map_carc_code(raw_code, carc_mapping)

            # Determine if this adjustment is CO or PR category
            # Standard codes 1 (deductible), 2 (coinsurance), 3 (copay) are PR
            # Everything else is typically CO
            if mapped in ("1", "2", "3"):
                # These will be handled below as PR adjustments
                continue
            else:
                co_adjustments.append(mapped)

        # Calculate CO adjustment amount:
        # Total adj amount minus patient responsibility portions
        co_amount = svc["adj_amount"]
        # The adj_amount on the line covers ALL adjustments.  We need to
        # separate out the patient responsibility (deductible + copay + coinsurance)
        # and attribute the remainder to CO.
        pr_total_from_line = pr_deductible + pr_copay + coinsurance
        co_net = round(co_amount - pr_total_from_line, 2)
        if co_net < 0:
            co_net = 0.0

        # Build CO CAS segment(s)
        if co_adjustments and co_net > 0:
            # If multiple CO adjustment codes, split amount among them
            # or use the first as the primary reason
            primary_code = co_adjustments[0]
            seg = "CAS*CO*" + primary_code + "*" + format_amount(co_net)
            # Add additional codes if present (up to 5 more per CAS segment)
            for extra_code in co_adjustments[1:6]:
                seg += "*" + extra_code + "*0.00"
            seg += "~"
            segments.append(seg)
            seg_count += 1
        elif co_adjustments and svc["adj_amount"] > 0 and pr_total_from_line == 0:
            # All adjustment is CO
            primary_code = co_adjustments[0]
            seg = "CAS*CO*" + primary_code + "*" + format_amount(svc["adj_amount"])
            for extra_code in co_adjustments[1:6]:
                seg += "*" + extra_code + "*0.00"
            seg += "~"
            segments.append(seg)
            seg_count += 1
        elif not co_adjustments and svc["billed"] > svc["allowed"] and svc["allowed"] > 0:
            # Contractual write-off with no explicit CO code
            writeoff = round(svc["billed"] - svc["allowed"], 2)
            if writeoff > 0:
                seg = "CAS*CO*45*" + format_amount(writeoff) + "~"
                segments.append(seg)
                seg_count += 1
        elif not co_adjustments and svc["paid"] == 0 and svc["billed"] > 0:
            # Fully denied line, no specific code
            seg = "CAS*CO*45*" + format_amount(svc["billed"]) + "~"
            segments.append(seg)
            seg_count += 1

        # PR CAS segments
        if pr_deductible > 0:
            seg = "CAS*PR*1*" + format_amount(pr_deductible) + "~"
            segments.append(seg)
            seg_count += 1

        if coinsurance > 0:
            seg = "CAS*PR*2*" + format_amount(coinsurance) + "~"
            segments.append(seg)
            seg_count += 1

        if pr_copay > 0:
            seg = "CAS*PR*3*" + format_amount(pr_copay) + "~"
            segments.append(seg)
            seg_count += 1

        # AMT*B6 (allowed amount)
        seg = "AMT*B6*" + format_amount(svc["allowed"]) + "~"
        segments.append(seg)
        seg_count += 1

    # --- SE Segment ---
    seg_count += 1  # SE itself
    segments.append("SE*" + str(seg_count) + "*" + st_control + "~")

    # --- GE Segment ---
    segments.append("GE*1*" + str(group_control) + "~")

    # --- IEA Segment ---
    segments.append("IEA*1*" + str(control_number).zfill(9) + "~")

    return "\n".join(segments)


def _plan_type_to_code(plan_type):
    """Convert plan type text to EDI code."""
    pt = plan_type.upper()
    mapping = {
        "HMO": "HM",
        "PPO": "PP",
        "MEDICARE": "MA",
        "MEDICAID": "MC",
        "GOVERNMENT": "GV",
        "INDEMNITY": "IN",
        "EPO": "EP",
        "POS": "PS",
    }
    for key, code in mapping.items():
        if key in pt:
            return code
    return "ZZ"


# ---------------------------------------------------------------------------
# Tracking structures for reporting
# ---------------------------------------------------------------------------

class ConversionTracker:
    """Tracks conversion results for reporting."""

    def __init__(self):
        self.conversion_logs = {}       # practice_type -> list of log dicts
        self.carc_mapping_usage = {}    # original_code -> {mapped, description, count}
        self.total_by_practice = {}     # practice_type -> {processed, success, fail}
        self.total_denied_dollars = 0.0
        self.total_paid_dollars = 0.0

    def record_conversion(self, practice_type, eob_file, claim_id,
                          original_carcs, mapped_carcs, was_remapped,
                          total_billed, total_paid, status):
        if practice_type not in self.conversion_logs:
            self.conversion_logs[practice_type] = []
        self.conversion_logs[practice_type].append({
            "eob_file": eob_file,
            "claim_id": claim_id,
            "original_carc_codes": original_carcs,
            "mapped_carc_codes": mapped_carcs,
            "was_remapped": was_remapped,
            "total_billed": total_billed,
            "total_paid": total_paid,
            "conversion_status": status,
        })

    def record_carc_mapping(self, original_code, original_desc, mapped_code, mapped_desc):
        key = original_code
        if key not in self.carc_mapping_usage:
            self.carc_mapping_usage[key] = {
                "original_description": original_desc,
                "mapped_code": mapped_code,
                "mapped_description": mapped_desc,
                "count": 0,
            }
        self.carc_mapping_usage[key]["count"] += 1

    def record_practice_result(self, practice_type, success):
        if practice_type not in self.total_by_practice:
            self.total_by_practice[practice_type] = {
                "processed": 0, "success": 0, "fail": 0
            }
        self.total_by_practice[practice_type]["processed"] += 1
        if success:
            self.total_by_practice[practice_type]["success"] += 1
        else:
            self.total_by_practice[practice_type]["fail"] += 1

    def add_financials(self, total_paid, total_billed, is_denied):
        self.total_paid_dollars += total_paid
        if is_denied:
            self.total_denied_dollars += total_billed


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def write_conversion_logs(tracker, output_dir):
    """Write per-practice-type conversion log CSVs."""
    results_dir = os.path.join(output_dir, "results")
    os.makedirs(results_dir, exist_ok=True)

    for practice_type, logs in tracker.conversion_logs.items():
        filepath = os.path.join(results_dir, practice_type + "_conversion_log.csv")
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "eob_file", "claim_id", "original_carc_codes", "mapped_carc_codes",
                "was_remapped", "total_billed", "total_paid", "conversion_status",
            ])
            writer.writeheader()
            for log_entry in logs:
                writer.writerow(log_entry)


def write_carc_mapping_report(tracker, output_dir):
    """Write consolidated CARC mapping report."""
    results_dir = os.path.join(output_dir, "results")
    os.makedirs(results_dir, exist_ok=True)

    filepath = os.path.join(results_dir, "carc_mapping_report.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "original_code", "original_description", "mapped_code",
            "mapped_description", "occurrence_count",
        ])
        writer.writeheader()
        for code in sorted(tracker.carc_mapping_usage.keys()):
            info = tracker.carc_mapping_usage[code]
            writer.writerow({
                "original_code": code,
                "original_description": info["original_description"],
                "mapped_code": info["mapped_code"],
                "mapped_description": info["mapped_description"],
                "occurrence_count": info["count"],
            })


def write_conversion_summary(tracker, output_dir):
    """Write the human-readable conversion summary report."""
    results_dir = os.path.join(output_dir, "results")
    os.makedirs(results_dir, exist_ok=True)

    filepath = os.path.join(results_dir, "conversion_summary.txt")

    lines = []
    lines.append("=" * 80)
    lines.append("EOB TO 835 CONVERSION SUMMARY")
    lines.append("=" * 80)
    lines.append("Generated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    lines.append("")

    # --- Per practice type ---
    lines.append("-" * 80)
    lines.append("TOTAL EOBs PROCESSED PER PRACTICE TYPE")
    lines.append("-" * 80)

    total_processed = 0
    total_success = 0
    total_fail = 0

    for pt in sorted(tracker.total_by_practice.keys()):
        info = tracker.total_by_practice[pt]
        lines.append("  {:<30s}  Processed: {:>4d}  Success: {:>4d}  Failed: {:>4d}".format(
            pt, info["processed"], info["success"], info["fail"]
        ))
        total_processed += info["processed"]
        total_success += info["success"]
        total_fail += info["fail"]

    lines.append("")
    lines.append("  {:<30s}  Processed: {:>4d}  Success: {:>4d}  Failed: {:>4d}".format(
        "TOTAL", total_processed, total_success, total_fail
    ))
    lines.append("")

    # --- Conversion success/failure ---
    lines.append("-" * 80)
    lines.append("CONVERSION SUCCESS/FAILURE COUNTS")
    lines.append("-" * 80)
    lines.append("  Total conversions attempted:  {}".format(total_processed))
    lines.append("  Successful conversions:       {}".format(total_success))
    lines.append("  Failed conversions:           {}".format(total_fail))
    if total_processed > 0:
        rate = (total_success / total_processed) * 100
        lines.append("  Success rate:                 {:.1f}%".format(rate))
    lines.append("")

    # --- Non-standard CARC mappings ---
    lines.append("-" * 80)
    lines.append("NON-STANDARD CARC CODES ENCOUNTERED AND THEIR MAPPINGS")
    lines.append("-" * 80)

    if tracker.carc_mapping_usage:
        lines.append("  {:<12s} {:<45s} {:<10s} {:<10s}".format(
            "Original", "Description", "Mapped To", "Count"
        ))
        lines.append("  " + "-" * 77)
        total_remapped = 0
        for code in sorted(tracker.carc_mapping_usage.keys()):
            info = tracker.carc_mapping_usage[code]
            lines.append("  {:<12s} {:<45s} {:<10s} {:>6d}".format(
                code,
                info["original_description"][:45],
                info["mapped_code"],
                info["count"],
            ))
            total_remapped += info["count"]
        lines.append("")
        lines.append("  Total non-standard CARC codes remapped: {}".format(total_remapped))
    else:
        lines.append("  No non-standard CARC codes encountered.")
    lines.append("")

    # --- Financial summary ---
    lines.append("-" * 80)
    lines.append("FINANCIAL SUMMARY")
    lines.append("-" * 80)
    lines.append("  Total dollars in paid claims:   ${:>12s}".format(
        format_amount(tracker.total_paid_dollars)))
    lines.append("  Total dollars in denied claims: ${:>12s}".format(
        format_amount(tracker.total_denied_dollars)))
    lines.append("")

    # --- Mapping accuracy ---
    lines.append("-" * 80)
    lines.append("MAPPING ACCURACY SUMMARY")
    lines.append("-" * 80)
    total_ns_codes_in_mapping = len(DEFAULT_CARC_MAPPING)
    codes_encountered = len(tracker.carc_mapping_usage)
    lines.append("  Built-in non-standard codes defined: {}".format(total_ns_codes_in_mapping))
    lines.append("  Non-standard codes encountered:      {}".format(codes_encountered))
    unmapped = [c for c in tracker.carc_mapping_usage
                if tracker.carc_mapping_usage[c]["mapped_code"] == c]
    lines.append("  Codes successfully mapped:           {}".format(codes_encountered - len(unmapped)))
    if unmapped:
        lines.append("  Codes with no mapping (passed through): {}".format(len(unmapped)))
        for c in unmapped:
            lines.append("    - {} ({})".format(c, tracker.carc_mapping_usage[c]["original_description"]))
    else:
        lines.append("  All non-standard codes were successfully mapped.")
    lines.append("")

    lines.append("=" * 80)
    lines.append("END OF REPORT")
    lines.append("=" * 80)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def discover_practice_types(input_dir):
    """
    Discover practice type subdirectories under the input directory.
    Excludes known output directories like '835_converted' and 'results'.
    """
    excluded = {"835_converted", "results"}
    practice_types = []
    if not os.path.isdir(input_dir):
        return practice_types
    for entry in sorted(os.listdir(input_dir)):
        if entry in excluded:
            continue
        full = os.path.join(input_dir, entry)
        if os.path.isdir(full):
            practice_types.append(entry)
    return practice_types


def process_practice_type(practice_type, input_dir, output_dir,
                          carc_mapping, tracker, verbose=False):
    """Process all EOB files for a single practice type."""
    practice_dir = os.path.join(input_dir, practice_type)
    if not os.path.isdir(practice_dir):
        if verbose:
            print("  [SKIP] Directory not found: {}".format(practice_dir))
        return

    # Collect .txt files
    eob_files = sorted([
        f for f in os.listdir(practice_dir) if f.endswith(".txt")
    ])

    if not eob_files:
        if verbose:
            print("  [SKIP] No .txt files in: {}".format(practice_dir))
        return

    # Prepare output directory
    edi_out_dir = os.path.join(output_dir, "835_converted", practice_type)
    os.makedirs(edi_out_dir, exist_ok=True)

    for idx, eob_file in enumerate(eob_files, start=1):
        filepath = os.path.join(practice_dir, eob_file)
        if verbose:
            print("  Processing: {}".format(filepath))

        try:
            eob = parse_eob_text(filepath)
        except Exception as e:
            if verbose:
                print("    [ERROR] Parse failed: {}".format(e))
            tracker.record_practice_result(practice_type, success=False)
            tracker.record_conversion(
                practice_type, eob_file, "UNKNOWN",
                "", "", "N/A", 0.0, 0.0, "PARSE_ERROR: {}".format(str(e))
            )
            continue

        # Collect all CARC codes from service lines, perform mapping
        all_original_codes = set()
        all_mapped_codes = set()
        any_remapped = False

        for svc in eob["service_lines"]:
            for raw_code in svc["adj_codes"]:
                all_original_codes.add(raw_code)
                mapped = map_carc_code(raw_code, carc_mapping)
                all_mapped_codes.add(mapped)

                if mapped != raw_code:
                    any_remapped = True
                    # Record in tracker
                    orig_desc = eob["adjustment_reason_codes"].get(raw_code, "")
                    mapped_desc = STANDARD_CARC_DESCRIPTIONS.get(mapped, "")
                    tracker.record_carc_mapping(raw_code, orig_desc, mapped, mapped_desc)

        # Generate 835
        try:
            control_number = idx
            group_control = idx
            edi_content = generate_835(eob, carc_mapping, control_number,
                                       group_control, verbose=verbose)

            # Determine output filename
            out_filename = "835_from_eob_{}_{:03d}.edi".format(practice_type, idx)
            out_path = os.path.join(edi_out_dir, out_filename)

            with open(out_path, "w", encoding="utf-8") as f:
                f.write(edi_content)

            if verbose:
                print("    [OK] Generated: {}".format(out_path))

            # Track
            is_denied = (eob["claim_status"].upper() == "DENIED" or eob["total_paid"] == 0.0)
            tracker.add_financials(eob["total_paid"], eob["total_billed"], is_denied)
            tracker.record_practice_result(practice_type, success=True)
            tracker.record_conversion(
                practice_type,
                eob_file,
                eob["claim_number"],
                ";".join(sorted(all_original_codes)),
                ";".join(sorted(all_mapped_codes)),
                "Yes" if any_remapped else "No",
                eob["total_billed"],
                eob["total_paid"],
                "SUCCESS",
            )

        except Exception as e:
            if verbose:
                print("    [ERROR] 835 generation failed: {}".format(e))
            tracker.record_practice_result(practice_type, success=False)
            tracker.record_conversion(
                practice_type,
                eob_file,
                eob.get("claim_number", "UNKNOWN"),
                ";".join(sorted(all_original_codes)),
                ";".join(sorted(all_mapped_codes)),
                "Yes" if any_remapped else "No",
                eob.get("total_billed", 0.0),
                eob.get("total_paid", 0.0),
                "GENERATION_ERROR: {}".format(str(e)),
            )


def main():
    parser = argparse.ArgumentParser(
        description="Convert text-based EOB files into EDI 835 format with CARC code mapping."
    )
    parser.add_argument(
        "--input-dir",
        default="test_data/eob_pdf",
        help="Root input directory containing practice type subdirectories "
             "(default: test_data/eob_pdf)",
    )
    parser.add_argument(
        "--output-dir",
        default="test_data/eob_pdf",
        help="Root output directory for 835 files and reports "
             "(default: test_data/eob_pdf)",
    )
    parser.add_argument(
        "--mapping-file",
        default=None,
        help="Optional CSV file with custom CARC code mappings "
             "(columns: nonstandard_code, standard_code, description)",
    )
    parser.add_argument(
        "--practice-type",
        default=None,
        help="Process only this practice type (default: all)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    # Load CARC mapping
    carc_mapping = load_carc_mapping(args.mapping_file)

    if args.verbose:
        print("CARC mapping loaded: {} non-standard codes defined".format(len(carc_mapping)))

    # Discover practice types
    if args.practice_type:
        practice_types = [args.practice_type]
    else:
        practice_types = discover_practice_types(args.input_dir)

    if not practice_types:
        print("No practice types found in: {}".format(args.input_dir))
        sys.exit(1)

    if args.verbose:
        print("Practice types to process: {}".format(", ".join(practice_types)))
        print("")

    # Process
    tracker = ConversionTracker()

    for pt in practice_types:
        if args.verbose:
            print("Processing practice type: {}".format(pt))
        process_practice_type(
            pt, args.input_dir, args.output_dir,
            carc_mapping, tracker, verbose=args.verbose,
        )
        if args.verbose:
            print("")

    # Write reports
    write_conversion_logs(tracker, args.output_dir)
    write_carc_mapping_report(tracker, args.output_dir)
    write_conversion_summary(tracker, args.output_dir)

    # Print summary to stdout
    total_processed = sum(v["processed"] for v in tracker.total_by_practice.values())
    total_success = sum(v["success"] for v in tracker.total_by_practice.values())
    total_fail = sum(v["fail"] for v in tracker.total_by_practice.values())

    print("Conversion complete.")
    print("  Total EOBs processed: {}".format(total_processed))
    print("  Successful:           {}".format(total_success))
    print("  Failed:               {}".format(total_fail))
    print("  Non-standard CARC codes remapped: {}".format(
        len(tracker.carc_mapping_usage)))
    print("  Total paid:           ${:.2f}".format(tracker.total_paid_dollars))
    print("  Total denied:         ${:.2f}".format(tracker.total_denied_dollars))
    print("")
    print("Output:")
    print("  835 files:        {}/835_converted/".format(args.output_dir))
    print("  Conversion logs:  {}/results/".format(args.output_dir))
    print("  CARC mapping:     {}/results/carc_mapping_report.csv".format(args.output_dir))
    print("  Summary:          {}/results/conversion_summary.txt".format(args.output_dir))


if __name__ == "__main__":
    main()
