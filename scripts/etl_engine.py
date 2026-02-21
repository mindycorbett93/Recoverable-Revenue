#!/usr/bin/env python3
"""
ETL Engine for Standardizing HL7 Data from Multiple Systems.

Extracts HL7 v2.x messages from system_a, system_b, and system_c directories,
transforms them into a common standardized format, and loads the results into
CSV files per practice type plus a consolidated JSON repository.

Systems:
  - system_a: HL7 v2.3 format
  - system_b: HL7 v2.5.1 format
  - system_c: HL7 v2.4 format

Usage:
    python etl_engine.py [--input-dir DIR] [--output-dir DIR]
                         [--practice-type TYPE] [--verbose]
"""

import argparse
import csv
import json
import os
import re
from collections import OrderedDict, defaultdict
from datetime import datetime


# ============================================================
# CONSTANTS
# ============================================================

SYSTEM_VERSIONS = {
    "system_a": "2.3",
    "system_b": "2.5.1",
    "system_c": "2.4",
}

# Map raw sending facility names to standardized names.
FACILITY_NAME_MAP = {
    # system_a facilities
    "METRO_HEALTH": "Metro Health Medical Center",
    "METROHEALTHMC": "Metro Health Medical Center",
    "METRO HEALTH MEDICAL CENTER": "Metro Health Medical Center",
    "COMMUNITY_GEN": "Community General Hospital",
    "COMMUNITYGENHOSP": "Community General Hospital",
    "COMMUNITY GENERAL HOSPITAL": "Community General Hospital",
    "VALLEY_MED": "Valley Medical Associates",
    "VALLEYMEDASSOC": "Valley Medical Associates",
    "VALLEY MEDICAL ASSOCIATES": "Valley Medical Associates",
    "REGIONAL_HC": "Regional Healthcare System",
    "REGIONALHCS": "Regional Healthcare System",
    "REGIONAL HEALTHCARE SYSTEM": "Regional Healthcare System",
    "UNIV_MED": "University Medical Partners",
    "UNIVMEDPARTNERS": "University Medical Partners",
    "UNIVERSITY MEDICAL PARTNERS": "University Medical Partners",
    # system_b facilities
    "SUMMIT_HEALTH": "Summit Health Network",
    "SUMMITHEALTHNET": "Summit Health Network",
    "SUMMIT HEALTH NETWORK": "Summit Health Network",
    "PACIFIC_CARE": "Pacific Care Medical Group",
    "PACIFICCAREMG": "Pacific Care Medical Group",
    "PACIFIC CARE MEDICAL GROUP": "Pacific Care Medical Group",
    "ATLANTIC_HP": "Atlantic Health Partners",
    "ATLANTICHEALTHP": "Atlantic Health Partners",
    "ATLANTIC HEALTH PARTNERS": "Atlantic Health Partners",
    "MIDWEST_CLIN": "Midwest Clinical Services",
    "MIDWESTCLINSVCS": "Midwest Clinical Services",
    "MIDWEST CLINICAL SERVICES": "Midwest Clinical Services",
    "NATIONAL_HA": "National Health Alliance",
    "NATIONALHLTHAL": "National Health Alliance",
    "NATIONAL HEALTH ALLIANCE": "National Health Alliance",
    # system_c facilities
    "HERITAGE_HS": "Heritage Health System",
    "HERITAGEHEALTHSYS": "Heritage Health System",
    "HERITAGE HEALTH SYSTEM": "Heritage Health System",
    "PREMIER_MED": "Premier Medical Associates",
    "PREMIERMEDASSOC": "Premier Medical Associates",
    "PREMIER MEDICAL ASSOCIATES": "Premier Medical Associates",
    "ADVANCED_CARE": "Advanced Care Network",
    "ADVANCEDCARENET": "Advanced Care Network",
    "ADVANCED CARE NETWORK": "Advanced Care Network",
    "INTEGRATED_HS": "Integrated Health Services",
    "INTEGRATEDHSVCS": "Integrated Health Services",
    "INTEGRATED HEALTH SERVICES": "Integrated Health Services",
    "PINNACLE_HCG": "Pinnacle Healthcare Group",
    "PINNACLEHCG": "Pinnacle Healthcare Group",
    "PINNACLE HEALTHCARE GROUP": "Pinnacle Healthcare Group",
}

PATIENT_CSV_FIELDS = [
    "patient_id", "source_system", "practice_type", "last_name", "first_name",
    "dob", "gender", "ssn", "address_street", "address_city", "address_state",
    "address_zip", "phone", "facility",
]

ENCOUNTER_CSV_FIELDS = [
    "encounter_id", "patient_id", "source_system", "practice_type",
    "message_type", "visit_type", "attending_physician", "attending_npi",
    "admit_date", "facility", "message_timestamp",
]

DIAGNOSIS_CSV_FIELDS = [
    "diagnosis_id", "patient_id", "encounter_id", "source_system",
    "practice_type", "diagnosis_code", "diagnosis_description",
    "diagnosis_type", "sequence_number",
]

ORDER_RESULT_CSV_FIELDS = [
    "record_id", "patient_id", "encounter_id", "source_system",
    "practice_type", "record_type", "order_procedure_code",
    "order_procedure_desc", "result_value", "result_units",
    "reference_range", "abnormal_flag", "result_status",
]

INSURANCE_CSV_FIELDS = [
    "insurance_id", "patient_id", "encounter_id", "source_system",
    "practice_type", "payer_name", "plan_type", "member_id",
    "group_number", "subscriber_name", "relationship",
]


# ============================================================
# HL7 PARSING UTILITIES
# ============================================================

def split_segments(raw_message):
    """Split an HL7 message into segments using \\r or \\n."""
    lines = re.split(r"[\r\n]+", raw_message.strip())
    return [line for line in lines if line.strip()]


def split_fields(segment):
    """Split a segment into fields by pipe character.

    For MSH segments the field separator '|' is MSH-1 and the first
    real data field starts at index 1 in the resulting list.  We
    prepend an empty string so that field indices align with the HL7
    specification (MSH-0 = segment name, MSH-1 = '|', MSH-2 = encoding
    chars, etc.).
    """
    parts = segment.split("|")
    if parts and parts[0] == "MSH":
        # Insert empty element so MSH-1 = '|' aligns with index 1
        return ["MSH", "|"] + parts[1:]
    return parts


def get_component(field, index=0):
    """Get a component from a field split by '^'.

    Returns empty string if the component does not exist.
    """
    if not field:
        return ""
    components = field.split("^")
    if index < len(components):
        return components[index].strip()
    return ""


def get_field(fields, index, default=""):
    """Safely get a field by index, returning *default* if out of range."""
    if index < len(fields):
        return fields[index]
    return default


def get_subcomponent(component_str, index=0):
    """Get a sub-component split by '&'."""
    if not component_str:
        return ""
    parts = component_str.split("&")
    if index < len(parts):
        return parts[index].strip()
    return ""


# ============================================================
# DATA STANDARDIZATION
# ============================================================

def standardize_date(raw_date):
    """Convert various HL7 date formats to YYYY-MM-DD.

    Handles: YYYYMMDD, YYYYMMDDHHMMSS, YYYY-MM-DD, MM/DD/YYYY,
             YYYYMMDDHHMMSS.SSS, etc.
    """
    if not raw_date:
        return ""
    # Strip any trailing timezone or precision info
    cleaned = re.sub(r"[+-]\d{4}$", "", raw_date.strip())
    cleaned = cleaned.split(".")[0]  # remove fractional seconds
    cleaned = cleaned.replace("-", "").replace("/", "").replace(":", "").replace(" ", "")

    # If it looks like MM/DD/YYYY originally, reorder
    if re.match(r"^\d{2}/\d{2}/\d{4}$", raw_date.strip()):
        parts = raw_date.strip().split("/")
        cleaned = parts[2] + parts[0] + parts[1]

    # Now cleaned should be pure digits
    cleaned = re.sub(r"[^0-9]", "", cleaned)

    if len(cleaned) >= 8:
        year = cleaned[0:4]
        month = cleaned[4:6]
        day = cleaned[6:8]
        # Basic validation
        try:
            datetime(int(year), int(month), int(day))
            return f"{year}-{month}-{day}"
        except (ValueError, TypeError):
            return raw_date.strip()
    return raw_date.strip()


def standardize_datetime(raw_dt):
    """Convert HL7 datetime to YYYY-MM-DD HH:MM:SS."""
    if not raw_dt:
        return ""
    cleaned = re.sub(r"[+-]\d{4}$", "", raw_dt.strip())
    cleaned = cleaned.split(".")[0]
    cleaned = re.sub(r"[^0-9]", "", cleaned)

    if len(cleaned) >= 14:
        return (
            f"{cleaned[0:4]}-{cleaned[4:6]}-{cleaned[6:8]} "
            f"{cleaned[8:10]}:{cleaned[10:12]}:{cleaned[12:14]}"
        )
    if len(cleaned) >= 12:
        return (
            f"{cleaned[0:4]}-{cleaned[4:6]}-{cleaned[6:8]} "
            f"{cleaned[8:10]}:{cleaned[10:12]}:00"
        )
    if len(cleaned) >= 8:
        return f"{cleaned[0:4]}-{cleaned[4:6]}-{cleaned[6:8]} 00:00:00"
    return raw_dt.strip()


def standardize_name(raw_name_field):
    """Convert HL7 name (LAST^FIRST^MIDDLE^SUFFIX^PREFIX) to LAST, FIRST.

    Also handles 'LAST, FIRST' or 'FIRST LAST' inputs.
    """
    if not raw_name_field:
        return "", ""

    if "^" in raw_name_field:
        parts = raw_name_field.split("^")
        last = parts[0].strip() if len(parts) > 0 else ""
        first = parts[1].strip() if len(parts) > 1 else ""
        return last.upper(), first.upper()

    if "," in raw_name_field:
        parts = raw_name_field.split(",", 1)
        last = parts[0].strip()
        first = parts[1].strip() if len(parts) > 1 else ""
        return last.upper(), first.upper()

    parts = raw_name_field.strip().split()
    if len(parts) >= 2:
        return parts[-1].upper(), parts[0].upper()
    if len(parts) == 1:
        return parts[0].upper(), ""
    return "", ""


def standardize_phone(raw_phone):
    """Convert phone to (XXX) XXX-XXXX format.

    Handles: XXXXXXXXXX, (XXX)XXX-XXXX, XXX-XXX-XXXX, XXX.XXX.XXXX,
             (XXX) XXX-XXXX, and HL7 component format with area code.
    """
    if not raw_phone:
        return ""
    # Extract only digits
    digits = re.sub(r"[^0-9]", "", raw_phone)
    # If there's a leading 1 for country code and we have 11 digits
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    # Return cleaned version if we can't standardize
    return raw_phone.strip()


def standardize_address(street, city, state, zipcode):
    """Standardize address components."""
    street = street.strip().title() if street else ""
    city = city.strip().title() if city else ""
    state = state.strip().upper() if state else ""
    zipcode = re.sub(r"[^0-9-]", "", zipcode.strip()) if zipcode else ""
    # Normalize zip to 5 digits if we have at least 5
    if len(zipcode.replace("-", "")) >= 5:
        zipcode = zipcode.replace("-", "")[:5]
    return street, city, state, zipcode


def standardize_gender(raw_gender):
    """Normalize gender to single character M/F/U."""
    if not raw_gender:
        return "U"
    g = raw_gender.strip().upper()
    if g in ("M", "MALE"):
        return "M"
    if g in ("F", "FEMALE"):
        return "F"
    return "U"


def standardize_facility(raw_facility, source_system):
    """Map a sending facility name to a standardized facility name."""
    if not raw_facility:
        return f"Unknown Facility ({source_system})"
    key = raw_facility.strip().upper()
    # Direct lookup
    for k, v in FACILITY_NAME_MAP.items():
        if k.upper() == key:
            return v
    # Fuzzy: check if the raw name is contained in any key
    for k, v in FACILITY_NAME_MAP.items():
        if key in k.upper() or k.upper() in key:
            return v
    return raw_facility.strip()


def validate_icd10(code):
    """Validate that a code looks like a valid ICD-10 code.

    ICD-10 codes start with a letter, followed by digits, optionally
    a dot, and more alphanumerics.  Returns a tuple (is_valid, cleaned_code).
    """
    if not code:
        return False, ""
    cleaned = code.strip().upper()
    # Pattern: letter, 2 digits, optional dot, up to 4 alphanumerics
    pattern = r"^[A-Z]\d{2}\.?[A-Z0-9]{0,4}$"
    if re.match(pattern, cleaned):
        return True, cleaned
    # Some codes have additional specificity
    pattern_extended = r"^[A-Z]\d{2}\.?[A-Z0-9]{0,7}$"
    if re.match(pattern_extended, cleaned):
        return True, cleaned
    return False, cleaned


def standardize_ssn(raw_ssn):
    """Mask SSN to XXX-XX-XXXX format (or return masked)."""
    if not raw_ssn:
        return ""
    digits = re.sub(r"[^0-9]", "", raw_ssn)
    if len(digits) == 9:
        return f"{digits[0:3]}-{digits[3:5]}-{digits[5:9]}"
    return raw_ssn.strip()


# ============================================================
# SEGMENT PARSERS
# ============================================================

def parse_msh(fields, source_system):
    """Parse MSH (Message Header) segment."""
    data = {}
    # MSH-3: Sending Application
    data["sending_application"] = get_field(fields, 3)
    # MSH-4: Sending Facility
    raw_facility = get_component(get_field(fields, 4), 0)
    data["sending_facility_raw"] = raw_facility
    data["sending_facility"] = standardize_facility(raw_facility, source_system)
    # MSH-7: Message Date/Time
    data["message_datetime"] = standardize_datetime(get_field(fields, 7))
    # MSH-9: Message Type (e.g., ADT^A01)
    msg_type_field = get_field(fields, 9)
    data["message_type"] = get_component(msg_type_field, 0)
    data["message_event"] = get_component(msg_type_field, 1)
    data["message_type_full"] = f"{data['message_type']}^{data['message_event']}" if data["message_event"] else data["message_type"]
    # MSH-10: Message Control ID
    data["message_control_id"] = get_field(fields, 10)
    # MSH-12: Version ID
    data["version_id"] = get_component(get_field(fields, 12), 0)
    data["source_system"] = source_system
    return data


def parse_pid(fields, source_system):
    """Parse PID (Patient Identification) segment."""
    data = {}
    # PID-2: Patient ID (External) -- used in v2.3
    external_id = get_component(get_field(fields, 2), 0)
    # PID-3: Patient Identifier List (primary in v2.5+)
    primary_id = get_component(get_field(fields, 3), 0)
    data["patient_id"] = primary_id or external_id or ""

    # PID-5: Patient Name (LAST^FIRST^MIDDLE^SUFFIX^PREFIX)
    name_field = get_field(fields, 5)
    last, first = standardize_name(name_field)
    data["last_name"] = last
    data["first_name"] = first
    data["full_name"] = f"{last}, {first}" if first else last

    # PID-7: Date of Birth
    data["dob"] = standardize_date(get_field(fields, 7))

    # PID-8: Gender
    data["gender"] = standardize_gender(get_field(fields, 8))

    # PID-11: Patient Address
    addr_field = get_field(fields, 11)
    raw_street = get_component(addr_field, 0)
    raw_city = get_component(addr_field, 2)
    raw_state = get_component(addr_field, 3)
    raw_zip = get_component(addr_field, 4)
    # Component index 1 is "other designation" (Apt, Suite, etc.)
    other_desig = get_component(addr_field, 1)
    if other_desig:
        raw_street = f"{raw_street} {other_desig}"
    street, city, state, zipcode = standardize_address(
        raw_street, raw_city, raw_state, raw_zip
    )
    data["address_street"] = street
    data["address_city"] = city
    data["address_state"] = state
    data["address_zip"] = zipcode

    # PID-13: Phone Number - Home
    phone_field = get_field(fields, 13)
    # HL7 phone can be in component format: (area)number or just digits
    raw_phone = get_component(phone_field, 0)
    # In some versions, area code is in a sub-component
    if not raw_phone:
        raw_phone = phone_field
    data["phone"] = standardize_phone(raw_phone)

    # PID-19: SSN
    data["ssn"] = standardize_ssn(get_field(fields, 19))

    data["source_system"] = source_system
    return data


def parse_pv1(fields, source_system):
    """Parse PV1 (Patient Visit) segment."""
    data = {}
    # PV1-2: Patient Class (I=Inpatient, O=Outpatient, E=Emergency, etc.)
    patient_class = get_field(fields, 2).strip().upper()
    class_map = {
        "I": "Inpatient", "O": "Outpatient", "E": "Emergency",
        "P": "Preadmit", "R": "Recurring", "B": "Obstetrics",
    }
    data["visit_type"] = class_map.get(patient_class, patient_class or "Unknown")

    # PV1-3: Assigned Patient Location
    data["patient_location"] = get_field(fields, 3)

    # PV1-7: Attending Doctor (ID^LAST^FIRST^MIDDLE^SUFFIX^PREFIX^DEGREE)
    attending_field = get_field(fields, 7)
    attending_id = get_component(attending_field, 0)
    attending_last = get_component(attending_field, 1)
    attending_first = get_component(attending_field, 2)
    if attending_last and attending_first:
        data["attending_physician"] = f"{attending_last.upper()}, {attending_first.upper()}"
    elif attending_last:
        data["attending_physician"] = attending_last.upper()
    else:
        data["attending_physician"] = ""
    data["attending_npi"] = attending_id

    # PV1-19: Visit Number
    data["visit_number"] = get_component(get_field(fields, 19), 0)

    # PV1-39: Servicing Facility (if present)
    data["servicing_facility"] = get_component(get_field(fields, 39), 0)

    # PV1-44: Admit Date/Time
    data["admit_date"] = standardize_date(get_field(fields, 44))

    # PV1-45: Discharge Date/Time
    data["discharge_date"] = standardize_date(get_field(fields, 45))

    data["source_system"] = source_system
    return data


def parse_dg1(fields, source_system, seq_counter=1):
    """Parse DG1 (Diagnosis) segment."""
    data = {}
    # DG1-1: Set ID
    data["sequence_number"] = get_field(fields, 1) or str(seq_counter)

    # DG1-3: Diagnosis Code
    diag_code_field = get_field(fields, 3)
    raw_code = get_component(diag_code_field, 0)
    is_valid, cleaned_code = validate_icd10(raw_code)
    data["diagnosis_code"] = cleaned_code
    data["diagnosis_code_valid"] = is_valid

    # DG1-4: Diagnosis Description
    data["diagnosis_description"] = (
        get_component(diag_code_field, 1) or get_field(fields, 4)
    ).strip()

    # DG1-6: Diagnosis Type (A=Admitting, W=Working, F=Final)
    diag_type_raw = get_field(fields, 6).strip().upper()
    type_map = {"A": "Admitting", "W": "Working", "F": "Final"}
    data["diagnosis_type"] = type_map.get(diag_type_raw, diag_type_raw or "Working")

    # DG1-5: Diagnosis Date/Time
    data["diagnosis_date"] = standardize_date(get_field(fields, 5))

    data["source_system"] = source_system
    return data


def parse_in1(fields, source_system):
    """Parse IN1 (Insurance) segment."""
    data = {}
    # IN1-1: Set ID
    data["set_id"] = get_field(fields, 1)

    # IN1-2: Insurance Plan ID
    data["plan_id"] = get_component(get_field(fields, 2), 0)
    data["plan_type"] = get_component(get_field(fields, 2), 1) or ""

    # IN1-3: Insurance Company ID
    data["insurance_company_id"] = get_component(get_field(fields, 3), 0)

    # IN1-4: Insurance Company Name
    data["payer_name"] = get_component(get_field(fields, 4), 0) or get_field(fields, 4)

    # IN1-15: Plan Type (if not from IN1-2)
    if not data["plan_type"]:
        data["plan_type"] = get_field(fields, 15)

    # IN1-36: Policy Number (Member ID)
    member_id = get_field(fields, 36)
    if not member_id:
        # Fallback to IN1-2 component or IN1-49
        member_id = get_component(get_field(fields, 2), 0) or get_field(fields, 49, "")
    data["member_id"] = member_id.strip()

    # IN1-8: Group Number
    data["group_number"] = get_field(fields, 8)

    # IN1-16: Name of Insured (subscriber)
    subscriber_field = get_field(fields, 16)
    sub_last, sub_first = standardize_name(subscriber_field)
    data["subscriber_name"] = f"{sub_last}, {sub_first}" if sub_first else sub_last

    # IN1-17: Insured's Relationship to Patient
    rel_raw = get_field(fields, 17).strip()
    rel_map = {
        "01": "Self", "SEL": "Self", "18": "Self",
        "02": "Spouse", "SPO": "Spouse",
        "03": "Child", "CHD": "Child",
        "04": "Other", "OTH": "Other",
    }
    data["relationship"] = rel_map.get(rel_raw.upper(), rel_raw or "Self")

    data["source_system"] = source_system
    return data


def parse_obr(fields, source_system):
    """Parse OBR (Observation Request / Order) segment."""
    data = {}
    # OBR-1: Set ID
    data["set_id"] = get_field(fields, 1)

    # OBR-2: Placer Order Number
    data["placer_order_number"] = get_component(get_field(fields, 2), 0)

    # OBR-3: Filler Order Number
    data["filler_order_number"] = get_component(get_field(fields, 3), 0)

    # OBR-4: Universal Service Identifier (procedure code)
    service_field = get_field(fields, 4)
    data["procedure_code"] = get_component(service_field, 0)
    data["procedure_description"] = get_component(service_field, 1)

    # OBR-7: Observation Date/Time
    data["observation_datetime"] = standardize_datetime(get_field(fields, 7))

    # OBR-22: Results Report/Status Change Date/Time
    data["results_datetime"] = standardize_datetime(get_field(fields, 22))

    # OBR-25: Result Status
    status_raw = get_field(fields, 25).strip().upper()
    status_map = {
        "F": "Final", "P": "Preliminary", "C": "Corrected",
        "R": "Results entered", "I": "Pending", "O": "Order received",
    }
    data["result_status"] = status_map.get(status_raw, status_raw or "Final")

    data["source_system"] = source_system
    return data


def parse_obx(fields, source_system):
    """Parse OBX (Observation/Result) segment."""
    data = {}
    # OBX-1: Set ID
    data["set_id"] = get_field(fields, 1)

    # OBX-2: Value Type (NM=Numeric, ST=String, CE=Coded Entry, etc.)
    data["value_type"] = get_field(fields, 2)

    # OBX-3: Observation Identifier
    obs_id_field = get_field(fields, 3)
    data["observation_code"] = get_component(obs_id_field, 0)
    data["observation_description"] = get_component(obs_id_field, 1)

    # OBX-5: Observation Value
    data["result_value"] = get_field(fields, 5)

    # OBX-6: Units
    units_field = get_field(fields, 6)
    data["result_units"] = get_component(units_field, 0) or units_field

    # OBX-7: Reference Range
    data["reference_range"] = get_field(fields, 7)

    # OBX-8: Abnormal Flags
    flag_raw = get_field(fields, 8).strip().upper()
    flag_map = {
        "N": "Normal", "H": "High", "L": "Low", "HH": "Critical High",
        "LL": "Critical Low", "A": "Abnormal", "AA": "Critical Abnormal",
        ">": "Above High", "<": "Below Low",
    }
    data["abnormal_flag"] = flag_map.get(flag_raw, flag_raw or "Normal")

    # OBX-11: Observation Result Status
    status_raw = get_field(fields, 11).strip().upper()
    status_map = {"F": "Final", "P": "Preliminary", "C": "Corrected", "D": "Deleted"}
    data["result_status"] = status_map.get(status_raw, status_raw or "Final")

    data["source_system"] = source_system
    return data


# ============================================================
# MESSAGE-LEVEL PARSER
# ============================================================

def parse_hl7_message(raw_message, source_system, practice_type, filepath):
    """Parse a single HL7 message into structured data dictionaries.

    Returns a dict with keys: msh, pid, pv1, dg1_list, in1_list,
    obr_list, obx_list, and metadata.
    """
    segments = split_segments(raw_message)
    if not segments:
        return None

    result = {
        "msh": {},
        "pid": {},
        "pv1": {},
        "dg1_list": [],
        "in1_list": [],
        "obr_list": [],
        "obx_list": [],
        "metadata": {
            "source_system": source_system,
            "practice_type": practice_type,
            "source_file": filepath,
        },
    }

    dg1_seq = 0
    current_obr = None

    for segment_str in segments:
        fields = split_fields(segment_str)
        seg_name = fields[0] if fields else ""

        if seg_name == "MSH":
            result["msh"] = parse_msh(fields, source_system)
        elif seg_name == "PID":
            result["pid"] = parse_pid(fields, source_system)
        elif seg_name == "PV1":
            result["pv1"] = parse_pv1(fields, source_system)
        elif seg_name == "DG1":
            dg1_seq += 1
            result["dg1_list"].append(parse_dg1(fields, source_system, dg1_seq))
        elif seg_name == "IN1":
            result["in1_list"].append(parse_in1(fields, source_system))
        elif seg_name == "OBR":
            current_obr = parse_obr(fields, source_system)
            result["obr_list"].append(current_obr)
        elif seg_name == "OBX":
            obx_data = parse_obx(fields, source_system)
            # Link OBX to the most recent OBR
            if current_obr:
                obx_data["parent_order_code"] = current_obr.get("procedure_code", "")
                obx_data["parent_order_desc"] = current_obr.get("procedure_description", "")
            result["obx_list"].append(obx_data)

    return result


# ============================================================
# FILE-LEVEL EXTRACTION
# ============================================================

def extract_messages_from_file(filepath):
    """Read an HL7 file and return a list of raw message strings.

    HL7 files may contain a single message or multiple messages
    delimited by MSH headers.
    """
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except (IOError, OSError) as e:
        return [], str(e)

    if not content.strip():
        return [], "Empty file"

    # Split on MSH boundaries (each MSH starts a new message)
    # We look for MSH appearing at the start of a line
    parts = re.split(r"(?=^MSH\|)", content, flags=re.MULTILINE)
    messages = [p.strip() for p in parts if p.strip() and p.strip().startswith("MSH")]

    if not messages:
        # Entire content might be one message that doesn't start with MSH on its own line
        if "MSH|" in content:
            idx = content.index("MSH|")
            messages = [content[idx:].strip()]

    return messages, None


# ============================================================
# DEDUPLICATION
# ============================================================

def build_dedup_key(patient):
    """Build a deduplication key from patient last_name + first_name + dob."""
    last = patient.get("last_name", "").strip().upper()
    first = patient.get("first_name", "").strip().upper()
    dob = patient.get("dob", "").strip()
    return f"{last}|{first}|{dob}"


def deduplicate_patients(patient_records):
    """Deduplicate patient records across systems.

    Matches on last_name + first_name + DOB.  Keeps the first
    occurrence and tracks duplicate count.

    Returns: (deduplicated_list, duplicate_count, duplicate_details)
    """
    seen = {}
    deduplicated = []
    duplicate_count = 0
    duplicate_details = []

    for patient in patient_records:
        key = build_dedup_key(patient)
        if not key or key == "||":
            # Cannot deduplicate without name/dob -- keep as is
            deduplicated.append(patient)
            continue

        if key in seen:
            duplicate_count += 1
            duplicate_details.append({
                "patient_key": key,
                "duplicate_system": patient.get("source_system", ""),
                "original_system": seen[key].get("source_system", ""),
                "patient_id_dup": patient.get("patient_id", ""),
                "patient_id_orig": seen[key].get("patient_id", ""),
            })
            # Merge: fill in blanks from the duplicate if the original is empty
            for field in patient:
                if field not in seen[key] or not seen[key][field]:
                    seen[key][field] = patient[field]
        else:
            seen[key] = patient
            deduplicated.append(patient)

    return deduplicated, duplicate_count, duplicate_details


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

def check_data_quality(parsed_messages):
    """Run data quality checks and return a list of issue strings."""
    issues = []

    for idx, msg in enumerate(parsed_messages):
        msg_id = msg.get("msh", {}).get("message_control_id", f"msg_{idx}")
        source = msg.get("metadata", {}).get("source_file", "unknown")

        # Check: Patient ID present
        if not msg.get("pid", {}).get("patient_id"):
            issues.append(f"[{source}] Message {msg_id}: Missing patient ID")

        # Check: Patient name present
        if not msg.get("pid", {}).get("last_name"):
            issues.append(f"[{source}] Message {msg_id}: Missing patient last name")

        # Check: DOB present and valid
        dob = msg.get("pid", {}).get("dob", "")
        if not dob:
            issues.append(f"[{source}] Message {msg_id}: Missing date of birth")
        elif not re.match(r"^\d{4}-\d{2}-\d{2}$", dob):
            issues.append(f"[{source}] Message {msg_id}: Invalid DOB format: {dob}")

        # Check: Gender valid
        gender = msg.get("pid", {}).get("gender", "")
        if gender not in ("M", "F", "U"):
            issues.append(f"[{source}] Message {msg_id}: Invalid gender: {gender}")

        # Check: Diagnosis codes valid ICD-10
        for dg in msg.get("dg1_list", []):
            if not dg.get("diagnosis_code_valid", True):
                code = dg.get("diagnosis_code", "")
                issues.append(
                    f"[{source}] Message {msg_id}: Invalid ICD-10 code: {code}"
                )

        # Check: Message timestamp present
        if not msg.get("msh", {}).get("message_datetime"):
            issues.append(f"[{source}] Message {msg_id}: Missing message timestamp")

        # Check: Visit type present if PV1 exists
        if msg.get("pv1") and not msg["pv1"].get("visit_type"):
            issues.append(f"[{source}] Message {msg_id}: Missing visit type in PV1")

    return issues


# ============================================================
# SYSTEM-SPECIFIC TRANSFORMATIONS
# ============================================================

def apply_system_transforms(parsed_message, source_system):
    """Apply system-specific transformations based on HL7 version differences.

    system_a (v2.3):
      - Patient ID may be in PID-2 instead of PID-3
      - Dates may lack precision (YYYYMMDD only)
      - Phone may be in basic format without area code component

    system_b (v2.5.1):
      - Uses repetition separator ~ in some fields
      - More structured name components
      - Extended person identifiers

    system_c (v2.4):
      - Intermediate format between v2.3 and v2.5
      - May have varying date precisions
    """
    if not parsed_message:
        return parsed_message

    pid = parsed_message.get("pid", {})
    msh = parsed_message.get("msh", {})

    if source_system == "system_a":
        # v2.3: Ensure patient_id is populated (may be in PID-2 already handled)
        pass

    elif source_system == "system_b":
        # v2.5.1: Handle repetition separator in phone numbers
        phone = pid.get("phone", "")
        if "~" in phone:
            # Take first repetition
            phone = phone.split("~")[0]
            pid["phone"] = standardize_phone(phone)

    elif source_system == "system_c":
        # v2.4: Normalize any v2.4-specific quirks
        pass

    # Ensure facility is standardized with system context
    if msh.get("sending_facility_raw"):
        msh["sending_facility"] = standardize_facility(
            msh["sending_facility_raw"], source_system
        )

    return parsed_message


# ============================================================
# OUTPUT GENERATION
# ============================================================

def write_csv(filepath, fieldnames, rows):
    """Write rows to a CSV file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_patient_row(msg, practice_type):
    """Build a patient CSV row from a parsed message."""
    pid = msg.get("pid", {})
    msh = msg.get("msh", {})
    return {
        "patient_id": pid.get("patient_id", ""),
        "source_system": msg.get("metadata", {}).get("source_system", ""),
        "practice_type": practice_type,
        "last_name": pid.get("last_name", ""),
        "first_name": pid.get("first_name", ""),
        "dob": pid.get("dob", ""),
        "gender": pid.get("gender", ""),
        "ssn": pid.get("ssn", ""),
        "address_street": pid.get("address_street", ""),
        "address_city": pid.get("address_city", ""),
        "address_state": pid.get("address_state", ""),
        "address_zip": pid.get("address_zip", ""),
        "phone": pid.get("phone", ""),
        "facility": msh.get("sending_facility", ""),
    }


def build_encounter_row(msg, practice_type, encounter_counter):
    """Build an encounter CSV row from a parsed message."""
    msh = msg.get("msh", {})
    pid = msg.get("pid", {})
    pv1 = msg.get("pv1", {})
    encounter_id = (
        pv1.get("visit_number")
        or msh.get("message_control_id")
        or f"ENC{encounter_counter:06d}"
    )
    return {
        "encounter_id": encounter_id,
        "patient_id": pid.get("patient_id", ""),
        "source_system": msg.get("metadata", {}).get("source_system", ""),
        "practice_type": practice_type,
        "message_type": msh.get("message_type_full", ""),
        "visit_type": pv1.get("visit_type", ""),
        "attending_physician": pv1.get("attending_physician", ""),
        "attending_npi": pv1.get("attending_npi", ""),
        "admit_date": pv1.get("admit_date", ""),
        "facility": msh.get("sending_facility", ""),
        "message_timestamp": msh.get("message_datetime", ""),
    }


def build_diagnosis_rows(msg, practice_type, encounter_id, diag_id_start):
    """Build diagnosis CSV rows from a parsed message."""
    rows = []
    pid = msg.get("pid", {})
    for i, dg in enumerate(msg.get("dg1_list", [])):
        rows.append({
            "diagnosis_id": f"DG{diag_id_start + i:06d}",
            "patient_id": pid.get("patient_id", ""),
            "encounter_id": encounter_id,
            "source_system": msg.get("metadata", {}).get("source_system", ""),
            "practice_type": practice_type,
            "diagnosis_code": dg.get("diagnosis_code", ""),
            "diagnosis_description": dg.get("diagnosis_description", ""),
            "diagnosis_type": dg.get("diagnosis_type", ""),
            "sequence_number": dg.get("sequence_number", ""),
        })
    return rows


def build_order_result_rows(msg, practice_type, encounter_id, record_id_start):
    """Build order/result CSV rows from a parsed message."""
    rows = []
    pid = msg.get("pid", {})
    rid = record_id_start

    # OBR entries (orders)
    for obr in msg.get("obr_list", []):
        rows.append({
            "record_id": f"OR{rid:06d}",
            "patient_id": pid.get("patient_id", ""),
            "encounter_id": encounter_id,
            "source_system": msg.get("metadata", {}).get("source_system", ""),
            "practice_type": practice_type,
            "record_type": "Order",
            "order_procedure_code": obr.get("procedure_code", ""),
            "order_procedure_desc": obr.get("procedure_description", ""),
            "result_value": "",
            "result_units": "",
            "reference_range": "",
            "abnormal_flag": "",
            "result_status": obr.get("result_status", ""),
        })
        rid += 1

    # OBX entries (results)
    for obx in msg.get("obx_list", []):
        rows.append({
            "record_id": f"OR{rid:06d}",
            "patient_id": pid.get("patient_id", ""),
            "encounter_id": encounter_id,
            "source_system": msg.get("metadata", {}).get("source_system", ""),
            "practice_type": practice_type,
            "record_type": "Result",
            "order_procedure_code": obx.get("observation_code", ""),
            "order_procedure_desc": obx.get("observation_description", ""),
            "result_value": obx.get("result_value", ""),
            "result_units": obx.get("result_units", ""),
            "reference_range": obx.get("reference_range", ""),
            "abnormal_flag": obx.get("abnormal_flag", ""),
            "result_status": obx.get("result_status", ""),
        })
        rid += 1

    return rows, rid


def build_insurance_rows(msg, practice_type, encounter_id, ins_id_start):
    """Build insurance CSV rows from a parsed message."""
    rows = []
    pid = msg.get("pid", {})
    for i, ins in enumerate(msg.get("in1_list", [])):
        rows.append({
            "insurance_id": f"INS{ins_id_start + i:06d}",
            "patient_id": pid.get("patient_id", ""),
            "encounter_id": encounter_id,
            "source_system": msg.get("metadata", {}).get("source_system", ""),
            "practice_type": practice_type,
            "payer_name": ins.get("payer_name", ""),
            "plan_type": ins.get("plan_type", ""),
            "member_id": ins.get("member_id", ""),
            "group_number": ins.get("group_number", ""),
            "subscriber_name": ins.get("subscriber_name", ""),
            "relationship": ins.get("relationship", ""),
        })
    return rows


# ============================================================
# MAIN ETL PIPELINE
# ============================================================

def discover_hl7_files(input_dir, practice_type_filter=None):
    """Scan system directories for .hl7 files.

    Returns a dict: {(system, practice_type): [filepath, ...]}
    """
    file_map = defaultdict(list)
    systems = ["system_a", "system_b", "system_c"]

    for system in systems:
        system_dir = os.path.join(input_dir, system)
        if not os.path.isdir(system_dir):
            continue

        for practice_type in sorted(os.listdir(system_dir)):
            if practice_type_filter and practice_type != practice_type_filter:
                continue
            pt_dir = os.path.join(system_dir, practice_type)
            if not os.path.isdir(pt_dir):
                continue

            for filename in sorted(os.listdir(pt_dir)):
                if filename.lower().endswith(".hl7"):
                    filepath = os.path.join(pt_dir, filename)
                    file_map[(system, practice_type)].append(filepath)

    return file_map


def run_etl(input_dir, output_dir, practice_type_filter=None, verbose=False):
    """Execute the full ETL pipeline.

    1. Discover and extract HL7 files
    2. Parse and transform messages
    3. Validate and deduplicate
    4. Write output CSVs and consolidated JSON
    5. Generate summary report

    Returns summary statistics dict.
    """
    start_time = datetime.now()

    if verbose:
        print(f"[ETL] Starting ETL pipeline at {start_time.isoformat()}")
        print(f"[ETL] Input directory:  {input_dir}")
        print(f"[ETL] Output directory: {output_dir}")
        if practice_type_filter:
            print(f"[ETL] Practice type filter: {practice_type_filter}")

    # -- Step 1: Discover files --
    file_map = discover_hl7_files(input_dir, practice_type_filter)
    total_files = sum(len(files) for files in file_map.values())

    if verbose:
        print(f"[ETL] Discovered {total_files} HL7 file(s) across "
              f"{len(file_map)} system/practice combinations")

    # Statistics tracking
    stats = {
        "files_per_system_practice": defaultdict(int),
        "messages_parsed": 0,
        "parse_errors": [],
        "patients_total": 0,
        "patients_deduplicated": 0,
        "duplicates_found": 0,
        "duplicate_details": [],
        "data_quality_issues": [],
        "encounters_total": 0,
        "diagnoses_total": 0,
        "orders_results_total": 0,
        "insurance_records_total": 0,
        "practice_types_processed": set(),
        "start_time": start_time.isoformat(),
    }

    # -- Step 2 & 3: Parse, transform, validate per practice type --
    # Group by practice type for output
    practice_data = defaultdict(lambda: {
        "parsed_messages": [],
        "patients": [],
        "encounters": [],
        "diagnoses": [],
        "orders_results": [],
        "insurance": [],
    })

    encounter_counter = 0
    diag_id_counter = 0
    order_result_id_counter = 0
    insurance_id_counter = 0

    for (system, practice_type), filepaths in sorted(file_map.items()):
        stats["practice_types_processed"].add(practice_type)
        stats["files_per_system_practice"][f"{system}/{practice_type}"] = len(filepaths)

        if verbose:
            print(f"[ETL] Processing {system}/{practice_type}: "
                  f"{len(filepaths)} file(s)")

        for filepath in filepaths:
            messages, error = extract_messages_from_file(filepath)

            if error:
                stats["parse_errors"].append(f"{filepath}: {error}")
                if verbose:
                    print(f"  [WARN] Error reading {filepath}: {error}")
                continue

            for raw_msg in messages:
                parsed = parse_hl7_message(raw_msg, system, practice_type, filepath)
                if not parsed:
                    stats["parse_errors"].append(f"{filepath}: Failed to parse message")
                    continue

                # Apply system-specific transformations
                parsed = apply_system_transforms(parsed, system)
                stats["messages_parsed"] += 1

                pd = practice_data[practice_type]
                pd["parsed_messages"].append(parsed)

                # Build patient record
                patient_row = build_patient_row(parsed, practice_type)
                pd["patients"].append(patient_row)

                # Build encounter record
                encounter_counter += 1
                enc_row = build_encounter_row(parsed, practice_type, encounter_counter)
                encounter_id = enc_row["encounter_id"]
                pd["encounters"].append(enc_row)

                # Build diagnosis records
                diag_rows = build_diagnosis_rows(
                    parsed, practice_type, encounter_id, diag_id_counter + 1
                )
                diag_id_counter += len(diag_rows)
                pd["diagnoses"].extend(diag_rows)

                # Build order/result records
                or_rows, order_result_id_counter = build_order_result_rows(
                    parsed, practice_type, encounter_id, order_result_id_counter + 1
                )
                pd["orders_results"].extend(or_rows)

                # Build insurance records
                ins_rows = build_insurance_rows(
                    parsed, practice_type, encounter_id, insurance_id_counter + 1
                )
                insurance_id_counter += len(ins_rows)
                pd["insurance"].extend(ins_rows)

    # -- Step 4: Deduplicate patients and run quality checks --
    all_quality_issues = []
    consolidated = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "input_directory": input_dir,
            "output_directory": output_dir,
            "practice_type_filter": practice_type_filter,
        },
        "practice_types": {},
    }

    for practice_type in sorted(practice_data.keys()):
        pd = practice_data[practice_type]

        if verbose:
            print(f"[ETL] Post-processing {practice_type}: "
                  f"{len(pd['patients'])} patient record(s)")

        # Deduplicate patients
        deduped_patients, dup_count, dup_details = deduplicate_patients(pd["patients"])
        stats["duplicates_found"] += dup_count
        stats["duplicate_details"].extend(dup_details)
        pd["patients"] = deduped_patients

        # Data quality checks
        quality_issues = check_data_quality(pd["parsed_messages"])
        all_quality_issues.extend(quality_issues)

        if verbose and quality_issues:
            for issue in quality_issues:
                print(f"  [QUALITY] {issue}")

        # Update totals
        stats["patients_total"] += len(pd["patients"])
        stats["encounters_total"] += len(pd["encounters"])
        stats["diagnoses_total"] += len(pd["diagnoses"])
        stats["orders_results_total"] += len(pd["orders_results"])
        stats["insurance_records_total"] += len(pd["insurance"])

        # Build consolidated JSON entry
        consolidated["practice_types"][practice_type] = {
            "patients": pd["patients"],
            "encounters": pd["encounters"],
            "diagnoses": pd["diagnoses"],
            "orders_results": pd["orders_results"],
            "insurance": pd["insurance"],
            "statistics": {
                "patient_count": len(pd["patients"]),
                "encounter_count": len(pd["encounters"]),
                "diagnosis_count": len(pd["diagnoses"]),
                "order_result_count": len(pd["orders_results"]),
                "insurance_count": len(pd["insurance"]),
                "duplicates_removed": dup_count,
            },
        }

    stats["data_quality_issues"] = all_quality_issues
    stats["patients_deduplicated"] = stats["patients_total"]

    # -- Step 5: Write output files --
    os.makedirs(output_dir, exist_ok=True)

    for practice_type in sorted(practice_data.keys()):
        pd = practice_data[practice_type]

        # Patients CSV
        patients_path = os.path.join(output_dir, f"{practice_type}_patients.csv")
        write_csv(patients_path, PATIENT_CSV_FIELDS, pd["patients"])

        # Encounters CSV
        encounters_path = os.path.join(output_dir, f"{practice_type}_encounters.csv")
        write_csv(encounters_path, ENCOUNTER_CSV_FIELDS, pd["encounters"])

        # Diagnoses CSV
        diagnoses_path = os.path.join(output_dir, f"{practice_type}_diagnoses.csv")
        write_csv(diagnoses_path, DIAGNOSIS_CSV_FIELDS, pd["diagnoses"])

        # Orders/Results CSV
        orders_path = os.path.join(output_dir, f"{practice_type}_orders_results.csv")
        write_csv(orders_path, ORDER_RESULT_CSV_FIELDS, pd["orders_results"])

        # Insurance CSV
        insurance_path = os.path.join(output_dir, f"{practice_type}_insurance.csv")
        write_csv(insurance_path, INSURANCE_CSV_FIELDS, pd["insurance"])

        if verbose:
            print(f"[ETL] Wrote output files for {practice_type}")

    # Consolidated JSON
    consolidated_path = os.path.join(output_dir, "consolidated_repository.json")
    with open(consolidated_path, "w", encoding="utf-8") as f:
        json.dump(consolidated, f, indent=2, default=str)

    if verbose:
        print(f"[ETL] Wrote consolidated repository: {consolidated_path}")

    # -- Step 6: Generate summary report --
    end_time = datetime.now()
    stats["end_time"] = end_time.isoformat()
    stats["duration_seconds"] = (end_time - start_time).total_seconds()

    report_path = os.path.join(output_dir, "etl_summary_report.txt")
    write_summary_report(report_path, stats)

    if verbose:
        print(f"[ETL] Wrote summary report: {report_path}")
        print(f"[ETL] Pipeline completed in {stats['duration_seconds']:.2f}s")

    return stats


def write_summary_report(report_path, stats):
    """Generate a human-readable summary report."""
    lines = []
    lines.append("=" * 72)
    lines.append("  ETL SUMMARY REPORT")
    lines.append("  HL7 Data Standardization Engine")
    lines.append("=" * 72)
    lines.append("")

    # Processing timestamps
    lines.append("PROCESSING TIMESTAMPS")
    lines.append("-" * 40)
    lines.append(f"  Start Time:    {stats.get('start_time', 'N/A')}")
    lines.append(f"  End Time:      {stats.get('end_time', 'N/A')}")
    lines.append(f"  Duration:      {stats.get('duration_seconds', 0):.2f} seconds")
    lines.append("")

    # Files processed per system per practice type
    lines.append("FILES PROCESSED PER SYSTEM / PRACTICE TYPE")
    lines.append("-" * 40)
    fps = stats.get("files_per_system_practice", {})
    if fps:
        for key in sorted(fps.keys()):
            lines.append(f"  {key:<45s} {fps[key]:>5d} file(s)")
    else:
        lines.append("  No files processed.")
    total_files = sum(fps.values())
    lines.append(f"  {'TOTAL':<45s} {total_files:>5d} file(s)")
    lines.append("")

    # Messages parsed
    lines.append("MESSAGES PARSED")
    lines.append("-" * 40)
    lines.append(f"  Total messages parsed:   {stats.get('messages_parsed', 0)}")
    lines.append(f"  Parse errors:            {len(stats.get('parse_errors', []))}")
    if stats.get("parse_errors"):
        for err in stats["parse_errors"][:20]:
            lines.append(f"    - {err}")
        if len(stats["parse_errors"]) > 20:
            lines.append(f"    ... and {len(stats['parse_errors']) - 20} more")
    lines.append("")

    # Patient records consolidated
    lines.append("PATIENT RECORDS CONSOLIDATED")
    lines.append("-" * 40)
    lines.append(f"  Total patient records (after dedup):  {stats.get('patients_deduplicated', 0)}")
    lines.append(f"  Total patient records (before dedup): {stats.get('patients_total', 0) + stats.get('duplicates_found', 0)}")
    lines.append("")

    # Duplicate records found
    lines.append("DUPLICATE RECORDS FOUND")
    lines.append("-" * 40)
    lines.append(f"  Duplicates detected:     {stats.get('duplicates_found', 0)}")
    if stats.get("duplicate_details"):
        for dup in stats["duplicate_details"][:20]:
            lines.append(
                f"    - Patient: {dup.get('patient_key', 'N/A')} | "
                f"Dup system: {dup.get('duplicate_system', '')} "
                f"(ID: {dup.get('patient_id_dup', '')}) | "
                f"Orig system: {dup.get('original_system', '')} "
                f"(ID: {dup.get('patient_id_orig', '')})"
            )
        if len(stats["duplicate_details"]) > 20:
            lines.append(f"    ... and {len(stats['duplicate_details']) - 20} more")
    lines.append("")

    # Record counts
    lines.append("RECORD COUNTS")
    lines.append("-" * 40)
    lines.append(f"  Encounters:              {stats.get('encounters_total', 0)}")
    lines.append(f"  Diagnoses:               {stats.get('diagnoses_total', 0)}")
    lines.append(f"  Orders/Results:          {stats.get('orders_results_total', 0)}")
    lines.append(f"  Insurance Records:       {stats.get('insurance_records_total', 0)}")
    lines.append("")

    # Practice types processed
    lines.append("PRACTICE TYPES PROCESSED")
    lines.append("-" * 40)
    pts = sorted(stats.get("practice_types_processed", set()))
    if pts:
        for pt in pts:
            lines.append(f"  - {pt}")
    else:
        lines.append("  None")
    lines.append("")

    # Data quality issues detected
    quality_issues = stats.get("data_quality_issues", [])
    lines.append("DATA QUALITY ISSUES DETECTED")
    lines.append("-" * 40)
    lines.append(f"  Total issues:            {len(quality_issues)}")
    if quality_issues:
        # Categorize issues
        categories = defaultdict(int)
        for issue in quality_issues:
            if "Missing patient ID" in issue:
                categories["Missing Patient ID"] += 1
            elif "Missing patient last name" in issue:
                categories["Missing Patient Name"] += 1
            elif "Missing date of birth" in issue:
                categories["Missing DOB"] += 1
            elif "Invalid DOB format" in issue:
                categories["Invalid DOB Format"] += 1
            elif "Invalid gender" in issue:
                categories["Invalid Gender"] += 1
            elif "Invalid ICD-10 code" in issue:
                categories["Invalid ICD-10 Code"] += 1
            elif "Missing message timestamp" in issue:
                categories["Missing Timestamp"] += 1
            elif "Missing visit type" in issue:
                categories["Missing Visit Type"] += 1
            else:
                categories["Other"] += 1

        for cat in sorted(categories.keys()):
            lines.append(f"    {cat:<35s} {categories[cat]:>5d}")

        lines.append("")
        lines.append("  Sample Issues (first 30):")
        for issue in quality_issues[:30]:
            lines.append(f"    - {issue}")
        if len(quality_issues) > 30:
            lines.append(f"    ... and {len(quality_issues) - 30} more")
    lines.append("")

    lines.append("=" * 72)
    lines.append("  END OF REPORT")
    lines.append("=" * 72)
    lines.append("")

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ============================================================
# CLI ENTRY POINT
# ============================================================

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="ETL Engine for standardizing HL7 data from multiple systems.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python etl_engine.py\n"
            "  python etl_engine.py --verbose\n"
            "  python etl_engine.py --practice-type cardiology --verbose\n"
            "  python etl_engine.py --input-dir /data/hl7 --output-dir /data/output\n"
        ),
    )
    parser.add_argument(
        "--input-dir",
        default="test_data/hl7",
        help="Root directory containing system_a/system_b/system_c HL7 data "
             "(default: test_data/hl7)",
    )
    parser.add_argument(
        "--output-dir",
        default="test_data/hl7/standardized",
        help="Output directory for standardized CSV/JSON files "
             "(default: test_data/hl7/standardized)",
    )
    parser.add_argument(
        "--practice-type",
        default=None,
        help="Process only a single practice type (e.g., cardiology, primary_care)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed processing information",
    )
    return parser.parse_args()


def main():
    """Main entry point for the ETL engine."""
    args = parse_args()

    # Resolve paths relative to the script's parent directory if not absolute
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    input_dir = args.input_dir
    if not os.path.isabs(input_dir):
        input_dir = os.path.join(project_root, input_dir)

    output_dir = args.output_dir
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(project_root, output_dir)

    stats = run_etl(
        input_dir=input_dir,
        output_dir=output_dir,
        practice_type_filter=args.practice_type,
        verbose=args.verbose,
    )

    # Print final summary to stdout
    print(f"\nETL Pipeline Complete")
    print(f"  Messages parsed:     {stats['messages_parsed']}")
    print(f"  Patients (deduped):  {stats['patients_deduplicated']}")
    print(f"  Duplicates found:    {stats['duplicates_found']}")
    print(f"  Encounters:          {stats['encounters_total']}")
    print(f"  Diagnoses:           {stats['diagnoses_total']}")
    print(f"  Orders/Results:      {stats['orders_results_total']}")
    print(f"  Insurance records:   {stats['insurance_records_total']}")
    print(f"  Quality issues:      {len(stats['data_quality_issues'])}")
    print(f"  Duration:            {stats['duration_seconds']:.2f}s")
    print(f"  Output:              {output_dir}")


if __name__ == "__main__":
    main()
