"""
HL7 v2.x test file generator for 15 practice types across 3 systems.
Generates 5 HL7 files per practice type per system (225 total).

Each system uses slightly different HL7 formatting to simulate real-world variation:
  - system_a: HL7 v2.3, sending facility "METROEMR", varied date formats, more PID fields
  - system_b: HL7 v2.5.1, sending facility "SUMMITEMR", extra OBX segments, different segment ordering
  - system_c: HL7 v2.4, sending facility "HERITAGEEHR", less field detail, different coding system names
"""
import os
import random
from datetime import datetime, timedelta

from generators.test_data_commons import *

# ============================================================
# SYSTEM CONFIGURATION
# ============================================================
SYSTEMS = ["system_a", "system_b", "system_c"]
FILES_PER_PRACTICE_PER_SYSTEM = 5

SYSTEM_CONFIG = {
    "system_a": {
        "version": "2.3",
        "sending_facility": "METROEMR",
        "sending_app": "METRO_ADT",
        "receiving_app": "CLEARINGHOUSE",
        "receiving_facility": "CENTRAL_HUB",
    },
    "system_b": {
        "version": "2.5.1",
        "sending_facility": "SUMMITEMR",
        "sending_app": "SUMMIT_HIS",
        "receiving_app": "SUMMIT_ROUTE",
        "receiving_facility": "REGIONAL_HUB",
    },
    "system_c": {
        "version": "2.4",
        "sending_facility": "HERITAGEEHR",
        "sending_app": "HERITAGE_EHR",
        "receiving_app": "HERITAGE_GW",
        "receiving_facility": "DATA_CENTER",
    },
}

# HL7 message types to cycle through per file
MESSAGE_TYPES = [
    ("ADT", "A01", "ADT_A01", "Admit/Visit Notification"),
    ("ADT", "A04", "ADT_A04", "Register a Patient"),
    ("ADT", "A08", "ADT_A08", "Update Patient Information"),
    ("ORM", "O01", "ORM_O01", "Order Message"),
    ("ORU", "R01", "ORU_R01", "Unsolicited Observation Result"),
]

# Patient class codes by specialty (for PV1 segment)
PATIENT_CLASS_MAP = {
    "primary_care": "O",        # Outpatient
    "internal_medicine": "I",   # Inpatient
    "allergy_immunology": "O",
    "orthopaedics": "O",
    "cardiology": "I",
    "behavioral_health": "O",
    "radiology": "O",
    "pathology": "O",
    "gastroenterology": "O",
    "ob_gyn": "O",
    "dermatology": "O",
    "anesthesiology": "I",
    "urgent_care": "E",         # Emergency
    "pediatrics": "O",
    "clinical_laboratory": "O",
}

# Admit source codes
ADMIT_SOURCES = ["1", "2", "3", "4", "5", "7"]  # Physician referral, Transfer, ER, etc.

# Discharge disposition codes
DISCHARGE_DISPOSITIONS = ["01", "02", "03", "04", "06", "09"]


# ============================================================
# SEGMENT BUILDERS
# ============================================================

def _esc(value):
    """Escape HL7 special characters in a field value."""
    if value is None:
        return ""
    return str(value).replace("\\", "\\E\\").replace("|", "\\F\\").replace("^", "\\S\\").replace("~", "\\R\\").replace("&", "\\T\\")


def build_msh(system, msg_type_tuple, msg_datetime, control_id, practice):
    """Build MSH segment. Varies by system."""
    cfg = SYSTEM_CONFIG[system]
    msg_type, trigger, structure, _ = msg_type_tuple

    if system == "system_a":
        # system_a: HL7 v2.3 -- date as YYYYMMDDHHMMSS
        ts = msg_datetime.strftime("%Y%m%d%H%M%S")
        security = ""
        msh = (
            f"MSH|^~\\&|{cfg['sending_app']}|{cfg['sending_facility']}|"
            f"{cfg['receiving_app']}|{cfg['receiving_facility']}|{ts}|{security}|"
            f"{msg_type}^{trigger}|{control_id}|P|{cfg['version']}|||AL|NE"
        )
    elif system == "system_b":
        # system_b: HL7 v2.5.1 -- includes message structure, longer timestamp
        ts = msg_datetime.strftime("%Y%m%d%H%M%S.0000%z") if msg_datetime.tzinfo else msg_datetime.strftime("%Y%m%d%H%M%S.0000")
        msh = (
            f"MSH|^~\\&|{cfg['sending_app']}|{cfg['sending_facility']}|"
            f"{cfg['receiving_app']}|{cfg['receiving_facility']}|{ts}||"
            f"{msg_type}^{trigger}^{structure}|{control_id}|P|{cfg['version']}|||AL|NE|||"
            f"ASCII|"
        )
    else:
        # system_c: HL7 v2.4 -- shorter timestamp YYYYMMDDHHMM
        ts = msg_datetime.strftime("%Y%m%d%H%M")
        msh = (
            f"MSH|^~\\&|{cfg['sending_app']}|{cfg['sending_facility']}|"
            f"{cfg['receiving_app']}|{cfg['receiving_facility']}|{ts}||"
            f"{msg_type}^{trigger}|{control_id}|P|{cfg['version']}|||AL|NE"
        )
    return msh


def build_evn(system, msg_type_tuple, event_datetime):
    """Build EVN segment."""
    _, trigger, _, _ = msg_type_tuple

    if system == "system_a":
        ts = event_datetime.strftime("%Y%m%d%H%M%S")
        evn = f"EVN|{trigger}|{ts}|||ADMIN"
    elif system == "system_b":
        ts = event_datetime.strftime("%Y%m%d%H%M%S.0000")
        planned = (event_datetime + timedelta(minutes=15)).strftime("%Y%m%d%H%M%S.0000")
        evn = f"EVN|{trigger}|{ts}|{planned}||SYSTEM|{ts}"
    else:
        ts = event_datetime.strftime("%Y%m%d%H%M")
        evn = f"EVN|{trigger}|{ts}"
    return evn


def build_pid(system, patient, visit_num):
    """Build PID segment. system_a includes more fields."""
    p = patient

    if system == "system_a":
        # system_a: More PID fields -- includes SSN, race, ethnicity, birth place
        dob = p["dob"]
        pid = (
            f"PID|1|{p['patient_id']}^^^METROEMR^MR|{p['patient_id']}^^^METROEMR^MR~"
            f"{p['member_id']}^^^INSCO^MA|{p['patient_id']}|"
            f"{p['last']}^{p['first']}^^^||{dob}|{p['gender']}||2106-3^White^HL70005|"
            f"{p['street']}^^{p['city']}^{p['state']}^{p['zip']}^US||"
            f"^PRN^PH^^^{p['phone'][:3]}^{p['phone'][3:]}|"
            f"^WPN^PH||{_marital_status(p)}|CHR|{visit_num}|{p['ssn']}|||||||||||N"
        )
    elif system == "system_b":
        # system_b: Standard PID with slightly different phone formatting
        dob = p["dob"]
        pid = (
            f"PID|1||{p['patient_id']}^^^SUMMITEMR^MR~"
            f"{p['member_id']}^^^INSURANCE^MI||"
            f"{p['last']}^{p['first']}^^||{dob}|{p['gender']}||"
            f"|{p['street']}^^{p['city']}^{p['state']}^{p['zip']}^USA^H||"
            f"({p['phone'][:3]}){p['phone'][3:6]}-{p['phone'][6:]}^PRN^PH||"
            f"|{_marital_status(p)}||{visit_num}|{p['ssn']}"
        )
    else:
        # system_c: Less detail -- no SSN, simpler address
        dob = p["dob"]
        pid = (
            f"PID|1||{p['patient_id']}^^^HERITAGE^MR||"
            f"{p['last']}^{p['first']}||{dob}|{p['gender']}|||"
            f"{p['street']}^^{p['city']}^{p['state']}^{p['zip']}||"
            f"{p['phone']}|||||{visit_num}"
        )
    return pid


def build_pv1(system, provider, practice, admit_datetime, discharge_datetime, visit_num):
    """Build PV1 segment."""
    patient_class = PATIENT_CLASS_MAP.get(practice, "O")
    facility_display = PRACTICE_DISPLAY_NAMES.get(practice, practice)

    if system == "system_a":
        admit_ts = admit_datetime.strftime("%Y%m%d%H%M%S")
        disch_ts = discharge_datetime.strftime("%Y%m%d%H%M%S") if discharge_datetime else ""
        pv1 = (
            f"PV1|1|{patient_class}|{facility_display}^^^METROEMR||||"
            f"{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}^^^NPI|"
            f"{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}^^^NPI|"
            f"{facility_display}|||||||"
            f"{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}^^^NPI|"
            f"{patient_class}||||||||||||||||||||||||||"
            f"{admit_ts}|{disch_ts}|||||||{visit_num}"
        )
    elif system == "system_b":
        admit_ts = admit_datetime.strftime("%Y%m%d%H%M%S.0000")
        disch_ts = discharge_datetime.strftime("%Y%m%d%H%M%S.0000") if discharge_datetime else ""
        admit_src = random.choice(ADMIT_SOURCES)
        disch_disp = random.choice(DISCHARGE_DISPOSITIONS) if discharge_datetime else ""
        pv1 = (
            f"PV1|1|{patient_class}|{facility_display}^^^SUMMITEMR^^^^^DEPT||||"
            f"{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}^^"
            f"^SUMMIT^L^^^NPI|"
            f"{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}^^"
            f"^SUMMIT^L^^^NPI|"
            f"{facility_display}||{admit_src}|||||"
            f"{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}^^"
            f"^SUMMIT^L^^^NPI|"
            f"{patient_class}|{visit_num}||||||||||||||||||||{disch_disp}|||"
            f"{admit_ts}|{disch_ts}"
        )
    else:
        admit_ts = admit_datetime.strftime("%Y%m%d%H%M")
        disch_ts = discharge_datetime.strftime("%Y%m%d%H%M") if discharge_datetime else ""
        pv1 = (
            f"PV1|1|{patient_class}|{facility_display}^^^^||||"
            f"{provider['npi']}^{provider['last']}^{provider['first']}|"
            f"{provider['npi']}^{provider['last']}^{provider['first']}|"
            f"|||||||"
            f"{provider['npi']}^{provider['last']}^{provider['first']}|"
            f"{patient_class}|||||||||||||||||||||||||"
            f"{admit_ts}|{disch_ts}"
        )
    return pv1


def build_obr(system, practice, cpt_tuple, order_datetime, provider, set_id):
    """Build OBR segment for orders/results."""
    cpt_code, cpt_desc, _ = cpt_tuple

    if system == "system_a":
        ts = order_datetime.strftime("%Y%m%d%H%M%S")
        obr = (
            f"OBR|{set_id}|ORD{set_id:04d}^METROEMR|RES{set_id:04d}^METRO_LAB|"
            f"{cpt_code}^{cpt_desc}^CPT4|R||{ts}|||||||{ts}|"
            f"|||{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}^^^NPI|"
            f"||||||{ts}|||F|||||||"
            f"{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}"
        )
    elif system == "system_b":
        ts = order_datetime.strftime("%Y%m%d%H%M%S.0000")
        obr = (
            f"OBR|{set_id}|ORD{set_id:04d}^SUMMIT_OE|RES{set_id:04d}^SUMMIT_LAB|"
            f"{cpt_code}^{cpt_desc}^CPT^{cpt_code}^{cpt_desc}^L|R||{ts}|||||"
            f"|{ts}||"
            f"{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}^^"
            f"^SUMMIT^L^^^NPI|"
            f"||||||{ts}|||F|||||||"
            f"{provider['npi']}^{provider['last']}^{provider['first']}^^^{provider['suffix']}"
        )
    else:
        # system_c: uses different coding system name
        ts = order_datetime.strftime("%Y%m%d%H%M")
        obr = (
            f"OBR|{set_id}|ORD{set_id:04d}|RES{set_id:04d}|"
            f"{cpt_code}^{cpt_desc}^C4||{ts}||||||"
            f"||{ts}|"
            f"{provider['npi']}^{provider['last']}^{provider['first']}|"
            f"||||||{ts}|||F"
        )
    return obr


def build_obx(system, cpt_tuple, observation_datetime, set_id, sub_id=1):
    """Build OBX segment for observation results."""
    cpt_code, cpt_desc, amount = cpt_tuple

    # Generate plausible observation values based on the code
    obs_value, units, ref_range, abnormal_flag = _generate_observation_value(cpt_code, cpt_desc, set_id)

    if system == "system_a":
        ts = observation_datetime.strftime("%Y%m%d%H%M%S")
        obx = (
            f"OBX|{sub_id}|NM|{cpt_code}^{cpt_desc}^CPT4||"
            f"{obs_value}|{units}|{ref_range}|{abnormal_flag}|||F|||{ts}"
        )
    elif system == "system_b":
        ts = observation_datetime.strftime("%Y%m%d%H%M%S.0000")
        obx = (
            f"OBX|{sub_id}|NM|{cpt_code}^{cpt_desc}^CPT^{cpt_code}^{cpt_desc}^L||"
            f"{obs_value}|{units}|{ref_range}|{abnormal_flag}|||F|||{ts}||"
            f"SUMMIT_LAB^Summit Reference Lab^L"
        )
    else:
        ts = observation_datetime.strftime("%Y%m%d%H%M")
        obx = (
            f"OBX|{sub_id}|NM|{cpt_code}^{cpt_desc}^C4||"
            f"{obs_value}|{units}|{ref_range}|{abnormal_flag}|||F|||{ts}"
        )
    return obx


def build_obx_text(system, text_value, observation_datetime, sub_id):
    """Build a text-type OBX segment (used by system_b for extra detail)."""
    ts = observation_datetime.strftime("%Y%m%d%H%M%S.0000")
    obx = (
        f"OBX|{sub_id}|TX|COMMENT^Observation Comment^L||"
        f"{text_value}||||||F|||{ts}"
    )
    return obx


def build_dg1(system, icd_tuple, set_id, diagnosis_datetime):
    """Build DG1 segment."""
    icd_code, icd_desc = icd_tuple

    if system == "system_a":
        ts = diagnosis_datetime.strftime("%Y%m%d%H%M%S")
        dg1 = (
            f"DG1|{set_id}|ICD10|{icd_code}^{icd_desc}^ICD10CM||{ts}|A"
        )
    elif system == "system_b":
        ts = diagnosis_datetime.strftime("%Y%m%d%H%M%S.0000")
        diag_type = "A" if set_id == 1 else "S"
        dg1 = (
            f"DG1|{set_id}|I10|{icd_code}^{icd_desc}^ICD-10-CM||{ts}|{diag_type}||||||||1"
        )
    else:
        ts = diagnosis_datetime.strftime("%Y%m%d%H%M")
        dg1 = (
            f"DG1|{set_id}|I10|{icd_code}^{icd_desc}^I10||{ts}|A"
        )
    return dg1


def build_in1(system, payer, patient, set_id):
    """Build IN1 insurance segment."""
    p = patient

    if system == "system_a":
        in1 = (
            f"IN1|{set_id}|{payer['plan_type']}^{payer['name']}|{payer['payer_id']}|"
            f"{payer['name']}|"
            f"123 Insurance Ave^^Hartford^CT^06101|"
            f"^WPN^PH^^^800^5551234|"
            f"GRP{random.randint(10000,99999)}||{payer['name']}|||||"
            f"{p['last']}^{p['first']}|SELF|{p['dob']}|"
            f"{p['street']}^^{p['city']}^{p['state']}^{p['zip']}|Y||"
            f"1|{p['member_id']}||||||||||||||||"
            f"{payer['id']}|||||||||{p['gender']}"
        )
    elif system == "system_b":
        in1 = (
            f"IN1|{set_id}|{payer['plan_type']}^{payer['name']}^SUMMIT_INS|{payer['payer_id']}|"
            f"{payer['name']}|"
            f"PO Box 12345^^Metropolis^NY^10101^USA|"
            f"(800)555-1234^WPN^PH|"
            f"GRP{random.randint(10000,99999)}||{payer['name']}|||||"
            f"{p['last']}^{p['first']}^^|01^Self^HL70063|{p['dob']}|"
            f"{p['street']}^^{p['city']}^{p['state']}^{p['zip']}^USA|Y||"
            f"1|{p['member_id']}^{payer['payer_id']}|||||||||||||||"
            f"||||||||{p['gender']}"
        )
    else:
        in1 = (
            f"IN1|{set_id}|{payer['plan_type']}|{payer['payer_id']}|"
            f"{payer['name']}|"
            f"|||"
            f"GRP{random.randint(10000,99999)}|{payer['name']}|||||"
            f"{p['last']}^{p['first']}|SELF|{p['dob']}|"
            f"{p['street']}^^{p['city']}^{p['state']}^{p['zip']}|||||"
            f"{p['member_id']}"
        )
    return in1


# ============================================================
# HELPER UTILITIES
# ============================================================

def _marital_status(patient):
    """Derive a plausible marital status from seeded randomness."""
    return random.choice(["S", "M", "M", "M", "D", "W"])


def _generate_observation_value(cpt_code, cpt_desc, seed_offset):
    """Generate plausible observation values based on the procedure."""
    random.seed(hash(cpt_code) + seed_offset)

    # Map common lab CPT codes to realistic value ranges
    lab_values = {
        "80053": ("142", "mEq/L", "136-145", "N"),      # CMP - sodium
        "80048": ("98", "mEq/L", "96-106", "N"),         # BMP - chloride
        "85025": ("12.5", "g/dL", "12.0-16.0", "N"),     # CBC - hemoglobin
        "85027": ("7.2", "K/uL", "4.5-11.0", "N"),       # CBC
        "82947": ("105", "mg/dL", "70-100", "H"),         # Glucose
        "83036": ("6.8", "%", "4.0-5.6", "H"),            # HbA1c
        "80061": ("210", "mg/dL", "0-200", "H"),          # Lipid panel
        "84443": ("2.5", "mIU/L", "0.4-4.0", "N"),       # TSH
        "81001": ("NEG", "", "NEG", "N"),                  # UA
        "81002": ("NEG", "", "NEG", "N"),                  # UA non-auto
        "87086": ("POSITIVE", "", "NEG", "A"),             # Urine culture
        "82306": ("28", "ng/mL", "30-100", "L"),           # Vitamin D
        "84153": ("3.2", "ng/mL", "0.0-4.0", "N"),        # PSA
        "82607": ("450", "pg/mL", "200-900", "N"),         # B12
        "36415": ("COMPLETE", "", "", ""),                  # Venipuncture
        "93000": ("72", "bpm", "60-100", "N"),             # EKG HR
        "86003": ("0.45", "kU/L", "0.00-0.34", "H"),      # IgE
        "86580": ("NEGATIVE", "mm", "0-5", "N"),           # TB skin test
    }

    if cpt_code in lab_values:
        val, units, ref, flag = lab_values[cpt_code]
        # Add some variation
        try:
            fval = float(val)
            fval += random.uniform(-fval * 0.1, fval * 0.1)
            val = f"{fval:.1f}"
        except ValueError:
            pass
        return val, units, ref, flag

    # Default: numeric result
    val = round(random.uniform(0.5, 500.0), 1)
    return str(val), "units", "N/A", ""


def _generate_visit_number(system, practice_idx, file_idx):
    """Generate visit/encounter number."""
    prefix_map = {
        "system_a": "MV",
        "system_b": "SV",
        "system_c": "HV",
    }
    prefix = prefix_map.get(system, "VN")
    return f"{prefix}{practice_idx:02d}{file_idx:03d}{random.randint(1000,9999)}"


# ============================================================
# MESSAGE ASSEMBLY
# ============================================================

def build_hl7_message(system, practice, practice_idx, file_idx):
    """Build a complete HL7 message for the given system/practice/file."""
    # Deterministic seed for reproducibility
    base_seed = hash((system, practice, file_idx)) & 0x7FFFFFFF

    # Select message type -- cycle through the 5 types
    msg_type_tuple = MESSAGE_TYPES[file_idx % len(MESSAGE_TYPES)]
    msg_type, trigger, structure, description = msg_type_tuple

    # Generate patient, provider, payer
    patient = get_random_patient(base_seed)
    provider = get_random_provider(practice, base_seed + 100)
    payer = get_random_payer(base_seed + 200)

    # Generate dates
    random.seed(base_seed + 300)
    service_date = get_random_date(start_days_ago=180, end_days_ago=5, seed=base_seed + 300)
    admit_datetime = service_date
    discharge_datetime = service_date + timedelta(hours=random.randint(1, 72)) if PATIENT_CLASS_MAP.get(practice) == "I" else None
    msg_datetime = service_date + timedelta(minutes=random.randint(0, 30))

    # Visit number
    visit_num = _generate_visit_number(system, practice_idx, file_idx)

    # Control ID
    control_id = f"{system[0].upper()}{practice_idx:02d}{file_idx:03d}{random.randint(100000,999999)}"

    # Get specialty codes
    random.seed(base_seed + 400)
    num_cpt = random.randint(1, 4)
    num_icd = random.randint(1, 3)
    cpt_list = random.sample(
        SPECIALTY_CPT_CODES.get(practice, SPECIALTY_CPT_CODES["primary_care"]),
        min(num_cpt, len(SPECIALTY_CPT_CODES.get(practice, [])))
    )
    icd_list = random.sample(
        SPECIALTY_ICD10_CODES.get(practice, SPECIALTY_ICD10_CODES["primary_care"]),
        min(num_icd, len(SPECIALTY_ICD10_CODES.get(practice, [])))
    )

    # ---- Build segments ----
    segments = []

    # MSH
    segments.append(build_msh(system, msg_type_tuple, msg_datetime, control_id, practice))

    # EVN
    segments.append(build_evn(system, msg_type_tuple, service_date))

    # PID
    segments.append(build_pid(system, patient, visit_num))

    if system == "system_b":
        # system_b: PV1 comes after NK1 placeholder, different ordering
        # Add a NK1 (Next of Kin) placeholder for system_b
        nk1 = f"NK1|1|{patient['last']}^Emergency^Contact|SPO^Spouse^HL70063|{patient['street']}^^{patient['city']}^{patient['state']}^{patient['zip']}|{patient['phone']}"
        segments.append(nk1)

    # PV1
    segments.append(build_pv1(system, provider, practice, admit_datetime, discharge_datetime, visit_num))

    # For ADT messages, add DG1 before OBR/OBX
    if msg_type == "ADT":
        for diag_idx, icd in enumerate(icd_list, start=1):
            segments.append(build_dg1(system, icd, diag_idx, service_date))

    # For ORM and ORU messages, include OBR and OBX segments
    if msg_type in ("ORM", "ORU"):
        for obr_idx, cpt in enumerate(cpt_list, start=1):
            order_dt = service_date + timedelta(minutes=obr_idx * 10)
            segments.append(build_obr(system, practice, cpt, order_dt, provider, obr_idx))

            if msg_type == "ORU":
                # Add OBX for results
                obs_dt = order_dt + timedelta(hours=random.randint(1, 24))
                segments.append(build_obx(system, cpt, obs_dt, obr_idx, sub_id=1))

                if system == "system_b":
                    # system_b adds extra OBX text segments
                    comments = [
                        "Specimen received in good condition.",
                        "Results verified by technologist.",
                        f"Reference lab: Summit Reference Lab, Test performed at {random.choice(FACILITY_NAMES[system])}.",
                        "Patient was fasting per protocol.",
                    ]
                    extra_comment = random.choice(comments)
                    segments.append(build_obx_text(system, extra_comment, obs_dt, sub_id=2))
                    # system_b sometimes adds a third OBX with method info
                    if random.random() > 0.4:
                        method_note = f"Method: Automated analyzer. Batch ID: B{random.randint(10000,99999)}"
                        segments.append(build_obx_text(system, method_note, obs_dt, sub_id=3))

        # DG1 segments after OBR/OBX for order/result messages
        for diag_idx, icd in enumerate(icd_list, start=1):
            segments.append(build_dg1(system, icd, diag_idx, service_date))

    # For ADT A01/A04 with certain specialties, add OBR/OBX even in ADT context
    # to simulate orders placed at admission
    if msg_type == "ADT" and trigger in ("A01", "A04") and len(cpt_list) > 0:
        order_dt = service_date + timedelta(minutes=5)
        segments.append(build_obr(system, practice, cpt_list[0], order_dt, provider, 1))
        if system == "system_b":
            obs_dt = order_dt + timedelta(hours=2)
            segments.append(build_obx(system, cpt_list[0], obs_dt, 1, sub_id=1))
            segments.append(build_obx_text(system, "Order placed at time of registration.", obs_dt, sub_id=2))

    # IN1 segment
    segments.append(build_in1(system, payer, patient, 1))

    # system_a: Add a second insurance if patient has secondary
    if system == "system_a" and random.random() > 0.6:
        payer2 = get_random_payer(base_seed + 999)
        if payer2["payer_id"] != payer["payer_id"]:
            segments.append(build_in1(system, payer2, patient, 2))

    return "\r".join(segments)


# ============================================================
# FILE OUTPUT
# ============================================================

def write_hl7_file(output_dir, system, practice, file_idx, content):
    """Write an HL7 message to the appropriate file path."""
    dir_path = os.path.join(output_dir, system, practice)
    os.makedirs(dir_path, exist_ok=True)
    filename = f"hl7_{practice}_{system}_{file_idx}.hl7"
    filepath = os.path.join(dir_path, filename)
    with open(filepath, "w", newline="") as f:
        f.write(content)
    return filepath


# ============================================================
# MAIN
# ============================================================

def main():
    """Generate all HL7 test files."""
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test_data", "hl7")

    total_files = 0
    print(f"HL7 v2.x Test File Generator")
    print(f"Output directory: {output_dir}")
    print(f"Systems: {', '.join(SYSTEMS)}")
    print(f"Practice types: {len(PRACTICE_TYPES)}")
    print(f"Files per practice per system: {FILES_PER_PRACTICE_PER_SYSTEM}")
    print(f"Total files to generate: {len(SYSTEMS) * len(PRACTICE_TYPES) * FILES_PER_PRACTICE_PER_SYSTEM}")
    print("-" * 60)

    for system in SYSTEMS:
        sys_count = 0
        for practice_idx, practice in enumerate(PRACTICE_TYPES):
            for file_idx in range(FILES_PER_PRACTICE_PER_SYSTEM):
                content = build_hl7_message(system, practice, practice_idx, file_idx)
                filepath = write_hl7_file(output_dir, system, practice, file_idx, content)
                total_files += 1
                sys_count += 1
        cfg = SYSTEM_CONFIG[system]
        print(f"  {system} ({cfg['sending_facility']}, HL7 v{cfg['version']}): {sys_count} files generated")

    print("-" * 60)
    print(f"Total HL7 files generated: {total_files}")


if __name__ == "__main__":
    main()
