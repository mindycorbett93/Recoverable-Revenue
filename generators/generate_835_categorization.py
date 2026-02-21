"""
Generate EDI 835 test files for denial categorization and preventable denial analysis.

Produces 12 files per practice type (180 total) organized by denial preventability:
  Files 1-3:   PREVENTABLE - Front-End/Registration Errors (CARC 31, 27, 22)
  Files 4-6:   PREVENTABLE - Coding/Documentation Errors (CARC 4, 11, 97, 167)
  Files 7-8:   PREVENTABLE - Authorization/Pre-cert Failures (CARC 197, 50)
  Files 9-10:  NON-PREVENTABLE - Payer-Driven Denials (CARC 96, 222, 109)
  Files 11-12: MIXED - Complex Scenarios (multiple CLP segments, mixed denials)

Output: test_data/835_denial_categorization/{practice_type}/835_cat_{practice}_{idx:03d}.edi
"""

import os
import random

from generators.test_data_commons import *

# ============================================================
# OUTPUT CONFIGURATION
# ============================================================
BASE_OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "test_data", "835_denial_categorization"
)

FILES_PER_PRACTICE = 12

# ============================================================
# DENIAL CATEGORY DEFINITIONS
# ============================================================
# Files 1-3: PREVENTABLE - Front-End / Registration Errors
FRONTEND_REGISTRATION_CARCS = [
    {"carc": "31", "group": "CO", "rarc": "N362"},
    {"carc": "31", "group": "OA", "rarc": "N362"},
    {"carc": "27", "group": "CO", "rarc": "N362"},
    {"carc": "27", "group": "OA", "rarc": "N362"},
    {"carc": "22", "group": "CO", "rarc": "N362"},
    {"carc": "22", "group": "OA", "rarc": "N362"},
]

# Files 4-6: PREVENTABLE - Coding / Documentation Errors
CODING_DOCUMENTATION_CARCS = [
    {"carc": "4",   "group": "CO", "rarc": "N386"},
    {"carc": "11",  "group": "CO", "rarc": "N386"},
    {"carc": "97",  "group": "CO", "rarc": "N386"},
    {"carc": "167", "group": "CO", "rarc": "N386"},
]

# Files 7-8: PREVENTABLE - Authorization / Pre-cert Failures
AUTH_PRECERT_CARCS = [
    {"carc": "197", "group": "CO", "rarc": "MA130"},
    {"carc": "197", "group": "PI", "rarc": "MA130"},
    {"carc": "50",  "group": "CO", "rarc": "MA130"},
    {"carc": "50",  "group": "PI", "rarc": "MA130"},
]

# Files 9-10: NON-PREVENTABLE - Payer-Driven Denials
PAYER_DRIVEN_CARCS = [
    {"carc": "96",  "group": "PI", "rarc": "N381"},
    {"carc": "96",  "group": "OA", "rarc": "N381"},
    {"carc": "222", "group": "PI", "rarc": "N381"},
    {"carc": "222", "group": "OA", "rarc": "N381"},
    {"carc": "109", "group": "PI", "rarc": "N381"},
    {"carc": "109", "group": "OA", "rarc": "N381"},
]

# Files 11-12: MIXED - Complex Scenarios (patient responsibility CARCs + denial CARCs)
MIXED_DENIAL_CARCS = ["4", "11", "97", "167", "197", "50", "96", "222", "31", "27", "109"]
MIXED_PATIENT_RESP_CARCS = ["1", "2", "3"]
MIXED_RARC_POOL = ["N362", "N386", "MA130", "N479", "M15", "N95", "N381", "MA04"]
MIXED_GROUP_CODES = ["CO", "PR", "PI", "OA"]


# ============================================================
# ISA/GS/ST ENVELOPE BUILDERS
# ============================================================

def build_isa_segment(sender_id, receiver_id, interchange_date, interchange_time, control_number):
    """Build ISA segment with proper padding."""
    sender_padded = f"{sender_id:<15}"
    receiver_padded = f"{receiver_id:<15}"
    return (
        f"ISA*00*          *00*          "
        f"*ZZ*{sender_padded}"
        f"*ZZ*{receiver_padded}"
        f"*{interchange_date}*{interchange_time}"
        f"*^*00501*{control_number:>9}*0*P*:~"
    )


def build_gs_segment(sender_id, receiver_id, gs_date, gs_time, group_control):
    """Build GS (functional group header) segment."""
    return (
        f"GS*HP*{sender_id}*{receiver_id}"
        f"*{gs_date}*{gs_time}*{group_control}*X*005010X221A1~"
    )


def build_st_segment(transaction_control):
    """Build ST (transaction set header) segment."""
    return f"ST*835*{transaction_control}~"


def build_se_segment(segment_count, transaction_control):
    """Build SE (transaction set trailer) segment."""
    return f"SE*{segment_count}*{transaction_control}~"


def build_ge_segment(tx_count, group_control):
    """Build GE (functional group trailer) segment."""
    return f"GE*{tx_count}*{group_control}~"


def build_iea_segment(group_count, control_number):
    """Build IEA (interchange control trailer) segment."""
    return f"IEA*{group_count}*{control_number:>9}~"


# ============================================================
# 835 HEADER SEGMENTS (BPR, TRN, DTM, N1/N3/N4)
# ============================================================

def build_bpr_segment(total_paid, payment_date):
    """Build BPR (financial information) segment.
    For denial files total_paid is 0.00 since all claims are denied.
    """
    return (
        f"BPR*H*{total_paid:.2f}*C*NON************"
        f"{payment_date}~"
    )


def build_trn_segment(trace_number, payer_id):
    """Build TRN (reassociation trace number) segment."""
    return f"TRN*1*{trace_number}*1{payer_id}~"


def build_dtm_production(production_date):
    """Build DTM*405 (production date) segment."""
    return f"DTM*405*{production_date}~"


def build_payer_id_loop(payer):
    """Build N1/N3/N4 loop for payer identification."""
    segments = []
    segments.append(f"N1*PR*{payer['name']}*XV*{payer['payer_id']}~")
    segments.append(f"N3*PO Box {random.randint(10000, 99999)}~")
    city, state, zipcode = random.choice(CITIES_STATES_ZIPS)
    segments.append(f"N4*{city}*{state}*{zipcode}~")
    return segments


def build_payee_id_loop(provider, practice_type):
    """Build N1/N3/N4/REF loop for payee (provider) identification."""
    segments = []
    segments.append(f"N1*PE*{provider['full_name']}*XX*{provider['npi']}~")
    street_num = random.randint(100, 9999)
    street = random.choice(STREETS)
    segments.append(f"N3*{street_num} {street}~")
    city, state, zipcode = random.choice(CITIES_STATES_ZIPS)
    segments.append(f"N4*{city}*{state}*{zipcode}~")
    segments.append(f"REF*TJ*{random.randint(100000000, 999999999)}~")
    return segments


# ============================================================
# CLAIM-LEVEL (CLP) AND SERVICE-LEVEL (SVC) BUILDERS
# ============================================================

def build_denied_claim(claim_id, patient, provider, payer, cpt_tuple, icd_tuple,
                       carc, group_code, rarc, service_date, payer_ref, patient_resp=0.00):
    """Build a single denied claim with CLP, CAS, NM1, DTM, SVC, REF, AMT, LQ segments.

    Args:
        claim_id: Unique claim identifier
        patient: Patient demographics dict
        provider: Provider info dict
        payer: Payer info dict
        cpt_tuple: (code, description, billed_amount) tuple
        icd_tuple: (code, description) tuple
        carc: CARC code string
        group_code: CAS group code (CO, OA, PI, PR)
        rarc: RARC code string
        service_date: datetime object for service date
        payer_ref: Payer claim reference number
        patient_resp: Patient responsibility amount (default 0)
    """
    cpt_code, cpt_desc, billed_amount = cpt_tuple
    icd_code, icd_desc = icd_tuple
    svc_date_str = format_date_edi(service_date)
    svc_end_date = service_date + timedelta(days=random.choice([0, 0, 0, 1]))
    svc_end_str = format_date_edi(svc_end_date)
    units = random.choice([1, 1, 1, 2, 3])

    segments = []

    # CLP - Claim payment information (status 4 = denied)
    segments.append(
        f"CLP*{claim_id}*4*{billed_amount:.2f}*0.00"
        f"*{patient_resp:.2f}*{payer['plan_type']}*{payer_ref}~"
    )

    # CAS - Claim-level adjustment
    segments.append(f"CAS*{group_code}*{carc}*{billed_amount:.2f}~")

    # NM1*QC - Patient name
    segments.append(
        f"NM1*QC*1*{patient['last']}*{patient['first']}"
        f"****MI*{patient['member_id']}~"
    )

    # NM1*82 - Rendering provider
    segments.append(
        f"NM1*82*1*{provider['last']}*{provider['first']}"
        f"****XX*{provider['npi']}~"
    )

    # DTM*232 - Service from date
    segments.append(f"DTM*232*{svc_date_str}~")

    # DTM*233 - Service to date
    segments.append(f"DTM*233*{svc_end_str}~")

    # SVC - Service payment information (paid=0 for denials)
    segments.append(
        f"SVC*HC:{cpt_code}*{billed_amount:.2f}*0.00**{units}~"
    )

    # CAS at service level
    segments.append(f"CAS*{group_code}*{carc}*{billed_amount:.2f}~")

    # DTM*472 - Service date at line level
    segments.append(f"DTM*472*{svc_date_str}~")

    # REF*6R - Line item reference
    line_ref = f"LN{claim_id}{random.randint(1000, 9999)}"
    segments.append(f"REF*6R*{line_ref}~")

    # AMT*B6 - Allowed amount (0 for full denials)
    segments.append(f"AMT*B6*0.00~")

    # LQ*HE - RARC remark code
    segments.append(f"LQ*HE*{rarc}~")

    return segments


def build_mixed_claim(claim_id, patient, provider, payer, cpt_tuple, icd_tuple,
                      denial_carc, denial_group, pr_carc, pr_amount, rarc,
                      service_date, payer_ref, is_partial_pay=False):
    """Build a claim for mixed scenarios that may have partial payment plus denial adjustments.

    For mixed files, some claims are fully denied, others are partially paid with
    both CO/OA denial adjustments and PR patient responsibility adjustments.
    """
    cpt_code, cpt_desc, billed_amount = cpt_tuple
    icd_code, icd_desc = icd_tuple
    svc_date_str = format_date_edi(service_date)
    svc_end_date = service_date + timedelta(days=random.choice([0, 0, 1]))
    svc_end_str = format_date_edi(svc_end_date)
    units = random.choice([1, 1, 2])

    segments = []

    if is_partial_pay:
        # Partially paid claim: some paid, some adjusted
        paid_amount = round(billed_amount * random.uniform(0.15, 0.45), 2)
        denial_adj = round(billed_amount - paid_amount - pr_amount, 2)
        if denial_adj < 0:
            denial_adj = 0.00
            paid_amount = round(billed_amount - pr_amount, 2)
        clp_status = "2"  # status 2 = claim processed as secondary
        patient_resp = pr_amount
    else:
        # Fully denied
        paid_amount = 0.00
        denial_adj = round(billed_amount - pr_amount, 2)
        if denial_adj < 0:
            denial_adj = billed_amount
            pr_amount = 0.00
        clp_status = "4"  # status 4 = denied
        patient_resp = pr_amount

    segments.append(
        f"CLP*{claim_id}*{clp_status}*{billed_amount:.2f}*{paid_amount:.2f}"
        f"*{patient_resp:.2f}*{payer['plan_type']}*{payer_ref}~"
    )

    # CAS - Denial adjustment (CO or OA)
    if denial_adj > 0:
        segments.append(f"CAS*{denial_group}*{denial_carc}*{denial_adj:.2f}~")

    # CAS - Patient responsibility adjustment (PR)
    if pr_amount > 0:
        segments.append(f"CAS*PR*{pr_carc}*{pr_amount:.2f}~")

    # NM1*QC - Patient
    segments.append(
        f"NM1*QC*1*{patient['last']}*{patient['first']}"
        f"****MI*{patient['member_id']}~"
    )

    # NM1*82 - Rendering provider
    segments.append(
        f"NM1*82*1*{provider['last']}*{provider['first']}"
        f"****XX*{provider['npi']}~"
    )

    # DTM*232 - Service from date
    segments.append(f"DTM*232*{svc_date_str}~")
    # DTM*233 - Service to date
    segments.append(f"DTM*233*{svc_end_str}~")

    # SVC - Service line
    segments.append(
        f"SVC*HC:{cpt_code}*{billed_amount:.2f}*{paid_amount:.2f}**{units}~"
    )

    # CAS at service level (denial)
    if denial_adj > 0:
        segments.append(f"CAS*{denial_group}*{denial_carc}*{denial_adj:.2f}~")

    # CAS at service level (patient resp)
    if pr_amount > 0:
        segments.append(f"CAS*PR*{pr_carc}*{pr_amount:.2f}~")

    # DTM*472 - Service date at line level
    segments.append(f"DTM*472*{svc_date_str}~")

    # REF*6R - Line item reference
    line_ref = f"LN{claim_id}{random.randint(1000, 9999)}"
    segments.append(f"REF*6R*{line_ref}~")

    # AMT*B6 - Allowed amount
    if is_partial_pay:
        allowed = round(paid_amount + pr_amount, 2)
        segments.append(f"AMT*B6*{allowed:.2f}~")
    else:
        segments.append(f"AMT*B6*0.00~")

    # LQ*HE - RARC remark
    segments.append(f"LQ*HE*{rarc}~")

    return segments


# ============================================================
# FILE GENERATION FUNCTIONS BY CATEGORY
# ============================================================

def generate_frontend_registration_file(practice_type, practice_idx, file_idx, file_sub):
    """Generate files 1-3: PREVENTABLE - Front-End/Registration Errors."""
    seed_base = practice_idx * 10000 + file_idx * 100
    random.seed(seed_base)

    payer = get_random_payer(seed_base + 1)
    provider = get_random_provider(practice_type, seed_base + 2)
    cpts, icds = get_specialty_codes(practice_type, seed_base + 3, num_codes=5)

    # Pick 2-3 claims per file
    num_claims = random.choice([2, 3])
    payment_date = get_random_date(90, 5, seed=seed_base + 4)
    payment_date_str = format_date_edi(payment_date)
    interchange_date, interchange_time = format_datetime_edi(payment_date)

    isa_control = f"{practice_idx:03d}{file_idx:06d}"
    gs_control = f"{practice_idx}{file_idx:04d}"
    tx_control = f"{practice_idx:04d}{file_idx:04d}"

    segments = []

    # Envelope
    segments.append(build_isa_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, isa_control
    ))
    segments.append(build_gs_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, gs_control
    ))
    segments.append(build_st_segment(tx_control))

    # BPR - $0 payment for denials
    segments.append(build_bpr_segment(0.00, payment_date_str))

    # TRN
    trace_num = f"TRC{practice_idx:02d}{file_idx:03d}{random.randint(1000, 9999)}"
    segments.append(build_trn_segment(trace_num, payer['payer_id']))

    # DTM*405
    segments.append(build_dtm_production(payment_date_str))

    # Payer N1 loop
    segments.extend(build_payer_id_loop(payer))

    # Payee N1 loop
    segments.extend(build_payee_id_loop(provider, practice_type))

    # Claims
    for c in range(num_claims):
        claim_seed = seed_base + 50 + c * 10
        random.seed(claim_seed)
        patient = get_random_patient(claim_seed)
        denial = random.choice(FRONTEND_REGISTRATION_CARCS)
        cpt = random.choice(cpts)
        icd = random.choice(icds)
        svc_date = get_random_date(180, 30, seed=claim_seed + 1)
        claim_id = generate_claim_id(practice_idx, file_idx, c)
        payer_ref = f"PYR{practice_idx:02d}{file_idx:03d}{c:02d}{random.randint(100, 999)}"

        segments.extend(build_denied_claim(
            claim_id, patient, provider, payer, cpt, icd,
            denial['carc'], denial['group'], denial['rarc'],
            svc_date, payer_ref
        ))

    # Count segments between ST and SE (inclusive)
    st_idx = next(i for i, s in enumerate(segments) if s.startswith("ST*"))
    seg_count = len(segments) - st_idx + 1  # +1 for SE itself

    segments.append(build_se_segment(seg_count, tx_control))
    segments.append(build_ge_segment(1, gs_control))
    segments.append(build_iea_segment(1, isa_control))

    return "\n".join(segments)


def generate_coding_documentation_file(practice_type, practice_idx, file_idx, file_sub):
    """Generate files 4-6: PREVENTABLE - Coding/Documentation Errors."""
    seed_base = practice_idx * 10000 + file_idx * 100 + 3000
    random.seed(seed_base)

    payer = get_random_payer(seed_base + 1)
    provider = get_random_provider(practice_type, seed_base + 2)
    cpts, icds = get_specialty_codes(practice_type, seed_base + 3, num_codes=6)

    num_claims = random.choice([2, 3, 3])
    payment_date = get_random_date(90, 5, seed=seed_base + 4)
    payment_date_str = format_date_edi(payment_date)
    interchange_date, interchange_time = format_datetime_edi(payment_date)

    isa_control = f"{practice_idx:03d}{file_idx:06d}"
    gs_control = f"{practice_idx}{file_idx:04d}"
    tx_control = f"{practice_idx:04d}{file_idx:04d}"

    segments = []

    segments.append(build_isa_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, isa_control
    ))
    segments.append(build_gs_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, gs_control
    ))
    segments.append(build_st_segment(tx_control))
    segments.append(build_bpr_segment(0.00, payment_date_str))

    trace_num = f"TRC{practice_idx:02d}{file_idx:03d}{random.randint(1000, 9999)}"
    segments.append(build_trn_segment(trace_num, payer['payer_id']))
    segments.append(build_dtm_production(payment_date_str))
    segments.extend(build_payer_id_loop(payer))
    segments.extend(build_payee_id_loop(provider, practice_type))

    for c in range(num_claims):
        claim_seed = seed_base + 50 + c * 10
        random.seed(claim_seed)
        patient = get_random_patient(claim_seed)
        denial = random.choice(CODING_DOCUMENTATION_CARCS)
        cpt = random.choice(cpts)
        icd = random.choice(icds)
        svc_date = get_random_date(180, 30, seed=claim_seed + 1)
        claim_id = generate_claim_id(practice_idx, file_idx, c)
        payer_ref = f"PYR{practice_idx:02d}{file_idx:03d}{c:02d}{random.randint(100, 999)}"

        segments.extend(build_denied_claim(
            claim_id, patient, provider, payer, cpt, icd,
            denial['carc'], denial['group'], denial['rarc'],
            svc_date, payer_ref
        ))

    st_idx = next(i for i, s in enumerate(segments) if s.startswith("ST*"))
    seg_count = len(segments) - st_idx + 1
    segments.append(build_se_segment(seg_count, tx_control))
    segments.append(build_ge_segment(1, gs_control))
    segments.append(build_iea_segment(1, isa_control))

    return "\n".join(segments)


def generate_auth_precert_file(practice_type, practice_idx, file_idx, file_sub):
    """Generate files 7-8: PREVENTABLE - Authorization/Pre-cert Failures."""
    seed_base = practice_idx * 10000 + file_idx * 100 + 6000
    random.seed(seed_base)

    payer = get_random_payer(seed_base + 1)
    provider = get_random_provider(practice_type, seed_base + 2)
    cpts, icds = get_specialty_codes(practice_type, seed_base + 3, num_codes=5)

    num_claims = random.choice([2, 3])
    payment_date = get_random_date(90, 5, seed=seed_base + 4)
    payment_date_str = format_date_edi(payment_date)
    interchange_date, interchange_time = format_datetime_edi(payment_date)

    isa_control = f"{practice_idx:03d}{file_idx:06d}"
    gs_control = f"{practice_idx}{file_idx:04d}"
    tx_control = f"{practice_idx:04d}{file_idx:04d}"

    segments = []

    segments.append(build_isa_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, isa_control
    ))
    segments.append(build_gs_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, gs_control
    ))
    segments.append(build_st_segment(tx_control))
    segments.append(build_bpr_segment(0.00, payment_date_str))

    trace_num = f"TRC{practice_idx:02d}{file_idx:03d}{random.randint(1000, 9999)}"
    segments.append(build_trn_segment(trace_num, payer['payer_id']))
    segments.append(build_dtm_production(payment_date_str))
    segments.extend(build_payer_id_loop(payer))
    segments.extend(build_payee_id_loop(provider, practice_type))

    for c in range(num_claims):
        claim_seed = seed_base + 50 + c * 10
        random.seed(claim_seed)
        patient = get_random_patient(claim_seed)
        denial = random.choice(AUTH_PRECERT_CARCS)
        cpt = random.choice(cpts)
        icd = random.choice(icds)
        svc_date = get_random_date(180, 30, seed=claim_seed + 1)
        claim_id = generate_claim_id(practice_idx, file_idx, c)
        payer_ref = f"PYR{practice_idx:02d}{file_idx:03d}{c:02d}{random.randint(100, 999)}"

        segments.extend(build_denied_claim(
            claim_id, patient, provider, payer, cpt, icd,
            denial['carc'], denial['group'], denial['rarc'],
            svc_date, payer_ref
        ))

    st_idx = next(i for i, s in enumerate(segments) if s.startswith("ST*"))
    seg_count = len(segments) - st_idx + 1
    segments.append(build_se_segment(seg_count, tx_control))
    segments.append(build_ge_segment(1, gs_control))
    segments.append(build_iea_segment(1, isa_control))

    return "\n".join(segments)


def generate_payer_driven_file(practice_type, practice_idx, file_idx, file_sub):
    """Generate files 9-10: NON-PREVENTABLE - Payer-Driven Denials."""
    seed_base = practice_idx * 10000 + file_idx * 100 + 8000
    random.seed(seed_base)

    payer = get_random_payer(seed_base + 1)
    provider = get_random_provider(practice_type, seed_base + 2)
    cpts, icds = get_specialty_codes(practice_type, seed_base + 3, num_codes=5)

    num_claims = random.choice([2, 3])
    payment_date = get_random_date(90, 5, seed=seed_base + 4)
    payment_date_str = format_date_edi(payment_date)
    interchange_date, interchange_time = format_datetime_edi(payment_date)

    isa_control = f"{practice_idx:03d}{file_idx:06d}"
    gs_control = f"{practice_idx}{file_idx:04d}"
    tx_control = f"{practice_idx:04d}{file_idx:04d}"

    segments = []

    segments.append(build_isa_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, isa_control
    ))
    segments.append(build_gs_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, gs_control
    ))
    segments.append(build_st_segment(tx_control))
    segments.append(build_bpr_segment(0.00, payment_date_str))

    trace_num = f"TRC{practice_idx:02d}{file_idx:03d}{random.randint(1000, 9999)}"
    segments.append(build_trn_segment(trace_num, payer['payer_id']))
    segments.append(build_dtm_production(payment_date_str))
    segments.extend(build_payer_id_loop(payer))
    segments.extend(build_payee_id_loop(provider, practice_type))

    for c in range(num_claims):
        claim_seed = seed_base + 50 + c * 10
        random.seed(claim_seed)
        patient = get_random_patient(claim_seed)
        denial = random.choice(PAYER_DRIVEN_CARCS)
        cpt = random.choice(cpts)
        icd = random.choice(icds)
        svc_date = get_random_date(180, 30, seed=claim_seed + 1)
        claim_id = generate_claim_id(practice_idx, file_idx, c)
        payer_ref = f"PYR{practice_idx:02d}{file_idx:03d}{c:02d}{random.randint(100, 999)}"

        segments.extend(build_denied_claim(
            claim_id, patient, provider, payer, cpt, icd,
            denial['carc'], denial['group'], denial['rarc'],
            svc_date, payer_ref
        ))

    st_idx = next(i for i, s in enumerate(segments) if s.startswith("ST*"))
    seg_count = len(segments) - st_idx + 1
    segments.append(build_se_segment(seg_count, tx_control))
    segments.append(build_ge_segment(1, gs_control))
    segments.append(build_iea_segment(1, isa_control))

    return "\n".join(segments)


def generate_mixed_complex_file(practice_type, practice_idx, file_idx, file_sub):
    """Generate files 11-12: MIXED - Complex Scenarios.

    Multiple CLP segments (4-6 claims), mix of preventable and non-preventable denials,
    some partially paid with both CO and PR adjustments.
    """
    seed_base = practice_idx * 10000 + file_idx * 100 + 11000
    random.seed(seed_base)

    payer = get_random_payer(seed_base + 1)
    provider = get_random_provider(practice_type, seed_base + 2)
    cpts, icds = get_specialty_codes(practice_type, seed_base + 3, num_codes=8)

    # 4-6 claims per mixed file
    num_claims = random.choice([4, 5, 5, 6])
    payment_date = get_random_date(90, 5, seed=seed_base + 4)
    payment_date_str = format_date_edi(payment_date)
    interchange_date, interchange_time = format_datetime_edi(payment_date)

    isa_control = f"{practice_idx:03d}{file_idx:06d}"
    gs_control = f"{practice_idx}{file_idx:04d}"
    tx_control = f"{practice_idx:04d}{file_idx:04d}"

    segments = []

    # Calculate total paid for BPR (some claims may be partially paid)
    # We generate claims first in a pre-pass to compute the total, then build segments
    random.seed(seed_base + 20)
    partial_pay_flags = []
    for c in range(num_claims):
        # ~40% of claims in mixed files are partially paid
        partial_pay_flags.append(random.random() < 0.40)

    # For BPR, compute an estimate of total paid
    # We will compute exact amounts during claim generation
    total_paid = 0.00

    segments.append(build_isa_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, isa_control
    ))
    segments.append(build_gs_segment(
        payer['payer_id'], provider['npi'],
        interchange_date, interchange_time, gs_control
    ))
    segments.append(build_st_segment(tx_control))

    # Placeholder for BPR - we will update after generating claims
    bpr_idx = len(segments)
    segments.append("")  # placeholder

    trace_num = f"TRC{practice_idx:02d}{file_idx:03d}{random.randint(1000, 9999)}"
    segments.append(build_trn_segment(trace_num, payer['payer_id']))
    segments.append(build_dtm_production(payment_date_str))
    segments.extend(build_payer_id_loop(payer))
    segments.extend(build_payee_id_loop(provider, practice_type))

    for c in range(num_claims):
        claim_seed = seed_base + 50 + c * 10
        random.seed(claim_seed)
        patient = get_random_patient(claim_seed)

        denial_carc = random.choice(MIXED_DENIAL_CARCS)
        denial_group = random.choice(["CO", "OA", "PI"])
        pr_carc = random.choice(MIXED_PATIENT_RESP_CARCS)
        rarc = random.choice(MIXED_RARC_POOL)

        cpt = random.choice(cpts)
        icd = random.choice(icds)
        svc_date = get_random_date(180, 30, seed=claim_seed + 1)
        claim_id = generate_claim_id(practice_idx, file_idx, c)
        payer_ref = f"PYR{practice_idx:02d}{file_idx:03d}{c:02d}{random.randint(100, 999)}"

        is_partial = partial_pay_flags[c]
        billed = cpt[2]

        if is_partial:
            pr_amount = round(billed * random.uniform(0.05, 0.20), 2)
        else:
            # For fully denied, some have patient responsibility, some don't
            if random.random() < 0.3:
                pr_amount = round(billed * random.uniform(0.05, 0.15), 2)
            else:
                pr_amount = 0.00

        claim_segments = build_mixed_claim(
            claim_id, patient, provider, payer, cpt, icd,
            denial_carc, denial_group, pr_carc, pr_amount, rarc,
            svc_date, payer_ref, is_partial_pay=is_partial
        )

        # Extract paid amount from CLP segment for BPR total
        for seg in claim_segments:
            if seg.startswith("CLP*"):
                parts = seg.rstrip("~").split("*")
                paid = float(parts[4])
                total_paid += paid
                break

        segments.extend(claim_segments)

    # Update BPR with actual total
    if total_paid > 0:
        segments[bpr_idx] = build_bpr_segment(total_paid, payment_date_str)
    else:
        segments[bpr_idx] = build_bpr_segment(0.00, payment_date_str)

    st_idx = next(i for i, s in enumerate(segments) if s.startswith("ST*"))
    seg_count = len(segments) - st_idx + 1
    segments.append(build_se_segment(seg_count, tx_control))
    segments.append(build_ge_segment(1, gs_control))
    segments.append(build_iea_segment(1, isa_control))

    return "\n".join(segments)


# ============================================================
# FILE CATEGORY DISPATCH
# ============================================================
# Maps file sub-index (0-based within the 12 files) to generator function
FILE_CATEGORY_MAP = {
    0: generate_frontend_registration_file,   # File 1
    1: generate_frontend_registration_file,   # File 2
    2: generate_frontend_registration_file,   # File 3
    3: generate_coding_documentation_file,    # File 4
    4: generate_coding_documentation_file,    # File 5
    5: generate_coding_documentation_file,    # File 6
    6: generate_auth_precert_file,            # File 7
    7: generate_auth_precert_file,            # File 8
    8: generate_payer_driven_file,            # File 9
    9: generate_payer_driven_file,            # File 10
    10: generate_mixed_complex_file,          # File 11
    11: generate_mixed_complex_file,          # File 12
}


# ============================================================
# MAIN
# ============================================================

def main():
    """Generate all 835 denial categorization test files."""
    total_files = 0
    total_errors = 0

    print("=" * 70)
    print("EDI 835 Denial Categorization Test File Generator")
    print("=" * 70)
    print(f"Practice types: {len(PRACTICE_TYPES)}")
    print(f"Files per practice: {FILES_PER_PRACTICE}")
    print(f"Total files to generate: {len(PRACTICE_TYPES) * FILES_PER_PRACTICE}")
    print(f"Output directory: {BASE_OUTPUT_DIR}")
    print("=" * 70)

    for practice_idx, practice_type in enumerate(PRACTICE_TYPES):
        practice_dir = os.path.join(BASE_OUTPUT_DIR, practice_type)
        os.makedirs(practice_dir, exist_ok=True)

        practice_files = 0
        for file_sub in range(FILES_PER_PRACTICE):
            file_idx = file_sub + 1  # 1-based file index for naming
            filename = f"835_cat_{practice_type}_{file_idx:03d}.edi"
            filepath = os.path.join(practice_dir, filename)

            try:
                generator_fn = FILE_CATEGORY_MAP[file_sub]
                content = generator_fn(
                    practice_type, practice_idx, file_idx, file_sub
                )

                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(content)

                practice_files += 1
                total_files += 1

            except Exception as e:
                total_errors += 1
                print(f"  ERROR generating {filename}: {e}")

        # Determine category breakdown for display
        print(
            f"  [{practice_type:<25s}] {practice_files:>3d} files generated "
            f"(3 front-end, 3 coding, 2 auth, 2 payer-driven, 2 mixed)"
        )

    print("=" * 70)
    print(f"Generation complete: {total_files} files created, {total_errors} errors")
    print("=" * 70)


if __name__ == "__main__":
    main()
