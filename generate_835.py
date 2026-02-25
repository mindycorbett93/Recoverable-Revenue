"""
Generator for EDI 835 (Electronic Remittance Advice) test files with denial codes.

Produces 12 files per practice type (180 total) across 15 specialties.
Each file follows ANSI X12 5010 format with varied denial scenarios:
  Files 1-3:   Soft denials (CARC 16, 197, 50) - recoverable
  Files 4-5:   Hard denials (CARC 29, 27, 96)
  Files 6-7:   Partial payments with patient responsibility + contractual
  Files 8-9:   Coding denials (CARC 4, 11, 167)
  Files 10-11:  Eligibility denials (CARC 185, 109, 31)
  File 12:     Mixed denial types in one file
"""
import os
import random
from datetime import datetime, timedelta

from generators.test_data_commons import *

# ============================================================
# OUTPUT CONFIGURATION
# ============================================================
OUTPUT_BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "test_data", "835_denials")
FILES_PER_PRACTICE = 12

# ============================================================
# MODIFIER POOL
# ============================================================
MODIFIERS = ["25", "26", "59", "76", "77", "TC", "LT", "RT", "50", "51", ""]

# ============================================================
# PLAN TYPE CODES (for CLP08 - facility type code)
# ============================================================
PLAN_TYPE_CODES = {
    "PPO": "12", "HMO": "13", "Medicare": "MA", "Medicaid": "MC", "Government": "15"
}

# ============================================================
# ROUTING / ACCOUNT NUMBER POOLS
# ============================================================
ROUTING_NUMBERS = [
    "021000021", "026009593", "021202337", "091000019", "074000078",
    "071000013", "081000032", "101000019", "111000025", "122000247",
]
ACCOUNT_NUMBERS = [
    "1234567890", "2345678901", "3456789012", "4567890123", "5678901234",
    "6789012345", "7890123456", "8901234567", "9012345678", "0123456789",
]


def _pad(val, length):
    """Pad a string value to the specified length with trailing spaces."""
    return str(val).ljust(length)[:length]


def _build_isa(payer_id, provider_id, date_str, time_str, control_num):
    """Build the ISA interchange header segment."""
    return (
        f"ISA*00*{_pad('', 10)}*00*{_pad('', 10)}"
        f"*ZZ*{_pad(payer_id, 15)}*ZZ*{_pad(provider_id, 15)}"
        f"*{date_str[2:]}*{time_str}*^*00501*{control_num:>09}*0*P*:~"
    )


def _build_gs(payer_id, provider_id, date_str, time_str, group_ctrl):
    """Build the GS functional group header."""
    return (
        f"GS*HP*{payer_id}*{provider_id}*{date_str}*{time_str}"
        f"*{group_ctrl}*X*005010X221A1~"
    )


def _build_st(txn_ctrl):
    """Build the ST transaction set header."""
    return f"ST*835*{txn_ctrl}*005010X221A1~"


def _build_bpr(total_paid, payer_id, date_str, seed):
    """Build the BPR financial information segment."""
    random.seed(seed)
    routing1 = random.choice(ROUTING_NUMBERS)
    account1 = random.choice(ACCOUNT_NUMBERS)
    routing2 = random.choice(ROUTING_NUMBERS)
    account2 = random.choice(ACCOUNT_NUMBERS)
    return (
        f"BPR*I*{total_paid:.2f}*C*ACH*CCP*01*{routing1}*DA*{account1}"
        f"*{payer_id}**01*{routing2}*DA*{account2}*{date_str}~"
    )


def _build_trn(trace_number, payer_tax_id):
    """Build the TRN reassociation trace number segment."""
    return f"TRN*1*{trace_number}*{payer_tax_id}~"


def _build_dtm_production(date_str):
    """Build the DTM production date segment."""
    return f"DTM*405*{date_str}~"


def _build_loop_1000a(payer):
    """Build Loop 1000A - Payer Identification."""
    random.seed(hash(payer["name"]))
    city, state, zipcode = random.choice(CITIES_STATES_ZIPS)
    street_num = random.randint(100, 9999)
    street = random.choice(STREETS)
    segments = [
        f"N1*PR*{payer['name']}~",
        f"N3*{street_num} {street}~",
        f"N4*{city}*{state}*{zipcode}~",
        f"REF*2U*{payer['payer_id']}~",
    ]
    return segments


def _build_loop_1000b(provider, practice_type, seed):
    """Build Loop 1000B - Payee Identification."""
    random.seed(seed)
    city, state, zipcode = random.choice(CITIES_STATES_ZIPS)
    street_num = random.randint(100, 9999)
    street = random.choice(STREETS)
    tax_id = f"{random.randint(10, 99)}{random.randint(1000000, 9999999)}"
    segments = [
        f"N1*PE*{provider['last']} {provider['first']}*XX*{provider['npi']}~",
        f"N3*{street_num} {street}~",
        f"N4*{city}*{state}*{zipcode}~",
        f"REF*TJ*{tax_id}~",
    ]
    return segments, tax_id


def _build_claim_loop(claim_id, clp_status, billed, paid, patient_resp,
                      plan_type_code, payer_claim_id, cas_segments,
                      patient, provider, service_from, service_to,
                      svc_lines):
    """
    Build Loop 2100 (Claim Payment) and nested Loop 2110 (Service Lines).

    Returns a list of segment strings.
    """
    facility_code = "11"  # Office
    freq_code = "1"       # Original
    segments = []

    # CLP - Claim Payment
    segments.append(
        f"CLP*{claim_id}*{clp_status}*{billed:.2f}*{paid:.2f}"
        f"*{patient_resp:.2f}*{plan_type_code}*{payer_claim_id}"
        f"*{facility_code}*{freq_code}~"
    )

    # CAS - Claim-level adjustments
    for cas in cas_segments:
        segments.append(cas)

    # NM1*QC - Patient
    segments.append(
        f"NM1*QC*1*{patient['last']}*{patient['first']}****MI*{patient['member_id']}~"
    )

    # NM1*82 - Rendering Provider
    segments.append(
        f"NM1*82*1*{provider['last']}*{provider['first']}****XX*{provider['npi']}~"
    )

    # DTM - Service dates
    segments.append(f"DTM*232*{service_from}~")
    segments.append(f"DTM*233*{service_to}~")

    # Loop 2110 - Service lines
    for svc in svc_lines:
        segments.extend(svc)

    return segments


def _build_svc_line(cpt, modifier, billed, paid, units, service_date,
                    cas_group, carc, adj_amount, line_ref, allowed_amount,
                    rarc_code):
    """Build Loop 2110 - Service Payment Information."""
    segments = []
    if modifier:
        segments.append(f"SVC*HC:{cpt}:{modifier}*{billed:.2f}*{paid:.2f}**{units}~")
    else:
        segments.append(f"SVC*HC:{cpt}*{billed:.2f}*{paid:.2f}**{units}~")
    segments.append(f"DTM*472*{service_date}~")
    segments.append(f"CAS*{cas_group}*{carc}*{adj_amount:.2f}*{units}~")
    segments.append(f"REF*6R*{line_ref}~")
    segments.append(f"AMT*B6*{allowed_amount:.2f}~")
    segments.append(f"LQ*HE*{rarc_code}~")
    return segments


def _pick_rarc(seed):
    """Pick a RARC code from the designated pool."""
    random.seed(seed)
    rarc_pool = ["N362", "N386", "MA130", "N479", "M15", "N95"]
    return random.choice(rarc_pool)


def _generate_claims_for_file(practice_type, practice_idx, file_idx, provider, payer):
    """
    Generate claim data structures for a single 835 file based on file_idx
    to determine the denial pattern.

    Returns: list of claim dicts, each with keys needed to build segments.
    """
    base_seed = practice_idx * 10000 + file_idx * 100
    plan_type_code = PLAN_TYPE_CODES.get(payer["plan_type"], "12")

    # Determine number of claims: file 12 gets 4-5 claims, others get 2-3
    random.seed(base_seed + 50)
    if file_idx == 11:  # File 12 (0-indexed as 11)
        num_claims = random.randint(4, 5)
    else:
        num_claims = random.randint(2, 3)

    claims = []
    for claim_line in range(num_claims):
        seed = base_seed + claim_line * 7
        random.seed(seed)

        patient = get_random_patient(seed + 3)
        service_date_obj = get_random_date(start_days_ago=180, end_days_ago=10, seed=seed + 5)
        service_from = format_date_edi(service_date_obj)
        service_to = format_date_edi(service_date_obj + timedelta(days=random.randint(0, 1)))

        # Get specialty CPT/ICD codes
        cpts, icds = get_specialty_codes(practice_type, seed + 11, num_codes=3)

        # Pick 1-2 service lines per claim
        random.seed(seed + 20)
        num_svc = random.randint(1, 2)
        selected_cpts = random.sample(cpts, min(num_svc, len(cpts)))

        claim_id = generate_claim_id(practice_idx, file_idx, claim_line)
        payer_claim_id = f"PCN{random.randint(100000000, 999999999)}"

        # Determine denial pattern based on file_idx (0-indexed)
        claim_data = _assign_denial_pattern(
            file_idx, claim_line, num_claims, selected_cpts, seed,
            plan_type_code, claim_id, payer_claim_id, patient, provider,
            service_from, service_to
        )
        claims.append(claim_data)

    return claims


def _assign_denial_pattern(file_idx, claim_line, num_claims, selected_cpts,
                           seed, plan_type_code, claim_id, payer_claim_id,
                           patient, provider, service_from, service_to):
    """
    Assign denial type based on file index (0-indexed).

    Files 0-2:  Soft denials - CARC 16, 197, 50
    Files 3-4:  Hard denials - CARC 29, 27, 96
    Files 5-6:  Partial payment - CARC 1/2/3 + 45/97
    Files 7-8:  Coding denials - CARC 4, 11, 167
    Files 9-10: Eligibility denials - CARC 185, 109, 31
    File 11:    Mixed denial types
    """
    random.seed(seed + 99)

    if file_idx in (0, 1, 2):
        # Soft denials - recoverable
        carc_choices = ["16", "197", "50"]
        carc = carc_choices[file_idx % 3]
        clp_status = "4"  # Denied
        return _build_denied_claim(
            clp_status, carc, "CO", selected_cpts, seed,
            plan_type_code, claim_id, payer_claim_id, patient, provider,
            service_from, service_to
        )

    elif file_idx in (3, 4):
        # Hard denials
        carc_choices = ["29", "27", "96"]
        carc = carc_choices[(file_idx + claim_line) % 3]
        clp_status = "4"
        return _build_denied_claim(
            clp_status, carc, "CO", selected_cpts, seed,
            plan_type_code, claim_id, payer_claim_id, patient, provider,
            service_from, service_to
        )

    elif file_idx in (5, 6):
        # Partial payment with patient responsibility + contractual
        return _build_partial_payment_claim(
            selected_cpts, seed, plan_type_code, claim_id, payer_claim_id,
            patient, provider, service_from, service_to
        )

    elif file_idx in (7, 8):
        # Coding denials
        carc_choices = ["4", "11", "167"]
        carc = carc_choices[(file_idx + claim_line) % 3]
        clp_status = "4"
        return _build_denied_claim(
            clp_status, carc, "CO", selected_cpts, seed,
            plan_type_code, claim_id, payer_claim_id, patient, provider,
            service_from, service_to
        )

    elif file_idx in (9, 10):
        # Eligibility denials
        carc_choices = ["185", "109", "31"]
        carc = carc_choices[(file_idx + claim_line) % 3]
        clp_status = "4"
        group = "PI" if carc == "185" else "OA"
        return _build_denied_claim(
            clp_status, carc, group, selected_cpts, seed,
            plan_type_code, claim_id, payer_claim_id, patient, provider,
            service_from, service_to
        )

    else:
        # File 12 (index 11): Mix of all denial types
        mixed_patterns = [
            ("4", "16", "CO"),     # Soft
            ("4", "29", "CO"),     # Hard
            ("1", None, None),     # Partial payment
            ("4", "4", "CO"),      # Coding
            ("4", "185", "PI"),    # Eligibility
        ]
        pattern = mixed_patterns[claim_line % len(mixed_patterns)]
        if pattern[1] is None:
            return _build_partial_payment_claim(
                selected_cpts, seed, plan_type_code, claim_id, payer_claim_id,
                patient, provider, service_from, service_to
            )
        else:
            return _build_denied_claim(
                pattern[0], pattern[1], pattern[2], selected_cpts, seed,
                plan_type_code, claim_id, payer_claim_id, patient, provider,
                service_from, service_to
            )


def _build_denied_claim(clp_status, carc, group_code, selected_cpts, seed,
                        plan_type_code, claim_id, payer_claim_id,
                        patient, provider, service_from, service_to):
    """Build a fully denied claim (paid = 0)."""
    random.seed(seed + 200)
    total_billed = 0.0
    svc_lines = []

    for i, cpt_info in enumerate(selected_cpts):
        cpt_code, cpt_desc, cpt_price = cpt_info
        modifier = random.choice(MODIFIERS)
        units = random.randint(1, 3)
        billed = round(cpt_price * units, 2)
        total_billed += billed
        line_ref = f"LN{claim_id}{i:02d}"
        rarc = _pick_rarc(seed + i * 13)

        svc_line = _build_svc_line(
            cpt=cpt_code,
            modifier=modifier,
            billed=billed,
            paid=0.00,
            units=units,
            service_date=service_from,
            cas_group=group_code,
            carc=carc,
            adj_amount=billed,
            line_ref=line_ref,
            allowed_amount=0.00,
            rarc_code=rarc,
        )
        svc_lines.append(svc_line)

    # Claim-level CAS
    cas_segments = [
        f"CAS*{group_code}*{carc}*{total_billed:.2f}*1~"
    ]

    return {
        "claim_id": claim_id,
        "clp_status": clp_status,
        "billed": total_billed,
        "paid": 0.00,
        "patient_resp": 0.00,
        "plan_type_code": plan_type_code,
        "payer_claim_id": payer_claim_id,
        "cas_segments": cas_segments,
        "patient": patient,
        "provider": provider,
        "service_from": service_from,
        "service_to": service_to,
        "svc_lines": svc_lines,
    }


def _build_partial_payment_claim(selected_cpts, seed, plan_type_code,
                                 claim_id, payer_claim_id, patient,
                                 provider, service_from, service_to):
    """Build a partially paid claim with patient responsibility and contractual adjustments."""
    random.seed(seed + 300)
    total_billed = 0.0
    total_paid = 0.0
    total_patient_resp = 0.0
    svc_lines = []

    # Patient responsibility CARC: pick from 1, 2, 3
    pr_carcs = ["1", "2", "3"]
    # Contractual CARC: 45 (charges exceed schedule) or 97 (bundling)
    contractual_carcs = ["45", "97"]

    for i, cpt_info in enumerate(selected_cpts):
        cpt_code, cpt_desc, cpt_price = cpt_info
        modifier = random.choice(MODIFIERS)
        units = random.randint(1, 2)
        billed = round(cpt_price * units, 2)
        total_billed += billed

        # Allowed amount: 60-85% of billed
        allowed = round(billed * random.uniform(0.60, 0.85), 2)

        # Patient responsibility: 10-30% of allowed
        pr_carc = random.choice(pr_carcs)
        patient_share = round(allowed * random.uniform(0.10, 0.30), 2)

        # Contractual adjustment
        contractual = round(billed - allowed, 2)
        co_carc = random.choice(contractual_carcs)

        # Plan pays the rest
        paid = round(allowed - patient_share, 2)

        total_paid += paid
        total_patient_resp += patient_share

        line_ref = f"LN{claim_id}{i:02d}"
        rarc = _pick_rarc(seed + i * 17)

        # Build service line segments
        svc_segs = []
        if modifier:
            svc_segs.append(f"SVC*HC:{cpt_code}:{modifier}*{billed:.2f}*{paid:.2f}**{units}~")
        else:
            svc_segs.append(f"SVC*HC:{cpt_code}*{billed:.2f}*{paid:.2f}**{units}~")
        svc_segs.append(f"DTM*472*{service_from}~")
        # Contractual adjustment CAS
        svc_segs.append(f"CAS*CO*{co_carc}*{contractual:.2f}*{units}~")
        # Patient responsibility CAS
        svc_segs.append(f"CAS*PR*{pr_carc}*{patient_share:.2f}*{units}~")
        svc_segs.append(f"REF*6R*{line_ref}~")
        svc_segs.append(f"AMT*B6*{allowed:.2f}~")
        svc_segs.append(f"LQ*HE*{rarc}~")
        svc_lines.append(svc_segs)

    # Claim-level CAS segments
    contractual_total = round(total_billed - total_paid - total_patient_resp, 2)
    cas_segments = [
        f"CAS*CO*{random.choice(contractual_carcs)}*{contractual_total:.2f}*1~",
        f"CAS*PR*{random.choice(pr_carcs)}*{total_patient_resp:.2f}*1~",
    ]

    return {
        "claim_id": claim_id,
        "clp_status": "1",  # Processed as primary
        "billed": total_billed,
        "paid": total_paid,
        "patient_resp": total_patient_resp,
        "plan_type_code": plan_type_code,
        "payer_claim_id": payer_claim_id,
        "cas_segments": cas_segments,
        "patient": patient,
        "provider": provider,
        "service_from": service_from,
        "service_to": service_to,
        "svc_lines": svc_lines,
    }


def generate_835_file(practice_type, practice_idx, file_idx):
    """
    Generate a single EDI 835 file.

    Returns the EDI content as a string.
    """
    base_seed = practice_idx * 10000 + file_idx * 100
    random.seed(base_seed)

    # Select payer and provider
    payer = get_random_payer(base_seed + 1)
    provider = get_random_provider(practice_type, base_seed + 2)

    # Date/time for envelope
    prod_date_obj = get_random_date(start_days_ago=90, end_days_ago=1, seed=base_seed + 3)
    date_str, time_str = format_datetime_edi(prod_date_obj)

    # Control numbers
    control_num = base_seed + 1
    group_ctrl = base_seed + 2
    txn_ctrl = f"{base_seed + 3:04d}"

    # Generate claims
    claims = _generate_claims_for_file(practice_type, practice_idx, file_idx, provider, payer)

    # Compute total paid across all claims
    total_paid = sum(c["paid"] for c in claims)

    # Payer tax ID for TRN
    random.seed(base_seed + 77)
    payer_tax_id = f"1{random.randint(100000000, 999999999)}"
    trace_number = f"T{base_seed:012d}"

    # Provider tax ID for PLB
    random.seed(base_seed + 88)
    provider_tax_id = f"{random.randint(10, 99)}{random.randint(1000000, 9999999)}"

    # ---- Build segments ----
    segments = []

    # Envelope
    segments.append(_build_isa(payer["payer_id"], provider["npi"], date_str, time_str, control_num))
    segments.append(_build_gs(payer["payer_id"], provider["npi"], date_str, time_str, group_ctrl))
    segments.append(_build_st(txn_ctrl))

    # Financial info
    segments.append(_build_bpr(total_paid, payer["payer_id"], date_str, base_seed + 10))
    segments.append(_build_trn(trace_number, payer_tax_id))
    segments.append(_build_dtm_production(date_str))

    # Loop 1000A - Payer
    segments.extend(_build_loop_1000a(payer))

    # Loop 1000B - Payee
    loop_1000b_segs, payee_tax_id = _build_loop_1000b(provider, practice_type, base_seed + 20)
    segments.extend(loop_1000b_segs)

    # Loop 2100 + 2110 - Claims and service lines
    for claim in claims:
        claim_segs = _build_claim_loop(
            claim_id=claim["claim_id"],
            clp_status=claim["clp_status"],
            billed=claim["billed"],
            paid=claim["paid"],
            patient_resp=claim["patient_resp"],
            plan_type_code=claim["plan_type_code"],
            payer_claim_id=claim["payer_claim_id"],
            cas_segments=claim["cas_segments"],
            patient=claim["patient"],
            provider=claim["provider"],
            service_from=claim["service_from"],
            service_to=claim["service_to"],
            svc_lines=claim["svc_lines"],
        )
        segments.extend(claim_segs)

    # PLB - Provider Level Balance (optional, include on some files)
    random.seed(base_seed + 999)
    if random.random() < 0.4:
        plb_amount = round(random.uniform(-500.0, -10.0), 2)
        fiscal_year = date_str[:4] + "1231"
        plb_reason = random.choice(["WO", "L6", "FB", "CS"])
        segments.append(
            f"PLB*{provider_tax_id}*{fiscal_year}*{plb_reason}*{plb_amount:.2f}~"
        )

    # SE - count of segments from ST to SE inclusive
    se_count = len(segments) - 2 + 1  # exclude ISA and GS, include SE itself
    segments.append(f"SE*{se_count}*{txn_ctrl}~")

    # GE / IEA
    segments.append(f"GE*1*{group_ctrl}~")
    segments.append(f"IEA*1*{control_num:>09}~")

    return "\n".join(segments)


def main():
    """Generate all 835 denial test files across all practice types."""
    total_files = 0

    for practice_idx, practice_type in enumerate(PRACTICE_TYPES):
        practice_dir = os.path.join(OUTPUT_BASE, practice_type)
        os.makedirs(practice_dir, exist_ok=True)

        for file_idx in range(FILES_PER_PRACTICE):
            edi_content = generate_835_file(practice_type, practice_idx, file_idx)
            filename = f"835_{practice_type}_{file_idx + 1:03d}.edi"
            filepath = os.path.join(practice_dir, filename)

            with open(filepath, "w") as f:
                f.write(edi_content)

            total_files += 1

        print(f"  Generated {FILES_PER_PRACTICE} files for {practice_type}")

    print(f"\nTotal 835 files generated: {total_files}")
    print(f"Output directory: {OUTPUT_BASE}")


if __name__ == "__main__":
    main()
