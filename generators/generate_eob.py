"""
Generator for text-based EOB (Explanation of Benefits) test files.
Produces 12 EOB files per practice type (180 total) simulating parsed PDF EOBs
in a structured text format with standard and non-standard CARC codes.
"""
import os
import random
from datetime import timedelta

from generators.test_data_commons import *


# ============================================================
# PLACE OF SERVICE CODES BY SPECIALTY
# ============================================================
POS_CODES = {
    "primary_care": ("11", "Office"),
    "internal_medicine": ("11", "Office"),
    "allergy_immunology": ("11", "Office"),
    "orthopaedics": ("11", "Office"),
    "cardiology": ("11", "Office"),
    "behavioral_health": ("11", "Office"),
    "radiology": ("22", "Outpatient Hospital"),
    "pathology": ("22", "Outpatient Hospital"),
    "gastroenterology": ("24", "Ambulatory Surgical Center"),
    "ob_gyn": ("11", "Office"),
    "dermatology": ("11", "Office"),
    "anesthesiology": ("21", "Inpatient Hospital"),
    "urgent_care": ("20", "Urgent Care Facility"),
    "pediatrics": ("11", "Office"),
    "clinical_laboratory": ("81", "Independent Laboratory"),
}

# Standard CARC codes used in files 1-5
STANDARD_CARC_SUBSET = ["1", "2", "3", "4", "16", "18", "45", "50", "97", "167", "197"]

# Non-standard codes used in files 6-8
NON_STANDARD_CARC_SUBSET = ["N001", "N002", "N003", "N004", "N005", "N006", "N007", "N008", "N009", "N010"]

# CARC code 45 description (not in commons, commonly used on EOBs)
CARC_45_DESC = "Charge exceeds fee schedule/maximum allowable or contracted/legislated fee arrangement."


def get_carc_description(code):
    """Get description for a CARC code (standard or non-standard)."""
    if code == "45":
        return CARC_45_DESC
    if code in CARC_CODES:
        return CARC_CODES[code]["desc"]
    if code in NON_STANDARD_CARC_CODES:
        return NON_STANDARD_CARC_CODES[code]["desc"]
    return "Adjustment"


def compute_line_amounts(billed, scenario, adj_codes, rng):
    """
    Compute allowed, deductible, copay, adjustment, and paid amounts for a service line.

    Parameters:
        billed: The billed amount
        scenario: One of 'paid', 'partial', 'denied'
        adj_codes: List of adjustment codes applied to this line
        rng: Random instance for reproducibility

    Returns:
        dict with allowed, deductible, copay, adj_amt, paid, adj_reasons (list of (code, amount))
    """
    if scenario == "denied":
        return {
            "allowed": 0.00,
            "deductible": 0.00,
            "copay": 0.00,
            "adj_amt": billed,
            "paid": 0.00,
            "adj_reasons": [(adj_codes[0], billed)] if adj_codes else [("50", billed)],
        }

    # For paid/partial, compute allowed as percentage of billed
    allowed_pct = rng.uniform(0.60, 0.95)
    allowed = round(billed * allowed_pct, 2)

    deductible = 0.00
    copay = 0.00
    adj_amt = round(billed - allowed, 2)  # contractual adjustment (fee schedule difference)
    adj_reasons = []

    # Fee schedule adjustment is always present for paid/partial
    adj_reasons.append(("45", adj_amt))

    if scenario == "partial":
        # Add patient responsibility components
        has_deductible = any(c in ("1", "N009") for c in adj_codes) or rng.random() < 0.4
        has_copay = any(c in ("3",) for c in adj_codes) or rng.random() < 0.3
        has_coinsurance = any(c in ("2",) for c in adj_codes)

        if has_deductible:
            deductible = round(min(rng.uniform(25.0, 150.0), allowed * 0.4), 2)
            adj_reasons.append(("1", deductible))
        if has_copay:
            copay = round(min(rng.choice([20.0, 25.0, 30.0, 40.0, 50.0]), allowed * 0.3), 2)
            adj_reasons.append(("3", copay))
        if has_coinsurance and not has_copay:
            coins = round(allowed * rng.uniform(0.10, 0.20), 2)
            copay = coins  # coinsurance shown in copay column on EOB
            adj_reasons.append(("2", coins))

        # Check for denial-type adjustments on partial claims
        for code in adj_codes:
            if code not in ("1", "2", "3", "45", "N009"):
                extra_adj = round(allowed * rng.uniform(0.05, 0.25), 2)
                adj_reasons.append((code, extra_adj))
                adj_amt = round(adj_amt + extra_adj, 2)

        paid = round(max(allowed - deductible - copay, 0.0), 2)
    else:
        # Fully paid (minus fee schedule adjustment only)
        paid = allowed

    return {
        "allowed": allowed,
        "deductible": deductible,
        "copay": copay,
        "adj_amt": adj_amt,
        "paid": paid,
        "adj_reasons": adj_reasons,
    }


def determine_file_scenario(file_idx):
    """
    Determine the code category and claim scenarios for a file based on its index (1-12).

    Returns:
        (code_category, line_scenarios_fn)
        code_category: 'standard', 'nonstandard', 'mixed', 'complex'
        line_scenarios_fn: callable(num_lines, rng) -> list of (scenario, [adj_codes])
    """
    if 1 <= file_idx <= 5:
        # Standard CARC codes only, mix of paid/partially paid/denied
        return "standard"
    elif 6 <= file_idx <= 8:
        # Non-standard CARC codes, some fully denied
        return "nonstandard"
    elif 9 <= file_idx <= 10:
        # Mix of standard and non-standard
        return "mixed"
    else:
        # Complex: multiple adjustment codes per line
        return "complex"


def pick_adj_codes(category, rng, num_codes=1):
    """Pick adjustment codes based on category."""
    if category == "standard":
        return rng.sample(STANDARD_CARC_SUBSET, min(num_codes, len(STANDARD_CARC_SUBSET)))
    elif category == "nonstandard":
        return rng.sample(NON_STANDARD_CARC_SUBSET, min(num_codes, len(NON_STANDARD_CARC_SUBSET)))
    elif category == "mixed":
        std = rng.sample(STANDARD_CARC_SUBSET, min(max(1, num_codes // 2), len(STANDARD_CARC_SUBSET)))
        nonstd = rng.sample(NON_STANDARD_CARC_SUBSET, min(max(1, num_codes - len(std)), len(NON_STANDARD_CARC_SUBSET)))
        combined = std + nonstd
        rng.shuffle(combined)
        return combined[:num_codes]
    else:
        # complex: 2-3 codes mixing both
        std = rng.sample(STANDARD_CARC_SUBSET, min(2, len(STANDARD_CARC_SUBSET)))
        nonstd = rng.sample(NON_STANDARD_CARC_SUBSET, min(1, len(NON_STANDARD_CARC_SUBSET)))
        combined = std + nonstd
        rng.shuffle(combined)
        return combined[:num_codes]


def generate_line_scenarios(file_idx, num_lines, rng):
    """
    Generate a list of (scenario, [adj_codes]) for each service line.

    scenario: 'paid', 'partial', 'denied'
    """
    category = determine_file_scenario(file_idx)
    lines = []

    for i in range(num_lines):
        if category == "standard":
            # Files 1-5: mix of paid/partial/denied
            if file_idx <= 2:
                # Mostly paid
                scenario = rng.choices(["paid", "partial", "denied"], weights=[50, 35, 15])[0]
            elif file_idx <= 4:
                # More partial
                scenario = rng.choices(["paid", "partial", "denied"], weights=[30, 45, 25])[0]
            else:
                # File 5: heavier denial
                scenario = rng.choices(["paid", "partial", "denied"], weights=[20, 30, 50])[0]
            codes = pick_adj_codes("standard", rng, num_codes=1)
            lines.append((scenario, codes))

        elif category == "nonstandard":
            # Files 6-8: non-standard codes, some fully denied
            if file_idx == 8:
                # Fully denied file
                scenario = "denied"
            else:
                scenario = rng.choices(["paid", "partial", "denied"], weights=[20, 30, 50])[0]
            codes = pick_adj_codes("nonstandard", rng, num_codes=1)
            lines.append((scenario, codes))

        elif category == "mixed":
            # Files 9-10: mix of standard and non-standard
            scenario = rng.choices(["paid", "partial", "denied"], weights=[30, 40, 30])[0]
            codes = pick_adj_codes("mixed", rng, num_codes=1)
            lines.append((scenario, codes))

        else:
            # Files 11-12: complex, multiple adj codes per line
            scenario = rng.choices(["paid", "partial", "denied"], weights=[15, 50, 35])[0]
            num_codes = rng.randint(2, 3)
            codes = pick_adj_codes("complex", rng, num_codes=num_codes)
            lines.append((scenario, codes))

    return lines


def generate_eob_text(practice_type, practice_idx, file_idx, rng):
    """
    Generate a complete EOB text file content.

    Parameters:
        practice_type: e.g. 'primary_care'
        practice_idx: index of practice in PRACTICE_TYPES (0-14)
        file_idx: 1-12 (file number within this practice)
        rng: Random instance

    Returns:
        str: The complete EOB text content
    """
    base_seed = practice_idx * 1000 + file_idx * 10

    # Patient
    patient = get_random_patient(base_seed)

    # Provider
    provider = get_random_provider(practice_type, base_seed + 1)

    # Payer
    payer = get_random_payer(base_seed + 2)

    # Dates
    service_date = get_random_date(start_days_ago=180, end_days_ago=30, seed=base_seed + 3)
    service_to = service_date + timedelta(days=rng.randint(0, 2))
    received_date = service_date + timedelta(days=rng.randint(5, 20))
    eob_date = received_date + timedelta(days=rng.randint(10, 30))

    # EOB metadata
    eob_number = f"EOB{practice_idx:02d}{file_idx:03d}{rng.randint(10000,99999)}"
    claim_number = generate_claim_id(practice_idx, file_idx)
    tax_id = f"{rng.randint(10,99)}-{rng.randint(1000000,9999999)}"
    group_number = f"GRP{rng.randint(100000,999999)}"

    # Place of service
    pos_code, pos_desc = POS_CODES.get(practice_type, ("11", "Office"))

    # Service lines: pick 2-6 CPT codes
    num_lines = rng.randint(2, 6)
    cpts, icds = get_specialty_codes(practice_type, base_seed + 4, num_codes=num_lines)

    # Pad if we got fewer codes than requested
    while len(cpts) < num_lines:
        cpts.append(rng.choice(SPECIALTY_CPT_CODES[practice_type]))
    cpts = cpts[:num_lines]

    # Generate line scenarios (paid/partial/denied + adj codes)
    line_scenarios = generate_line_scenarios(file_idx, num_lines, rng)

    # Determine overall claim status
    all_denied = all(s[0] == "denied" for s in line_scenarios)
    claim_status = "DENIED" if all_denied else "PROCESSED"

    # Provider address
    prov_street_num = rng.randint(100, 9999)
    prov_street = rng.choice(STREETS)
    prov_city, prov_state, prov_zip = rng.choice(CITIES_STATES_ZIPS)
    provider_address = f"{prov_street_num} {prov_street}"

    # Build service detail lines and collect all adjustment codes used
    service_lines = []
    all_adj_codes_used = {}
    all_rarc_codes_used = {}
    total_billed = 0.0
    total_allowed = 0.0
    total_deductible = 0.0
    total_copay = 0.0
    total_adj = 0.0
    total_paid = 0.0

    for i, (cpt_tuple, (scenario, adj_codes)) in enumerate(zip(cpts, line_scenarios)):
        cpt_code, cpt_desc, billed_amt = cpt_tuple

        amounts = compute_line_amounts(billed_amt, scenario, adj_codes, rng)

        # Track adjustment codes and descriptions
        for code, amt in amounts["adj_reasons"]:
            desc = get_carc_description(code)
            all_adj_codes_used[code] = desc

        # Pick a RARC code for some lines
        rarc_code = ""
        if rng.random() < 0.6:
            rarc_key = rng.choice(list(RARC_CODES.keys()))
            rarc_code = rarc_key
            all_rarc_codes_used[rarc_key] = RARC_CODES[rarc_key]["desc"]

        # Primary adjustment code for the line display
        primary_adj_code = adj_codes[0] if adj_codes else "45"
        primary_adj_amt = amounts["adj_amt"]

        # For complex files with multiple codes, show all
        if len(adj_codes) > 1:
            adj_display = ",".join(adj_codes)
        else:
            adj_display = primary_adj_code

        service_lines.append({
            "line_num": i + 1,
            "cpt": cpt_code,
            "description": cpt_desc[:24],
            "billed": billed_amt,
            "allowed": amounts["allowed"],
            "deductible": amounts["deductible"],
            "copay": amounts["copay"],
            "adj_code": adj_display,
            "adj_amt": primary_adj_amt,
            "paid": amounts["paid"],
        })

        total_billed += billed_amt
        total_allowed += amounts["allowed"]
        total_deductible += amounts["deductible"]
        total_copay += amounts["copay"]
        total_adj += primary_adj_amt
        total_paid += amounts["paid"]

    # Round totals
    total_billed = round(total_billed, 2)
    total_allowed = round(total_allowed, 2)
    total_deductible = round(total_deductible, 2)
    total_copay = round(total_copay, 2)
    total_adj = round(total_adj, 2)
    total_paid = round(total_paid, 2)
    patient_resp = round(total_deductible + total_copay, 2)

    # Format dates for display
    eob_date_str = eob_date.strftime("%m/%d/%Y")
    service_from_str = service_date.strftime("%m/%d/%Y")
    service_to_str = service_to.strftime("%m/%d/%Y")
    received_date_str = received_date.strftime("%m/%d/%Y")
    dob_display = f"{patient['dob'][4:6]}/{patient['dob'][6:8]}/{patient['dob'][:4]}"

    # Build EOB text
    separator = "=" * 80
    dash_sep = "-" * 80

    lines = []
    lines.append(separator)
    lines.append("                        EXPLANATION OF BENEFITS")
    lines.append(separator)
    lines.append(f"PAYER: {payer['name']:<40s}  DATE: {eob_date_str}")
    lines.append(f"PAYER ID: {payer['payer_id']:<37s}  EOB NUMBER: {eob_number}")
    lines.append(f"PLAN TYPE: {payer['plan_type']:<36s}  TAX ID: {tax_id}")
    lines.append(separator)
    lines.append("PATIENT INFORMATION")
    lines.append(dash_sep)
    lines.append(f"Patient Name: {patient['last']}, {patient['first']:<24s}  Member ID: {patient['member_id']}")
    lines.append(f"Date of Birth: {dob_display:<23s}  Group: {group_number}")
    lines.append(f"Patient Account: {patient['patient_id']:<21s}  Relationship: Self")
    lines.append(separator)
    lines.append("PROVIDER INFORMATION")
    lines.append(dash_sep)
    lines.append(f"Provider Name: {provider['full_name']:<23s}  NPI: {provider['npi']}")
    lines.append(f"Provider Address: {provider_address}")
    lines.append(f"{prov_city}, {prov_state} {prov_zip}")
    lines.append(separator)
    lines.append("CLAIM INFORMATION")
    lines.append(dash_sep)
    lines.append(f"Claim Number: {claim_number:<24s}  Received Date: {received_date_str}")
    lines.append(f"Service From: {service_from_str:<24s}  Service To: {service_to_str}")
    lines.append(f"Place of Service: {pos_code} - {pos_desc:<16s}  Claim Status: {claim_status}")
    lines.append(separator)
    lines.append("SERVICE DETAILS")
    lines.append(dash_sep)

    # Header row for service lines
    lines.append(
        f"{'Line':<6s}{'CPT':<8s}{'Description':<26s}{'Billed':>10s}{'Allowed':>10s}"
        f"{'Deduct':>9s}{'Copay':>8s}{'Adj Code':>10s}{'Adj Amt':>10s}{'Paid':>10s}"
    )
    lines.append(
        f"{'----':<6s}{'-----':<8s}{'----------------------':<26s}{'--------':>10s}{'--------':>10s}"
        f"{'-------':>9s}{'------':>8s}{'--------':>10s}{'--------':>10s}{'--------':>10s}"
    )

    for svc in service_lines:
        lines.append(
            f"{svc['line_num']:<6d}{svc['cpt']:<8s}{svc['description']:<26s}"
            f"${svc['billed']:>8.2f}  ${svc['allowed']:>7.2f}"
            f"  ${svc['deductible']:>6.2f}"
            f"  ${svc['copay']:>5.2f}"
            f"  {svc['adj_code']:>8s}"
            f"  ${svc['adj_amt']:>7.2f}"
            f"  ${svc['paid']:>7.2f}"
        )

    lines.append(separator)
    lines.append("TOTALS")
    lines.append(dash_sep)
    lines.append(f"Total Billed:     ${total_billed:>10.2f}")
    lines.append(f"Total Allowed:    ${total_allowed:>10.2f}")
    lines.append(f"Total Deductible: ${total_deductible:>10.2f}")
    lines.append(f"Total Copay:      ${total_copay:>10.2f}")
    lines.append(f"Total Adjustment: ${total_adj:>10.2f}")
    lines.append(f"Total Paid:       ${total_paid:>10.2f}")
    lines.append(f"Patient Responsibility: ${patient_resp:>10.2f}")
    lines.append(separator)
    lines.append("ADJUSTMENT REASON CODES")
    lines.append(dash_sep)

    for code in sorted(all_adj_codes_used.keys()):
        desc = all_adj_codes_used[code]
        lines.append(f"{code}: {desc}")

    lines.append(separator)
    lines.append("REMARKS")
    lines.append(dash_sep)

    if all_rarc_codes_used:
        for code in sorted(all_rarc_codes_used.keys()):
            desc = all_rarc_codes_used[code]
            lines.append(f"{code}: {desc}")
    else:
        lines.append("No additional remarks.")

    lines.append(separator)

    return "\n".join(lines) + "\n"


def main():
    """Generate 12 EOB files per practice type (180 total)."""
    base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test_data", "eob_pdf")
    total_files = 0

    for practice_idx, practice_type in enumerate(PRACTICE_TYPES):
        practice_dir = os.path.join(base_dir, practice_type)
        os.makedirs(practice_dir, exist_ok=True)

        for file_idx in range(1, 13):
            # Use a seeded RNG for reproducibility
            rng = random.Random(practice_idx * 10000 + file_idx * 100)

            eob_content = generate_eob_text(practice_type, practice_idx, file_idx, rng)

            filename = f"eob_{practice_type}_{file_idx:03d}.txt"
            filepath = os.path.join(practice_dir, filename)

            with open(filepath, "w") as f:
                f.write(eob_content)

            total_files += 1

    print(f"Generated {total_files} EOB files across {len(PRACTICE_TYPES)} practice types.")
    print(f"Output directory: {base_dir}")


if __name__ == "__main__":
    main()
