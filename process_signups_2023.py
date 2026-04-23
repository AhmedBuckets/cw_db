import pandas as pd
import re
from pathlib import Path

# ------------- CONFIG -------------

INSTITUTIONS_CSV = Path("institutions.csv")        # from your institutions build script
INPUT_2023_CSV   = Path("2023.csv")                # raw 2023 export
OUTPUT_2023_CSV  = Path("signups_2023_clean_with_ids.csv")

CW_YEAR = 2023
RAW_SOURCE_FORM_VERSION = "2023_main"

# Country mapping (same as institutions)
COUNTRY_CODE_MAP = {
    "Canada": "CA",
    "Germany": "DE",
    "Grenada": "GD",
    "Ireland": "IE",
    "Malaysia": "MY",
    "Other": "OTHER",   # or "ZZ" if you prefer
    "Pakistan": "PK",
    "Qatar": "QA",
    "South Africa": "ZA",
    "Uganda": "UG",
    "UK": "GB",
    "USA": "US",
}

# ------------- HELPERS -------------

def normalize_country_code(country: str) -> str:
    if pd.isna(country):
        return "OTHER"
    c = str(country).strip()
    if c in COUNTRY_CODE_MAP:
        return COUNTRY_CODE_MAP[c]
    # Fallback: treat unknowns as OTHER
    return "OTHER"


def normalize_name(name: str) -> str:
    """
    Same normalization as used in institutions:
    - lowercase
    - strip leading/trailing spaces
    - replace non-alphanumeric chars with space
    - collapse multiple spaces
    """
    if pd.isna(name):
        return ""
    s = str(name).strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def choose_first_non_null(row, cols):
    for c in cols:
        if c in row and pd.notna(row[c]) and str(row[c]).strip() != "":
            return str(row[c]).strip()
    return None


def load_institutions(institutions_csv: Path) -> pd.DataFrame:
    if not institutions_csv.exists():
        raise FileNotFoundError(f"Institutions CSV not found: {institutions_csv}")
    inst = pd.read_csv(institutions_csv)
    required = {"institution_id", "country_code", "institution_name_norm"}
    missing = required - set(inst.columns)
    if missing:
        raise ValueError(f"Missing columns in institutions.csv: {missing}")

    inst["institution_name_norm"] = inst["institution_name_norm"].apply(normalize_name)
    inst["country_code"] = inst["country_code"].astype(str).str.strip()
    return inst


# ------------- MAIN PROCESS -------------

def main():
    # Load institutions
    inst_df = load_institutions(INSTITUTIONS_CSV)

    # Load raw 2023 CSV
    if not INPUT_2023_CSV.exists():
        raise FileNotFoundError(f"2023 CSV not found: {INPUT_2023_CSV}")

    df_raw = pd.read_csv(INPUT_2023_CSV)

    # For debugging, you can uncomment:
    # print(df_raw.columns.tolist())

    # Shorten names for convenience
    # Using the exact headers you provided
    col_ts            = "Timestamp"
    col_full_name     = "What is your full name?"
    col_email         = "What is your email address?"
    col_contact       = "What is your contact number? (including any relevant calling codes)"
    col_rep_inst      = "Are you representing an Institution?"
    col_part_type     = "How would you like to take part in Charity Week?"
    col_inst_name_raw = "Name of your Institution?"
    col_inst_taken    = "Has your Institution taken part in Charity Week before?"
    col_inst_role     = "What is your role at your Institution?"
    col_inst_ig       = "What is your institution's Instagram handle?"
    col_head_name     = "Name of the Head of your Institution (Isoc/MSA President, School Headmaster etc): "
    col_head_email    = "Email Address for the Head of your Institution: "
    col_head_contact  = "Contact number for the  Head of your Institution (including any relevant calling codes):"
    col_rep_name      = "Name of your Charity Week Rep (the person in charge of running Charity Week in your group):"
    col_rep_email     = "Email address of your Charity Week Rep:"
    col_rep_contact   = "Contact number of your Charity Week Rep (including any relevant calling codes):"

    # Because of branching logic, we have multiple Country/City/State/Heard/Prefs/Consent columns
    # Pandas will rename duplicate headers as "Country:.1", "Country:.2", etc.
    # We'll collect them by pattern.

    cols_country = [c for c in df_raw.columns if c.startswith("Country:")]
    cols_country_other = [c for c in df_raw.columns if c.startswith("If 'Other' country, please specify:")]
    cols_city = [c for c in df_raw.columns if c.startswith("What city are you based in?") or c == "City:"]
    cols_state = [c for c in df_raw.columns if c.startswith("State:")]
    cols_consent = [c for c in df_raw.columns if c.startswith("By checking this box you confirm your participation")]
    cols_heard = [c for c in df_raw.columns if c.startswith("Where did you hear about Charity Week?")]
    cols_prefs = [c for c in df_raw.columns if c.startswith("Please tick the boxes below to tell us all the ways you prefer to hear from us:")]
    col_region_uk = "What region of the UK are you based in?"
    col_state_us  = "What state are you based in?"

    # Build a working copy
    df = df_raw.copy()

    # Canonical simple fields
    df["signup_timestamp"]      = df[col_ts]
    df["full_name"]             = df[col_full_name]
    df["email"]                 = df[col_email]
    df["contact_number"]        = df[col_contact]
    df["representing_institution"] = df[col_rep_inst]
    df["participation_type"]    = df[col_part_type]
    df["institution_name_raw"]  = df[col_inst_name_raw]
    df["institution_full_name"] = ""  # not present in 2023
    df["institution_any_issues"] = "" # not present in 2023
    df["institution_has_taken_part_before"] = df[col_inst_taken]
    df["previous_institution_flag"] = ""  # not present in 2023
    df["institution_role"]      = df[col_inst_role]
    df["institution_instagram"] = df[col_inst_ig]

    df["head_name"]             = df[col_head_name]
    df["head_email"]            = df[col_head_email]
    df["head_contact_number"]   = df[col_head_contact]

    df["rep_name"]              = df[col_rep_name]
    df["rep_email"]             = df[col_rep_email]
    df["rep_contact_number"]    = df[col_rep_contact]

    # Location – canonical country/city/state
    # Primary country raw: first non-null among all Country: columns
    df["country_raw_primary"] = df.apply(lambda row: choose_first_non_null(row, cols_country), axis=1)
    df["country_other_raw"]   = df.apply(lambda row: choose_first_non_null(row, cols_country_other), axis=1)

    # Canonical "country" label: prefer explicit dropdown, else other
    def choose_country(row):
        if row["country_raw_primary"]:
            return row["country_raw_primary"]
        if row["country_other_raw"]:
            return row["country_other_raw"]
        return None

    df["country"] = df.apply(choose_country, axis=1)
    df["country_code"] = df["country"].apply(normalize_country_code)

    # City: first non-null among all city columns
    df["city"] = df.apply(lambda row: choose_first_non_null(row, cols_city), axis=1)

    # State-like fields: we want a generic state_region
    # Prefer explicit region_uk or state_us if present, else first non-null State:
    def choose_state_region(row):
        val_region_uk = row[col_region_uk] if col_region_uk in row else None
        val_state_us = row[col_state_us] if col_state_us in row else None
        if isinstance(val_region_uk, str) and val_region_uk.strip():
            return val_region_uk.strip()
        if isinstance(val_state_us, str) and val_state_us.strip():
            return val_state_us.strip()
        # fallback: first non-null among State: columns
        val = choose_first_non_null(row, cols_state)
        return val

    df["state_region"] = df.apply(choose_state_region, axis=1)

    # Raw regional fields for reference
    df["region_uk_raw"] = df[col_region_uk] if col_region_uk in df.columns else ""
    df["region_us_raw"] = ""  # no explicit Region US in 2023
    df["state_raw"]     = df.apply(lambda row: choose_first_non_null(row, cols_state), axis=1)

    # Comms + consent
    df["heard_about_cw"] = df.apply(lambda row: choose_first_non_null(row, cols_heard), axis=1)
    df["contact_preferences"] = df.apply(lambda row: choose_first_non_null(row, cols_prefs), axis=1)
    df["consent_confirmed"]   = df.apply(lambda row: choose_first_non_null(row, cols_consent), axis=1)

    # Fields not present in 2023 but in schema
    df["shipping_address"]      = ""
    df["board_members_count"]   = pd.NA
    df["approx_members_count"]  = pd.NA
    df["final_country_raw"]     = ""   # only 2024
    df["country_joined_raw"]    = ""   # only 2025

    # institution_name_for_match: for 2023, just use institution_name_raw
    df["institution_name_for_match"] = df["institution_name_raw"]
    df["institution_name_norm"] = df["institution_name_for_match"].apply(normalize_name)

    # Join with institutions on (country_code, institution_name_norm)
    inst_key = inst_df[["institution_id", "country_code", "institution_name_norm"]].copy()
    inst_key = inst_key.rename(
        columns={
            "country_code": "inst_country_code",
            "institution_name_norm": "inst_institution_name_norm",
        }
    )

    df["join_country_code"] = df["country_code"]
    df["join_institution_name_norm"] = df["institution_name_norm"]

    merged = df.merge(
        inst_key,
        left_on=["join_country_code", "join_institution_name_norm"],
        right_on=["inst_country_code", "inst_institution_name_norm"],
        how="left",
    )

    merged["institution_match_status"] = merged["institution_id"].apply(
        lambda x: "matched" if pd.notna(x) else "unmatched"
    )

    # Add cw_year, signup_id, raw_row_number, raw_source_form_version
    merged["cw_year"] = CW_YEAR
    merged = merged.reset_index(drop=True)
    merged["signup_id"] = merged.index + 1
    merged["raw_row_number"] = merged.index + 1
    merged["raw_source_form_version"] = RAW_SOURCE_FORM_VERSION

    # Build final dataframe aligned with signups table DDL
    out_cols = [
        "signup_id",
        "cw_year",
        "signup_timestamp",
        "full_name",
        "email",
        "contact_number",
        "representing_institution",
        "participation_type",

        "institution_id",
        "institution_name_raw",
        "institution_full_name",
        "institution_any_issues",
        "institution_name_for_match",
        "institution_name_norm",
        "institution_has_taken_part_before",
        "previous_institution_flag",
        "institution_role",
        "institution_instagram",

        "head_name",
        "head_email",
        "head_contact_number",
        "rep_name",
        "rep_email",
        "rep_contact_number",

        "country",
        "country_code",
        "state_region",
        "city",
        "country_raw_primary",
        "country_other_raw",
        "final_country_raw",
        "country_joined_raw",
        "region_uk_raw",
        "region_us_raw",
        "state_raw",

        "shipping_address",
        "board_members_count",
        "approx_members_count",

        "heard_about_cw",
        "contact_preferences",
        "consent_confirmed",

        "institution_match_status",
        "raw_row_number",
        "raw_source_form_version",
    ]

    # Keep only columns that exist
    out_cols = [c for c in out_cols if c in merged.columns]
    out_df = merged[out_cols].copy()

        # ---- FIX INTEGER-LIKE COLUMNS BEFORE EXPORT ----
    # Ensure integer-ish columns are written without ".0" and with blanks for nulls
    def int_or_blank(x):
        if pd.isna(x):
            return ""
        try:
            return int(x)
        except Exception:
            return ""

    int_cols = [
        "signup_id",
        "cw_year",
        "institution_id",
        "board_members_count",
        "approx_members_count",
        "raw_row_number",
    ]

    for col in int_cols:
        if col in merged.columns:
            merged[col] = merged[col].apply(int_or_blank)

    # Rebuild out_df from the cleaned merged
    out_df = merged[out_cols].copy()


    # Write to CSV
    OUTPUT_2023_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUTPUT_2023_CSV, index=False)

    total = len(out_df)
    unmatched = (out_df["institution_match_status"] == "unmatched").sum()
    print(f"[2023] Wrote {total} rows to {OUTPUT_2023_CSV}")
    print(f"[2023] Unmatched institutions: {unmatched}")


if __name__ == "__main__":
    main()
