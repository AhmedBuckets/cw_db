import pandas as pd
import re
from pathlib import Path

# ------------- CONFIG -------------

INSTITUTIONS_CSV = Path("institutions.csv")        # from your institutions build script
INPUT_2024_CSV   = Path("2024.csv")                # raw 2024 export
OUTPUT_2024_CSV  = Path("signups_2024_clean_with_ids.csv")

CW_YEAR = 2024
RAW_SOURCE_FORM_VERSION = "2024_main"

COUNTRY_CODE_MAP = {
    "Canada": "CA",
    "Germany": "DE",
    "Grenada": "GD",
    "Ireland": "IE",
    "Malaysia": "MY",
    "Other": "OTHER",   # or "ZZ"
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
    return COUNTRY_CODE_MAP.get(c, "OTHER")


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
    inst_df = load_institutions(INSTITUTIONS_CSV)

    if not INPUT_2024_CSV.exists():
        raise FileNotFoundError(f"2024 CSV not found: {INPUT_2024_CSV}")

    df_raw = pd.read_csv(INPUT_2024_CSV)

    # Shorthand for columns (exact headers you provided)
    col_ts            = "Timestamp"
    col_full_name     = "What is your full name?"
    col_email         = "What is your email address?"
    col_contact       = "What is your contact number? (including any relevant calling codes)"
    col_part_type     = "How would you like to take part in Charity Week?"
    col_inst_taken    = "Has your Institution taken part in Charity Week before?"
    col_inst_name_raw = "Name of your Institution? "
    col_inst_issues   = "Any issues with your Institution name or clarification needed?"
    col_inst_full     = "What is the full name of your Institution?"
    col_inst_role     = "What is your role at your Institution?"
    col_inst_ig       = "What is your institution's Instagram handle?"
    col_head_name     = "Name of the Head of your Institution (Isoc/MSA President, School Headmaster etc): "
    col_head_email    = "Email Address for the Head of your Institution: "
    col_head_contact  = "Contact number for the  Head of your Institution (including any relevant calling codes):"
    col_rep_name      = "Name of your Charity Week Rep (the person in charge of running Charity Week in your group):"
    col_rep_email     = "Email address of your Charity Week Rep:"
    col_rep_contact   = "Contact number of your Charity Week Rep (including any relevant calling codes):"

    # Country / other / state / region columns
    cols_country = [c for c in df_raw.columns if c.startswith("Country:")]
    cols_country_other = [c for c in df_raw.columns if c.startswith("If 'Other' country, please specify:")]
    col_state_generic = "State:"
    col_region_uk_q1  = "What region of the UK are you based in?"
    col_state_us_q1   = "What state are you based in?"
    col_city_q1       = "What city are you based in?"

    # second Country / Other exist, plus consent/heard/prefs around them
    col_consent       = "By checking this box you confirm your participation with Charity Week and that all the information above is correct. "
    col_heard         = "Where did you hear about Charity Week?"
    col_prefs         = "Please tick the boxes below to tell us all the ways you prefer to hear from us:"
    col_shipping      = "Provide an address we can use to ship CW resources out to:"
    col_board_count   = "How many board members are in your organization?"
    col_member_count  = "Approximately how many members are in your organization?"
    col_region_us     = "Region US"
    col_inst_final    = "Institution "
    col_prev_inst     = "Previous Inst?"
    col_region_uk2    = "Region UK"
    col_final_country = "Final Country:"

    df = df_raw.copy()

    # Basic person fields
    df["signup_timestamp"]      = df[col_ts]
    df["full_name"]             = df[col_full_name]
    df["email"]                 = df[col_email]
    df["contact_number"]        = df[col_contact]

    # 2024 form does not explicitly ask "Are you representing an Institution?"
    df["representing_institution"] = ""

    df["participation_type"]    = df[col_part_type]
    df["institution_name_raw"]  = df[col_inst_name_raw]
    df["institution_full_name"] = df[col_inst_full]
    df["institution_any_issues"] = df[col_inst_issues]
    df["institution_has_taken_part_before"] = df[col_inst_taken]
    df["previous_institution_flag"] = df[col_prev_inst] if col_prev_inst in df.columns else ""

    df["institution_role"]      = df[col_inst_role]
    df["institution_instagram"] = df[col_inst_ig]

    df["head_name"]             = df[col_head_name]
    df["head_email"]            = df[col_head_email]
    df["head_contact_number"]   = df[col_head_contact]

    df["rep_name"]              = df[col_rep_name]
    df["rep_email"]             = df[col_rep_email]
    df["rep_contact_number"]    = df[col_rep_contact]

    # Country / location
    # primary country from first Country: column
    def primary_country(row):
        if cols_country:
            c = row.get(cols_country[0], None)
            return str(c).strip() if pd.notna(c) and str(c).strip() != "" else None
        return None

    df["country_raw_primary"] = df.apply(primary_country, axis=1)

    # first "If 'Other'..." column for other country spec
    df["country_other_raw"] = df.apply(
        lambda row: choose_first_non_null(row, cols_country_other), axis=1
    )

    # final_country_raw from "Final Country:"
    df["final_country_raw"] = df[col_final_country] if col_final_country in df.columns else None

    # Canonical country label: prefer final, else primary, else other
    def choose_country(row):
        if row["final_country_raw"] and str(row["final_country_raw"]).strip():
            return str(row["final_country_raw"]).strip()
        if row["country_raw_primary"]:
            return row["country_raw_primary"]
        if row["country_other_raw"]:
            return row["country_other_raw"]
        return None

    df["country"] = df.apply(choose_country, axis=1)
    df["country_code"] = df["country"].apply(normalize_country_code)

    # City
    df["city"] = df[col_city_q1] if col_city_q1 in df.columns else ""

    # Region/state fields
    df["region_uk_raw"] = df[col_region_uk2] if col_region_uk2 in df.columns else (
        df[col_region_uk_q1] if col_region_uk_q1 in df.columns else ""
    )
    df["region_us_raw"] = df[col_region_us] if col_region_us in df.columns else ""

    df["state_raw"] = df[col_state_generic] if col_state_generic in df.columns else (
        df[col_state_us_q1] if col_state_us_q1 in df.columns else ""
    )

    def choose_state_region(row):
        if isinstance(row["region_uk_raw"], str) and row["region_uk_raw"].strip():
            return row["region_uk_raw"].strip()
        if isinstance(row["region_us_raw"], str) and row["region_us_raw"].strip():
            return row["region_us_raw"].strip()
        if isinstance(row["state_raw"], str) and row["state_raw"].strip():
            return row["state_raw"].strip()
        return None

    df["state_region"] = df.apply(choose_state_region, axis=1)

    # Comms + consent
    df["heard_about_cw"]      = df[col_heard]
    df["contact_preferences"] = df[col_prefs]
    df["consent_confirmed"]   = df[col_consent]

    # Logistics / ops
    df["shipping_address"]     = df[col_shipping]
    df["board_members_count"]  = df[col_board_count]
    df["approx_members_count"] = df[col_member_count]

    # Extra country fields for reference
    df["country_joined_raw"] = ""  # 2025 only, so blank here

    # Institution name for match: prefer final "Institution " column, then full, then raw
    def choose_inst_name_for_match(row):
        vals = []
        if col_inst_final in row:
            vals.append(row[col_inst_final])
        if col_inst_full in row:
            vals.append(row[col_inst_full])
        if col_inst_name_raw in row:
            vals.append(row[col_inst_name_raw])
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""

    df["institution_name_for_match"] = df.apply(choose_inst_name_for_match, axis=1)
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

    # keep only columns that exist
    out_cols = [c for c in out_cols if c in merged.columns]

    # ---- FIX INTEGER-LIKE COLUMNS BEFORE EXPORT ----
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

    out_df = merged[out_cols].copy()

    # Write to CSV
    OUTPUT_2024_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUTPUT_2024_CSV, index=False)

    total = len(out_df)
    unmatched = (out_df["institution_match_status"] == "unmatched").sum()
    print(f"[2024] Wrote {total} rows to {OUTPUT_2024_CSV}")
    print(f"[2024] Unmatched institutions: {unmatched}")


if __name__ == "__main__":
    main()
