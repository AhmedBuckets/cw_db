import pandas as pd
import re
from pathlib import Path

# ------------- CONFIG -------------

INSTITUTIONS_CSV = Path("institutions.csv")       # from your institutions build script
INPUT_2025_CSV   = Path("2025.csv")               # raw 2025 export
OUTPUT_2025_CSV  = Path("signups_2025_clean_with_ids.csv")

CW_YEAR = 2025
RAW_SOURCE_FORM_VERSION = "2025_main"

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

    if not INPUT_2025_CSV.exists():
        raise FileNotFoundError(f"2025 CSV not found: {INPUT_2025_CSV}")

    df_raw = pd.read_csv(INPUT_2025_CSV)

    # ---- Resolve column names (some have funky quotes/newlines) ----

    # Straightforward columns (exact labels from your schema)
    col_ts            = "Timestamp"
    col_full_name     = "What is your full name?"
    col_email         = "What is your email address?"
    col_part_type     = "How would you like to take part in Charity Week?"
    col_inst_taken    = "Has your Institution taken part in Charity Week before?"
    col_inst_name_raw = "Name of your institution?"
    col_inst_issues   = "Any issues with your Institution name or clarification needed?"
    col_inst_role     = "What is your role at your Institution?"
    col_inst_ig       = "What is your institution's Instagram handle?"
    col_head_name     = "Name of the Head of your Institution (Isoc/MSA President, School Headmaster etc): "
    col_head_email    = "Email Address for the Head of your Institution: "
    col_head_contact  = "Contact number for the  Head of your Institution (including any relevant calling codes):"
    col_rep_name_maybe = "Column 14"  # almost certainly "Name of your Charity Week Rep" responses
    col_rep_email     = "Email address of your Charity Week Rep:"
    col_rep_contact   = "Contact number of your Charity Week Rep (including any relevant calling codes):"
    col_country1      = "Country:"
    col_country_other1 = "If 'Other' country, please specify:"
    col_state_generic = "State:"
    col_shipping      = "Provide an address we can use to ship CW resources out to:"
    col_board_count   = "How many board members are in your organization?"
    col_member_count  = "Approximately how many members are in your organization?"
    col_region_uk     = "What region of the UK are you based in?"
    col_state_us      = "What state are you based in?"
    col_city_q1       = "What city are you based in?"
    col_country2      = "Country:.1"  # pandas will rename duplicate "Country:" headings as "Country:.1"
    col_country_other2 = "If 'Other' country, please specify:.1"
    col_consent       = "By checking this box you confirm your participation with Charity Week and that all the information above is correct. "
    col_heard         = "Where did you hear about Charity Week?"
    col_prefs         = "Please tick the boxes below to tell us all the ways you prefer to hear from us:"
    col_inst_final    = "Institution"
    col_country_joined = "Country Joined"
    col_prev_inst     = "Previous Inst"

    # contact column (header contains extra text & quotes)
    contact_cols = [c for c in df_raw.columns
                    if c.startswith("What is your contact number? (including any relevant calling codes)")]
    if not contact_cols:
        raise ValueError("Could not find contact number column in 2025 CSV.")
    col_contact = contact_cols[0]

    # full institution name column (header has multiple lines in the form)
    full_inst_cols = [c for c in df_raw.columns
                      if c.startswith("What is the full name of your Institution/university?")]
    if not full_inst_cols:
        # Fallback: maybe someone renamed slightly
        raise ValueError("Could not find 'full name of your Institution/university' column in 2025 CSV.")
    col_inst_full = full_inst_cols[0]

    # Country columns (pandas may or may not have added .1 suffix depending on duplicates)
    # We'll detect all "Country:" columns
    country_cols_all = [c for c in df_raw.columns if c.startswith("Country:")]
    # Primary = first in order
    primary_country_col = country_cols_all[0] if country_cols_all else col_country1
    # Secondary (if exists) will be used for "final" country fallback
    secondary_country_col = country_cols_all[1] if len(country_cols_all) > 1 else None

    # Same for "If 'Other' country, please specify:"
    other_country_cols_all = [c for c in df_raw.columns if c.startswith("If 'Other' country, please specify:")]
    primary_other_country_col = other_country_cols_all[0] if other_country_cols_all else col_country_other1

    df = df_raw.copy()

    # ---- Basic person fields ----
    df["signup_timestamp"] = df[col_ts]
    df["full_name"]        = df[col_full_name]
    df["email"]            = df[col_email]
    df["contact_number"]   = df[col_contact]

    # Drop rows with missing/blank timestamps first
    df["signup_timestamp"] = df["signup_timestamp"].astype(str)
    df = df[df["signup_timestamp"].str.strip() != ""]

    # Parse as day-first (DD/MM/YYYY ...) then output ISO
    ts_parsed = pd.to_datetime(
        df["signup_timestamp"],
        dayfirst=True,
        errors="coerce"
    )

    # Drop any rows that still failed to parse
    df = df[ts_parsed.notna()].copy()
    df["signup_timestamp"] = ts_parsed[ts_parsed.notna()].dt.strftime("%Y-%m-%d %H:%M:%S")

    # No explicit "Are you representing an Institution?" in 2025
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

    # Treat Column 14 as rep_name (very likely that’s what it holds)
    df["rep_name"]              = df[col_rep_name_maybe] if col_rep_name_maybe in df.columns else ""
    df["rep_email"]             = df[col_rep_email]
    df["rep_contact_number"]    = df[col_rep_contact]

    # ---- Country / location ----
    # primary country from first Country: column
    def primary_country(row):
        c = row.get(primary_country_col, None)
        return str(c).strip() if pd.notna(c) and str(c).strip() != "" else None

    df["country_raw_primary"] = df.apply(primary_country, axis=1)

    # "If 'Other' country, please specify:" primary
    def primary_country_other(row):
        c = row.get(primary_other_country_col, None)
        return str(c).strip() if pd.notna(c) and str(c).strip() != "" else None

    df["country_other_raw"] = df.apply(primary_country_other, axis=1)

    # capture second Country: if present (for reference / possible fallback)
    if secondary_country_col:
        df["final_country_raw"] = df[secondary_country_col]
    else:
        df["final_country_raw"] = ""

    # Country Joined (2025-specific)
    df["country_joined_raw"] = df[col_country_joined] if col_country_joined in df.columns else ""

    # Canonical country label: prefer Country Joined, else final country, else primary, else other
    def choose_country(row):
        if row["country_joined_raw"] and str(row["country_joined_raw"]).strip():
            return str(row["country_joined_raw"]).strip()
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
    df["region_uk_raw"] = df[col_region_uk] if col_region_uk in df.columns else ""
    df["region_us_raw"] = ""  # 2025 has "What state are you based in?" rather than "Region US"

    # state_raw gets either generic State: or the US-specific one
    if col_state_generic in df.columns:
        df["state_raw"] = df[col_state_generic]
    elif col_state_us in df.columns:
        df["state_raw"] = df[col_state_us]
    else:
        df["state_raw"] = ""

    def choose_state_region(row):
        if isinstance(row["region_uk_raw"], str) and row["region_uk_raw"].strip():
            return row["region_uk_raw"].strip()
        if isinstance(row["state_raw"], str) and row["state_raw"].strip():
            return row["state_raw"].strip()
        return None

    df["state_region"] = df.apply(choose_state_region, axis=1)

    # ---- Comms + consent ----
    df["heard_about_cw"]      = df[col_heard]
    df["contact_preferences"] = df[col_prefs]
    df["consent_confirmed"]   = df[col_consent]

    # ---- Logistics / ops ----
    df["shipping_address"]     = df[col_shipping]
    df["board_members_count"]  = df[col_board_count]
    df["approx_members_count"] = df[col_member_count]

    # ---- Institution name for match ----
    # Prefer final "Institution" column, then full name, then raw
    def choose_inst_name_for_match(row):
        vals = []
        if col_inst_final in row:
            vals.append(row[col_inst_final])
        vals.append(row["institution_full_name"])
        vals.append(row["institution_name_raw"])
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""

    df["institution_name_for_match"] = df.apply(choose_inst_name_for_match, axis=1)
    df["institution_name_norm"] = df["institution_name_for_match"].apply(normalize_name)

    # ---- Join with institutions on (country_code, institution_name_norm) ----
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

    # Add cw_year, raw_row_number, raw_source_form_version
    merged["cw_year"] = CW_YEAR
    merged = merged.reset_index(drop=True)
    merged["raw_row_number"] = merged.index + 1
    merged["raw_source_form_version"] = RAW_SOURCE_FORM_VERSION

    # ---- Build final dataframe aligned with signups table DDL ----
    out_cols = [
        # no signup_id (Supabase will assign)
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

    # ---- Write to CSV ----
    OUTPUT_2025_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUTPUT_2025_CSV, index=False)

    total = len(out_df)
    unmatched = (out_df["institution_match_status"] == "unmatched").sum()
    print(f"[2025] Wrote {total} rows to {OUTPUT_2025_CSV}")
    print(f"[2025] Unmatched institutions: {unmatched}")


if __name__ == "__main__":
    main()
