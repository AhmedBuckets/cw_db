import pandas as pd
import re
from pathlib import Path

# ---- CONFIG ----
# Path to your deduplicated institutions file
RAW_CSV = Path("institutions_dedup.csv")

# Output CSV to upload into Supabase
OUT_CSV = Path("institutions.csv")

# Column names in your deduped CSV:
# Change these if your headers differ
COLUMN_COUNTRY = "Final Country:"      # as in your sample
COLUMN_NAME = "Institution"            # e.g. "Institution" / "Name"
COLUMN_LEGACY_ID = "ID"    # e.g. "Institution ID"


# ---- HELPERS ----

# Controlled mapping based on your dropdown values
COUNTRY_CODE_MAP = {
    "Canada": "CA",
    "Germany": "DE",
    "Grenada": "GD",
    "Ireland": "IE",
    "Malaysia": "MY",
    "Other": "OTHER",      # or "ZZ" if you prefer
    "Pakistan": "PK",
    "Qatar": "QA",
    "South Africa": "ZA",
    "Uganda": "UG",
    "UK": "GB",
    "USA": "US",
}

def normalize_country_code(country: str) -> str:
    if pd.isna(country):
        return "OTHER"
    c = str(country).strip()
    if c not in COUNTRY_CODE_MAP:
        raise ValueError(f"Unexpected country value: {c!r}. Add it to COUNTRY_CODE_MAP.")
    return COUNTRY_CODE_MAP[c]


def normalize_name(name: str) -> str:
    """
    Simple normalization:
    - lowercase
    - strip leading/trailing spaces
    - replace non-alphanumeric (except spaces) with space
    - collapse multiple spaces
    """
    if pd.isna(name):
        return ""

    s = str(name).strip().lower()

    # replace punctuation and special chars with space
    s = re.sub(r"[^a-z0-9]+", " ", s)

    # collapse multiple spaces
    s = re.sub(r"\s+", " ", s)

    return s.strip()


def list_to_json_str(values):
    """
    Convert a Python list to a JSON-like string for CSV import into jsonb.
    e.g. ["583"] -> '["583"]'
    """
    if not values:
        return "[]"
    str_values = [str(v) for v in values]
    inner = ",".join(f'"{v}"' for v in str_values)
    return f"[{inner}]"


# ---- MAIN ----

def main():
    if not RAW_CSV.exists():
        raise FileNotFoundError(f"Input CSV not found: {RAW_CSV}")

    # Read deduplicated CSV
    df_raw = pd.read_csv(RAW_CSV)

    # Ensure required columns exist
    for col in [COLUMN_COUNTRY, COLUMN_NAME, COLUMN_LEGACY_ID]:
        if col not in df_raw.columns:
            raise ValueError(
                f"Column '{col}' not found in CSV. "
                f"Available columns: {list(df_raw.columns)}"
            )

    # Rename to internal standard names
    df = df_raw.rename(
        columns={
            COLUMN_COUNTRY: "country",
            COLUMN_NAME: "institution_name_raw",
            COLUMN_LEGACY_ID: "legacy_id",
        }
    ).copy()

    # Normalize country and country_code
    df["country"] = df["country"].astype(str).str.strip()
    df["country_code"] = df["country"].apply(normalize_country_code)

    # Clean institution name
    df["institution_name_clean"] = df["institution_name_raw"].astype(str).str.strip()
    df["institution_name_norm"] = df["institution_name_clean"].apply(normalize_name)

    # Legacy IDs: treat your existing Institution ID as a legacy ID
    df["legacy_id"] = df["legacy_id"].astype(str).str.strip()
    df.loc[df["legacy_id"] == "", "legacy_id"] = pd.NA

    # Since the file is already deduplicated, we assume:
    # 1 row = 1 institution. No grouping needed.
    # Assign new global institution_id (sequential ints starting at 1)
    df = df.reset_index(drop=True)
    df["institution_id"] = df.index + 1

    # Build legacy_ids array (usually single element)
    df["legacy_ids_json"] = df["legacy_id"].apply(
        lambda x: list_to_json_str([x]) if pd.notna(x) else "[]"
    )

    # For now, aliases array will be empty; you can populate later if needed
    df["aliases_json"] = "[]"

    # Build final institutions dataframe matching the Supabase schema
    institutions = pd.DataFrame({
        "institution_id": df["institution_id"],
        "country": df["country"],
        "country_code": df["country_code"],
        "name_canonical": df["institution_name_clean"],
        "name_display": df["institution_name_clean"],   # can customize later
        "institution_name_norm": df["institution_name_norm"],
        "legacy_ids": df["legacy_ids_json"],
        "aliases": df["aliases_json"],
    })[
        [
            "institution_id",
            "country",
            "country_code",
            "name_canonical",
            "name_display",
            "institution_name_norm",
            "legacy_ids",
            "aliases",
        ]
    ]

    institutions.to_csv(OUT_CSV, index=False)

    print("Done.")
    print(f"Input rows (institutions): {len(df)}")
    print(f"Institutions written to: {OUT_CSV}")


if __name__ == "__main__":
    main()
