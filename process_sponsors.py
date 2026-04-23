import pandas as pd
import re
from pathlib import Path

# ------------- CONFIG -------------

SPONSORS_CSV   = Path("sponsors.csv")
INSTITUTIONS_CSV = Path("institutions.csv")
OUTPUT_CSV     = Path("sponsors_clean.csv")

CW_YEAR = 2025  # <-- set this to the correct year

COLUMN_NAMES = [
    "region",
    "institution_name",
    "community_sponsor_id",
    "launch_good",
    "venmo",
    "check_amount",
    "school_check",
    "corporate_match",
    "money_order_other",
    "total",
    "banking_status",
    "notes",
    "state",
]

MONEY_COLUMNS = [
    "launch_good",
    "venmo",
    "check_amount",
    "school_check",
    "corporate_match",
    "money_order_other",
    "total",
]


# ------------- HELPERS -------------

def parse_money(val) -> float:
    """
    Convert money strings to float.
    Handles: ' 1,572.00 ', ' -   ', '', None, NaN
    """
    if pd.isna(val):
        return 0.0
    s = str(val).strip()
    if s in ("-", "", "-   "):
        return 0.0
    # Remove dollar signs, commas, spaces
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return round(float(s), 2)
    except ValueError:
        return 0.0


def normalize_name(name: str) -> str:
    """
    Same normalization as used in institutions/signups:
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


def load_institutions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Institutions CSV not found: {path}")
    inst = pd.read_csv(path)
    required = {"institution_id", "country_code", "institution_name_norm"}
    missing = required - set(inst.columns)
    if missing:
        raise ValueError(f"Missing columns in institutions.csv: {missing}")
    inst["institution_name_norm"] = inst["institution_name_norm"].apply(normalize_name)
    inst["country_code"] = inst["country_code"].astype(str).str.strip()
    return inst


# ------------- MAIN PROCESS -------------

def main():
    # Load raw CSV (no header)
    if not SPONSORS_CSV.exists():
        raise FileNotFoundError(f"Sponsors CSV not found: {SPONSORS_CSV}")

    df = pd.read_csv(SPONSORS_CSV, header=None, names=COLUMN_NAMES)

    print(f"Loaded {len(df)} rows from {SPONSORS_CSV}")

    # --- Clean money columns ---
    for col in MONEY_COLUMNS:
        df[col] = df[col].apply(parse_money)

    # --- Recalculate total to verify ---
    df["total_calculated"] = df[MONEY_COLUMNS[:-1]].sum(axis=1).round(2)
    mismatches = df[abs(df["total"] - df["total_calculated"]) > 0.01]
    if len(mismatches) > 0:
        print(f"\nWARNING: {len(mismatches)} rows have total mismatches:")
        for _, row in mismatches.iterrows():
            print(f"  Sponsor {row['community_sponsor_id']} "
                  f"({row['institution_name']}): "
                  f"stated={row['total']}, calculated={row['total_calculated']}")
    df.drop(columns=["total_calculated"], inplace=True)

    # --- Clean text fields ---
    df["institution_name"] = df["institution_name"].astype(str).str.strip()
    df["region"] = df["region"].astype(str).str.strip()
    df["state"] = df["state"].apply(lambda x: str(x).strip() if pd.notna(x) and str(x).strip() else None)
    df["banking_status"] = df["banking_status"].astype(str).str.strip()
    df["notes"] = df["notes"].apply(lambda x: str(x).strip() if pd.notna(x) and str(x).strip() else None)
    df["community_sponsor_id"] = df["community_sponsor_id"].astype(int)

    # --- Normalise institution name for matching ---
    df["institution_name_norm"] = df["institution_name"].apply(normalize_name)

    # --- Match against institutions table ---
    # All sponsors in this file are US-based
    df["country_code"] = "US"

    inst_df = load_institutions(INSTITUTIONS_CSV)
    # Filter to US institutions for matching
    inst_us = inst_df[inst_df["country_code"] == "US"][
        ["institution_id", "country_code", "institution_name_norm"]
    ].copy()

    df = df.merge(
        inst_us,
        on=["country_code", "institution_name_norm"],
        how="left",
    )

    matched = df["institution_id"].notna().sum()
    unmatched = df["institution_id"].isna().sum()

    # --- Add CW year ---
    df["cw_year"] = CW_YEAR

    # --- Fix integer columns ---
    def int_or_blank(x):
        if pd.isna(x):
            return ""
        try:
            return int(x)
        except Exception:
            return ""

    df["institution_id"] = df["institution_id"].apply(int_or_blank)

    # --- Reorder for output ---
    out_cols = [
        "cw_year",
        "community_sponsor_id",
        "institution_name",
        "institution_id",
        "region",
        "state",
        "launch_good",
        "venmo",
        "check_amount",
        "school_check",
        "corporate_match",
        "money_order_other",
        "total",
        "banking_status",
        "notes",
    ]
    out_df = df[out_cols].copy()

    # --- Write output ---
    out_df.to_csv(OUTPUT_CSV, index=False)

    print(f"\nWrote {len(out_df)} rows to {OUTPUT_CSV}")
    print(f"  Matched to institutions: {matched}")
    print(f"  Unmatched:               {unmatched}")

    # --- Show unmatched for review ---
    if unmatched > 0:
        print(f"\nUnmatched sponsors:")
        unmatched_df = df[df["institution_id"] == ""][
            ["community_sponsor_id", "institution_name", "institution_name_norm", "region"]
        ]
        for _, row in unmatched_df.iterrows():
            print(f"  [{row['community_sponsor_id']}] {row['institution_name']}")


if __name__ == "__main__":
    main()
