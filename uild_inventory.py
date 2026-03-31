import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from dateutil import parser as dateparser

# -----------------------------
# CONFIG
# -----------------------------
REPO_ROOT = Path(r"C:\Users\Empok\Documents\GitHub\Sofie")
RAW_ROOT = REPO_ROOT / "Data" / "raw"
PROCESSED_ROOT = REPO_ROOT / "Data" / "processed"
PROCESSED_ROOT.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = PROCESSED_ROOT / "inventory_files.parquet"

# Regex patterns to *find* date-like substrings
DATE_CANDIDATE_PATTERNS = [
    r"\d{4}-\d{2}-\d{2}",        # 2026-03-21
    r"\d{4}_\d{2}_\d{2}",        # 2026_03_21
    r"\d{2}-\d{2}-\d{4}",        # 21-03-2026 or 03-21-2026
    r"\d{2}_\d{2}_\d{4}",        # 21_03_2026 or 03_21_2026
    r"\d{2}/\d{2}/\d{4}",        # 21/03/2026 or 03/21/2026
    r"\d{4}-\d{2}",              # 2026-03
    r"\d{4}",                    # 2026
    r"\d{1,2}[A-Za-z]{3}\d{4}",  # 13Mar2026
]

# Explicit formats we want to try in order
EXPLICIT_FORMATS = [
    "%Y-%m-%d",
    "%Y_%m_%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%d_%m_%Y",
    "%m_%d_%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y-%m",
    "%Y",
    "%d%b%Y",
]


def find_date_candidate(s: str) -> str | None:
    """Return the first date-like substring found in s, or None."""
    for pattern in DATE_CANDIDATE_PATTERNS:
        match = re.search(pattern, s)
        if match:
            return match.group(0)
    return None


def parse_date_flexible(s: str) -> datetime | None:
    """
    Try to parse a date from a string using:
    1) explicit formats (d/m/y, m/d/y, etc.)
    2) dateutil with dayfirst=True and False
    """
    if not s:
        return None

    candidate = find_date_candidate(s)
    if not candidate:
        return None

    # 1) Try explicit formats
    for fmt in EXPLICIT_FORMATS:
        try:
            return datetime.strptime(candidate, fmt)
        except ValueError:
            continue

    # 2) Try dateutil with dayfirst=True then False
    for dayfirst in (True, False):
        try:
            return dateparser.parse(candidate, dayfirst=dayfirst)
        except (ValueError, TypeError):
            continue

    return None


def infer_event_type(path: Path) -> str:
    """Infer event type from folder names."""
    parts = set(path.parts)
    categories = [
        "Black Swan", "Conflict", "Earth Observation", "EIA", "Events",
        "Hazards", "Macro", "Market", "Migration & Refugee Flows",
        "OWID", "Research", "Sovereign Risk", "Supply Chain", "Surveys", "Trade"
    ]
    for c in categories:
        if c in parts:
            return c
    return "Unknown"


def build_current_scan() -> pd.DataFrame:
    """Walk RAW_ROOT and build a DataFrame of all files + inferred metadata."""
    records = []

    for root, dirs, files in os.walk(RAW_ROOT):
        for f in files:
            full_path = Path(root) / f
            rel_path = full_path.relative_to(RAW_ROOT)

            # Try to infer date from filename and relative path
            date = (
                parse_date_flexible(f)
                or parse_date_flexible(str(rel_path))
            )

            stat = full_path.stat()
            modified = datetime.fromtimestamp(stat.st_mtime)

            event_type = infer_event_type(full_path)

            records.append({
                "file_name": f,
                "relative_path": str(rel_path),
                "absolute_path": str(full_path),
                "event_type": event_type,
                "inferred_date": date,
                "year": date.year if date else None,
                "month": date.month if date else None,
                "week": date.isocalendar().week if date else None if date else None,
                "file_modified": modified,
                "file_size_bytes": stat.st_size,
                "extension": full_path.suffix.lower(),
            })

    df = pd.DataFrame(records)
    return df


def load_existing_inventory() -> pd.DataFrame | None:
    """Load existing processed inventory if it exists."""
    if OUTPUT_PATH.exists():
        return pd.read_parquet(OUTPUT_PATH)
    return None


def merge_inventories(old: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    """
    Merge old and new inventories.
    Key: relative_path + file_modified.
    If a file is updated, the new row replaces the old one.
    """
    if old is None or old.empty:
        return new

    # Create keys
    old = old.copy()
    new = new.copy()

    old["key"] = old["relative_path"] + "|" + old["file_modified"].astype(str)
    new["key"] = new["relative_path"] + "|" + new["file_modified"].astype(str)

    # Drop any old rows that are now superseded
    old_filtered = old[~old["key"].isin(new["key"])]

    combined = pd.concat([old_filtered, new], ignore_index=True)
    combined = combined.drop(columns=["key"])

    return combined


def main():
    print(f"Scanning raw data under: {RAW_ROOT}")
    new_scan = build_current_scan()
    print(f"Found {len(new_scan)} files in current scan.")

    existing = load_existing_inventory()
    if existing is not None:
        print(f"Loaded existing inventory with {len(existing)} rows.")
    else:
        print("No existing inventory found. Creating a new one.")

    merged = merge_inventories(existing, new_scan)
    print(f"Final inventory has {len(merged)} rows.")

    merged = merged.sort_values(["event_type", "year", "month", "week", "relative_path"], na_position="last")

    merged.to_parquet(OUTPUT_PATH, index=False)
    print(f"Inventory written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
