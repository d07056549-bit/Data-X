import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from dateutil import parser as dateparser


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

REPO_ROOT = Path(r"C:\Users\Empok\Documents\GitHub\Sofie")
RAW_ROOT = REPO_ROOT / "Data" / "raw"
PROCESSED_ROOT = REPO_ROOT / "Data" / "processed"
PROCESSED_ROOT.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = PROCESSED_ROOT / "inventory_files.parquet"


# ---------------------------------------------------------
# DATE PATTERNS
# ---------------------------------------------------------

DATE_CANDIDATE_PATTERNS = [
    r"\b\d{4}-\d{2}-\d{2}\b",        # 2024-03-21
    r"\b\d{4}_\d{2}_\d{2}\b",        # 2024_03_21
    r"\b\d{2}-\d{2}-\d{4}\b",        # 21-03-2024
    r"\b\d{2}_\d{2}_\d{4}\b",        # 21_03_2024
    r"\b\d{2}/\d{2}/\d{4}\b",        # 21/03/2024
    r"\b\d{4}-\d{2}\b",              # 2024-03
    r"\b\d{1,2}[A-Za-z]{3}\d{4}\b",  # 13Mar2024
]

if date.year < 1900 or date.year > 2100:
    return None

return datetime.fromtimestamp(stat.st_mtime)

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


# ---------------------------------------------------------
# DATE HELPERS
# ---------------------------------------------------------

def find_date_candidate(s: str) -> str | None:
    for pattern in DATE_CANDIDATE_PATTERNS:
        match = re.search(pattern, s)
        if match:
            return match.group(0)

def parse_date_flexible(s: str) -> datetime | None:
    if not s:
        return None

    candidate = find_date_candidate(s)
    if not candidate:
        return None

    for fmt in EXPLICIT_FORMATS:
        try:
            return datetime.strptime(candidate, fmt)
        except ValueError:
            continue

    for dayfirst in (True, False):
        try:
            return dateparser.parse(candidate, dayfirst=dayfirst)
        except Exception:
            continue

    return None


def looks_like_date_column(colname: str) -> bool:
    col = colname.lower()
    keywords = ["date", "time", "year", "period", "timestamp", "dt"]
    return any(k in col for k in keywords)


# ---------------------------------------------------------
# FILE CONTENT DATE EXTRACTION
# ---------------------------------------------------------

def extract_date_from_file(full_path: Path) -> datetime | None:
    suffix = full_path.suffix.lower()

    # CSV / TXT
    if suffix in [".csv", ".txt"]:
        try:
            df = pd.read_csv(full_path, nrows=500)
            date_cols = [c for c in df.columns if looks_like_date_column(c)]

            for col in date_cols:
                try:
                    dates = pd.to_datetime(df[col], errors="coerce")
                    valid = dates.dropna()
                    if not valid.empty:
                        return valid.max().to_pydatetime()
                except Exception:
                    continue
        except Exception:
            pass

    # JSON
    if suffix == ".json":
        try:
            import json
            with open(full_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            def walk(obj):
                if isinstance(obj, dict):
                    for v in obj.values():
                        yield from walk(v)
                elif isinstance(obj, list):
                    for v in obj:
                        yield from walk(v)
                else:
                    yield obj

            for value in walk(data):
                try:
                    dt = pd.to_datetime(value, errors="coerce")
                    if pd.notnull(dt):
                        return dt.to_pydatetime()
                except Exception:
                    continue
        except Exception:
            pass

    # Excel
    if suffix in [".xlsx", ".xls"]:
        try:
            df = pd.read_excel(full_path, nrows=500)
            date_cols = [c for c in df.columns if looks_like_date_column(c)]

            for col in date_cols:
                try:
                    dates = pd.to_datetime(df[col], errors="coerce")
                    valid = dates.dropna()
                    if not valid.empty:
                        return valid.max().to_pydatetime()
                except Exception:
                    continue
        except Exception:
            pass

    return None

# ---------------------------------------------------------
# FILE CONTENT DATE EXTRACTION
# ---------------------------------------------------------

def extract_date_from_file(full_path: Path) -> datetime | None:
    # ... your existing CSV/TXT/Excel/JSON logic ...
    return None


# ---------------------------------------------------------
# UNSTRUCTURED DATE EXTRACTION (NEW)
# ---------------------------------------------------------

def extract_unstructured_date(full_path: Path) -> datetime | None:
    """
    Scan the first ~200 lines of a text-like file for any date pattern.
    """
    try:
        with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
            for _ in range(200):
                line = f.readline()
                if not line:
                    break

                candidate = find_date_candidate(line)
                if candidate:
                    try:
                        return dateparser.parse(candidate, dayfirst=True)
                    except Exception:
                        continue
    except Exception:
        pass

    return None

# ---------------------------------------------------------
# MULTI-TIER DATE INFERENCE
# ---------------------------------------------------------

def infer_date(full_path: Path, rel_path: Path, stat) -> datetime:
    # Layer 1: filename
    date = parse_date_flexible(full_path.name)
    if date:
        return date

    # Layer 2: folder path
    date = parse_date_flexible(str(rel_path))
    if date:
        return date

    # Layer 3: structured content
    date = extract_date_from_file(full_path)
    if date:
        return date

    # Layer 4: unstructured content
    date = extract_unstructured_date(full_path)
    if date:
        return date

    # Layer 5: domain-specific (optional, next step)
    # e.g., ACLED, UCDP, IMF, EIA, Market

    # Layer 6: fallback
    return datetime.fromtimestamp(stat.st_mtime)

# ---------------------------------------------------------
# EVENT TYPE INFERENCE
# ---------------------------------------------------------

def infer_event_type(path: Path) -> str:
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


# ---------------------------------------------------------
# SCANNER
# ---------------------------------------------------------

def build_current_scan() -> pd.DataFrame:
    records = []

    for root, dirs, files in os.walk(RAW_ROOT):
        for f in files:
            full_path = Path(root) / f
            rel_path = full_path.relative_to(RAW_ROOT)
            stat = full_path.stat()

            date = infer_date(full_path, rel_path, stat)
            event_type = infer_event_type(full_path)

            records.append({
                "file_name": f,
                "relative_path": str(rel_path),
                "absolute_path": str(full_path),
                "event_type": event_type,
                "inferred_date": date,
                "year": date.year,
                "month": date.month,
                "week": date.isocalendar().week,
                "file_modified": datetime.fromtimestamp(stat.st_mtime),
                "file_size_bytes": stat.st_size,
                "extension": full_path.suffix.lower(),
            })

    return pd.DataFrame(records)


# ---------------------------------------------------------
# MERGE LOGIC
# ---------------------------------------------------------

def load_existing_inventory() -> pd.DataFrame | None:
    if OUTPUT_PATH.exists():
        return pd.read_parquet(OUTPUT_PATH)
    return None


def merge_inventories(old: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    if old is None or old.empty:
        return new

    old = old.copy()
    new = new.copy()

    old["key"] = old["relative_path"] + "|" + old["file_modified"].astype(str)
    new["key"] = new["relative_path"] + "|" + new["file_modified"].astype(str)

    old_filtered = old[~old["key"].isin(new["key"])]

    combined = pd.concat([old_filtered, new], ignore_index=True)
    combined = combined.drop(columns=["key"])

    return combined


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

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

    merged = merged.sort_values(
        ["event_type", "year", "month", "week", "relative_path"],
        na_position="last"
    )

    merged.to_parquet(OUTPUT_PATH, index=False)
    print(f"Inventory written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
