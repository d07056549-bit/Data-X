#!/usr/bin/env python
import os
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List

import pandas as pd


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

# Project root: adjust if you move this script
PROJECT_ROOT = Path(r"C:\Users\Empok\Documents\GitHub\Sofie")

RAW_ROOT = PROJECT_ROOT / "Data" / "raw"
PROCESSED_ROOT = PROJECT_ROOT / "Data" / "processed"
PROCESSED_ROOT.mkdir(parents=True, exist_ok=True)

INVENTORY_PATH = PROCESSED_ROOT / "inventory_files.parquet"


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def md5_for_file(path: Path, block_size: int = 1 << 20) -> str:
    """Compute MD5 hash for a file."""
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()


def infer_event_type(path: Path) -> Optional[str]:
    """
    Infer high-level event_type from the first folder under Data/raw.
    Example: Data/raw/Conflict/ACLED/file.xlsx -> 'Conflict'
    """
    try:
        parts = path.relative_to(RAW_ROOT).parts
    except ValueError:
        return None
    return parts[0] if len(parts) > 0 else None


def infer_source(path: Path) -> Optional[str]:
    """
    Infer source from the second folder under Data/raw (if present),
    otherwise from the stem of the file.
    Example: Data/raw/Conflict/ACLED/file.xlsx -> 'ACLED'
    """
    try:
        parts = path.relative_to(RAW_ROOT).parts
    except ValueError:
        return None

    if len(parts) >= 2:
        return parts[1]
    return path.stem


def try_parse_dates_from_df(df: pd.DataFrame) -> (Optional[pd.Timestamp], Optional[pd.Timestamp]):
    """
    Try to infer min/max dates from a DataFrame by looking for
    likely date columns.
    """
    if df.empty:
        return None, None

    # Candidate column names (lowercased)
    candidates = [
        "date", "time", "datetime", "timestamp",
        "period", "year", "year_week", "year_month"
    ]

    # 1) Try obvious datetime-like columns
    for col in df.columns:
        col_l = str(col).lower()
        if any(c in col_l for c in ["date", "time", "period", "timestamp"]):
            try:
                s = pd.to_datetime(df[col], errors="coerce")
                s = s.dropna()
                if not s.empty:
                    return s.min(), s.max()
            except Exception:
                pass

    # 2) Try to parse any column that looks numeric year
    for col in df.columns:
        col_l = str(col).lower()
        if "year" in col_l:
            try:
                s = pd.to_numeric(df[col], errors="coerce")
                s = s.dropna()
                if not s.empty:
                    years = s.astype(int)
                    # represent as timestamps at start of year
                    return (
                        pd.Timestamp(int(years.min()), 1, 1),
                        pd.Timestamp(int(years.max()), 12, 31),
                    )
            except Exception:
                pass

    return None, None


def extract_file_metadata(path: Path) -> Dict[str, Any]:
    """
    Extract generic file metadata (size, times, hash) and
    attempt to infer date coverage for tabular files.
    """
    stat = path.stat()
    size_bytes = stat.st_size
    mtime = datetime.fromtimestamp(stat.st_mtime)
    ctime = datetime.fromtimestamp(stat.st_ctime)

    # Basic metadata
    meta: Dict[str, Any] = {
        "path_full": str(path),
        "path_relative": str(path.relative_to(PROJECT_ROOT)),
        "filename": path.name,
        "extension": path.suffix.lower(),
        "size_bytes": size_bytes,
        "modified_time": mtime,
        "created_time": ctime,
        "hash_md5": None,
        "event_type": infer_event_type(path),
        "source": infer_source(path),
        "n_rows": None,
        "n_cols": None,
        "first_valid_date": None,
        "last_valid_date": None,
    }

    # Compute hash
    try:
        meta["hash_md5"] = md5_for_file(path)
    except Exception:
        meta["hash_md5"] = None

    # Try to read tabular content for date coverage
    df: Optional[pd.DataFrame] = None
    try:
        if meta["extension"] in [".csv"]:
            df = pd.read_csv(path, low_memory=False)
        elif meta["extension"] in [".xlsx", ".xls"]:
            df = pd.read_excel(path)
        elif meta["extension"] in [".parquet"]:
            df = pd.read_parquet(path)
    except Exception:
        df = None

    if df is not None:
        meta["n_rows"] = int(df.shape[0])
        meta["n_cols"] = int(df.shape[1])
        first_dt, last_dt = try_parse_dates_from_df(df)
        meta["first_valid_date"] = first_dt
        meta["last_valid_date"] = last_dt

    return meta


def scan_raw_tree() -> List[Dict[str, Any]]:
    """
    Recursively walk RAW_ROOT and collect metadata for all files.
    """
    records: List[Dict[str, Any]] = []
    for root, dirs, files in os.walk(RAW_ROOT):
        # Optionally skip hidden dirs
        dirs[:] = [d for d in dirs if not d.startswith(".")]

        for fname in files:
            if fname.startswith("."):
                continue
            path = Path(root) / fname
            records.append(extract_file_metadata(path))

    return records


def load_existing_inventory() -> Optional[pd.DataFrame]:
    if INVENTORY_PATH.exists():
        inv = pd.read_parquet(INVENTORY_PATH)
        # Ensure datetime types are normalized (avoid ns warnings)
        for col in ["modified_time", "created_time", "first_valid_date", "last_valid_date"]:
            if col in inv.columns:
                inv[col] = pd.to_datetime(inv[col], errors="coerce")
        return inv
    return None


def merge_inventories(old: Optional[pd.DataFrame], new: pd.DataFrame) -> pd.DataFrame:
    """
    Merge old and new inventories. If a file (by full path + hash)
    is unchanged, keep the old row; otherwise, use the new row.
    """
    if old is None or old.empty:
        return new

    # Use path_full + hash_md5 as identity
    key_cols = ["path_full", "hash_md5"]

    old_keyed = old.set_index(key_cols, drop=False)
    new_keyed = new.set_index(key_cols, drop=False)

    # Start from new; where old has the same key, prefer old row
    combined = new_keyed.copy()
    overlapping_keys = old_keyed.index.intersection(new_keyed.index)

    combined.loc[overlapping_keys] = old_keyed.loc[overlapping_keys]

    # Also include any old rows whose files are no longer present (optional)
    # If you want to keep them, uncomment:
    # missing_keys = old_keyed.index.difference(new_keyed.index)
    # combined = pd.concat([combined, old_keyed.loc[missing_keys]], axis=0)

    combined = combined.reset_index(drop=True)
    return combined


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main() -> None:
    print(f"Scanning raw tree under: {RAW_ROOT}")
    records = scan_raw_tree()
    print(f"Found {len(records)} files in current scan.")

    new_inv = pd.DataFrame.from_records(records)

    # Normalize datetime columns to avoid ns warnings
    for col in ["modified_time", "created_time", "first_valid_date", "last_valid_date"]:
        if col in new_inv.columns:
            new_inv[col] = pd.to_datetime(new_inv[col], errors="coerce")

    # Load existing inventory if present
    old_inv = load_existing_inventory()
    if old_inv is not None:
        print(f"Loaded existing inventory with {len(old_inv)} rows.")
    else:
        print("No existing inventory found; creating a new one.")

    final_inv = merge_inventories(old_inv, new_inv)
    print(f"Final inventory has {len(final_inv)} rows.")

    # Write to parquet
    final_inv.to_parquet(INVENTORY_PATH, index=False)
    print(f"Inventory written to: {INVENTORY_PATH}")


if __name__ == "__main__":
    main()
