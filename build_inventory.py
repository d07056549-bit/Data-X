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

PROJECT_ROOT = Path(r"C:\Users\Empok\Documents\GitHub\Data-X")

RAW_ROOT = PROJECT_ROOT / "Data" / "raw"
PARQUET_CACHE = RAW_ROOT / "_parquet_cache"
PARQUET_CACHE.mkdir(parents=True, exist_ok=True)

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
    """Infer domain from top-level folder under Data/raw."""
    try:
        parts = path.relative_to(RAW_ROOT).parts
    except ValueError:
        return None
    return parts[0] if len(parts) > 0 else None


def infer_source(path: Path) -> Optional[str]:
    """Infer source from second-level folder or filename."""
    try:
        parts = path.relative_to(RAW_ROOT).parts
    except ValueError:
        return None

    if len(parts) >= 2:
        return parts[1]
    return path.stem


def parquet_cache_name(event_type: str, source: str, filename: str) -> Path:
    """Construct Parquet filename using N1 scheme."""
    safe_name = filename.replace(" ", "_").replace("-", "_")
    return PARQUET_CACHE / f"{event_type}_{source}_{safe_name}.parquet"


def load_raw_as_dataframe(path: Path, ext: str) -> Optional[pd.DataFrame]:
    """Load raw file safely into a DataFrame."""
    try:
        if ext == ".csv":
            return pd.read_csv(path, low_memory=False)
        elif ext in [".xlsx", ".xls"]:
            return pd.read_excel(path)
        elif ext == ".json":
            return pd.read_json(path)
        elif ext == ".txt":
            # Try CSV-like TXT
            try:
                return pd.read_csv(path, sep=None, engine="python", low_memory=False)
            except Exception:
                return None
        else:
            return None
    except Exception:
        return None


def extract_date_range(df: pd.DataFrame) -> (Optional[pd.Timestamp], Optional[pd.Timestamp]):
    """Infer date range from DataFrame."""
    if df is None or df.empty:
        return None, None

    date_cols = [c for c in df.columns if "date" in str(c).lower() or "time" in str(c).lower()]

    for col in date_cols:
        try:
            s = pd.to_datetime(df[col], errors="coerce")
            s = s.dropna()
            if not s.empty:
                return s.min(), s.max()
        except Exception:
            pass

    # Try year columns
    for col in df.columns:
        if "year" in str(col).lower():
            try:
                years = pd.to_numeric(df[col], errors="coerce").dropna().astype(int)
                if not years.empty:
                    return (
                        pd.Timestamp(years.min(), 1, 1),
                        pd.Timestamp(years.max(), 12, 31),
                    )
            except Exception:
                pass

    return None, None


def convert_to_parquet_if_needed(path: Path, event_type: str, source: str) -> Optional[Path]:
    """Convert raw file to Parquet if not already cached."""
    ext = path.suffix.lower()
    parquet_path = parquet_cache_name(event_type, source, path.name)

    if parquet_path.exists():
        return parquet_path

    df = load_raw_as_dataframe(path, ext)
    if df is None:
        return None

    try:
        df.to_parquet(parquet_path, index=False)
        print(f"[CACHE] Created Parquet: {parquet_path.name}")
        return parquet_path
    except Exception:
        return None


def extract_file_metadata(path: Path) -> Dict[str, Any]:
    """Extract metadata for raw + parquet versions."""
    stat = path.stat()
    ext = path.suffix.lower()

    event_type = infer_event_type(path) or "Unknown"
    source = infer_source(path) or "Unknown"

    parquet_path = convert_to_parquet_if_needed(path, event_type, source)

    df = None
    if parquet_path and parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
        except Exception:
            df = None

    first_dt, last_dt = extract_date_range(df) if df is not None else (None, None)

    print(f"[SCAN] {event_type}/{source} → {path.name} → dates: {first_dt} → {last_dt}")

       meta = {
    "path_full": str(path),
    "path_relative": str(path.relative_to(PROJECT_ROOT)),
    "parquet_path": str(parquet_path) if parquet_path else None,
    "filename": path.name,
    "extension": ext,
    "event_type": event_type,
    "source": source,
    "size_bytes": stat.st_size,
    "modified_time": datetime.fromtimestamp(stat.st_mtime),
    "created_time": datetime.fromtimestamp(stat.st_ctime),
    "hash_md5": md5_for_file(path),
    "n_rows": df.shape[0] if df is not None else None,
    "n_cols": df.shape[1] if df is not None else None,
    "first_valid_date": first_dt,
    "last_valid_date": last_dt,
}

return meta


def scan_raw_tree() -> List[Dict[str, Any]]:
    """Recursively scan all raw files."""
    records = []
    for root, dirs, files in os.walk(RAW_ROOT):
        dirs[:] = [d for d in dirs if not d.startswith("_parquet_cache")]
        for fname in files:
            if fname.startswith("."):
                continue
            path = Path(root) / fname
            records.append(extract_file_metadata(path))
    return records


def load_existing_inventory() -> Optional[pd.DataFrame]:
    if INVENTORY_PATH.exists():
        df = pd.read_parquet(INVENTORY_PATH)
        for col in ["modified_time", "created_time", "first_valid_date", "last_valid_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    return None


def merge_inventories(old: Optional[pd.DataFrame], new: pd.DataFrame) -> pd.DataFrame:
    """Merge inventories safely."""
    if old is None or old.empty:
        return new

    required = {"path_full", "hash_md5"}
    if not required.issubset(old.columns):
        print("⚠️ Old inventory missing required columns — replacing with new scan.")
        return new

    key_cols = ["path_full", "hash_md5"]

    old_keyed = old.set_index(key_cols, drop=False)
    new_keyed = new.set_index(key_cols, drop=False)

    combined = new_keyed.copy()
    overlap = old_keyed.index.intersection(new_keyed.index)

    combined.loc[overlap] = old_keyed.loc[overlap]
    return combined.reset_index(drop=True)


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    print(f"Scanning raw tree under: {RAW_ROOT}")

    new_records = scan_raw_tree()
    new_inv = pd.DataFrame(new_records)

    for col in ["modified_time", "created_time", "first_valid_date", "last_valid_date"]:
    if col in new_inv.columns:
        new_inv[col] = pd.to_datetime(new_inv[col], errors="coerce")

    old_inv = load_existing_inventory()
    final_inv = merge_inventories(old_inv, new_inv)

    final_inv.to_parquet(INVENTORY_PATH, index=False)
    print(f"Inventory written to: {INVENTORY_PATH}")
    print(f"Total files indexed: {len(final_inv)}")


if __name__ == "__main__":
    main()
