import os
import re
import pandas as pd
from datetime import datetime
from dateutil.parser import parse as dateparse

# Point to your existing raw data
ROOT = r"C:\Users\Empok\Documents\GitHub\Sofie\Data\raw"

DATE_PATTERNS = [
    r"\d{4}-\d{2}-\d{2}",
    r"\d{4}_\d{2}_\d{2}",
    r"\d{2}-\d{2}-\d{4}",
    r"\d{2}_\d{2}_\d{4}",
    r"\d{4}-\d{2}",
    r"\d{4}",
]

def extract_date(filename):
    for pattern in DATE_PATTERNS:
        match = re.search(pattern, filename)
        if match:
            text = match.group(0)

            try:
                return dateparse(text, dayfirst=True, yearfirst=True)
            except Exception:
                pass

            formats = [
                "%Y-%m-%d", "%Y_%m_%d",
                "%d-%m-%Y", "%d_%m_%Y",
                "%m-%d-%Y", "%m_%d_%Y",
                "%Y-%m", "%Y",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(text, fmt)
                except Exception:
                    continue
    return None

def classify_event(path_parts, filename):
    path_str = "/".join(path_parts).lower()
    fname = filename.lower()

    if "acled" in path_str:
        return "conflict_acled"
    if "ucdp" in path_str:
        return "conflict_ucdp"
    if "election" in path_str:
        return "election"
    if "crypto" in path_str:
        return "market_crypto"
    if "commodities" in path_str:
        return "market_commodities"
    if "hazard" in path_str or "disaster" in path_str:
        return "natural_disaster"
    if "imf" in path_str:
        return "macro_imf"
    if "world bank" in path_str:
        return "macro_worldbank"
    if "mobility" in path_str:
        return "mobility"
    if "night time lights" in path_str:
        return "earth_observation_ntl"

    return "unknown"

def scan(root):
    records = []

    for dirpath, _, files in os.walk(root):
        for f in files:
            full = os.path.join(dirpath, f)
            rel = os.path.relpath(full, root)
            parts = rel.split(os.sep)

            date = extract_date(f)
            event = classify_event(parts[:-1], f)

            rec = {
                "file_path": rel,
                "file_name": f,
                "file_type": os.path.splitext(f)[1].replace(".", ""),
                "category": parts[0] if len(parts) > 1 else "root",
                "event_type": event,
                "date": date,
                "year": date.year if date else None,
                "month": date.month if date else None,
                "week": date.isocalendar().week if date else None,
                "quarter": (date.month - 1)//3 + 1 if date else None,
            }

            records.append(rec)

    return pd.DataFrame(records)

if __name__ == "__main__":
    df = scan(ROOT)
    out_path = "dataset_inventory.csv"
    df.to_csv(out_path, index=False)
    print(f"Inventory created: {out_path}, rows={len(df)}")
