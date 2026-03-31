import pandas as pd
from pathlib import Path

REPO_ROOT = Path(r"C:\Users\Empok\Documents\GitHub\Sofie")
PROCESSED = REPO_ROOT / "Data" / "processed"
INVENTORY = PROCESSED / "inventory_files.parquet"
TIMELINE = PROCESSED / "event_timeline.parquet"

def build_timeline():
    df = pd.read_parquet(INVENTORY)

    # core event fields
    cols = [
        "event_type",
        "inferred_date",
        "year",
        "month",
        "week",
        "relative_path",
        "absolute_path",
        "extension",
        "file_modified",
        "file_size_bytes",
    ]
    df = df[cols].copy()

    # sort for readability
    df = df.sort_values(["inferred_date", "event_type", "relative_path"])

    df.to_parquet(TIMELINE, index=False)
    return df

if __name__ == "__main__":
    tl = build_timeline()
    print("Timeline rows:", len(tl))
    print(tl.head(10))
