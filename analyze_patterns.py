import pandas as pd
from pathlib import Path

REPO_ROOT = Path(r"C:\Users\Empok\Documents\GitHub\Sofie")
PROCESSED = REPO_ROOT / "Data" / "processed"

WEEKLY = PROCESSED / "weekly_event_cube.parquet"

def load_weekly():
    return pd.read_parquet(WEEKLY)

def correlations(weekly):
    return weekly.corr()

def lagged_corr(weekly, source, target, lag_weeks=1):
    return weekly[source].shift(lag_weeks).corr(weekly[target])

if __name__ == "__main__":
    w = load_weekly()
    print("Corr matrix:")
    print(correlations(w))

    for lag in [1, 2, 4, 8]:
        c = lagged_corr(w, "Conflict", "Market", lag_weeks=lag)
        print(f"Lag {lag}w: Conflict → Market corr = {c}")
