import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

REPO_ROOT = Path(r"C:\Users\Empok\Documents\GitHub\Sofie")
PROCESSED = REPO_ROOT / "Data" / "processed" / "inventory_files.parquet"


# ---------------------------------------------------------
# LOAD INVENTORY
# ---------------------------------------------------------

def load_inventory():
    """Load the processed inventory and ensure time dimensions exist."""
    df = pd.read_parquet(PROCESSED)

    # Ensure datetime
    df["inferred_date"] = pd.to_datetime(df["inferred_date"], errors="coerce")

    # Extract time dimensions
    df["year"] = df["inferred_date"].dt.year
    df["month"] = df["inferred_date"].dt.month
    df["week"] = df["inferred_date"].dt.isocalendar().week

    return df


# ---------------------------------------------------------
# WEEKLY EVENT CUBE
# ---------------------------------------------------------

def build_weekly_event_cube(df):
    """Aggregate events by year/week and pivot into a matrix."""
    weekly = (
        df
        .groupby(["year", "week", "event_type"])
        .size()
        .reset_index(name="count")
    )

    cube = weekly.pivot_table(
        index=["year", "week"],
        columns="event_type",
        values="count",
        fill_value=0
    )

    cube = cube.sort_index()
    return cube


# ---------------------------------------------------------
# MONTHLY EVENT CUBE
# ---------------------------------------------------------

def build_monthly_event_cube(df):
    """Aggregate events by year/month and pivot into a matrix."""
    monthly = (
        df
        .groupby(["year", "month", "event_type"])
        .size()
        .reset_index(name="count")
    )

    cube = monthly.pivot_table(
        index=["year", "month"],
        columns="event_type",
        values="count",
        fill_value=0
    )

    cube = cube.sort_index()
    return cube


# ---------------------------------------------------------
# YEARLY EVENT CUBE
# ---------------------------------------------------------

def build_yearly_event_cube(df):
    """Aggregate events by year and pivot into a matrix."""
    yearly = (
        df
        .groupby(["year", "event_type"])
        .size()
        .reset_index(name="count")
    )

    cube = yearly.pivot_table(
        index=["year"],
        columns="event_type",
        values="count",
        fill_value=0
    )

    cube = cube.sort_index()
    return cube


# ---------------------------------------------------------
# MAIN ORCHESTRATOR
# ---------------------------------------------------------

def main():
    print("Loading inventory...")
    df = load_inventory()
    print(f"Loaded {len(df)} inventory rows.")

    print("\nBuilding weekly cube...")
    weekly = build_weekly_event_cube(df)
    print(weekly.head())

    print("\nBuilding monthly cube...")
    monthly = build_monthly_event_cube(df)
    print(monthly.head())

    print("\nBuilding yearly cube...")
    yearly = build_yearly_event_cube(df)
    print(yearly.head())

    # Optional: save cubes for downstream analysis
    weekly.to_parquet(PROCESSED.parent / "weekly_event_cube.parquet")
    monthly.to_parquet(PROCESSED.parent / "monthly_event_cube.parquet")
    yearly.to_parquet(PROCESSED.parent / "yearly_event_cube.parquet")

    print("\nEvent cubes written to processed folder.")


if __name__ == "__main__":
    main()
