import os
import pandas as pd
from sqlalchemy import create_engine, text

STAGING_SCHEMA = "staging"
STAGING_TABLE = "stg_amazon_business_reports_sales_traffic_child_asin_daily"

WAREHOUSE_SCHEMA = "warehouse"
FACT_TABLE = "fct_amazon_business_reports_sales_traffic_child_asin_daily"


def main():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL environment variable not set")

    engine = create_engine(db_url)

    # 1) Ensure warehouse schema exists
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_SCHEMA};"))

    # 2) Read staging
    df = pd.read_sql_table(STAGING_TABLE, con=engine, schema=STAGING_SCHEMA)
    if df.empty:
        raise ValueError(f"Staging table {STAGING_SCHEMA}.{STAGING_TABLE} is empty")

    # 3) Identify key column for ASIN (after snake_case, usually 'child_asin')
    asin_col_candidates = ["child_asin", "asin", "asin1"]
    asin_col = next((c for c in asin_col_candidates if c in df.columns), None)
    if not asin_col:
        raise ValueError(f"Could not find child ASIN column. Columns: {list(df.columns)}")

    if "date" not in df.columns:
        raise ValueError("No 'date' column found in staging. Raw loader should add it.")

    # 4) Keep/standardize core fact columns (only include those that exist)
    keep = [
        "date",
        asin_col,
        "parent_asin",
        "sessions_total",
        "page_views_total",
        "units_ordered",
        "total_order_items",
        "ordered_product_sales_usd",
        "unit_session_percentage",
        "load_id",
        "load_ts",
        "source_file",
    ]
    keep = [c for c in keep if c in df.columns]
    fct = df[keep].copy()

    # 5) Rename asin column to a consistent name
    if asin_col != "child_asin":
        fct.rename(columns={asin_col: "child_asin"}, inplace=True)

    # 6) De-duplicate on business key (date + child_asin)
    # If duplicates exist, keep the last row (usually ok since staging is latest load snapshot)
    fct.sort_values(by=[c for c in ["load_ts"] if c in fct.columns], inplace=True)
    before = len(fct)
    fct = fct.drop_duplicates(subset=["date", "child_asin"], keep="last")
    after = len(fct)
    if before > after:
        print(f"Removed {before - after} duplicate (date, child_asin) rows")

    # 7) Derived measures (safe, only if source cols exist)
    if "ordered_product_sales_usd" in fct.columns and "sessions_total" in fct.columns:
        fct["revenue_per_session_usd"] = (
            fct["ordered_product_sales_usd"] / fct["sessions_total"]
        )

    if "units_ordered" in fct.columns and "sessions_total" in fct.columns:
        fct["conversion_rate_pct_calc"] = (
            (fct["units_ordered"] / fct["sessions_total"]) * 100
        )

    # 8) Write fact table (replace for now – clean and safe during development)
    fct.to_sql(
        FACT_TABLE,
        con=engine,
        schema=WAREHOUSE_SCHEMA,
        if_exists="replace",
        index=False
    )

    print(f"✓ Built FACT: {WAREHOUSE_SCHEMA}.{FACT_TABLE} rows={len(fct)}")
    print(f"✓ Date range: {fct['date'].min()} to {fct['date'].max()}")


if __name__ == "__main__":
    main()
