import os
import re
import pandas as pd
from sqlalchemy import create_engine, text

RAW_SCHEMA = "raw"
RAW_TABLE = "raw_amazon_catalog_listings_sku_asin_snapshot"

STAGING_SCHEMA = "staging"
STAGING_TABLE = "stg_amazon_catalog_listings_sku_asin_snapshot"


def snake_case(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_").lower()


def normalize_text(s: pd.Series) -> pd.Series:
    # Strip whitespace + convert blanks to NA
    s = s.astype(str).str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return s


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first matching column (after snake_case) from candidates."""
    for c in candidates:
        c2 = snake_case(c)
        if c2 in df.columns:
            return c2
    return None


def main():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL environment variable not set")

    engine = create_engine(db_url)

    # 1) Ensure staging schema exists
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};"))

    # 2) Get latest load_id (robust: uses max(load_ts) per load)
    latest_load_id_sql = text(f"""
        SELECT load_id
        FROM {RAW_SCHEMA}.{RAW_TABLE}
        GROUP BY load_id
        ORDER BY MAX(load_ts) DESC
        LIMIT 1
    """)
    with engine.begin() as conn:
        latest_load_id = conn.execute(latest_load_id_sql).scalar()

    if not latest_load_id:
        raise ValueError("No load_id found in raw catalog table. Run catalog raw loader first.")

    print(f"Processing load_id: {latest_load_id}")

    # 3) Pull only latest load
    df = pd.read_sql_query(
        text(f"SELECT * FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE load_id = :load_id"),
        engine,
        params={"load_id": latest_load_id},
    )
    print(f"Read {len(df)} rows from RAW")

    if df.empty:
        raise ValueError("Latest raw load returned 0 rows. Check raw loader / input file.")

    # 4) Standardize column names
    df.columns = [snake_case(c) for c in df.columns]

    # 5) Find SKU + ASIN columns (Amazon headers vary)
    sku_col = find_col(df, ["seller_sku", "seller-sku", "sku", "merchant_sku", "merchant-sku"])
    asin_col = find_col(df, ["asin1", "asin", "asin-1", "asin_1"])

    # Optional columns
    title_col = find_col(df, ["item_name", "item-name", "product_name", "title"])
    status_col = find_col(df, ["status", "listing_status"])
    fc_col = find_col(df, ["fulfillment_channel", "fulfillment-channel", "fulfillment"])

    if not sku_col or not asin_col:
        raise ValueError(
            f"Could not find required SKU/ASIN columns.\n"
            f"Detected sku_col={sku_col}, asin_col={asin_col}\n"
            f"Available columns: {list(df.columns)}"
        )

    # 6) Select + rename to standard staging names
    keep = [sku_col, asin_col]
    if title_col: keep.append(title_col)
    if status_col: keep.append(status_col)
    if fc_col: keep.append(fc_col)

    # keep metadata if present
    for meta in ["load_id", "load_ts", "source_file"]:
        if meta in df.columns:
            keep.append(meta)

    stg = df[keep].copy()

    # Normalize keys
    stg[sku_col] = normalize_text(stg[sku_col])
    stg[asin_col] = normalize_text(stg[asin_col])

    # Drop rows without keys
    stg = stg.dropna(subset=[sku_col, asin_col])

    # Rename columns to consistent names
    rename_map = {sku_col: "seller_sku", asin_col: "asin"}
    if title_col: rename_map[title_col] = "item_name"
    if status_col: rename_map[status_col] = "status"
    if fc_col: rename_map[fc_col] = "fulfillment_channel"
    stg.rename(columns=rename_map, inplace=True)

    # De-dupe
    before = len(stg)
    stg = stg.drop_duplicates(subset=["seller_sku", "asin"])
    after = len(stg)
    if before > after:
        print(f"Removed {before - after} duplicate rows")

    # 7) Write staging (replace snapshot)
    stg.to_sql(
        STAGING_TABLE,
        engine,
        schema=STAGING_SCHEMA,
        if_exists="replace",
        index=False
    )

    print(f"✓ Staged {len(stg)} rows into {STAGING_SCHEMA}.{STAGING_TABLE}")


if __name__ == "__main__":
    main()
