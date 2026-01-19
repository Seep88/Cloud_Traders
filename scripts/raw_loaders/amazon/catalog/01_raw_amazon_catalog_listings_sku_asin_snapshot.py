import os
import glob
import uuid
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", "..", ".."))

RAW_DIR = os.getenv(
    "AMZ_CATALOG_LISTINGS_RAW_DIR",
    os.path.join(PROJECT_ROOT, "data", "raw", "input", "amazon", "catalog", "listings_sku_asin_snapshot_monthly")
)

RAW_SCHEMA = "raw"
RAW_TABLE = "raw_amazon_catalog_listings_sku_asin_snapshot"


def get_latest_txt(folder: str) -> str:
    files = glob.glob(os.path.join(folder, "*.txt"))
    if not files:
        raise FileNotFoundError(f"No .txt files found in: {folder}")
    return max(files, key=os.path.getmtime)


def main():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL environment variable not set")

    engine = create_engine(db_url)

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};"))

    latest_file = get_latest_txt(RAW_DIR)
    filename = os.path.basename(latest_file)
    print(f"Latest catalog file: {filename}")

    # Amazon listings are typically tab-delimited
    df = pd.read_csv(latest_file, sep="\t", encoding="utf-8-sig")

    load_id = str(uuid.uuid4())
    load_ts = datetime.now(timezone.utc)

    df["load_id"] = load_id
    df["load_ts"] = load_ts
    df["source_file"] = filename

    print(f"Rows read: {len(df)} | load_id={load_id}")

    df.to_sql(
        RAW_TABLE,
        engine,
        schema=RAW_SCHEMA,
        if_exists="append",
        index=False
    )

    print(f"✓ Loaded RAW into {RAW_SCHEMA}.{RAW_TABLE}")


if __name__ == "__main__":
    main()
