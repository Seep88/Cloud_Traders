import os
from sqlalchemy import create_engine, text

STAGING_SCHEMA = "staging"
STAGING_TABLE = "stg_amazon_catalog_listings_sku_asin_snapshot"

WAREHOUSE_SCHEMA = "warehouse"
DIM_TABLE = "dim_amazon_sku_asin_current"


def table_columns(conn, schema: str, table: str) -> set[str]:
    rows = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
    """), {"schema": schema, "table": table}).fetchall()
    return {r[0] for r in rows}


def main():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL environment variable not set")

    engine = create_engine(db_url)

    with engine.begin() as conn:
        # 1) Ensure warehouse schema exists
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_SCHEMA};"))

        # 2) Create DIM table if not exists (superset schema)
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {WAREHOUSE_SCHEMA}.{DIM_TABLE} (
                seller_sku TEXT PRIMARY KEY,
                asin TEXT NOT NULL,
                item_name TEXT NULL,
                fulfillment_channel TEXT NULL,
                status TEXT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """))

        # 3) Discover what staging actually has
        stg_cols = table_columns(conn, STAGING_SCHEMA, STAGING_TABLE)
        required = {"seller_sku", "asin"}
        missing = required - stg_cols
        if missing:
            raise ValueError(f"Staging table missing required columns: {missing}. Found: {sorted(stg_cols)}")

        optional = ["item_name", "fulfillment_channel", "status"]
        present_optional = [c for c in optional if c in stg_cols]

        # Build column lists for SQL
        insert_cols = ["seller_sku", "asin"] + present_optional + ["updated_at"]
        select_cols = ["seller_sku", "asin"] + present_optional + ["NOW()"]

        # Update set clause
        update_sets = ["asin = EXCLUDED.asin"]
        for c in present_optional:
            update_sets.append(f"{c} = EXCLUDED.{c}")
        update_sets.append("updated_at = NOW()")

        # Update only if something changed
        change_checks = ["warehouse.{t}.asin IS DISTINCT FROM EXCLUDED.asin".format(t=DIM_TABLE)]
        for c in present_optional:
            change_checks.append(f"warehouse.{DIM_TABLE}.{c} IS DISTINCT FROM EXCLUDED.{c}")

        sql = f"""
            INSERT INTO {WAREHOUSE_SCHEMA}.{DIM_TABLE}
                ({", ".join(insert_cols)})
            SELECT
                {", ".join(select_cols)}
            FROM {STAGING_SCHEMA}.{STAGING_TABLE}
            ON CONFLICT (seller_sku)
            DO UPDATE SET
                {", ".join(update_sets)}
            WHERE
                {" OR ".join(change_checks)};
        """

        conn.execute(text(sql))

    print(f"✓ DIM table built/updated: {WAREHOUSE_SCHEMA}.{DIM_TABLE}")


if __name__ == "__main__":
    main()
