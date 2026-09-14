from __future__ import annotations

from pathlib import Path
import re
import sqlite3

import pandas as pd
import pendulum
from airflow.sdk import dag, task


BASE_DIR = Path("/opt/airflow")

RAW_FILE = BASE_DIR / "data" / "raw" / "order_history.csv"
INTERMEDIATE_FILE = BASE_DIR / "data" / "intermediate" / "cleaned_orders.csv"

DATABASE_FILE = BASE_DIR / "data" / "food_delivery.db"

SELECTED_COLUMNS = [
    "Restaurant ID",
    "Restaurant name",
    "Subzone",
    "City",
    "Order ID",
    "Order Placed At",
    "Order Status",
    "Delivery",
    "Distance",
    "Items in order",
    "Bill subtotal",
    "Packaging charges",
    "Restaurant discount (Promo)",
    "Gold discount",
    "Brand pack discount",
    "Total",
    "Rating",
    "KPT duration (minutes)",
    "Rider wait time (minutes)",
    "Customer ID",
]


@dag(
    dag_id="sales_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["sales", "learning", "food-delivery", "etl"],
)
def sales_pipeline():
    @task
    def extract() -> str:
        print("Extracting data from raw CSV file...")

        if not RAW_FILE.exists():
            raise FileNotFoundError(f"Dataset not found: {RAW_FILE}")

        df = pd.read_csv(RAW_FILE)
        print(f"Extracted {len(df)} rows and {len(df.columns)} columns")

        extracted_file = BASE_DIR / "data" / "intermediate" / "extracted_orders.csv"
        extracted_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(extracted_file, index=False)

        return str(extracted_file)

    @task
    def transform(extracted_file: str) -> str:
        df = pd.read_csv(extracted_file)

        missing_columns = [
            col for col in SELECTED_COLUMNS if col not in df.columns
        ]
        if missing_columns:
            raise ValueError(f"Missing expected columns: {missing_columns}")

        df = df[SELECTED_COLUMNS].copy()

        df = df.drop_duplicates()

        df = df.drop_duplicates(subset=["Order ID"], keep="first")

        text_columns = [
            "Restaurant name",
            "Subzone",
            "City",
            "Order Status",
            "Delivery",
            "Items in order",
        ]

        for col in text_columns:
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else x)

        df["Order Placed At"] = pd.to_datetime(df["Order Placed At"], errors="coerce")
        df["order_date"] = df["Order Placed At"].dt.date
        df["order_month"] = df["Order Placed At"].dt.to_period("M").astype("string")

        def clean_distance(value):
            if pd.isna(value):
                return None

            value = str(value).strip().lower()

            if value == "<1km":
                return 0.5

            match = re.search(r"([\d.]+)", value)

            if match:
                return float(match.group(1))
            return None

        df["distance_km"] = df["Distance"].apply(clean_distance)

        numeric_columns = [
            "Bill subtotal",
            "Packaging charges",
            "Restaurant discount (Promo)",
            "Gold discount",
            "Brand pack discount",
            "Total",
            "Rating",
            "KPT duration (minutes)",
            "Rider wait time (minutes)",
        ]

        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.rename(
            columns={
                "Restaurant ID": "restaurant_id",
                "Restaurant name": "restaurant_name",
                "Subzone": "subzone",
                "City": "city",
                "Order ID": "order_id",
                "Order Placed At": "order_placed_at",
                "Order Status": "order_status",
                "Delivery": "delivery_type",
                "Distance": "distance_raw",
                "Items in order": "items_in_order",
                "Bill subtotal": "bill_subtotal",
                "Packaging charges": "packaging_charges",
                "Restaurant discount (Promo)": "promo_discount",
                "Gold discount": "gold_discount",
                "Brand pack discount": "brand_pack_discount",
                "Total": "total",
                "Rating": "rating",
                "KPT duration (minutes)": "kpt_duration_minutes",
                "Rider wait time (minutes)": "rider_wait_minutes",
                "Customer ID": "customer_id",
            }
        )

        required_columns = [
            "order_id",
            "restaurant_id",
            "customer_id",
            "order_placed_at",
            "order_status",
            "total",
        ]

        null_counts = df[required_columns].isna().sum()

        for col, count in null_counts.items():
            if count > 0:
                print(f"Warning: {count} null values found in required column '{col}'")

        df = df.dropna(subset=required_columns)

        df["total_pre_delivery_minutes"] = (
            df["kpt_duration_minutes"].fillna(0) + df["rider_wait_minutes"].fillna(0)
        )

        INTERMEDIATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(INTERMEDIATE_FILE, index=False)

        return str(INTERMEDIATE_FILE)

    @task
    def validate(transformed_file: str) -> str:
        transformed_path = Path(transformed_file)

        if not transformed_path.exists():
            raise FileNotFoundError(f"Transformed file not found: {transformed_file}")

        df = pd.read_csv(transformed_file)

        if df.empty:
            raise ValueError("Transformed DataFrame is empty")

        required_columns = [
            "order_id",
            "customer_id",
            "restaurant_id",
            "order_placed_at",
            "order_status",
            "total",
        ]

        missing_columns = [
            column for column in required_columns if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                "Validation failed. Missing columns: " f"{missing_columns}"
            )

        duplicate_order_ids = df["order_id"][df["order_id"].duplicated()]

        if not duplicate_order_ids.empty:
            raise ValueError(
                "Validation failed. Duplicate order IDs found: "
                f"{duplicate_order_ids.tolist()}"
            )

        negative_total_values = df[df["total"] < 0]

        if not negative_total_values.empty:
            raise ValueError(
                "Validation failed. Negative total values found in the following rows: "
                f"{negative_total_values.index.tolist()}"
            )

        critical_nulls = df[required_columns].isna().sum().sum()

        if critical_nulls > 0:
            raise ValueError(
                "Validation failed: " f"{critical_nulls} NULL values " "in critical fields."
            )
        return transformed_file

    @task
    def load(validated_file: str) -> None:
        df = pd.read_csv(validated_file)

        DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DATABASE_FILE)

        try:
            df.to_sql("orders", conn, if_exists="replace", index=False)

            cursor = conn.cursor()

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_orders_customer
                ON orders(customer_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_orders_restaurant
                ON orders(restaurant_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_orders_date
                ON orders(order_date)
                """
            )

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM orders")

            loaded_rows = cursor.fetchone()[0]

            print(f"Rows loaded into SQLite: " f"{loaded_rows}")

            print(f"Database location: " f"{DATABASE_FILE}")

        finally:
            conn.close()

        print("Load completed successfully.")

    extracted_file = extract()
    transformed_file = transform(extracted_file)
    validated_file = validate(transformed_file)
    load(validated_file)


sales_pipeline()
