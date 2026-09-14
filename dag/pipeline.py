from __future__ import annotations
from pathlib import Path
import re
import sqlite3
import pandas as pd
import pendulum
from airflow.sdk import dag, task


BASE_DIR = Path("opt/airflow")

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
    dag_id = "sales_pipeline",
    schedule = None,
    start_date = pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup = False,
    tags = ["sales", "learning", "food delivery", "etl"],
)

def sales_pipeline():
    #extract
    @task
    def extract() -> str:
        print("Extracting data from raw CSV file...")
        
        if not RAW_FILE.exists():
            raise FileNotFoundError(f"Dataset not found: {RAW_FILE}")
        
        df = pd.read_csv(RAW_FILE)
        print(f"Extracter {len(df)} rows and {len(df.columns)}")
        
        extracted_file = BASE_DIR / "data" / "intermediate" / "extracted_orders.csv"
        
        df.to_csv(extracted_file, index=False)
        
        return str(extracted_file)
    
    #transform
    @task
    def transform(extracted_file: str) -> str:
        df = pd.read_csv(extracted_file)
        
        missing_columns = [col
                           for col in SELECTED_COLUMNS
                           if col not in df.columns
                           ]
        
        df = df[SELECTED_COLUMNS].copy()
        
        df = df.drop_duplicates()
        
        df = df.drop_duplicates(subset = ["Order ID"], keep = "first")
        
        text_columns = [
            "Restaurant name",
            "Subzone",
            "City",
            "Order Status",
            "Delivery",
            "Items in order",
        ]
        
        for col in text_columns:
            df[col] = df[col].astype(str).str.strip()
            
        df["Order Placed At"] = pd.to_datetime(df["Order Placed At"], errors = "coerce")
        df["order_date"] = df["Order Placed At"].dt.date
        df["order_month"] = df["Order Placed At"].dt.to_period("M").astype("string")
        
        
        