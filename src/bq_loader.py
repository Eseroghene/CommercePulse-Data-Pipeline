from google.cloud import bigquery
from dotenv import load_dotenv
import os
import pandas as pd
from transformer import run_transformation

load_dotenv()

PROJECT_ID = os.getenv("BQ_PROJECT")
DATASET_ID = os.getenv("BQ_DATASET")

client = bigquery.Client(project=PROJECT_ID)


def create_tables():
    """Create BigQuery tables if they don't exist"""
    print("Creating BigQuery tables...")
    
    tables = {
        "fact_orders": """
            CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_orders` (
              order_id        STRING    NOT NULL,
              customer_id     STRING,
              vendor          STRING,
              order_amount    FLOAT64,
              order_status    STRING,
              created_at      TIMESTAMP,
              event_id        STRING
            )
        """,
        "fact_payments": """
            CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_payments` (
              payment_id      STRING    NOT NULL,
              order_id        STRING,
              vendor          STRING,
              payment_amount  FLOAT64,
              payment_status  STRING,
              payment_method  STRING,
              payment_date    TIMESTAMP,
              event_id        STRING
            )
        """,
        "fact_refunds": """
            CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_refunds` (
              refund_id      STRING    NOT NULL,
              order_id       STRING,
              payment_id     STRING,
              vendor         STRING,
              refund_amount  FLOAT64,
              refund_reason  STRING,
              refund_type    STRING,
              refund_date    TIMESTAMP,
              event_id       STRING
            )
        """,
        "fact_order_daily": """
            CREATE TABLE IF NOT EXISTS `{project}.{dataset}.fact_order_daily` (
              order_date           DATE,
              vendor               STRING,
              gross_revenue        FLOAT64,
              total_refunds        FLOAT64,
              net_revenue          FLOAT64,
              order_count          INT64,
              paid_count           INT64,
              payment_success_rate FLOAT64,
              refund_rate          FLOAT64
            )
        """
    }
    
    for table_name, query in tables.items():
        formatted_query = query.format(project=PROJECT_ID, dataset=DATASET_ID)
        client.query(formatted_query).result()
        print(f"   {table_name}")


def load_dataframe_to_bq(df, table_name, write_disposition="WRITE_TRUNCATE"):
    """Load a DataFrame to BigQuery"""
    if df.empty:
        print(f"   {table_name}: No data to load")
        return
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=False  # Use schema from CREATE TABLE
    )
    
    # Load data
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion
    
    print(f" {table_name}: Loaded {len(df)} rows")


def run_pipeline():
    print("=" * 60)
    print("BIGQUERY DATA LOADER")
    print("=" * 60)
    print()
    
    # Step 1: Creating tables
    create_tables()
    print()
    
    # Step 2: Runing transformation to get DataFrames
    print("Running transformation...")
    orders_df, payments_df, refunds_df, daily_df = run_transformation()
    print()
    
    # Step 3: Preparing DataFrames for BigQuery
    print("Loading data to BigQuery...")
    
    # fact_orders - select only columns that match BQ schema
    orders_bq = orders_df[[
        "order_id", "customer_id", "vendor", "order_amount", 
        "order_status", "created_at", "event_id"
    ]].copy()
    
    # fact_payments
    payments_bq = payments_df[[
        "payment_id", "order_id", "vendor", "payment_amount",
        "payment_status", "payment_method", "payment_date", "event_id"
    ]].copy()
    
    # fact_refunds
    refunds_bq = refunds_df[[
        "refund_id", "order_id", "payment_id", "vendor",
        "refund_amount", "refund_reason", "refund_type", 
        "refund_date", "event_id"
    ]].copy()
    
    # fact_order_daily - already matches schema
    daily_bq = daily_df.copy()
    
    # Loading each table
    load_dataframe_to_bq(orders_bq, "fact_orders", "WRITE_TRUNCATE")
    load_dataframe_to_bq(payments_bq, "fact_payments", "WRITE_TRUNCATE")
    load_dataframe_to_bq(refunds_bq, "fact_refunds", "WRITE_TRUNCATE")
    load_dataframe_to_bq(daily_bq, "fact_order_daily", "WRITE_TRUNCATE")
    
    print()
    print("=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print()
    print("Data loaded to BigQuery:")
    print(f"  Project: {PROJECT_ID}")
    print(f"  Dataset: {DATASET_ID}")
    print()
    print("You can now query your data in BigQuery!")


if __name__ == "__main__":
    run_pipeline()
