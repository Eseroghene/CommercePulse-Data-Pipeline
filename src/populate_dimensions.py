
# Populate dimension tables and save to warehouse folder

from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import os
from pathlib import Path

load_dotenv()

PROJECT_ID = os.getenv("BQ_PROJECT")
DATASET_ID = os.getenv("BQ_DATASET")
client = bigquery.Client(project=PROJECT_ID)

#Generate date dimension for 2023-2026
def populate_dim_date():
    
    dates = pd.date_range(start='2023-01-01', end='2026-12-31', freq='D')
    
    dim_date = pd.DataFrame({
        'date_key': dates,
        'day_of_week': dates.day_name(),
        'week_number': dates.isocalendar().week,
        'month': dates.month,
        'quarter': dates.quarter,
        'year': dates.year,
        'is_weekend': dates.dayofweek.isin([5, 6])
    })
    
    # Load to BigQuery
    table_id = f"{PROJECT_ID}.{DATASET_ID}.dim_date"
    job = client.load_table_from_dataframe(dim_date, table_id)
    job.result()
    
    print(f"  ✓ dim_date: {len(dim_date)} rows loaded to BigQuery")
    
    return dim_date


def populate_dim_customer():
    """Extract unique customers from transformer output"""
    # Run transformer to get fresh data
    from transformer import run_transformation
    
    print("  → Running transformation to extract customers...")
    orders_df, _, _, _ = run_transformation()
    
    # Extract unique customers
    customers = orders_df[orders_df['customer_id'].notna()][['customer_id', 'created_at']].copy()
    customers = customers.groupby('customer_id').agg({'created_at': 'min'}).reset_index()
    customers['customer_name'] = None
    customers['email'] = None
    customers['country'] = None
    
    if not customers.empty:
        # Load to BigQuery
        table_id = f"{PROJECT_ID}.{DATASET_ID}.dim_customer"
        job = client.load_table_from_dataframe(customers, table_id)
        job.result()
        print(f"  ✓ dim_customer: {len(customers)} rows loaded to BigQuery")
    else:
        print("  ⚠ dim_customer: No data (all customer_ids are NULL)")
    
    return customers


def populate_dim_product():
    """Create placeholder product dimension"""
    dim_product = pd.DataFrame([{
        'product_id': 'UNKNOWN',
        'product_name': 'Product data not available',
        'category': 'N/A',
        'vendor_id': None,
        'unit_price': 0.0
    }])
    
    # Load to BigQuery
    table_id = f"{PROJECT_ID}.{DATASET_ID}.dim_product"
    job = client.load_table_from_dataframe(dim_product, table_id)
    job.result()
    
    print(f"  ✓ dim_product: {len(dim_product)} rows loaded to BigQuery (placeholder)")
    
    return dim_product


def save_to_warehouse(dim_date, dim_customer, dim_product):
    """Save dimension tables to warehouse folder as CSV"""
    warehouse_dir = Path("warehouse/dimensions")
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    
    print("\nSaving dimension tables to warehouse folder...")
    
    dim_date.to_csv(warehouse_dir / "dim_date.csv", index=False)
    print(f"  ✓ dim_date.csv ({len(dim_date)} rows)")
    
    if not dim_customer.empty:
        dim_customer.to_csv(warehouse_dir / "dim_customer.csv", index=False)
        print(f"  ✓ dim_customer.csv ({len(dim_customer)} rows)")
    
    dim_product.to_csv(warehouse_dir / "dim_product.csv", index=False)
    print(f"  ✓ dim_product.csv ({len(dim_product)} rows)")
    
    print(f"\nDimension tables saved to: {warehouse_dir.absolute()}")


if __name__ == "__main__":
    print("=" * 60)
    print("DIMENSION TABLE POPULATION")
    print("=" * 60)
    print()
    print("Loading to BigQuery...")
    
    dim_date = populate_dim_date()
    dim_customer = populate_dim_customer()
    dim_product = populate_dim_product()
    
    save_to_warehouse(dim_date, dim_customer, dim_product)
    
    print()
    print("=" * 60)
    print("✓ All dimension tables populated")
    print("=" * 60)
