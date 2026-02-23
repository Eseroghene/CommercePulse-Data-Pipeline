

import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB")]


def fetch_events(event_types):
    return list(db["events_raw"].find({"event_type": {"$in": event_types}}, {"_id": 0}))


def normalize_orders(events):
    if not events:
        return pd.DataFrame()
    
    rows = []
    for event in events:
        payload = event.get("payload", {})
        
        rows.append({
            "order_id": payload.get("order_id"),
            "customer_id": payload.get("customerId"),
            "order_amount": float(payload.get("totalAmount", 0)),
            "order_status": payload.get("state"),
            "created_at": pd.to_datetime(payload.get("created_at"), utc=True, errors="coerce"),
            "event_id": event.get("event_id"),
            "vendor": event.get("vendor"),
            "event_type": event.get("event_type")
        })
    
    df = pd.DataFrame(rows)
    df = df.sort_values("created_at", na_position="first").drop_duplicates(subset=["order_id"], keep="last")
    return df


def normalize_payments(events):
    if not events:
        return pd.DataFrame()
    
    rows = []
    for event in events:
        payload = event.get("payload", {})
        
        payment_id = (payload.get("transaction_id") or 
                     payload.get("payment_id") or 
                     payload.get("id") or 
                     payload.get("paymentId"))
        
        order_id = payload.get("order_id") or payload.get("orderId")
        
        amount = (payload.get("amountPaid") or 
                 payload.get("amount") or 
                 payload.get("payment_amount") or 
                 payload.get("totalAmount"))
        
        status = payload.get("payment_status") or payload.get("status") or payload.get("state")
        
        if status:
            status = status.lower()
            if status in ["failed", "fail", "error"]:
                status = "failed"
            elif status in ["success", "successful", "completed", "paid"]:
                status = "success"
        
        method = payload.get("channel") or payload.get("method") or payload.get("payment_method")
        date = payload.get("paid_at") or payload.get("payment_date") or payload.get("created_at")
        
        rows.append({
            "payment_id": payment_id,
            "order_id": order_id,
            "payment_amount": float(amount) if amount else 0.0,
            "payment_status": status,
            "payment_method": method,
            "payment_date": pd.to_datetime(date, utc=True, errors="coerce"),
            "event_id": event.get("event_id"),
            "vendor": event.get("vendor")
        })
    
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["payment_id"])
    return df


def normalize_refunds(events):
    if not events:
        return pd.DataFrame()
    
    rows = []
    for event in events:
        payload = event.get("payload", {})
        
        refund_id = payload.get("refund_id") or payload.get("id") or payload.get("transaction_id")
        order_id = payload.get("order_id") or payload.get("orderId")
        payment_id = payload.get("payment_id") or payload.get("paymentId") or payload.get("transaction_id")
        
        amount = (payload.get("amountRefunded") or 
                 payload.get("amount") or 
                 payload.get("refund_amount") or 
                 payload.get("totalAmount"))
        
        reason = payload.get("reason") or payload.get("refund_reason")
        refund_type = payload.get("type") or payload.get("refund_type")
        date = payload.get("refunded_at") or payload.get("refund_date") or payload.get("created_at")
        
        rows.append({
            "refund_id": refund_id,
            "order_id": order_id,
            "payment_id": payment_id,
            "refund_amount": float(amount) if amount else 0.0,
            "refund_reason": reason,
            "refund_type": refund_type,
            "refund_date": pd.to_datetime(date, utc=True, errors="coerce"),
            "event_id": event.get("event_id"),
            "vendor": event.get("vendor")
        })
    
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["refund_id"])
    return df


def build_fact_order_daily(orders_df, payments_df, refunds_df):
    if orders_df.empty:
        return pd.DataFrame()
    
    results = []
    orders_df["order_date"] = orders_df["created_at"].dt.date
    
    for (date, vendor), group in orders_df.groupby(["order_date", "vendor"]):
        order_ids = group["order_id"].tolist()
        
        payments_subset = payments_df[payments_df["order_id"].isin(order_ids)]
        gross_revenue = payments_subset["payment_amount"].sum()
        paid_count = int((payments_subset["payment_status"] == "success").sum())
        
        refunds_subset = refunds_df[refunds_df["order_id"].isin(order_ids)]
        total_refunds = refunds_subset["refund_amount"].sum()
        
        net_revenue = gross_revenue - total_refunds
        order_count = len(order_ids)
        
        payment_success_rate = round(paid_count / order_count, 4) if order_count > 0 else None
        refund_rate = round(total_refunds / gross_revenue, 4) if gross_revenue > 0 else None
        
        results.append({
            "order_date": date,
            "vendor": vendor,
            "gross_revenue": float(gross_revenue),
            "total_refunds": float(total_refunds),
            "net_revenue": float(net_revenue),
            "order_count": order_count,
            "paid_count": paid_count,
            "payment_success_rate": payment_success_rate,
            "refund_rate": refund_rate
        })
    
    return pd.DataFrame(results)


def save_dataframes(orders_df, payments_df, refunds_df, daily_df):
    """Save transformed DataFrames to CSV files"""
    output_dir = Path("reports/transformed_data")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("\nSaving transformed data to CSV...")
    
    orders_df.to_csv(output_dir / "fact_orders.csv", index=False)
    print(f"  ✓ fact_orders.csv saved ({len(orders_df)} rows)")
    
    payments_df.to_csv(output_dir / "fact_payments.csv", index=False)
    print(f"  ✓ fact_payments.csv saved ({len(payments_df)} rows)")
    
    refunds_df.to_csv(output_dir / "fact_refunds.csv", index=False)
    print(f"  ✓ fact_refunds.csv saved ({len(refunds_df)} rows)")
    
    daily_df.to_csv(output_dir / "fact_order_daily.csv", index=False)
    print(f"  ✓ fact_order_daily.csv saved ({len(daily_df)} rows)")
    
    print(f"\nFiles saved to: {output_dir.absolute()}")


def run_transformation():
    print("Fetching events from MongoDB...")
    ORDER_TYPES = ["historical_order", "order_created", "order_updated"]
    PAYMENT_TYPES = ["historical_payment", "payment_attempt", "payment_confirmed"]
    REFUND_TYPES = ["historical_refund", "refund_created", "refund_processed"]

    raw_orders = fetch_events(ORDER_TYPES)
    raw_payments = fetch_events(PAYMENT_TYPES)
    raw_refunds = fetch_events(REFUND_TYPES)

    print(f"  Orders: {len(raw_orders)} | Payments: {len(raw_payments)} | Refunds: {len(raw_refunds)}")
    print("\nNormalizing schemas...")
    
    orders_df = normalize_orders(raw_orders)
    payments_df = normalize_payments(raw_payments)
    refunds_df = normalize_refunds(raw_refunds)
    
    print("\nBuilding daily aggregates...")
    daily_df = build_fact_order_daily(orders_df, payments_df, refunds_df)

    print(f"\nTransformation complete.")
    print(f"  fact_orders rows:       {len(orders_df)}")
    print(f"  fact_payments rows:     {len(payments_df)}")
    print(f"  fact_refunds rows:      {len(refunds_df)}")
    print(f"  fact_order_daily rows:  {len(daily_df)}")
    
    print("\nSample daily aggregates:")
    print(daily_df.head(10))
    
    print("\nRevenue summary:")
    print(f"  Total gross revenue: ${daily_df['gross_revenue'].sum():,.2f}")
    print(f"  Total refunds: ${daily_df['total_refunds'].sum():,.2f}")
    print(f"  Total net revenue: ${daily_df['net_revenue'].sum():,.2f}")
    
    # Save to CSV files
    save_dataframes(orders_df, payments_df, refunds_df, daily_df)

    return orders_df, payments_df, refunds_df, daily_df

# save transformed dataframes to warehouse folder
def save_dataframes(orders_df, payments_df, refunds_df, daily_df):
    warehouse_dir = Path("warehouse/facts")
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    
    print("\nSaving fact tables to warehouse...")
    
    orders_df.to_csv(warehouse_dir / "fact_orders.csv", index=False)
    print(f"  ✓ fact_orders.csv ({len(orders_df)} rows)")
    
    payments_df.to_csv(warehouse_dir / "fact_payments.csv", index=False)
    print(f"  ✓ fact_payments.csv ({len(payments_df)} rows)")
    
    refunds_df.to_csv(warehouse_dir / "fact_refunds.csv", index=False)
    print(f"  ✓ fact_refunds.csv ({len(refunds_df)} rows)")
    
    daily_df.to_csv(warehouse_dir / "fact_order_daily.csv", index=False)
    print(f"  ✓ fact_order_daily.csv ({len(daily_df)} rows)")
    
    print(f"\nFact tables saved to: {warehouse_dir.absolute()}")



if __name__ == "__main__":
    run_transformation()
