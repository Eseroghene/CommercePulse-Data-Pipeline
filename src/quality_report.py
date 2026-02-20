import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from transformer import fetch_events, normalize_orders, normalize_payments, normalize_refunds

def run_quality_report():
    
    output_lines = []  
    
    def print_and_save(text=""):
        """Print to console and save to list"""
        print(text)
        output_lines.append(text)
    
    print_and_save("=" * 60)
    print_and_save("DATA QUALITY REPORT")
    print_and_save("=" * 60)
    print_and_save()
    
    # Fetching and normalizing data
    print_and_save("Fetching data from MongoDB...")
    ORDER_TYPES = ["historical_order", "order_created", "order_updated"]
    PAYMENT_TYPES = ["historical_payment", "payment_attempt", "payment_confirmed"]
    REFUND_TYPES = ["historical_refund", "refund_created", "refund_processed"]
    
    orders_df = normalize_orders(fetch_events(ORDER_TYPES))
    payments_df = normalize_payments(fetch_events(PAYMENT_TYPES))
    refunds_df = normalize_refunds(fetch_events(REFUND_TYPES))
    
    print_and_save(f"  Orders: {len(orders_df)} | Payments: {len(payments_df)} | Refunds: {len(refunds_df)}")
    print_and_save()
    
    # Initialize report
    report = {
        "report_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "total_orders": len(orders_df),
        "total_payments": len(payments_df),
        "total_refunds": len(refunds_df)
    }
    
    # COMPLETENESS CHECKS
    print_and_save("1. DATA COMPLETENESS")
    print_and_save("-" * 60)
    
    report["orders_missing_customer_id"] = int(orders_df["customer_id"].isna().sum())
    report["orders_missing_amount"] = int((orders_df["order_amount"] == 0).sum())
    report["payments_missing_order_id"] = int(payments_df["order_id"].isna().sum())
    report["refunds_missing_payment_id"] = int(refunds_df["payment_id"].isna().sum())
    
    print_and_save(f"  Orders missing customer_id:    {report['orders_missing_customer_id']}")
    print_and_save(f"  Orders with zero amount:       {report['orders_missing_amount']}")
    print_and_save(f"  Payments missing order_id:     {report['payments_missing_order_id']}")
    print_and_save(f"  Refunds missing payment_id:    {report['refunds_missing_payment_id']}")
    print_and_save()
    
    # ORPHAN RECORDS 
    print_and_save("2. ORPHAN RECORDS")
    print_and_save("-" * 60)
    
    # Payments with no matching order
    orphan_payments = ~payments_df["order_id"].isin(orders_df["order_id"])
    report["orphan_payments"] = int(orphan_payments.sum())
    
    # Refunds with no matching payment
    orphan_refunds = ~refunds_df["payment_id"].isin(payments_df["payment_id"])
    report["orphan_refunds"] = int(orphan_refunds.sum())
    
    print_and_save(f"  Payments without matching order:  {report['orphan_payments']}")
    print_and_save(f"  Refunds without matching payment: {report['orphan_refunds']}")
    print_and_save()
    
    # LATE ARRIVALS 
    print_and_save("3. LATE ARRIVAL DETECTION")
    print_and_save("-" * 60)
    
    # Join orders with payments to check timing
    merged = orders_df[["order_id", "created_at"]].merge(
        payments_df[["order_id", "payment_date"]], 
        on="order_id", 
        how="inner"
    )
    
    if not merged.empty:
        merged["days_to_payment"] = (
            (merged["payment_date"] - merged["created_at"]).dt.total_seconds() / 86400
        )
        
        report["payments_over_7_days"] = int((merged["days_to_payment"] > 7).sum())
        report["payments_over_30_days"] = int((merged["days_to_payment"] > 30).sum())
        report["avg_days_to_payment"] = round(float(merged["days_to_payment"].mean()), 2)
    else:
        report["payments_over_7_days"] = 0
        report["payments_over_30_days"] = 0
        report["avg_days_to_payment"] = 0
    
    print_and_save(f"  Payments arriving > 7 days after order:  {report['payments_over_7_days']}")
    print_and_save(f"  Payments arriving > 30 days after order: {report['payments_over_30_days']}")
    print_and_save(f"  Average days from order to payment:      {report['avg_days_to_payment']}")
    print_and_save()
    
    #  REVENUE INTEGRITY 
    print_and_save("4. REVENUE INTEGRITY")
    print_and_save("-" * 60)
    
    successful_payments = payments_df[payments_df["payment_status"] == "success"]
    report["gross_revenue"] = round(float(successful_payments["payment_amount"].sum()), 2)
    report["total_refunded"] = round(float(refunds_df["refund_amount"].sum()), 2)
    report["net_revenue"] = round(report["gross_revenue"] - report["total_refunded"], 2)
    
    # Payment success rate
    total_payments = len(payments_df)
    successful_count = len(successful_payments)
    report["payment_success_rate"] = round(successful_count / total_payments, 4) if total_payments > 0 else 0
    
    # Refund rate
    report["refund_rate"] = round(
        report["total_refunded"] / report["gross_revenue"], 4
    ) if report["gross_revenue"] > 0 else 0
    
    print_and_save(f"  Gross Revenue:          ${report['gross_revenue']:,.2f}")
    print_and_save(f"  Total Refunded:         ${report['total_refunded']:,.2f}")
    print_and_save(f"  Net Revenue:            ${report['net_revenue']:,.2f}")
    print_and_save(f"  Payment Success Rate:   {report['payment_success_rate']*100:.2f}%")
    print_and_save(f"  Refund Rate:            {report['refund_rate']*100:.2f}%")
    print_and_save()
    
    #  PAYMENT STATUS BREAKDOWN 
    print_and_save("5. PAYMENT STATUS BREAKDOWN")
    print_and_save("-" * 60)
    
    status_counts = payments_df["payment_status"].value_counts()
    for status, count in status_counts.items():
        pct = (count / len(payments_df)) * 100
        print_and_save(f"  {status:15} {count:5} ({pct:5.1f}%)")
    print_and_save()
    
    #  VENDOR BREAKDOWN 
    print_and_save("6. VENDOR BREAKDOWN")
    print_and_save("-" * 60)
    
    vendor_counts = orders_df["vendor"].value_counts()
    for vendor, count in vendor_counts.items():
        print_and_save(f"  {vendor:15} {count:5} orders")
    print_and_save()
    
    #  SAVE REPORT 
    Path("reports").mkdir(exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Save as CSV
    report_df = pd.DataFrame([report])
    csv_path = f"reports/quality_report_{today}.csv"
    report_df.to_csv(csv_path, index=False)
    
    # Save formatted text report
    txt_path = f"reports/quality_report_{today}.txt"
    with open(txt_path, "w") as f:
        f.write("\n".join(output_lines))
    
    print_and_save("=" * 60)
    print_and_save(f"✓ Report saved to {csv_path}")
    print_and_save(f"✓ Report saved to {txt_path}")
    print_and_save("=" * 60)
    
    return report


if __name__ == "__main__":
    run_quality_report()
