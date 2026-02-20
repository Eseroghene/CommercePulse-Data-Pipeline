import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB  = os.getenv("MONGO_DB")

client     = MongoClient(MONGO_URI)
db         = client[MONGO_DB]
collection = db["events_raw"]

# Ensuring event_id is always unique
collection.create_index("event_id", unique=True)


def make_event_id(event_type: str, natural_key: str) -> str:
    raw = f"{event_type}:{natural_key}"
    return hashlib.sha256(raw.encode()).hexdigest()


def extract_natural_key(event_type: str, record: dict) -> str:
    """
    Pull the best available unique key from a record.
    Falls back to hashing the whole record if no ID field exists.
    """
    key_map = {
        "historical_order":    ["order_id", "id"],
        "historical_payment":  ["payment_id", "id", "transaction_id"],
        "historical_shipment": ["shipment_id", "id", "tracking_id"],
        "historical_refund":   ["refund_id", "id"],
    }
    candidates = key_map.get(event_type, [])
    for field in candidates:
        if field in record and record[field]:
            return str(record[field])
    
    return hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest()


def extract_event_time(event_type: str, record: dict) -> str:
    """Try common timestamp fields; fall back to epoch start."""
    time_fields = ["created_at", "order_date", "payment_date",
                   "shipped_at",  "refund_date", "timestamp", "date"]
    for field in time_fields:
        if field in record and record[field]:
            return str(record[field])
    return "2023-01-01T00:00:00Z"


def extract_vendor(record: dict) -> str:
    for field in ["vendor_id", "vendor", "seller_id", "merchant_id"]:
        if field in record:
            return str(record[field])
    return "unknown"


def wrap_as_event(event_type: str, record: dict) -> dict:
    natural_key = extract_natural_key(event_type, record)
    return {
        "event_id":   make_event_id(event_type, natural_key),
        "event_type": event_type,
        "event_time": extract_event_time(event_type, record),
        "vendor":     extract_vendor(record),
        "payload":    record,                       
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source":     "historical_bootstrap",
    }


FILE_TO_EVENT_TYPE = {
    "orders_2023.json":    "historical_order",
    "payments_2023.json":  "historical_payment",
    "shipments_2023.json": "historical_shipment",
    "refunds_2023.json":   "historical_refund",
}


def load_bootstrap_file(filepath: Path, event_type: str):
    with open(filepath, "r") as f:
        records = json.load(f)

    if not isinstance(records, list):
        records = [records]

    ops = []
    for record in records:
        event = wrap_as_event(event_type, record)
        ops.append(
            UpdateOne(
                {"event_id": event["event_id"]},  
                {"$set": event},                   
                upsert=True
            )
        )

    if ops:
        result = collection.bulk_write(ops, ordered=False)
        print(f"  {filepath.name}: {result.upserted_count} inserted, "
              f"{result.modified_count} updated")


def run_bootstrap():
    bootstrap_dir = Path("data/bootstrap")
    print("Starting historical bootstrap...\n")

    for filename, event_type in FILE_TO_EVENT_TYPE.items():
        filepath = bootstrap_dir / filename
        if filepath.exists():
            print(f"Loading {filename} as [{event_type}]")
            load_bootstrap_file(filepath, event_type)
        else:
            print(f"WARNING: {filename} not found, skipping.")

    print("\nBootstrap complete.")
    total = collection.count_documents({"source": "historical_bootstrap"})
    print(f"Total historical events in MongoDB: {total}")


if __name__ == "__main__":
    run_bootstrap()