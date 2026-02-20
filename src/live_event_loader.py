import json
from pathlib import Path
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import os

load_dotenv()

client     = MongoClient(os.getenv("MONGO_URI"))
db         = client[os.getenv("MONGO_DB")]
collection = db["events_raw"]


def load_live_events(date_str: str):
    """
    Load events from data/live_events/YYYY-MM-DD/events.jsonl
    Upsert by event_id — handles duplicates and replays safely.
    """
    filepath = Path(f"data/live_events/{date_str}/events.jsonl")

    if not filepath.exists():
        print(f"No events file found for {date_str}")
        print(f"Expected: {filepath}")
        return

    ops     = []
    skipped = 0

    print(f"Loading live events from {filepath}...")

    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            if "event_id" not in event:
                skipped += 1
                continue

            # Tag ingestion time without modifying the original payload
            event["ingested_at"] = datetime.now(timezone.utc).isoformat()
            event["source"]      = "live_stream"

            ops.append(
                UpdateOne(
                    {"event_id": event["event_id"]},
                    {"$set": event},
                    upsert=True
                )
            )

    if ops:
        result = collection.bulk_write(ops, ordered=False)
        print(f"\n{date_str} results:")
        print(f"  {result.upserted_count} new events inserted")
        print(f"  {result.modified_count} existing events updated")
        print(f"  {skipped} invalid events skipped")
    else:
        print(f"{date_str}: No valid events to load.")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        # Default to today
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    print(f"Loading live events for {date_str}...")
    load_live_events(date_str)
    
    # Show total event count in MongoDB
    total = collection.count_documents({})
    print(f"\nTotal events in MongoDB: {total}")