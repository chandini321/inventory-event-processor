
import csv
import json
import os
from pathlib import Path

from google.cloud import pubsub_v1

PROJECT_ID = os.getenv(
    "GOOGLE_CLOUD_PROJECT",
    "pioneering-flow-436606-t9"
)

TOPIC_ID = "inventory-topic-practice"

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_FILE = BASE_DIR / "data" / "inventory_adjustments.csv"

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def publish_inventory_events():

    total_published = 0

    with open(CSV_FILE, "r", newline="", encoding="utf-8-sig") as file:

        reader = csv.DictReader(file)

        for row in reader:

            event = {
                "event_id": row["event_id"],
                "product_id": row["product_id"],
                "warehouse_id": row["warehouse_id"],
                "quantity_change": int(row["quantity_change"]),
                "reason": row["reason"],
                "event_timestamp": row["event_timestamp"]
            }

            message_data = json.dumps(event).encode("utf-8")

            future = publisher.publish(
                topic_path,
                message_data,
                event_id=event["event_id"],
                message_type="INVENTORY_JSON"
            )

            message_id = future.result()

            total_published += 1

            print(
                f"Published event: {event['event_id']} "
                f"| Message ID: {message_id}"
            )

    print(
        f"Publishing completed. "
        f"Total events: {total_published}"
    )


if __name__ == "__main__":
    publish_inventory_events()
