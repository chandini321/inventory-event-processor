
import json
import os
from pathlib import Path

from google.cloud import pubsub_v1
from jsonschema import validate, ValidationError, FormatChecker

PROJECT_ID = os.getenv(
    "GOOGLE_CLOUD_PROJECT",
    "pioneering-flow-436606-t9"
)

SUBSCRIPTION_ID = "inventory-sub"

BASE_DIR = Path(__file__).resolve().parent.parent

SCHEMA_FILE = BASE_DIR / "schemas" / "inventory_schema.json"

with open(SCHEMA_FILE, "r") as file:
    INVENTORY_SCHEMA = json.load(file)

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(
    PROJECT_ID,
    SUBSCRIPTION_ID
)

inventory_store = {}


def process_inventory_event(event):

    validate(
        instance=event,
        schema=INVENTORY_SCHEMA,
        format_checker=FormatChecker()
    )

    product_id = event["product_id"]
    warehouse_id = event["warehouse_id"]
    quantity_change = event["quantity_change"]

    key = (product_id, warehouse_id)

    current_quantity = inventory_store.get(key, 0)

    updated_quantity = current_quantity + quantity_change

    inventory_store[key] = updated_quantity

    print(
        f"Inventory updated | "
        f"Product: {product_id} | "
        f"Warehouse: {warehouse_id} | "
        f"Change: {quantity_change} | "
        f"New quantity: {updated_quantity}"
    )


def callback(message):

    try:

        event = json.loads(message.data.decode("utf-8"))

        process_inventory_event(event)

        message.ack()

    except ValidationError as error:

        print(f"Invalid inventory event: {error.message}")

        message.ack()

    except Exception as error:

        print(f"Processing error: {error}")

        message.nack()


def main():

    print("Inventory subscriber started...")

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback
    )

    try:
        streaming_pull_future.result()

    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Subscriber stopped.")


if __name__ == "__main__":
    main()
