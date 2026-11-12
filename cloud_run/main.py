
import os
import json
import base64
import logging
from pathlib import Path

from flask import Flask, request, jsonify
from jsonschema import validate, ValidationError, FormatChecker

from scheduler.scheduled_processor import run_scheduled_notification

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("inventory-processor")

BASE_DIR = Path(__file__).resolve().parent.parent

SCHEMA_FILE = BASE_DIR / "schemas" / "inventory_schema.json"

with open(SCHEMA_FILE, "r") as file:
    INVENTORY_SCHEMA = json.load(file)

# Temporary in-memory inventory store
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

    inventory_key = (product_id, warehouse_id)

    current_quantity = inventory_store.get(inventory_key, 0)

    updated_quantity = current_quantity + quantity_change

    inventory_store[inventory_key] = updated_quantity

    logger.info(
        "Inventory updated | event_id=%s | product_id=%s | "
        "warehouse_id=%s | quantity_change=%s | new_quantity=%s",
        event["event_id"],
        product_id,
        warehouse_id,
        quantity_change,
        updated_quantity
    )


@app.route("/health", methods=["GET"])
def health():

    return jsonify({
        "status": "healthy",
        "service": "inventory-event-processor"
    }), 200


@app.route("/", methods=["POST"])
def receive_pubsub_message():

    try:

        envelope = request.get_json(silent=True)

        if not envelope or "message" not in envelope:
            return jsonify({
                "error": "Invalid Pub/Sub envelope"
            }), 400

        message = envelope["message"]

        encoded_data = message.get("data")

        if not encoded_data:
            return jsonify({
                "error": "Missing message data"
            }), 400

        decoded_data = base64.b64decode(
            encoded_data,
            validate=True
        )

        event = json.loads(decoded_data.decode("utf-8"))

        process_inventory_event(event)

        return "", 204

    except (ValidationError, json.JSONDecodeError, ValueError) as error:

        logger.error("Invalid inventory event: %s", error)

        # Acknowledge invalid messages to avoid endless retries
        return "", 204

    except Exception:

        logger.exception("Inventory processing failed")

        # Pub/Sub retries messages after a non-success response
        return "", 500


@app.route("/scheduled", methods=["POST"])
def scheduled_notification():

    try:

        result = run_scheduled_notification()

        return jsonify(result), 200

    except Exception:

        logger.exception("Scheduled notification failed")

        return jsonify({
            "status": "FAILED",
            "message": "Inventory notification failed"
        }), 500


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080))
    )
