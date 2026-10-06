import os
import json
import time
import signal
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

# Configure structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [KitchenWorker] %(message)s")
logger = logging.getLogger("kitchen-worker")

# Configuration via environment variables
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "kitchen-workers")
COOKING_SIMULATION_SECONDS = float(os.getenv("COOKING_SIMULATION_SECONDS", "2.0"))

# Global flags for graceful shutdown
running = True


def handle_shutdown_signals():
    """Register signal handlers for graceful shutdown in Docker/Kubernetes."""
    global running

    def _signal_handler(sig, frame):
        global running
        logger.info(f"Received termination signal ({sig}). Shutting down worker gracefully...")
        running = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)


async def connect_to_mongodb() -> MongoClient:
    """
    Connect to MongoDB with exponential retry loop.
    Prevents worker from crashing if MongoDB is still booting.
    """
    logger.info(f"Connecting to MongoDB at {MONGO_URI}...")
    delay = 2
    max_delay = 10
    while running:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            logger.info("Successfully connected to MongoDB.")
            return client
        except Exception as err:
            logger.warning(f"MongoDB not ready yet ({err}). Retrying in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)
    raise RuntimeError("Shutdown requested before MongoDB connection succeeded.")


async def connect_to_kafka_consumer() -> AIOKafkaConsumer:
    """
    Connect to Kafka with exponential retry loop.
    Manual offset commit (enable_auto_commit=False) to guarantee zero message loss.
    """
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} (Group: {CONSUMER_GROUP})...")
    delay = 2
    max_delay = 10
    while running:
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # Critical: manual commit after DB write
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            await consumer.start()
            logger.info(f"Successfully joined Kafka topic '{KAFKA_TOPIC}'.")
            return consumer
        except Exception as err:
            logger.warning(f"Kafka not ready yet ({err}). Retrying in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)
    raise RuntimeError("Shutdown requested before Kafka connection succeeded.")


async def process_order(db, order_event: dict) -> bool:
    """
    Idempotent Order Processor:
    1. Checks if order was already finished ('ready'). If yes, skips it.
    2. Updates status to 'preparing'.
    3. Simulates kitchen prep time.
    4. Updates status to 'ready'.
    Returns True if update succeeded, False otherwise.
    """
    order_id = order_event.get("order_id")
    if not order_id:
        logger.error(f"Malformed order event received (missing order_id): {order_event}")
        return True  # Acknowledge malformed message to avoid blocking queue

    orders_col = db["orders"]

    # 1. Idempotency Check: Fetch current order document
    try:
        current_order = orders_col.find_one({"_id": order_id})
    except PyMongoError as err:
        logger.error(f"Failed to query order {order_id} from MongoDB: {err}")
        return False

    if not current_order:
        logger.warning(f"Order {order_id} not found in MongoDB. Skipping.")
        return True

    # If already cooked and ready, do NOT re-process (Idempotency!)
    if current_order.get("status") == "ready":
        logger.info(f"[IDEMPOTENT SKIP] Order {order_id} is already in 'ready' status. Skipping duplicate message.")
        return True

    now = datetime.now(timezone.utc).isoformat()

    # 2. Transition: received -> preparing
    try:
        logger.info(f"👨‍🍳 [PREPARING] Starting preparation for order {order_id} ({len(order_event.get('items', []))} items)")
        orders_col.update_one(
            {"_id": order_id},
            {"$set": {"status": "preparing", "preparing_at": now, "updated_at": now}}
        )
    except PyMongoError as err:
        logger.error(f"Failed to set status 'preparing' for order {order_id}: {err}")
        return False

    # 3. Simulate Kitchen Cooking Duration
    await asyncio.sleep(COOKING_SIMULATION_SECONDS)

    # 4. Transition: preparing -> ready
    now_ready = datetime.now(timezone.utc).isoformat()
    try:
        orders_col.update_one(
            {"_id": order_id},
            {"$set": {"status": "ready", "ready_at": now_ready, "updated_at": now_ready}}
        )
        logger.info(f"✅ [READY] Order {order_id} is completed and ready for pickup!")
        return True
    except PyMongoError as err:
        logger.error(f"Failed to set status 'ready' for order {order_id}: {err}")
        return False


async def run_worker():
    """Main worker loop."""
    handle_shutdown_signals()

    mongo_client = await connect_to_mongodb()
    db = mongo_client["outlet_db"]

    consumer = await connect_to_kafka_consumer()

    logger.info("Kitchen Worker loop started. Listening for incoming orders...")
    try:
        while running:
            # Poll for messages with a timeout so we can periodically check the 'running' flag
            message_batch = await consumer.getmany(timeout_ms=1000, max_records=5)

            for tp, messages in message_batch.items():
                for message in messages:
                    order_data = message.value
                    logger.info(f"Received message from partition {message.partition}, offset {message.offset}")

                    # Process the order idempotently
                    success = await process_order(db, order_data)

                    if success:
                        # CRITICAL: Commit offset ONLY after MongoDB write succeeds
                        await consumer.commit()
                        logger.info(f"Offset {message.offset} committed for order {order_data.get('order_id')}")
                    else:
                        logger.warning(f"Processing failed for order {order_data.get('order_id')}. Will retry on next poll.")
                        # Do not commit offset, so Kafka redelivers it
                        await asyncio.sleep(2)
                        break

    except asyncio.CancelledError:
        logger.info("Worker task cancelled.")
    finally:
        logger.info("Closing Kafka consumer and MongoDB connections...")
        await consumer.stop()
        mongo_client.close()
        logger.info("Worker shutdown complete.")


if __name__ == "__main__":
    asyncio.run(run_worker())
