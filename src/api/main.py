import os
import json
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import List, Optional
import uuid

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import redis
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

# Configure structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("order-api")

# Configuration via environment variables (with sensible local defaults)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", "60"))

# Global connection handles
mongo_client: Optional[MongoClient] = None
db = None
redis_client: Optional[redis.Redis] = None
kafka_producer: Optional[AIOKafkaProducer] = None

# Initial Menu Seed Data (simulating QSR outlet menu)
DEFAULT_MENU = [
    {"item_id": "burger-01", "name": "Classic Zinger Burger", "price": 5.99, "category": "Burgers"},
    {"item_id": "pizza-01", "name": "Pepperoni Feast Pizza", "price": 8.99, "category": "Pizzas"},
    {"item_id": "fries-01", "name": "Crispy French Fries", "price": 2.49, "category": "Sides"},
    {"item_id": "drink-01", "name": "Soft Drink (Large)", "price": 1.99, "category": "Beverages"},
]


def seed_menu_if_empty():
    """Seeds the restaurant menu in MongoDB if it does not already exist."""
    try:
        menu_collection = db["menu"]
        if menu_collection.count_documents({}) == 0:
            logger.info("Menu collection is empty. Seeding initial menu items...")
            menu_collection.insert_many(DEFAULT_MENU)
            logger.info("Menu successfully seeded.")
        else:
            logger.info("Menu collection already contains items. Skipping seed.")
    except Exception as e:
        logger.error(f"Failed to seed menu in MongoDB: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI Lifespan Context Manager:
    Initializes connections on startup and gracefully tears them down on shutdown.
    """
    global mongo_client, db, redis_client, kafka_producer
    
    # 1. Connect to MongoDB
    logger.info(f"Connecting to MongoDB at {MONGO_URI}...")
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = mongo_client["outlet_db"]
        # Trigger connection check
        mongo_client.admin.command("ping")
        logger.info("Connected to MongoDB successfully.")
        seed_menu_if_empty()
    except Exception as e:
        logger.warning(f"Could not connect to MongoDB on startup: {e}")

    # 2. Connect to Redis
    logger.info(f"Connecting to Redis at {REDIS_URL}...")
    try:
        redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=3)
        redis_client.ping()
        logger.info("Connected to Redis successfully.")
    except Exception as e:
        logger.warning(f"Could not connect to Redis on startup: {e}")

    # 3. Connect to Kafka Producer (KRaft mode)
    logger.info(f"Starting AIOKafkaProducer targeting {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
        )
        await kafka_producer.start()
        logger.info("Kafka Producer started successfully.")
    except Exception as e:
        logger.warning(f"Kafka Producer could not start on startup: {e}")

    yield  # Application serves incoming HTTP requests

    # Graceful Shutdown
    logger.info("Shutting down connections...")
    if kafka_producer:
        try:
            await kafka_producer.stop()
            logger.info("Kafka Producer stopped.")
        except Exception as e:
            logger.error(f"Error stopping Kafka producer: {e}")
    if mongo_client:
        mongo_client.close()
        logger.info("MongoDB connection closed.")
    if redis_client:
        redis_client.close()
        logger.info("Redis connection closed.")


app = FastAPI(
    title="Outlet Order Platform API",
    description="High-throughput QSR Order Service modeled for Americana outlets",
    version="1.0.0",
    lifespan=lifespan,
)


# --- Pydantic Request / Response Models ---
class OrderItem(BaseModel):
    item_id: str
    name: str
    quantity: int = Field(gt=0, description="Quantity must be greater than zero")
    customizations: Optional[List[str]] = Field(default_factory=list)


class CreateOrderRequest(BaseModel):
    outlet_id: str = Field(..., description="Unique ID of the restaurant outlet, e.g. outlet-dxb-01")
    customer_name: str = Field(..., description="Customer placing the order")
    items: List[OrderItem] = Field(..., min_length=1, description="List of items in the order")


class CreateOrderResponse(BaseModel):
    order_id: str
    status: str
    message: str


# --- Health & Diagnostic Endpoints ---
@app.get("/health", tags=["Monitoring"])
def health_check():
    """
    Lightweight probe for Kubernetes / Docker health checks.
    """
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# --- Core Order Endpoints ---
@app.post("/orders", status_code=status.HTTP_202_ACCEPTED, response_model=CreateOrderResponse, tags=["Orders"])
async def place_order(order_req: CreateOrderRequest):
    """
    Place an order:
    1. Write to MongoDB with status 'received'.
    2. Send order event to Kafka topic 'orders'.
    3. Return 202 Accepted.
    4. If Kafka fails: mark MongoDB status 'failed' and return 503 Service Unavailable.
    """
    order_id = f"ord-{uuid.uuid4().hex[:10]}"
    created_at = datetime.now(timezone.utc).isoformat()

    order_doc = {
        "_id": order_id,
        "order_id": order_id,
        "outlet_id": order_req.outlet_id,
        "customer_name": order_req.customer_name,
        "items": [item.model_dump() for item in order_req.items],
        "status": "received",
        "created_at": created_at,
        "updated_at": created_at,
    }

    # Step 1: Save to MongoDB
    try:
        if db is None:
            raise PyMongoError("Database connection not established")
        db["orders"].insert_one(order_doc)
    except PyMongoError as err:
        logger.error(f"MongoDB write failed for order {order_id}: {err}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Primary database unavailable. Order could not be created."
        )

    # Step 2: Publish event to Kafka
    order_event = {
        "order_id": order_id,
        "outlet_id": order_req.outlet_id,
        "customer_name": order_req.customer_name,
        "items": order_doc["items"],
        "status": "received",
        "created_at": created_at,
    }

    kafka_success = False
    if kafka_producer:
        try:
            await kafka_producer.send_and_wait(KAFKA_TOPIC, value=order_event)
            kafka_success = True
            logger.info(f"Order {order_id} published to Kafka topic '{KAFKA_TOPIC}'")
        except KafkaError as err:
            logger.error(f"Kafka publish failed for order {order_id}: {err}")
        except Exception as err:
            logger.error(f"Unexpected error publishing to Kafka for order {order_id}: {err}")

    # Step 3 & 4: Handle Kafka outcome
    if not kafka_success:
        # Mark order as failed in MongoDB so client/support knows it was not queued
        try:
            db["orders"].update_one(
                {"_id": order_id},
                {"$set": {"status": "failed", "error_reason": "Message queue unavailable", "updated_at": datetime.now(timezone.utc).isoformat()}}
            )
        except Exception as e:
            logger.critical(f"Failed to mark order {order_id} as failed in MongoDB: {e}")

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Order messaging queue unavailable. Order not processed to prevent data loss."
        )

    return CreateOrderResponse(
        order_id=order_id,
        status="received",
        message="Order accepted and queued for preparation"
    )


@app.get("/orders/{order_id}", tags=["Orders"])
def get_order_status(order_id: str):
    """
    Check real-time order status from MongoDB.
    Because status is persisted before Kafka enqueue, this never returns 404 immediately after placement.
    """
    if db is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable"
        )

    try:
        order = db["orders"].find_one({"_id": order_id})
    except PyMongoError as err:
        logger.error(f"MongoDB query failed for {order_id}: {err}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database query error"
        )

    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")

    # Format document for JSON response
    order["_id"] = str(order["_id"])
    return order


@app.get("/menu", tags=["Menu"])
def get_menu():
    """
    Cache-Aside Menu Endpoint:
    1. Check Redis for cached menu JSON.
    2. On cache hit: return immediately (<2ms).
    3. On cache miss: query MongoDB, cache in Redis with TTL, return menu.
    4. If both Redis and MongoDB are down: return 503.
    """
    cache_key = "outlet:menu:items"

    # Step 1: Check Redis Cache
    if redis_client:
        try:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.info("Cache HIT: Serving menu from Redis")
                return {"source": "redis_cache", "items": json.loads(cached_data)}
        except Exception as err:
            logger.warning(f"Redis cache check failed: {err}. Falling back to MongoDB.")

    # Step 2: Fallback to MongoDB (Cache Miss)
    if db is not None:
        try:
            logger.info("Cache MISS: Fetching menu from MongoDB")
            items_cursor = db["menu"].find({}, {"_id": 0})
            menu_items = list(items_cursor)

            if menu_items and redis_client:
                try:
                    redis_client.setex(cache_key, REDIS_TTL_SECONDS, json.dumps(menu_items))
                    logger.info(f"Cached menu in Redis with TTL={REDIS_TTL_SECONDS}s")
                except Exception as cache_err:
                    logger.warning(f"Failed to write menu to Redis cache: {cache_err}")

            return {"source": "mongodb", "items": menu_items}
        except PyMongoError as err:
            logger.error(f"MongoDB menu fetch failed: {err}")

    # Step 3: Failure if both are down
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Menu service temporarily unavailable. Both cache and primary database unreachable."
    )
