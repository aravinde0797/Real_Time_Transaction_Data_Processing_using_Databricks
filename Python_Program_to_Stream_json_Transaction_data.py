import asyncio
import json
import os
import logging
import signal
import random
import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable to control the loop
stop_event = asyncio.Event()

async def create_json_data(index):
    # Generate sample data
    data = {
        "id": index,
        "account_id": random.randint(100000, 999999),
        "limit": random.randint(5000, 10000),
        "products": [
            "Derivatives",
            "InvestmentStock"
        ],
        "username": f"user{index}",
        "name": f"Name {index}",
        "address": f"{random.randint(1000, 9999)} Address Street\nCity, State {random.randint(10000, 99999)}",
        "birthdate": {
            "$date": datetime.datetime(1977, 3, 2, 2, 20, 31).isoformat() + "Z"
        },
        "email": f"user{index}@example.com",
        "active": random.choice([True, False]),
        "tier_and_details": {
            "k": {
                "tier": "Bronze",
                "id": "0df078f33aa74a2e9696e0520c1a828a",
                "active": True,
                "benefits": [
                    "sports tickets"
                ]
            },
            "v": {
                "tier": "Bronze",
                "benefits": [
                    "24 hour dedicated line",
                    "concierge services"
                ],
                "active": True,
                "id": "699456451cc24f028d2aa99d7534c219"
            }
        },
        "transaction_count": random.randint(1, 10),
        "bucket_start_date": {
            "$date": {
                "$numberLong": str(int((datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365*10))).timestamp() * 1000))
            }
        },
        "bucket_end_date": {
            "$date": datetime.datetime.now().isoformat() + "Z"
        },
        "transactions": [
            {
                "date": {
                    "$date": datetime.datetime(2003, 9, 9).isoformat() + "Z"
                },
                "amount": random.randint(1000, 10000),
                "transaction_code": random.choice(["buy", "sell"]),
                "symbol": random.choice(["adbe", "team", "msft", "sap"]),
                "price": "{:.20f}".format(random.uniform(10, 100)),
                "total": "{:.20f}".format(random.uniform(10000, 200000))
            } for _ in range(random.randint(1, 5))
        ]
    }

    return json.dumps(data, indent=2)

async def send_event(producer, json_data):
    # Create a batch and add the event
    try:
        event_data_batch = await producer.create_batch()
        event_data_batch.add(EventData(json_data))
        await producer.send_batch(event_data_batch)
        logger.info(f"Sent event: {json_data}")
    except ValueError:
        logger.error("Event data is too large for the batch.")

async def run():
    # Get environment variables
    conn_str = os.getenv("EVENT_HUB_CONNECTION_STRING")
    eventhub_name = os.getenv("EVENT_HUB_NAME")
    
    if not conn_str or not eventhub_name:
        logger.error("Environment variables for EVENT_HUB_CONNECTION_STRING or EVENT_HUB_NAME are not set.")
        return

    producer = EventHubProducerClient.from_connection_string(
        conn_str=conn_str,
        eventhub_name=eventhub_name
    )
    
    async with producer:
        index = 1
        
        try:
            while not stop_event.is_set():
                json_data = await create_json_data(index)
                logger.info(f"Creating event {index} with data: {json_data}")
                
                # Send the event immediately
                await send_event(producer, json_data)
                
                index += 1
                
                # Sleep for 3 seconds before generating the next event
                await asyncio.sleep(3)  # Sleep for 3 seconds
                logger.info(f"Waiting 3 seconds before sending next event.")
        
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received, stopping.")
        finally:
            logger.info("Stopped sending events.")

def signal_handler(signum, frame):
    stop_event.set()

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Handle termination signals

# Run the asynchronous function
asyncio.run(run())
