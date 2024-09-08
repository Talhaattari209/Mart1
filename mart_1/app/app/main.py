from contextlib import asynccontextmanager
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
import asyncio

from .scehma import Order  # Assuming schema file is in 'app/app/scehma.py'

# Asynchronous context manager for lifespan event
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("LifeSpan Event...")
    task = asyncio.create_task(consume('order', 'broker:19092'))  # Corrected typo
    try:
        yield  # Pass control to the application
    finally:
        await task  # Wait for the consumer task to finish

# FastAPI application with lifespan and order creation endpoint
app = FastAPI(lifespan=lifespan)


# Function to handle Kafka consumption (asynchronous)
@app.get("/consumer")
async def consume(topic, broker):
    async with AIOKafkaConsumer(
        topic, bootstrap_servers=broker, group_id="my-group"
    ) as consumer:
        # Get cluster layout and join group "my-group"
        await consumer.start()
        try:
            # Consume messages and process them
            async for msg in consumer:
                try:
                    message = json.loads(msg.value)
                    # Process the message data (e.g., store in database, send notification)
                    print(f"Processed message: {message}")
                except Exception as e:
                    print(f"Error processing message: {e}")
        finally:
            # Leave consumer group; perform autocommit if enabled.
            await consumer.stop()
        



# Endpoint to create an order (asynchronous)
@app.post("/order")
async def create_order(order: Order):
    async with AIOKafkaProducer(bootstrap_servers='broker:19092') as producer:
        # Produce order data as JSON bytes
        order_json = json.dumps(order.dict()).encode('utf-8')  # Use order.dict()
        # Get cluster layout and initial leadership information
        await producer.start()
        try:
            # Produce message to topic "order"
            await producer.send_and_wait("order", order_json)
            return order  # Return the order object itself
        finally:
            # Wait for pending messages to be delivered or expire
            await producer.stop()
    
