from src import amqp_controller
import asyncio
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access environment variables
rabbitmq_server = os.getenv('RABBITMQ_SERVER')

queue_name = 'ecg:esp32'

async def main():
    receiver = amqp_controller.AMQPReceiver(rabbitmq_server, queue_name)
    await receiver.start()

asyncio.run(main())