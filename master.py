from src import amqp_controller
import asyncio
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access environment variables
rabbitmq_server = os.getenv('RABBITMQ_SERVER')

queue_name = os.getenv('QUEUE_NAME')

receiver = amqp_controller.AMQPReceiver(rabbitmq_server, queue_name)
async def main():
    await receiver.start()

asyncio.run(main())