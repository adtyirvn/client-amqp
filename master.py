from src import amqp_controller
import asyncio
from dotenv import load_dotenv
import os

load_dotenv()

rabbitmq_server = os.getenv('RABBITMQ_SERVER')

queue_name = os.getenv('QUEUE_NAME')


async def main():
    receiver = amqp_controller.AMQPReceiver(rabbitmq_server, queue_name)
    try:
        await receiver.start()
    except Exception as e:
        print(e)
try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Keyboard Interrupt\nClosing...")