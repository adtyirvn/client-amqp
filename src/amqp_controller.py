import asyncio
import aio_pika

class AMQPReceiver:
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

    async def start(self):
        # Create a connection and channel, and declare a durable queue
        queue = await self.create_queue()
        while True:
        # Consume messages from the queue
            await self.consume_messages(queue)

    async def create_queue(self):
        # Establish a connection to the RabbitMQ server
        connection = await aio_pika.connect_robust(self.host)

        # Create a channel
        channel = await connection.channel()

        # Create a durable queue
        queue = await channel.declare_queue(
            self.queue_name,
            durable=True
        )

        return queue
            
    async def consume_messages(self, queue):
        # Define a callback function to handle received messages
        async def on_message(message: aio_pika.IncomingMessage):
            async with message.process():
                print("Received message:", message.body.decode())

        # Register the callback function to consume messages from the queue
        await queue.consume(on_message)