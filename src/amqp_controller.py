import aio_pika
import asyncio
class AMQPReceiver:
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consumer = None

    async def start(self):
        try:
            # Create a connection and channel, and declare a durable queue
            queue = await self.create_queue()

            # Create a consumer to consume messages from the queue
            self.consumer = queue.iterator()

            # Start consuming messages
            async for message in self.consumer:
                await self.on_message(message)
        except Exception as e:
            raise Exception('Close connection')

    async def create_queue(self):
        try:
            # Establish a connection to the RabbitMQ server
            self.connection = await aio_pika.connect_robust(self.host)

            # Create a channel
            self.channel = await self.connection.channel()

            # Create a durable queue
            queue = await self.channel.declare_queue(
                self.queue_name,
                durable=False
            )

            return queue
        except Exception as e:
            await self.close()
            pass

    async def close(self):
        try:
            await self.connection.close()
        except Exception as e:
            pass

    async def on_message(self, message: aio_pika.IncomingMessage):
        try:
            async with message.process():
                print("Received message:", message.body.decode('utf-8'))
        except asyncio.CancelledError:
            pass
        