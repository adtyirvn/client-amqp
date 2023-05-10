import aio_pika

class AMQPReceiver:
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consumer = None

    async def start(self):
        # Create a connection and channel, and declare a durable queue
        queue = await self.create_queue()

        # Create a consumer to consume messages from the queue
        self.consumer = queue.iterator()

        # Start consuming messages
        async for message in self.consumer:
            await self.on_message(message)

    async def create_queue(self):
        # Establish a connection to the RabbitMQ server
        connection = await aio_pika.connect_robust(self.host)

        # Create a channel
        channel = await connection.channel()

        # Create a durable queue
        queue = await channel.declare_queue(
            self.queue_name,
            durable=False
        )

        return queue
    async def on_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            print("Received message:", message.body.decode('utf-8'))