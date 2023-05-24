import aio_pika
from . import ascon
from . import config
import json

asc = ascon.Ascon()
key_g = config.ENCRYPT_KEY
nonce_g = config.ENCRYPT_NONCE


class AMQPReceiver:
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consumer = None

    async def start(self):
        try:
            queue = await self.create_queue()

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    try:
                        await self.on_message(message)
                    except Exception as e:
                        print("Error processing message:", e)

        except Exception as e:
            print("Error starting AMQP receiver:", e)

        finally:
            await self.close_amqp()

    async def create_queue(self):
        try:
            connection = await aio_pika.connect_robust(self.host)
            channel = await connection.channel()
            queue = await channel.declare_queue(self.queue_name, durable=False)
            return queue

        except Exception as e:
            raise RuntimeError(e)

    async def on_message(self, message: aio_pika.IncomingMessage):
        try:
            async with message.process():
                global nonce_g

                def decryption(ascon, ciphertext, key, nonce, mode="ECB"):
                    plaintext = ascon.ascon_decrypt(
                        key, nonce, associateddata=b"", ciphertext=ciphertext, variant="Ascon-128"
                    )
                    if mode == "CBC":
                        new_nonce = ciphertext[:16]
                        return plaintext, new_nonce

                plaintext, nonce_g = decryption(asc, message.body, key_g, nonce_g, "CBC")
                # print(plaintext)
                message_json = plaintext.decode("utf-8")
                message_dict = json.loads(message_json)
                print(f"*** Received message ***\n{message.body}\n{message_dict}\n")

        except Exception as e:
            raise RuntimeError(e)
        
    async def close_amqp(self):
        if self.connection is not None:
            await self.connection.close()