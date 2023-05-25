import aio_pika
from . import ascon
from . import config
import json
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pytz
from dotenv import load_dotenv
import os

load_dotenv()

asc = ascon.Ascon()
key_g = config.ENCRYPT_KEY
nonce_g = config.ENCRYPT_NONCE
bucket_g = os.getenv('BUCKET_NAME')
org_g = os.getenv('BUCKET_ORG')
token_g = os.getenv('BUCKET_TOKEN')
url_g = os.getenv('INFLUXDB_SERVER')

class AMQPReceiver:
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consumer = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.host, heartbeat=30)
        self.channel = await self.connection.channel()
        queue = await self.channel.declare_queue(self.queue_name, durable=False)
        self.consumer = queue.iterator()

    async def start(self):
        try:
            await self.connect()
            await self.consume_messages()
        except Exception as e:
           print(f"Error starting AMQP receiver: {e}\n")
        finally:
            await self.close_amqp()

    async def consume_messages(self):
        async for message in self.consumer:
            try:
                await self.process_message(message)
            except Exception as e:
                print(f"Error processing message: {e}\n")

    async def process_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            global nonce_g

            def decryption(ascon, ciphertext, key, nonce, mode="ECB"):
                plaintext = ascon.ascon_decrypt(
                    key, nonce, associateddata=b"", ciphertext=ciphertext, variant="Ascon-128"
                )
                if mode == "CBC":
                    new_nonce = ciphertext[:16]
                    return plaintext, new_nonce
                
            def write_data_to_influxdb(data, bucket, org, token, url):
                client = InfluxDBClient(url=url, token=token, org=org)
                write_api = client.write_api(write_options=SYNCHRONOUS)
                timestamp = datetime(*data["tsp"][:3],*data["tsp"][4:7], microsecond=data["tsp"][7], tzinfo=pytz.UTC)
                iso_timestamp = timestamp.isoformat()
                dt = datetime(*data["tsp"][:3],*data["tsp"][4:6], data["tsp"][6]-1, microsecond=data["tsp"][7])
                rfc3339_time = dt.isoformat('T', timespec='microseconds') + 'Z'
                point = Point("measurement")\
                    .tag("id", data["id"])\
                    .field("temperature", data["t"])\
                    .field("humidity", data["h"])\
                    .time(rfc3339_time)
                write_api.write(bucket=bucket, org=org, record=point)

            plaintext, nonce_g = decryption(asc, message.body, key_g, nonce_g, "CBC")
            message_json = plaintext.decode("utf-8")
            message_dict = json.loads(message_json)
            print(f"*** Received message ***\n{message.body}\n{message_dict}\n")
            write_data_to_influxdb(data=message_dict, bucket=bucket_g, org=org_g, token=token_g, url=url_g)
   
    async def close_amqp(self):
        try:
            if self.connection is not None:
                await self.connection.close()
        except Exception as e:
            print("Error closing AMQP connection:", e)