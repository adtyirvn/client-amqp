from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pytz
import asyncio

async def write_data_to_influxdb(data, bucket, org, token):
    client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    timestamp = datetime(*data["timestamp"][:6], microsecond=data["timestamp"][6], tzinfo=pytz.UTC)
    iso_timestamp = timestamp.isoformat()
    point = Point("measurement")\
        .tag("id", data["id"])\
        .field("temperature", data["temperature"])\
        .field("humidity", data["humidity"])\
        .time(iso_timestamp)
    write_api.write(bucket=bucket, org=org, record=point)

async def main():
    data = {"id": "ESP32_30aea41ff180", "temperature": 28, "humidity": 95, "timestamp": [2023, 5, 11, 3, 21, 36, 25, 57092]}
    await write_data_to_influxdb(data, "test", "aditya", "f70829780625cb9a48c1beeb6607303b6b420e06f3ff7c2ffc5cf57a867e3b5d")

asyncio.run(main())