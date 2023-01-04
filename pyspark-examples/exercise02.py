import asyncio
import json

import websockets
from kafka3 import KafkaProducer

binanceWssUrl = "wss://stream.binance.com:9443/ws/btcusdt@trade"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


async def listen_trade_events(frames):
    async for frame in frames:
        trade = json.loads(frame)
        print(trade)
        producer.send(topic="binance-trades", value=trade)


async def connect():
    async with websockets.connect(binanceWssUrl) as ws:
        await listen_trade_events(ws)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(connect())
    loop.run_forever()
