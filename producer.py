import asyncio
import websockets
from kafka import KafkaProducer
import json

async def connect_to_websocket():
    uri = "wss://ws.bitstamp.net"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps({"event": "bts:subscribe", "data": {"channel": "live_orders_btcusd"}}))
                producer = KafkaProducer(bootstrap_servers=['kafka:9093'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                
                while True:
                    response = await websocket.recv()
                    data = json.loads(response)
                    
                    if "data" in data and "channel" in data:
                        producer.send('bitcoin_transactions', data)
        except Exception as e:
            print(f"Error connecting...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(connect_to_websocket())