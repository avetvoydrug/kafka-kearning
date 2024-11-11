import asyncio
import json
import random

from aiokafka import AIOKafkaProducer

from config import HOST, PORT, WEATHER_TOPIC

def serializer(value):
    """
    Обмен данными происходит в байтах, поэтому мы должны
    сначала перевести наше значение JSON, а затем в байты
    """
    return json.dumps(value).encode()

async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers=f"{HOST}:{PORT}",
        value_serializer=serializer,
        compression_type="gzip"
    )
    await producer.start()
    try:
        while True:
            data = {
                "temp": random.randint(10,20),
                "weather": random.choice(("sunny","rainy"))
            }
            await producer.send(WEATHER_TOPIC, value=data)
            print(f"sent: {data}")
            slp = random.randint(1,5)
            print(f"sleeping in {slp}")
            await asyncio.sleep(slp)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(produce())