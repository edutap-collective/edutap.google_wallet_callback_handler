from .crypto import decrypt_message
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer

import asyncio


async def process_messages(broker_url: str, topic: str, target_topic: str):
    consumer: AIOKafkaConsumer | None = None

    while consumer is None:
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=broker_url,
                group_id="google-wallet-notification-consumer",
                auto_offset_reset="earliest",
            )
            print(f"Starting consumer for topic {topic} on broker {broker_url}")
            await consumer.start()
            producer = AIOKafkaProducer(bootstrap_servers=broker_url)
            print(f"Starting producer for topic {target_topic} on broker {broker_url}")
            await producer.start()
        except Exception as e:
            print(f"Error connecting to broker {broker_url}: {e}, retrying in 1 second")
            consumer = None
            await asyncio.sleep(1)

    try:
        async for msg in consumer:
            print("Received message: ", msg.value.decode("utf-8"))
            print("Sending to topic: ", target_topic)
            try:
                decrypted_message = decrypt_message(msg.value.decode("utf-8"))
                await producer.send(target_topic, decrypted_message.encode("utf-8"))
            except Exception as e:
                print("Error decrypting message: ", e)
                continue
    except Exception as e:
        print("Error processing messages: ", e)
        raise
    finally:
        print("Stopping consumer")
        await consumer.stop()
