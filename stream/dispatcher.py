"""stream/dispatcher.py - Rapido / Zomato style Food & Ride Order Dispatcher.

Publishes event messages to RabbitMQ topic exchange 'rapido-dispatch'.
Routing key format: <city>.<zone>.<type> (e.g. bangalore.koramangala.food)
"""

import json
import os
import random
import time
import uuid
import pika

EXCHANGE_NAME = "rapido-dispatch"
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://localhost")

CITIES = ["bangalore", "chennai"]
ZONES = {
    "bangalore": ["koramangala", "indiranagar", "whitefield", "jayanagar"],
    "chennai": ["tnagar", "velachery", "adyar", "whitefield"],
}
TYPES = ["ride", "food"]


def get_connection():
    params = pika.URLParameters(RABBITMQ_URL)
    return pika.BlockingConnection(params)


def main():
    print(f"[*] Connecting to RabbitMQ at: {RABBITMQ_URL}")
    connection = get_connection()
    channel = connection.channel()

    # Declare topic exchange
    channel.exchange_declare(
        exchange=EXCHANGE_NAME, exchange_type="topic", durable=True
    )
    print(
        f"[✓] Declared topic exchange: '{EXCHANGE_NAME}'. Starting dispatch"
        " loop...\n"
    )

    msg_count = 0
    try:
        while True:
            msg_count += 1
            city = random.choice(CITIES)
            zone = random.choice(ZONES[city])
            order_type = random.choice(TYPES)

            order_id = f"ORD-{msg_count:04d}-{str(uuid.uuid4())[:6]}"
            ts = time.strftime("%Y-%m-%d %H:%M:%S")

            # D3 Requirement: Send 1 bad message (missing 'zone') every 10 messages
            is_bad_message = msg_count % 10 == 0

            if is_bad_message:
                routing_key = f"{city}.unknown.{order_type}"
                message_body = {
                    "id": order_id,
                    "city": city,
                    # 'zone' is intentionally omitted to trigger NACK in driver
                    "type": order_type,
                    "ts": ts,
                    "is_corrupt_test": True,
                }
                print(
                    f"\n[!] ---> DISPATCHING BAD MESSAGE #{msg_count} (missing"
                    f" zone) with routing key '{routing_key}'"
                )
            else:
                routing_key = f"{city}.{zone}.{order_type}"
                message_body = {
                    "id": order_id,
                    "city": city,
                    "zone": zone,
                    "type": order_type,
                    "ts": ts,
                }
                print(
                    f"[+] Dispatching order #{msg_count}: key='{routing_key}' |"
                    f" ID={order_id}"
                )

            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=json.dumps(message_body),
                properties=pika.BasicProperties(
                    delivery_mode=2  # make message persistent
                ),
            )

            time.sleep(2)  # Dispatch every 2 seconds

    except KeyboardInterrupt:
        print("\n[*] Dispatcher stopped by user.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

