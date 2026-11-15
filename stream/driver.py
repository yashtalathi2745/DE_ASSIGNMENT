"""stream/driver.py - Driver Consumer with ACK / NACK Error Handling.

Usage:
    python driver.py <NAME> <PATTERN>
Example:
    python driver.py Ravi "bangalore.koramangala.*"
    python driver.py Priya "bangalore.indiranagar.*"
    python driver.py Arjun "bangalore.*.ride"
    python driver.py Control "bangalore.#"
"""

import json
import os
import sys
import pika

EXCHANGE_NAME = "rapido-dispatch"
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://localhost")


def main():
    if len(sys.argv) < 3:
        print("Usage: python driver.py <NAME> <PATTERN>")
        print("Example: python driver.py Ravi 'bangalore.koramangala.*'")
        sys.exit(1)

    driver_name = sys.argv[1]
    binding_pattern = sys.argv[2]

    # Connect to RabbitMQ
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # 1. Declare the topic exchange (matching dispatcher)
    channel.exchange_declare(
        exchange=EXCHANGE_NAME, exchange_type="topic", durable=True
    )

    # 2. Declare a private queue exclusive to this driver instance
    result = channel.queue_declare(
        queue="", exclusive=True, auto_delete=True
    )
    queue_name = result.method.queue

    # 3. Bind the private queue to the exchange using the binding pattern
    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=queue_name,
        routing_key=binding_pattern,
    )

    # Process 1 message at a time
    channel.basic_qos(prefetch_count=1)

    print(f"\n==================================================")
    print(f"[*] Driver '{driver_name}' active and waiting for requests.")
    print(f"[*] Binding pattern : '{binding_pattern}'")
    print(f"[*] Private Queue    : '{queue_name}'")
    print(f"[*] Exchange         : '{EXCHANGE_NAME}'")
    print(f"==================================================\n")

    def on_message_received(ch, method, properties, body):
        routing_key = method.routing_key
        try:
            order = json.loads(body.decode("utf-8"))
        except Exception as e:
            print(f"[{driver_name}] [NACK] Failed to parse JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        order_id = order.get("id", "UNKNOWN")

        # ----------------------------------------------------
        # Part D3: ACK / NACK Validation Logic
        # Reject message if 'zone' is missing with basic_nack(requeue=False)
        # Otherwise acknowledge with basic_ack
        # ----------------------------------------------------
        if "zone" not in order or order.get("zone") is None:
            print(
                f"[{driver_name}] [NACK ❌] REJECTED Order {order_id} | Routing"
                f" Key: '{routing_key}'"
            )
            print(
                f"    └── Reason: Missing mandatory 'zone' field."
                " basic_nack(requeue=False) sent."
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            city = order.get("city")
            zone = order.get("zone")
            order_type = order.get("type")
            ts = order.get("ts")

            print(
                f"[{driver_name}] [ACK  ✅] ACCEPTED Order {order_id} | Routing"
                f" Key: '{routing_key}'"
            )
            print(
                f"    └── Details: {city.title()} · {zone.title()} ·"
                f" {order_type.upper()} at {ts}"
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=on_message_received,
        auto_ack=False,  # manual ACK/NACK required
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n[*] Driver '{driver_name}' stopped.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

