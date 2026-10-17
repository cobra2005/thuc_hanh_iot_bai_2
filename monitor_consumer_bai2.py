"""Bai 2 - Monitoring consumer: doc telemetry va canh bao nguong."""

import json
import os
import sys

import pika

BROKER = os.environ.get("AMQP_HOST", "localhost")
PORT = int(os.environ.get("AMQP_PORT", "5672"))
USERNAME = os.environ.get("AMQP_USER", "guest")
PASSWORD = os.environ.get("AMQP_PASSWORD", "guest")
QUEUE = "sensor_data_queue"
NGUONG_NHIET_DO = 35.0
NGUONG_DO_AM = 40.0


def ket_noi() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    params = pika.ConnectionParameters(
        host=BROKER,
        port=PORT,
        credentials=credentials,
        heartbeat=60,
    )
    return pika.BlockingConnection(params)


def on_message(channel, method, properties, body):
    try:
        data = json.loads(body.decode("utf-8"))
        device_id = data["device_id"]
        temperature = float(data["temperature"])
        humidity = float(data["humidity"])
    except (UnicodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        print(f"Payload khong hop le: {exc}")
        channel.basic_ack(method.delivery_tag)
        return

    print(f"Device: {device_id}")
    print(f"Temperature: {temperature}")
    print(f"Humidity: {humidity}")
    if "timestamp" in data:
        print(f"Timestamp: {data['timestamp']}")
    if temperature > NGUONG_NHIET_DO:
        print("CANH BAO: Nhiet do cao")
    if humidity < NGUONG_DO_AM:
        print("CANH BAO: Do am thap")
    print()
    channel.basic_ack(method.delivery_tag)


def main() -> None:
    print(f"Dang ket noi toi RabbitMQ {BROKER}:{PORT} ...")
    try:
        connection = ket_noi()
    except Exception as exc:
        print(f"Khong ket noi duoc broker: {exc}")
        sys.exit(1)

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
    print(f"Da ket noi broker. Dang lang nghe queue: {QUEUE}")
    print("Nhan Ctrl+C de dung.")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung monitor.")
        sys.exit(0)


if __name__ == "__main__":
    main()
