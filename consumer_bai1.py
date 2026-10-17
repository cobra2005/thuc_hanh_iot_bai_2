"""Bai 1 - Consumer: nhan message tu queue iot_lab_queue."""

import os
import sys
from datetime import datetime

import pika

BROKER = os.environ.get("AMQP_HOST", "localhost")
PORT = int(os.environ.get("AMQP_PORT", "5672"))
USERNAME = os.environ.get("AMQP_USER", "guest")
PASSWORD = os.environ.get("AMQP_PASSWORD", "guest")
QUEUE = "iot_lab_queue"


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
    noi_dung = body.decode("utf-8", errors="replace")
    thoi_diem = datetime.now().strftime("%H:%M:%S")
    print(f"Da nhan message: {noi_dung}")
    print(f"Thoi gian nhan: {thoi_diem}")
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
        print("\nDa dung consumer.")
        sys.exit(0)


if __name__ == "__main__":
    main()
