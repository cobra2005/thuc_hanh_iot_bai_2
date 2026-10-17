"""Bai 3 - Warning consumer: chi nhan routing key warning."""

import os
import sys
from datetime import datetime

import pika

BROKER = os.environ.get("AMQP_HOST", "localhost")
PORT = int(os.environ.get("AMQP_PORT", "5672"))
USERNAME = os.environ.get("AMQP_USER", "guest")
PASSWORD = os.environ.get("AMQP_PASSWORD", "guest")
EXCHANGE = "iot_alert_exchange"
QUEUE = "warning_queue"
ROUTING_KEY = "warning"
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alert.log")


def ket_noi() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    params = pika.ConnectionParameters(
        host=BROKER,
        port=PORT,
        credentials=credentials,
        heartbeat=60,
    )
    return pika.BlockingConnection(params)


def ghi_log(dong: str) -> None:
    thoi_diem = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as tep:
        tep.write(f"{thoi_diem} {dong}\n")


def on_message(channel, method, properties, body):
    noi_dung = body.decode("utf-8", errors="replace")
    dong = f"[{QUEUE}] Da nhan: {noi_dung}"
    print(dong)
    ghi_log(dong)
    channel.basic_ack(method.delivery_tag)


def main() -> None:
    print(f"Dang ket noi toi RabbitMQ {BROKER}:{PORT} ...")
    try:
        connection = ket_noi()
    except Exception as exc:
        print(f"Khong ket noi duoc broker: {exc}")
        sys.exit(1)

    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct")
    channel.queue_declare(queue=QUEUE)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
    print(f"Da ket noi broker. Queue {QUEUE} bind {EXCHANGE} key={ROUTING_KEY}")
    print(f"Ghi log vao: {LOG_FILE}")
    print("Nhan Ctrl+C de dung.")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDa dung warning consumer.")
        sys.exit(0)


if __name__ == "__main__":
    main()
