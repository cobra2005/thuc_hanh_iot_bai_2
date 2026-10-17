"""Bai 3 - Alert producer: gui canh bao qua direct exchange."""

import os
import sys
import time

import pika

BROKER = os.environ.get("AMQP_HOST", "localhost")
PORT = int(os.environ.get("AMQP_PORT", "5672"))
USERNAME = os.environ.get("AMQP_USER", "guest")
PASSWORD = os.environ.get("AMQP_PASSWORD", "guest")
EXCHANGE = "iot_alert_exchange"

# routing key -> noi dung. Muc info khong co queue bind nen bi bo.
CANH_BAO = [
    ("info", "He thong giam sat hoat dong binh thuong"),
    ("warning", "Nhiet do phong may vuot nguong warning"),
    ("critical", "Cam bien kho lanh mat ket noi critical"),
]


def ket_noi() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    params = pika.ConnectionParameters(
        host=BROKER,
        port=PORT,
        credentials=credentials,
        heartbeat=60,
    )
    return pika.BlockingConnection(params)


def main() -> None:
    print(f"Dang ket noi toi RabbitMQ {BROKER}:{PORT} ...")
    try:
        connection = ket_noi()
    except Exception as exc:
        print(f"Khong ket noi duoc broker: {exc}")
        sys.exit(1)

    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct")
    print(f"Da ket noi broker. Exchange: {EXCHANGE} (direct)")

    try:
        for routing_key, message in CANH_BAO:
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=message,
            )
            print(f"Da gui routing key {routing_key}: {message}")
            time.sleep(0.5)
        print("Muc info khong co queue nao bind nen khong consumer nao nhan.")
    finally:
        connection.close()
        print("Da ngat ket noi.")


if __name__ == "__main__":
    main()
