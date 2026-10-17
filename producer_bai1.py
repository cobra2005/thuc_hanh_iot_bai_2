"""Bai 1 - Producer: gui message vao queue iot_lab_queue."""

import os
import sys
import time

import pika

# Sua thong tin sinh vien truoc khi nop bai.
HO_TEN = os.environ.get("AMQP_HO_TEN", "Nguyen Van A")
MA_SINH_VIEN = os.environ.get("AMQP_MA_SV", "B21xxxx")
LOI_CHAO = "Xin chao tu ung dung Python AMQP"

BROKER = os.environ.get("AMQP_HOST", "localhost")
PORT = int(os.environ.get("AMQP_PORT", "5672"))
USERNAME = os.environ.get("AMQP_USER", "guest")
PASSWORD = os.environ.get("AMQP_PASSWORD", "guest")
QUEUE = "iot_lab_queue"
SO_MESSAGE = 3
KHOANG_CACH_GIAY = 1


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
    channel.queue_declare(queue=QUEUE)
    print(f"Da ket noi broker. Queue: {QUEUE}")

    payload = f"{LOI_CHAO} - {MA_SINH_VIEN} - {HO_TEN}"
    try:
        for i in range(1, SO_MESSAGE + 1):
            channel.basic_publish(exchange="", routing_key=QUEUE, body=payload)
            print(f"Da gui message {i}/{SO_MESSAGE}")
            print(f"Noi dung: {payload}")
            if i < SO_MESSAGE:
                time.sleep(KHOANG_CACH_GIAY)
    finally:
        connection.close()
        print("Da ngat ket noi.")


if __name__ == "__main__":
    main()
