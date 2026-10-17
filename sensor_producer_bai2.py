"""Bai 2 - Sensor producer: gui telemetry JSON vao sensor_data_queue."""

import json
import os
import random
import sys
import time
from datetime import datetime

import pika

BROKER = os.environ.get("AMQP_HOST", "localhost")
PORT = int(os.environ.get("AMQP_PORT", "5672"))
USERNAME = os.environ.get("AMQP_USER", "guest")
PASSWORD = os.environ.get("AMQP_PASSWORD", "guest")
QUEUE = "sensor_data_queue"
DEVICES = ["sensor01", "sensor02", "sensor03"]
CHU_KY_GIAY = 3


def ket_noi() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    params = pika.ConnectionParameters(
        host=BROKER,
        port=PORT,
        credentials=credentials,
        heartbeat=60,
    )
    return pika.BlockingConnection(params)


def sinh_du_lieu(device_id: str) -> dict:
    return {
        "device_id": device_id,
        "temperature": round(random.uniform(25.0, 40.0), 1),
        "humidity": round(random.uniform(30.0, 80.0), 1),
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    }


def main() -> None:
    print(f"Dang ket noi toi RabbitMQ {BROKER}:{PORT} ...")
    try:
        connection = ket_noi()
    except Exception as exc:
        print(f"Khong ket noi duoc broker: {exc}")
        sys.exit(1)

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    print(f"Da ket noi broker. Gui du lieu moi {CHU_KY_GIAY} giay vao {QUEUE}.")
    print("Nhan Ctrl+C de dung.")

    try:
        while True:
            for device_id in DEVICES:
                data = sinh_du_lieu(device_id)
                payload = json.dumps(data)
                channel.basic_publish(exchange="", routing_key=QUEUE, body=payload)
                print(f"Da gui: {payload}")
            time.sleep(CHU_KY_GIAY)
    except KeyboardInterrupt:
        print("\nDa dung sensor producer.")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
