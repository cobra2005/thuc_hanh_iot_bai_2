# Thực hành Python AMQP

Ba bài mô phỏng gửi, nhận và phân phối dữ liệu IoT qua RabbitMQ: queue cơ bản, telemetry cảm biến, và direct exchange theo mức cảnh báo.

## Broker sử dụng

Mặc định mọi chương trình kết nối tới RabbitMQ trên máy local:

- Host: `localhost`
- Port: `5672`
- User: `guest`
- Password: `guest`

User `guest` chỉ đăng nhập được từ localhost. Đổi broker bằng biến môi trường:

```bash
export AMQP_HOST=localhost
export AMQP_PORT=5672
export AMQP_USER=guest
export AMQP_PASSWORD=guest
```

### Cài RabbitMQ trên máy

Debian/Ubuntu:

```bash
sudo apt install rabbitmq-server
sudo systemctl start rabbitmq-server
```

Arch Linux:

```bash
sudo pacman -S rabbitmq
sudo systemctl start rabbitmq
```

### Chạy bằng Docker

Trong container, user `guest` không nhận kết nối từ máy host. Tạo user riêng:

```bash
docker run -d --name iot-rabbitmq \
  -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=iot \
  -e RABBITMQ_DEFAULT_PASS=iotlab \
  rabbitmq:3-alpine
```

Rồi trước khi chạy Python:

```bash
export AMQP_USER=iot
export AMQP_PASSWORD=iotlab
```

## Cài đặt

Yêu cầu Python 3.

```bash
pip install -r requirements.txt
```

## Sửa thông tin sinh viên (Bài 1)

Mở `producer_bai1.py` và sửa `HO_TEN`, `MA_SINH_VIEN`. Hoặc:

```bash
export AMQP_HO_TEN="Nguyen Van A"
export AMQP_MA_SV="B21xxxx"
```

## Cách chạy

Mỗi bài cần chương trình nhận chạy trước, chương trình gửi chạy sau. Consumer dừng bằng Ctrl+C.

### Bài 1 — Producer / Consumer

Terminal 1:

```bash
python consumer_bai1.py
```

Terminal 2:

```bash
python producer_bai1.py
```

Producer gửi 3 message liên tiếp vào queue `iot_lab_queue`. Consumer in nội dung và thời điểm nhận.

```text
Da nhan message: Xin chao tu ung dung Python AMQP - B21xxxx - Nguyen Van A
Thoi gian nhan: 14:05:21
```

### Bài 2 — Cảm biến và giám sát

Terminal 1:

```bash
python monitor_consumer_bai2.py
```

Terminal 2:

```bash
python sensor_producer_bai2.py
```

Ba cảm biến `sensor01`, `sensor02`, `sensor03` gửi JSON mỗi 3 giây vào queue `sensor_data_queue`. Payload có thêm trường `timestamp`.

```json
{"device_id": "sensor01", "temperature": 29.5, "humidity": 62.1, "timestamp": "14:05:21"}
```

Nhiệt độ ngẫu nhiên 25–40°C, độ ẩm 30–80%. Monitor cảnh báo khi nhiệt độ > 35 hoặc độ ẩm < 40.

```text
Device: sensor01
Temperature: 36.4
Humidity: 38.9
CANH BAO: Nhiet do cao
CANH BAO: Do am thap
```

### Bài 3 — Exchange và routing key

Terminal 1:

```bash
python warning_consumer_bai3.py
```

Terminal 2:

```bash
python critical_consumer_bai3.py
```

Terminal 3:

```bash
python alert_producer_bai3.py
```

Producer khai báo direct exchange `iot_alert_exchange` và gửi 3 message:

| Routing key | Queue nhận |
|---|---|
| `info` | Không consumer nào nhận |
| `warning` | `warning_queue` |
| `critical` | `critical_queue` |

```text
[warning_queue] Da nhan: Nhiet do phong may vuot nguong warning
[critical_queue] Da nhan: Cam bien kho lanh mat ket noi critical
```

Hai consumer ghi thêm cùng một dòng vào file `alert.log`.

## Kết quả đạt được

- Kết nối được RabbitMQ, gửi và nhận message qua queue.
- Bài 2 gửi JSON định kỳ và cảnh báo đúng ngưỡng nhiệt độ, độ ẩm.
- Bài 3 tạo direct exchange, bind đúng routing key, message `warning` và `critical` chỉ tới đúng consumer.
