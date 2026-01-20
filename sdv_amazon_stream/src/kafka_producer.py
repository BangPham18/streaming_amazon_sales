import time
import random
import json
import pandas as pd
import os
from kafka import KafkaProducer
from sdv.single_table import GaussianCopulaSynthesizer

# Lấy cấu hình từ biến môi trường (Environment Variables) trong docker-compose
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'amazon_sales')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:9092')
MODEL_PATH = 'models/amazon_sales_model.pkl'

# Mapping tên cột gốc sang tên chuẩn (lowercase + underscore)
COLUMN_MAPPING = {
    'Order ID': 'order_id',
    'Date': 'date',
    'Status': 'status',
    'Fulfilment': 'fulfilment',
    'Sales Channel': 'sales_channel',
    'ship-service-level': 'ship_service_level',
    'Style': 'style',
    'SKU': 'sku',
    'Category': 'category',
    'Size': 'size',
    'ASIN': 'asin',
    'Courier Status': 'courier_status',
    'Qty': 'qty',
    'currency': 'currency',
    'Amount': 'amount',
    'ship-city': 'ship_city',
    'ship-state': 'ship_state',
    'ship-postal-code': 'ship_postal_code',
    'ship-country': 'ship_country',
    'promotion-ids': 'promotion_ids',
    'B2B': 'b2b',
    'index': 'index',
}

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def normalize_record(record):
    """Chuyển đổi tên cột sang format chuẩn (lowercase + underscore)"""
    normalized = {}
    for key, value in record.items():
        # Chuyển đổi Timestamp sang string
        if isinstance(value, pd.Timestamp):
            value = value.strftime('%Y-%m-%d')
        # Áp dụng mapping tên cột
        new_key = COLUMN_MAPPING.get(key, key.lower().replace(' ', '_').replace('-', '_'))
        normalized[new_key] = value
    return normalized

def main():
    # Chờ Kafka khởi động xong (Retries)
    producer = None
    for i in range(10):
        try:
            print(f"Đang kết nối tới Kafka tại {BOOTSTRAP_SERVERS} (Lần thử {i+1})...")
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                value_serializer=json_serializer
            )
            print("Kết nối Kafka thành công!")
            break
        except Exception as e:
            print(f"Chưa kết nối được: {e}. Đợi 5 giây...")
            time.sleep(5)
    
    if not producer:
        print("Không thể kết nối Kafka. Dừng chương trình.")
        return

    print(f"Đang tải mô hình từ {MODEL_PATH}...")
    synthesizer = GaussianCopulaSynthesizer.load(MODEL_PATH)

    print(f"Bắt đầu gửi dữ liệu vào topic '{KAFKA_TOPIC}'...")
    try:
        while True:
            synthetic_data = synthesizer.sample(num_rows=1000)
            record = synthetic_data.to_dict(orient='records')[0]
            
            # Chuẩn hóa tên cột và xử lý Timestamp
            record = normalize_record(record)
            
            # Thêm timestamp ingestion
            record['ingestion_timestamp'] = time.time()

            producer.send(KAFKA_TOPIC, value=record)
            print(f"Đã gửi đơn hàng ID: {record.get('order_id', 'N/A')}")
            time.sleep(random.uniform(0.5, 2))  # Giả lập độ trễ
            
    except KeyboardInterrupt:
        producer.close()

if __name__ == "__main__":
    main()
