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
    
    # Đảm bảo ship_postal_code là string để khớp với Spark schema
    if 'ship_postal_code' in normalized and normalized['ship_postal_code'] is not None:
        normalized['ship_postal_code'] = str(int(normalized['ship_postal_code']))
    
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
    
    # Callback để xử lý lỗi gửi - giới hạn log để tránh spam
    error_count = 0
    last_error_log_time = 0
    
    def on_send_error(excp):
        nonlocal error_count, last_error_log_time
        error_count += 1
        current_time = time.time()
        # Chỉ log lỗi mỗi 60 giây để tránh spam log
        if current_time - last_error_log_time > 60:
            print(f"[CẢNH BÁO] {error_count} lỗi gửi tin nhắn. Lỗi gần nhất: {excp}")
            error_count = 0
            last_error_log_time = current_time
    
    total_sent = 0
    batch_count = 0
    start_time = time.time()
    
    try:
        while True:
            # Sample 1000 records mỗi batch
            synthetic_data = synthesizer.sample(num_rows=1000)
            records = synthetic_data.to_dict(orient='records')
            
            # Gửi từng record trong batch
            for record in records:
                # Chuẩn hóa tên cột và xử lý Timestamp
                record = normalize_record(record)
                
                # Thêm timestamp ingestion
                record['ingestion_timestamp'] = time.time()

                # Gửi với callback xử lý lỗi
                future = producer.send(KAFKA_TOPIC, value=record)
                future.add_errback(on_send_error)
            
            # Flush sau khi gửi hết batch để đảm bảo tin nhắn được gửi đi
            producer.flush()
            total_sent += 1000
            batch_count += 1
            
            # Chỉ log mỗi 100 batch (100,000 records) để giảm log
            if batch_count % 100 == 0:
                elapsed = time.time() - start_time
                print(f"[Tiến độ] {total_sent:,} records | {elapsed/3600:.1f} giờ | {total_sent/elapsed:.0f} records/s")
            
            # Nghỉ 1 giây giữa các batch để tránh quá tải
            time.sleep(1)
            
    except KeyboardInterrupt:
        producer.flush()  # Flush trước khi đóng
        producer.close()

if __name__ == "__main__":
    main()
