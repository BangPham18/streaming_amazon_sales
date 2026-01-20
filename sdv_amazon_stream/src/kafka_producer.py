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

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

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
            
            # Xử lý Timestamp
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.strftime('%Y-%m-%d')
            
            record['ingestion_timestamp'] = time.time()

            producer.send(KAFKA_TOPIC, value=record)
            print(f"Đã gửi đơn hàng ID: {record.get('Order ID', 'N/A')}")
            time.sleep(random.uniform(0.5, 2)) # Giả lập độ trễ
            
    except KeyboardInterrupt:
        producer.close()

if __name__ == "__main__":
    main()
