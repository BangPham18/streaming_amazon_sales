#!/bin/bash

MODEL_FILE="models/amazon_sales_model.pkl"

echo "--- Bắt đầu khởi tạo SDV Producer ---"

# Kiểm tra xem mô hình đã tồn tại chưa
if [ -f "$MODEL_FILE" ]; then
    echo "Mô hình '$MODEL_FILE' đã tồn tại. Bỏ qua bước huấn luyện."
else
    echo "Chưa tìm thấy mô hình. Đang chạy train_model.py..."
    python src/train_model.py
fi

# Sau khi đảm bảo đã có model, chạy producer
echo "Bắt đầu stream dữ liệu vào Kafka..."
python src/kafka_producer.py