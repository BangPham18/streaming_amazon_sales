import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import Metadata
import os

# Sử dụng đường dẫn tương đối trong Docker
DATA_PATH = 'data/Amazon Sales.csv'
MODEL_PATH = 'models/amazon_sales_model.pkl'

if __name__ == "__main__":
    print(f"Đang đọc dữ liệu từ {DATA_PATH}...")
    try:
        data = pd.read_csv(DATA_PATH)
        
        # Xử lý dữ liệu sơ bộ
        data['Date'] = pd.to_datetime(data['Date'], format='%y/%m/%d', errors='coerce') # Format theo file của bạn
        if 'index' in data.columns:
            data = data.drop(columns=['index'])

        print("Đang phát hiện Metadata...")
        metadata = Metadata.detect_from_dataframe(data)

        print("Đang huấn luyện mô hình (Training)...")
        synthesizer = GaussianCopulaSynthesizer(metadata)
        synthesizer.fit(data)

        synthesizer.save(MODEL_PATH)
        print(f"Đã lưu mô hình tại: {MODEL_PATH}")
        
    except Exception as e:
        print(f"Lỗi: {e}")