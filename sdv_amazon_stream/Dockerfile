# Sử dụng Python 3.9 (SDV hoạt động tốt nhất trên 3.8 - 3.10)
FROM python:3.9-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Cài đặt các thư viện hệ thống cần thiết (nếu có)
RUN apt-get update && apt-get install -y gcc

# Copy file requirements và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code và dữ liệu vào container
COPY src/ ./src/
COPY data/ ./data/
COPY entrypoint.sh .

# Cấp quyền thực thi cho entrypoint
RUN chmod +x entrypoint.sh

# Lệnh mặc định khi container khởi chạy
ENTRYPOINT ["./entrypoint.sh"]