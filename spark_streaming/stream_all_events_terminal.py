import os
from dotenv import load_dotenv
from streaming_functions import *
from schema import schema

# Load environment variables from .env file
load_dotenv()

# Kafka Topic for Amazon Sales
AMAZON_SALES_TOPIC = "amazon_sales"

KAFKA_PORT = "9092"

KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", 'localhost')

# Initialize a spark session
spark = create_or_get_spark_session('Amazon Sales Stream')
spark.streams.resetTerminated()

# Amazon sales stream
# LƯU Ý QUAN TRỌNG: Để đọc lại dữ liệu cũ, hãy chắc chắn hàm này 
# bên streaming_functions.py đang dùng startingOffsets="earliest"
amazon_sales = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, AMAZON_SALES_TOPIC) 

amazon_sales = process_stream(
    amazon_sales, schema[AMAZON_SALES_TOPIC], AMAZON_SALES_TOPIC)

# ----------------------------------------------------------------
# PHẦN THAY ĐỔI: Ghi ra Console thay vì GCS
# ----------------------------------------------------------------

print(">>> DANG CHAY CHE DO DEBUG: IN DU LIEU RA MAN HINH...")

# Cấu hình ghi ra màn hình console
query = amazon_sales.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start() # Bắt đầu stream ngay tại đây

# --- ĐOẠN CODE GHI VÀO BUCKET CŨ (ĐÃ COMMENT LẠI) ---
# amazon_sales_writer = create_file_write_stream(
#     amazon_sales,
#     f"{GCS_STORAGE_PATH}/{AMAZON_SALES_TOPIC}",
#     f"{GCS_STORAGE_PATH}/checkpoint/{AMAZON_SALES_TOPIC}"
# )
# amazon_sales_writer.start() 
# ----------------------------------------------------------------

spark.streams.awaitAnyTermination()