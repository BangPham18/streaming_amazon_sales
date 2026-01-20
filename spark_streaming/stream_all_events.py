# Run the script using the following command
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
# stream_all_events.py

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
GCP_GCS_BUCKET = os.getenv("GCP_GCS_BUCKET", 'streaming_amazon_sales')
GCS_STORAGE_PATH = f'gs://{GCP_GCS_BUCKET}'

# Initialize a spark session
spark = create_or_get_spark_session('Amazon Sales Stream')
spark.streams.resetTerminated()

# Amazon sales stream
amazon_sales = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, AMAZON_SALES_TOPIC)
amazon_sales = process_stream(
    amazon_sales, schema[AMAZON_SALES_TOPIC], AMAZON_SALES_TOPIC)

# Write to storage every 2 minutes in parquet format
amazon_sales_writer = create_file_write_stream(
    amazon_sales,
    f"{GCS_STORAGE_PATH}/{AMAZON_SALES_TOPIC}",
    f"{GCS_STORAGE_PATH}/checkpoint/{AMAZON_SALES_TOPIC}"
)

amazon_sales_writer.start()

spark.streams.awaitAnyTermination()
