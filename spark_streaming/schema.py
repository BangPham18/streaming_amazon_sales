from pyspark.sql.types import (IntegerType,
                               StringType,
                               DoubleType,
                               StructField,
                               StructType,
                               BooleanType,
                               TimestampType)

# Schema cho Amazon Sales data từ Kafka
amazon_sales_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Date", StringType(), True),  # Format: YYYY-MM-DD từ kafka_producer
    StructField("Status", StringType(), True),
    StructField("Fulfilment", StringType(), True),
    StructField("Sales Channel", StringType(), True),
    StructField("ship-service-level", StringType(), True),
    StructField("Style", StringType(), True),
    StructField("SKU", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Size", StringType(), True),
    StructField("ASIN", StringType(), True),
    StructField("Courier Status", StringType(), True),
    StructField("Qty", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("ship-city", StringType(), True),
    StructField("ship-state", StringType(), True),
    StructField("ship-postal-code", StringType(), True),
    StructField("ship-country", StringType(), True),
    StructField("promotion-ids", StringType(), True),
    StructField("B2B", BooleanType(), True),
    # Thêm bởi kafka_producer.py
    StructField("ingestion_timestamp", DoubleType(), True)
])

# Dictionary để dễ sử dụng với nhiều topic (nếu cần mở rộng sau này)
schema = {
    'amazon_sales': amazon_sales_schema
}
