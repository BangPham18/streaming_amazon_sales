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
    StructField("order_id", StringType(), True),
    StructField("date", StringType(), True),  # Format: YYYY-MM-DD từ kafka_producer
    StructField("status", StringType(), True),
    StructField("fulfilment", StringType(), True),
    StructField("sales_channel", StringType(), True),
    StructField("ship_service_level", StringType(), True),
    StructField("style", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("category", StringType(), True),
    StructField("size", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("courier_status", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("ship_city", StringType(), True),
    StructField("ship_state", StringType(), True),
    StructField("ship_postal_code", StringType(), True),
    StructField("ship_country", StringType(), True),
    StructField("promotion_ids", StringType(), True),
    StructField("b2b", BooleanType(), True),
    # Thêm bởi kafka_producer.py
    StructField("ingestion_timestamp", DoubleType(), True)
])

# Dictionary để dễ sử dụng với nhiều topic (nếu cần mở rộng sau này)
schema = {
    'amazon_sales': amazon_sales_schema
}
