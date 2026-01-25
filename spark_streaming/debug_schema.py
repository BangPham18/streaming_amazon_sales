import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, year, month, dayofmonth
from schema import schema

# Initialize Spark Session (Local)
spark = SparkSession.builder \
    .appName("Debug Schema") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Sample JSON message from user
sample_json = {
  "order_id": "406-4611677-3362720",
  "date": "2022-04-18",
  "status": "Shipped - Delivered to Buyer",
  "fulfilment": "Merchant",
  "sales_channel": "Amazon.in",
  "ship_service_level": "Expedited",
  "style": "SET331",
  "sku": "JNE3440-KR-N-XS",
  "category": "kurta",
  "size": "XXL",
  "asin": "B09831NCG7",
  "courier_status": "Shipped",
  "qty": 1,
  "currency": "INR",
  "amount": 1126.39,
  "ship_city": "Robertchester",
  "ship_state": "Wisconsin",
  "ship_postal_code": "98436",
  "ship_country": "IN",
  "promotion_ids": "...",
  "b2b": False,
  "ingestion_timestamp": 1769244441.3804066
}

json_str = json.dumps(sample_json)
print(f"Testing JSON: {json_str}")

# Create DataFrame
df = spark.createDataFrame([(json_str,)], ["value"])

# Apply the same logic as streaming_functions.py
parsed_df = df \
    .select(from_json(col("value"), schema['amazon_sales']).alias("data")) \
    .select("data.*")

print("\n--- Parsed DataFrame Schema ---")
parsed_df.printSchema()

print("\n--- Parsed Data ---")
parsed_df.show(truncate=False)

# Check for nulls in critical columns
row = parsed_df.first()
if row is None:
    print("Row is None!")
else:
    print(f"order_id: {row['order_id']}")
    print(f"date: {row['date']}")
    
    # Check date parsing logic
    date_df = parsed_df.withColumn("order_date", to_date(col("date"), "yyyy-MM-dd"))
    date_row = date_df.first()
    print(f"order_date: {date_row['order_date']}")
    
    # Check if index is null (expected)
    print(f"index: {row['index']}")

spark.stop()
