from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, dayofmonth, year, to_date


def create_or_get_spark_session(app_name, master="yarn"):
    """
    Creates or gets a Spark Session

    Parameters:
        app_name : str
            Pass the name of your app
        master : str
            Choosing the Spark master, yarn is the default
    Returns:
        spark: SparkSession
    """
    spark = (SparkSession
             .builder
             .appName(app_name)
             .master(master=master)
             .getOrCreate())

    return spark


def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset="earliest"):
    """
    Creates a kafka read stream

    Parameters:
        spark : SparkSession
            A SparkSession object
        kafka_address: str
            Host address of the kafka bootstrap server
        topic : str
            Name of the kafka topic
        starting_offset: str
            Starting offset configuration, "earliest" by default 
    Returns:
        read_stream: DataStreamReader
    """

    read_stream = (spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port}")
                   .option("failOnDataLoss", False)
                   .option("startingOffsets", starting_offset)
                   .option("subscribe", topic)
                   .option("maxOffsetsPerTrigger", 1000)
                   .load())

    return read_stream


def process_stream(stream, stream_schema, topic):
    """
    Process stream to fetch value from the kafka message.
    Parse the Date field and produce year, month, day columns for partitioning.
    
    Parameters:
        stream : DataStreamReader
            The data stream reader for your stream
        stream_schema : StructType
            The schema for parsing the JSON data
        topic : str
            The Kafka topic name
    Returns:
        stream: DataStreamReader
    """

    # Read only value from the incoming message and convert the contents
    # inside to the passed schema
    stream = (stream
              .selectExpr("CAST(value AS STRING)")
              .select(
                  from_json(col("value"), stream_schema).alias("data")
              )
              .select("data.*")
              )

    # For Amazon Sales data: parse Date field and add year, month, day for partitioning
    # The date field format is: YYYY-MM-DD (from kafka_producer)
    stream = (stream
              .withColumn("order_date", to_date(col("date"), "yyyy-MM-dd"))
              .withColumn("year", year(col("order_date")))
              .withColumn("month", month(col("order_date")))
              .withColumn("day", dayofmonth(col("order_date")))
              .drop("order_date")  # Drop temporary column
              )

    return stream


def create_file_write_stream(stream, storage_path, checkpoint_path, trigger="120 seconds", output_mode="append", file_format="parquet"):
    """
    Write the stream back to a file store

    Parameters:
        stream : DataStreamReader
            The data stream reader for your stream
        file_format : str
            parquet, csv, orc etc
        storage_path : str
            The file output path
        checkpoint_path : str
            The checkpoint location for spark
        trigger : str
            The trigger interval
        output_mode : str
            append, complete, update
    """

    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    .partitionBy("year", "month", "day")
                    .option("path", storage_path)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream