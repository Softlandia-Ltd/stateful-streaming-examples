"""Example of stateful streaming with Spark."""

import plac
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, from_unixtime, to_timestamp


@plac.opt("addr", "Broker address")
@plac.opt("topic", "Kafka topic")
@plac.opt("win", "Window length in seconds")
def main(addr: str = "127.0.0.1:9092", topic: str = "iot", win: int = 10):
    """Run the streaming program."""
    data_col = "data"
    ts_col = "ts"

    spark = SparkSession.builder.appName("IoTStream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # This is the raw Kafka payload with Kafka metadata attached
    stream_df = (
        spark.readStream.format("kafka")
        .option("startingOffsets", "latest")
        .option("kafka.bootstrap.servers", addr)
        .option("subscribe", topic)
        .load()
    )

    # Here we make a new DataFrame with just the JSON payload in one field
    json_df = stream_df.select(
        from_json(col("value").cast("string"), "id INT, time DOUBLE, value INT").alias(
            data_col
        )
    )

    # Unpack the JSON, discard the event time
    value_df = json_df.select(
        col(f"{data_col}.id"),
        col(f"{data_col}.value"),
        to_timestamp(from_unixtime(col(f"{data_col}.time"))).alias(ts_col),
    )

    # Tumbling window and grouping
    windowed_sum_df = (
        # Without the watermark we cannot use the append mode, only update and complete
        value_df.withWatermark(ts_col, f"{win} seconds").groupBy(
            window(ts_col, f"{win} seconds"), value_df.id
        )
    ).sum("value")

    query = (
        windowed_sum_df.writeStream.outputMode("update").format("console")
        # .trigger(processingTime=f"{win} seconds") # to reduce output spam
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    plac.call(main)
