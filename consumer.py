import sys
import os.path

from pyspark.sql import SparkSession


def main():
    spark_app_name = "kafka_consumer"
    kafka_address = "172.18.0.2:6667"
    kafka_topic = "test"
    dir_for_checkpoint = "checkpoint"

    try:
        output_dir = sys.argv[1]
        output_format = sys.argv[2]
        if output_format not in ("csv", "parquet"):
            exit("ERROR: Output format can be only csv or parquet")
    except IndexError:
        exit(
            "ERROR: Enter an output directory as a first argument "
            "and output format as a second"
        )

    checkpoint_location = os.path.join(output_dir, dir_for_checkpoint)
    output_path = os.path.join(output_dir, output_format)

    spark_session = SparkSession.builder.appName(spark_app_name).getOrCreate()

    lines = (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_address)
        .option("subscribe", kafka_topic)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )

    query = (
        lines.writeStream.outputMode("append")
        .format(output_format)
        .option("checkpointLocation", checkpoint_location)
        .option("path", output_path)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
