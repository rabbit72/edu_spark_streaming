import sys
import os.path

from pyspark.sql import SparkSession


def main():
    """
    This function for writing kafka messages from topic to files with selected format
    :return:
    """
    spark_app_name = "kafka_consumer"
    kafka_address = "172.18.0.2:6667"
    kafka_topic = "test"
    dir_for_checkpoint = "checkpoint"

    try:
        # read first argv as output dir for result
        output_dir = sys.argv[1]

        # read second argv as output format for result
        output_format = sys.argv[2]
        if output_format not in ("csv", "parquet"):
            exit("ERROR: Output format can be only csv or parquet")
    except IndexError:
        exit(
            "ERROR: Enter an output directory as a first argument "
            "and output format as a second"
        )
    # get dir name for checkpoint
    checkpoint_location = os.path.join(output_dir, dir_for_checkpoint)

    # get dir name for output files
    output_path = os.path.join(output_dir, output_format)

    spark_session = SparkSession.builder.appName(spark_app_name).getOrCreate()

    # read kafka topic with spark stream
    lines = (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_address)
        .option("subscribe", kafka_topic)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )

    # write spark stream to files
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
