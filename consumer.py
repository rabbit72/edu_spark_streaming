import sys
import os.path

from pyspark.sql import SparkSession


def main():
    """
    This function for writing kafka messages from topic to files or to elasticsearch
    :return:
    """
    spark_app_name = "kafka_consumer"
    kafka_address = "172.18.0.2:6667"
    kafka_topic = "test"
    dir_for_checkpoint = "checkpoint"
    es_nodes = "localhost"
    es_index = "test"
    es_document_type = "booking"

    try:
        # read first argv as work dir for checkpoint or result
        output_dir = sys.argv[1]

        # read second argv as output format for result
        output = sys.argv[2]
        if output not in ("hdfs", "es"):
            exit("ERROR: Output can be only hdfs or es")
    except IndexError:
        exit(
            "ERROR: Enter an output directory as a first argument "
            "and output as a second"
        )

    # get dir name for checkpoint
    checkpoint_location = os.path.join(output_dir, dir_for_checkpoint)

    spark_session = SparkSession.builder.appName(spark_app_name).getOrCreate()

    # read kafka topic with spark stream
    lines = (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_address)
        .option("subscribe", kafka_topic)
        .load()
        .selectExpr("CAST(timestamp as TIMESTAMP)", "CAST(value AS STRING)")
    )

    query = lines.writeStream.outputMode("append").option(
        "checkpointLocation", checkpoint_location
    )

    if output == "hdfs":

        output_format = "csv"
        # get dir name for output files
        output_path = os.path.join(output_dir, output_format)

        # write spark stream to files
        query = query.format(output_format).option("path", output_path).start()
    elif output == "es":
        # write spark stream to elasticsearch
        query = (
            query.format("org.elasticsearch.spark.sql")
            .option("es.resource", "{0}/{1}".format(es_index, es_document_type))
            .option("es.nodes", es_nodes)
            .start()
        )

    query.awaitTermination()


if __name__ == "__main__":
    main()
