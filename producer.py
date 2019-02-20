import multiprocessing
import time

import click

from kafka import KafkaProducer


def send_to_kafka(line):
    # sleep before sending for delay
    time.sleep(KAFKA_DELAY)
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDRESS)
    producer.send(KAFKA_TOPIC, line.rstrip().encode("utf8"))
    producer.flush()


def batch_generator(iterable: iter, batchsize: int) -> list:
    """
    Batch generator takes batchsize-elements and yield it as list
    :param iterable: Source for batch generating
    :param batchsize: quantity of elements in each batch
    :return: batch with batchsize-elements from iter
    :rtype list
    """
    while True:
        batch: list = []
        for _ in range(batchsize):
            try:
                # read next line and add to batch until StopIteration
                batch.append(next(iterable))
            except StopIteration:
                if batch:
                    yield batch
                raise StopIteration
        yield batch


@click.command()
@click.argument("csv_file", type=click.File("r"))
@click.option("--topic", "-t", default="test")
@click.option("--process_quantity", type=int, default=multiprocessing.cpu_count())
@click.option("--host", "-h", type=str, default="172.18.0.2")
@click.option("--port", "-p", type=int, default=6667)
@click.option("--header-pass", "-head", type=bool, default=True)
@click.option("--chunksize", "-s", type=int, default=500)
@click.option(
    "--delay",
    "-d",
    type=float,
    default=0,
    help="Seconds for delay before sending message",
)
def main(csv_file, topic, process_quantity, host, port, header_pass, delay, chunksize):

    # global variables for send_to_kafka function in other processes
    global KAFKA_TOPIC, KAFKA_ADDRESS, KAFKA_DELAY
    KAFKA_TOPIC = topic
    KAFKA_ADDRESS = f"{host}:{port}"
    KAFKA_DELAY = delay

    if header_pass:
        next(csv_file)  # skip header

    # pool processes for parallel sending
    with multiprocessing.Pool(process_quantity) as p:
        chunk_number = 1
        for batch in batch_generator(csv_file, chunksize):
            print(f"Sending {chunk_number} chank of {chunksize} rows")
            p.map(send_to_kafka, batch)
            chunk_number += 1
    print("All rows were sent successfully")


if __name__ == "__main__":
    main()
