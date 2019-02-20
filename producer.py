import multiprocessing
import time

import click

from kafka import KafkaProducer


def send_to_kafka(line: str) -> None:
    """
    Function for sending message to kafka topic
    :param str line: message for kafka topic
    :return: None
    """
    # sleep before sending for delay
    time.sleep(KAFKA_DELAY)
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDRESS)
    producer.send(KAFKA_TOPIC, line.rstrip().encode("utf8"))
    producer.flush()


def chunk_generator(iterable: iter, chunksize: int) -> list:
    """
    Chunk generator takes chunksize-elements and yield it as list
    :param iterable: Source for chunk generating
    :param chunksize: quantity of elements in each chunk
    :return: chunk with chunksize-elements from iter
    :rtype list
    """
    if not isinstance(chunksize, int) or chunksize <= 0:
        raise ValueError(
            f"Chunksize must be integer above zero. Current value: {chunksize}"
        )
    while True:
        chunk: list = []
        for _ in range(chunksize):
            try:
                # read next line and add to chunk until StopIteration
                chunk.append(next(iterable))
            except StopIteration:
                if chunk:
                    yield chunk
                raise StopIteration
        yield chunk


@click.command()
@click.argument("csv_file", type=click.File("r"))
@click.option("--topic", "-t", default="test")
@click.option("--process_quantity", type=int, default=multiprocessing.cpu_count())
@click.option("--host", "-h", type=str, default="172.18.0.2")
@click.option("--port", "-p", type=int, default=6667)
@click.option("--header-pass", "-head", is_flag=True, default=False)
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
        row_quantity = 0
        for chunk in chunk_generator(csv_file, chunksize):
            print(f"Sending {chunk_number} chank of {chunksize} rows")
            p.map(send_to_kafka, chunk)
            chunk_number += 1
            row_quantity += len(chunk)
    print(f"{row_quantity} rows were sent successfully")


if __name__ == "__main__":
    main()
