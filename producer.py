from kafka import KafkaProducer
import multiprocessing
import click


def send_to_kafka(line):
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDRESS)
    producer.send(KAFKA_TOPIC, line)
    producer.flush()


@click.command()
@click.argument('csv_file', type=click.File('rb'))
@click.option('--topic', '-t', default='test')
@click.option('--process_quantity', type=int, default=multiprocessing.cpu_count())
@click.option('--host', '-h', type=str, default='172.18.0.2')
@click.option('--port', '-p', type=int, default=6667)
@click.option('--header-pass', '-head', type=bool, default=True)
def main(csv_file, topic, process_quantity, host, port, header_pass):

    global KAFKA_TOPIC, KAFKA_ADDRESS
    KAFKA_TOPIC, KAFKA_ADDRESS = topic, f'{host}:{port}'
    if header_pass:
        next(csv_file)  # skip header
    with multiprocessing.Pool(process_quantity) as p:
        p.map(send_to_kafka, csv_file)


if __name__ == '__main__':
    main()
