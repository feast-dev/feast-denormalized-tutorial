import argparse
import logging
import json
import signal
import sys
import time
from random import uniform

from confluent_kafka import Producer
from session_generator.login_attempt import create_login_attempt_generator

logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate fake session data")
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--kafka-topic",
        default="fake_sessions",
        help="Kafka topic to produce to (default: fake_sessions)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    producer = Producer(
        {
            "bootstrap.servers": args.kafka_bootstrap_servers,
        }
    )

    logger.info(
        f"connection to {args.kafka_bootstrap_servers}, topic: {args.kafka_topic}"
    )

    def signal_handler(_sig, _frame):
        producer.flush()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    data_gen_func = create_login_attempt_generator(100, 50)

    logger.info("Event generator started...")
    while True:
        event = data_gen_func()
        producer.produce(
            args.kafka_topic,
            key=event.user_id,
            value=json.dumps(event.to_dict()).encode("utf-8"),
        )

        if len(producer) > 100:
            producer.flush()
        else:
            time.sleep(0.001)


if __name__ == "__main__":
    main()
