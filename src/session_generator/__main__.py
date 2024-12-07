import json
import signal
import time
import sys
from random import uniform

from confluent_kafka import Producer
from login_attempt import create_login_attempt_generator

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fake_sessions"


def main():
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
        }
    )

    def signal_handler(_sig, _frame):
        producer.flush()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    data_gen_func = create_login_attempt_generator(100, 50)

    print("Event generator started...")
    while True:
        event = data_gen_func()
        producer.produce(
            TOPIC,
            key=event.user_id,
            value=json.dumps(event.to_dict()).encode("utf-8"),
        )

        if len(producer) > 100:
            producer.flush()
        else:
            # time.sleep(uniform(0.0001, 0.001))
            time.sleep(0.001)


if __name__ == "__main__":
    main()
