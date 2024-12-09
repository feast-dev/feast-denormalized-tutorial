import argparse
import logging
import multiprocessing
import sys

from feast import feature

from pipeline.stream_job import PipelineConfig, run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s",
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run streaming pipelines with configurable Kafka settings"
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--kafka-topic",
        default="fake_sessions",
        help="Kafka topic to consume from (default: fake_sessions)",
    )
    return parser.parse_args()


def cleanup_processes(processes):
    for p in processes:
        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():
                p.kill()


def main():
    args = parse_args()
    logger = multiprocessing.log_to_stderr()
    logger.info("Starting main process")
    processes = []

    try:
        # create pipelines for 1s, 5s, 10s, and 15s rolling windows
        for window_length in [1, 5, 10, 15]:
            config = PipelineConfig(
                window_length_ms=window_length * 1000,
                slide_length_ms=1000,
                feature_prefix=f"{window_length}",
                kafka_bootstrap_servers=args.kafka_bootstrap_servers,
                kafka_topic=args.kafka_topic,
            )
            process = multiprocessing.Process(
                target=run_pipeline,
                args=(config,),
                name=f"PipelineProcess-{window_length}",
                daemon=False,
            )
            processes.append(process)

        for p in processes:
            try:
                p.start()
            except Exception as e:
                logger.error(f"Failed to start process {p.name}: {e}")
                cleanup_processes(processes)
                return

        while True:
            for p in processes:
                if not p.is_alive():
                    logger.error(f"Process {p.name} died unexpectedly")
                    cleanup_processes(processes)
                    sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        cleanup_processes(processes)
    except Exception as e:
        logger.error(f"Unexpected error in main process: {e}")
        cleanup_processes(processes)
        sys.exit(1)


if __name__ == "__main__":
    main()
