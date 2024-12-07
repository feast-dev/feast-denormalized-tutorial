import json
import logging
import multiprocessing
import sys

from stream_job import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s",
)


def cleanup_processes(processes):
    for p in processes:
        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():
                p.kill()


def main():
    logger = multiprocessing.log_to_stderr()
    logger.info("Starting main process")
    processes = []

    try:
        for window_length in [1, 5, 10, 15]:
            process = multiprocessing.Process(
                target=run_pipeline,
                args=(window_length * 1000 * 60, 1000, f"{window_length}"),
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
