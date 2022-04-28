import json
import logging
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from signal import SIG_IGN, SIGINT, SIGTERM, signal

import boto3

logger = logging.getLogger("squidslog")


class Consumer:
    def __init__(self, app, queue):
        self.app = app
        self.queue = queue

    def _prepare_task(self, message):
        body = json.loads(message.body)
        task = self.app._tasks[body["task"]]

        logger.info(
            f"Received task: {task.name}[{message.message_id}]",
            extra={
                "message_id": message.message_id,
                "task": task.name,
                "queue": self.queue.url,
            },
        )
        return task, message.message_id, body["args"], body["kwargs"]

    def consume_messages(self, options=None):
        if options is None:
            options = {}

        messages = self.queue.receive_messages(**options)
        for message in messages:
            yield message

    def consume(self, options=None):
        if options is None:
            options = {}

        for message in self.consume_messages(options):
            task, message_id, args, kwargs = self._prepare_task(message)
            task(message_id, *args, **kwargs)
            message.delete()


class ResourceLimitExceeded(Exception):
    pass


class ResourceTracker:
    def __init__(self, limit):
        self.limit = limit
        self._resources = set()

    def add(self, resource):
        if not self.has_available_space and resource not in self._resources:
            raise ResourceLimitExceeded()
        self._resources.add(resource)

    def remove(self, resource):
        self._resources.remove(resource)

    def available_space(self):
        return self.limit - len(self._resources)

    @property
    def has_available_space(self):
        return self.available_space() > 0


class ExitHandler:
    def __init__(self):
        self.should_exit = False
        self._install_soft_shutdown()

    def _install_soft_shutdown(self):
        signal(SIGINT, self._soft_signal_handler)
        signal(SIGTERM, self._soft_signal_handler)

    def _soft_signal_handler(self, signal, frame):
        logger.info(f"Received signal {signal}. Performing soft shutdown...")
        logger.info(
            f"Hit Ctrl+C again to perform a hard shutdown (will terminate all running tasks)."
        )
        self.should_exit = True
        self._install_hard_shutdown()

    def _install_hard_shutdown(self):
        signal(SIGINT, self._hard_signal_handler)
        signal(SIGTERM, self._hard_signal_handler)

    def _hard_signal_handler(self, signal, frame):
        sys.exit(signal)


def done_callback(future_tracker, task_name, queue_url, message, future):
    # this runs in the main loop process
    try:
        future.result()
    except Exception:
        logger.exception(
            "Task failed",
            extra={
                "message_id": message.message_id,
                "task": task_name,
                "queue": queue_url,
            },
        )
    else:
        # Q: is it ok to be doing these deletes in the main process instead of workers,
        #    or is the latency from this going to cause to much blocking?
        message.delete()
        logger.info(
            f"Completed task: {task_name}[{message.message_id}]",
            extra={
                "message_id": message.message_id,
                "task": task_name,
                "queue": queue_url,
            },
        )
    finally:
        future_tracker.remove(future)


def initializer():
    # Handles issue where KeyboardInterrupt isn't handled properly in child processes.
    signal(SIGINT, SIG_IGN)


def run_loop(app, queue_name, n_workers, report_interval, polling_wait_time):
    exit_handler = ExitHandler()
    future_tracker = ResourceTracker(limit=n_workers * 2)
    sqs_client = boto3.client("sqs", **app.config)
    queue = app.get_queue_by_name(queue_name)
    consumer = Consumer(app, queue)
    last_report_queue_stats = time.time()

    with ProcessPoolExecutor(
        max_workers=n_workers, initializer=initializer
    ) as executor:
        while not exit_handler.should_exit:
            elapsed = time.time() - last_report_queue_stats
            if elapsed >= report_interval and app._report_queue_stats is not None:
                logger.info(
                    f"Reporting queue stats after {elapsed} seconds",
                    extra={"queue": queue.url, "report_interval": report_interval},
                )

                attrs = sqs_client.get_queue_attributes(
                    QueueUrl=queue.url, AttributeNames=["All"]
                )
                app._report_queue_stats(queue_name, attrs)
                last_report_queue_stats = time.time()

            if future_tracker.has_available_space:
                for message in consumer.consume_messages(
                    options={
                        "MaxNumberOfMessages": min(
                            future_tracker.available_space(), 10
                        ),
                        "WaitTimeSeconds": polling_wait_time,
                    }
                ):
                    task, message_id, args, kwargs = consumer._prepare_task(message)
                    future = executor.submit(task, message_id, *args, **kwargs)
                    future_tracker.add(future)
                    future.add_done_callback(
                        partial(
                            done_callback,
                            future_tracker,
                            task.name,
                            queue.url,
                            message,
                        )
                    )
            else:
                time.sleep(0.1)
