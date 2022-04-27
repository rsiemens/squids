import json
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from queue import Queue
from signal import SIG_IGN, SIGINT, SIGTERM, signal

import boto3

logger = logging.getLogger("squidslog")
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)


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


class RoundRobinScheduler:
    def __init__(self, items):
        self._queue = Queue()
        for item in items:
            self._queue.put(item)

    def next(self):
        next = self._queue.get(block=False)
        self._queue.put(next)
        return next


class ExitHandler:
    def __init__(self):
        self.should_exit = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        logger.info(f"Received signal {signal}. Shutting down...")
        self.should_exit = True


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


def run_loop(app, queue_names, n_workers):
    exit_handler = ExitHandler()
    future_tracker = ResourceTracker(limit=n_workers * 2)
    sqs = boto3.resource("sqs", **app.config)
    sqs_queues = [sqs.get_queue_by_name(QueueName=q) for q in queue_names]
    scheduler = RoundRobinScheduler(sqs_queues)

    with ProcessPoolExecutor(
        max_workers=n_workers, initializer=initializer
    ) as executor:
        while not exit_handler.should_exit:
            if future_tracker.has_available_space:
                queue = scheduler.next()
                messages = queue.receive_messages(
                    MaxNumberOfMessages=min(future_tracker.available_space(), 10),
                    WaitTimeSeconds=1,
                )
                for message in messages:
                    body = json.loads(message.body)
                    task = app._tasks[body["task"]]

                    logger.info(
                        f"Received task: {task.name}[{message.message_id}]",
                        extra={
                            "message_id": message.message_id,
                            "task": task.name,
                            "queue": queue.url,
                        },
                    )
                    future = executor.submit(
                        task, message.message_id, *body["args"], **body["kwargs"]
                    )
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
