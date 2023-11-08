from __future__ import annotations

import logging
import sys
import time
from concurrent.futures import Future, ProcessPoolExecutor
from functools import partial
from signal import SIG_IGN, SIGINT, SIGTERM, signal
from typing import TYPE_CHECKING, Dict, Hashable, Optional, Set

import boto3

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import Message, Queue

    from squids import App

logger = logging.getLogger("squidslog")


class Consumer:
    """
    Object which consumes messages from the provided SQS queue and executes the appropriate
    :class:`.Task` for a message. Usually you'll create a consumer instance through the
    :meth:`.App.create_consumer` method instead of directly instantiating one yourself.

    :param app: An instance of :class:`.App`.
    :param queue: An instance of `SQS.Queue <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue>`_.
    """

    def __init__(self, app: App, queue: Queue):
        self.app = app
        self.queue = queue

    def _prepare_task(self, message: Message):
        body = self.app._serde.deserialize(message.body)
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

    def consume_messages(self, options: Optional[Dict] = None):
        """
        .. _SQS.Message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#message
        .. _SQS.Queue.receive_messages: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages

        Consume messages from the associated queue. Unlike :meth:`.Consumer.consume` this method
        returns a generator over all the `SQS.Message`_ s returned by calling
        `SQS.Queue.receive_messages`_.

        :param options: A dict of optional values to pass to `SQS.Queue.receive_messages`_.
        :return: A generator that yields `SQS.Message`_ s.
        """
        if options is None:
            options = {}

        messages = self.queue.receive_messages(**options)
        for message in messages:
            yield message

    def consume(self, options: Optional[Dict] = None):
        """
        .. _SQS.Queue.receive_messages: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages

        Consumes messages from the associated queue and runs the appropriate :class:`.Task` for
        each message. Calling this only consumes as many messages as `SQS.Queue.receive_messages`_
        returns. If you want to consume continuously then you'll need to put calls to in a loop.

        :param options: A dict of optional values to pass to `SQS.Queue.receive_messages`_.
        :return:
        """
        if options is None:
            options = {}

        for message in self.consume_messages(options):
            task, message_id, args, kwargs = self._prepare_task(message)
            task(message_id, *args, **kwargs)
            message.delete()


class ResourceLimitExceeded(Exception):
    pass


class ResourceTracker:
    def __init__(self, limit: int):
        self.limit = limit
        self._resources: Set[Hashable] = set()

    def add(self, resource: Hashable):
        if not self.has_available_space and resource not in self._resources:
            raise ResourceLimitExceeded()
        self._resources.add(resource)

    def remove(self, resource: Hashable):
        self._resources.remove(resource)

    def available_space(self) -> int:
        return self.limit - len(self._resources)

    @property
    def has_available_space(self) -> int:
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


def done_callback(
    future_tracker: ResourceTracker,
    task_name: str,
    queue_url: str,
    message: Message,
    future: Future,
):
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


def run_loop(
    app: App,
    queue_name: str,
    n_workers: int,
    polling_wait_time: int,
    visibility_timeout: int,
):
    exit_handler = ExitHandler()
    future_tracker = ResourceTracker(limit=n_workers * 2)
    sqs_client = boto3.client("sqs", **app.boto_config)
    queue = app.get_queue_by_name(queue_name)
    consumer = Consumer(app, queue)

    with ProcessPoolExecutor(
        max_workers=n_workers, initializer=initializer
    ) as executor:
        while not exit_handler.should_exit:
            if future_tracker.has_available_space:
                for message in consumer.consume_messages(
                    options={
                        "MaxNumberOfMessages": min(
                            future_tracker.available_space(), 10
                        ),
                        "WaitTimeSeconds": polling_wait_time,
                        "VisibilityTimeout": visibility_timeout,
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
