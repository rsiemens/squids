from __future__ import annotations

import logging
import sys
import time
from concurrent.futures import Future, ProcessPoolExecutor
from functools import partial
from signal import SIG_IGN, SIGINT, SIGTERM, signal
from typing import TYPE_CHECKING, Any, Dict, Hashable, Iterator, Optional, Set

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import MessageTypeDef

    from squids import App

logger = logging.getLogger("squidslog")


class Consumer:
    """
    Object which consumes messages from the provided SQS queue and executes the appropriate
    :class:`.Task` for a message. Usually you'll create a consumer instance through the
    :meth:`.App.create_consumer` method instead of directly instantiating one yourself.

    :param app: An instance of :class:`.App`.
    :param queue_url: The queue URL.
    """

    def __init__(self, app: App, queue_url: str):
        self.app = app
        self.queue_url = queue_url

    def _prepare_task(self, message: MessageTypeDef):
        body = self.app._serde.deserialize(message["Body"])
        task = self.app._tasks[body["task"]]
        message_id = message["MessageId"]

        return task, message_id, body["args"], body["kwargs"]

    def consume_messages(
        self, options: Optional[Dict] = None
    ) -> Iterator[MessageTypeDef]:
        """
        .. _SQS.Client.receive_message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html

        Consume messages from the associated queue. Unlike :meth:`.Consumer.consume` this method
        returns a generator over all the messages returned by calling `SQS.Client.receive_message`_.

        :param options: A dict of optional values to pass to `SQS.Client.receive_message`_.
        :return: A generator that yields message dicts.
        """
        if options is None:
            options = {}

        response = self.app.sqs.receive_message(QueueUrl=self.queue_url, **options)
        for message in response.get("Messages", []):
            yield message

    def consume(self, options: Optional[Dict] = None) -> None:
        """
        .. _SQS.Client.receive_message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html

        Consumes messages from the associated queue and runs the appropriate :class:`.Task` for
        each message. Calling this only consumes as many messages as `SQS.Client.receive_message`_
        returns. If you want to consume continuously then you'll need to put calls to in a loop.

        :param options: A dict of optional values to pass to `SQS.Client.receive_message`_.
        :return:
        """
        if options is None:
            options = {}

        for message in self.consume_messages(options):
            self.run_task(message)

    def run_task(self, message: MessageTypeDef) -> Any:
        task, message_id, args, kwargs = self._prepare_task(message)
        logger.info(
            f"Received task: {task.name}[{message_id}]",
            extra={
                "message_id": message_id,
                "task": task.name,
                "queue": self.queue_url,
            },
        )

        try:
            result = task(message_id, *args, **kwargs)
        except Exception as e:
            logger.exception(
                "Task failed",
                extra={
                    "message_id": message_id,
                    "task": task.name,
                    "queue": self.queue_url,
                },
            )
            raise e

        logger.info(
            f"Completed task: {task.name}[{message_id}]",
            extra={
                "message_id": message_id,
                "task": task.name,
                "queue": self.queue_url,
            },
        )
        # should this delete on exception?
        self.app.sqs.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"]
        )
        return result


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
    future: Future,
):
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
    queue_url = app.get_queue_by_name(queue_name)
    consumer = Consumer(app, queue_url)

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
                    future = executor.submit(consumer.run_task, message)
                    future_tracker.add(future)
                    future.add_done_callback(
                        partial(
                            done_callback,
                            future_tracker,
                        )
                    )
            else:
                time.sleep(0.1)
