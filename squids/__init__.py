import inspect
import json
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from signal import SIG_IGN, SIGINT, SIGTERM, signal

import boto3

logger = logging.getLogger("squidslog")
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)


class App:
    def __init__(self, name, config=None):
        """
        :param name: name for the app
        :param config: Dict of kwargs from https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.resource
        """
        config = config or {}
        self.name = name
        self.sqs = boto3.resource("sqs", **config)
        self._tasks = {}
        self._pre_task = None
        self._post_task = None
        self._pre_send = None
        self._post_send = None

    def task(self, queue_name):
        def wrapper(func):
            task = Task(
                queue_name,
                func,
                pre_task=self._pre_task,
                post_task=self._post_task,
                app=self,
            )
            self._tasks[func.__name__] = task
            # We need to return func to get around some pickling issues where pickle
            # will need to see the original function.
            # https://stackoverflow.com/questions/52185507/pickle-and-decorated-classes-picklingerror-not-the-same-object
            func.send = task.send
            return func

        return wrapper

    def pre_task(self, func):
        self._pre_task = func
        return func

    def post_task(self, func):
        self._post_task = func
        return func

    def pre_send(self, func):
        self._pre_send = func
        return func

    def post_send(self, func):
        self._post_send = func
        return func


class Task:
    def __init__(self, queue_name, func, pre_task, post_task, app=None):
        self.queue_name = queue_name
        self.name = func.__name__
        self.func = func
        self.pre_task = pre_task
        self.post_task = post_task
        self.app = app
        self.signature = inspect.signature(func)
        # set on the consumer side via __call__
        self.id = None

    def send(self, *args, **kwargs):
        queue = self.app.sqs.get_queue_by_name(QueueName=self.queue_name)
        # will raise TypeError if the signature doesn't match
        bound = self.signature.bind(*args, **kwargs)
        body = json.dumps(
            {
                "task": self.name,
                "args": bound.args,
                "kwargs": bound.kwargs,
            }
        )

        if self.app._pre_send is not None:
            self.app._pre_send(self.queue_name, body)

        response = queue.send_message(MessageBody=body)

        if self.app._post_send is not None:
            self.app._post_send(self.queue_name, body, response)

        return response

    def run(self, *args, **kwargs):
        if self.func is not None:
            return self.func(*args, **kwargs)

    def __getstate__(self):
        state = {**self.__dict__}
        # We only need the `app` for sending to SQS, but since boto3 does some dynamic
        # classes, the sqs resource isn't actually picklable. This solves that.
        state.pop("app")
        return state

    def __call__(self, message_id, *args, **kwargs):
        self.id = message_id

        if self.pre_task is not None:
            self.pre_task(self)

        result = self.run(*args, **kwargs)

        if self.post_task is not None:
            self.post_task(self)

        return result


class ExitHandler:
    def __init__(self):
        self.should_exit = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        self.should_exit = True


class TrackedResource:
    def __init__(self, limit=10):
        self.limit = limit
        self._resources = set()

    def add(self, resource):
        self._resources.add(resource)

    def remove(self, resource):
        self._resources.remove(resource)

    def available_space(self):
        return self.limit - len(self._resources)

    @property
    def has_available_space(self):
        return self.available_space() > 0


def done_callback(future_tracker, task_name, queue_name, message, future):
    # this runs in the main loop process
    try:
        future.result()
    except Exception:
        logger.exception(
            "Task failed",
            extra={
                "message_id": message.message_id,
                "task": task_name,
                "queue": queue_name,
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
                "queue": queue_name,
            },
        )
    finally:
        future_tracker.remove(future)


def initializer():
    # Handles issue where KeyboardInterrupt isn't handled properly in child processes.
    signal(SIGINT, SIG_IGN)


def run_loop(app, queue_name, n_workers):
    exit_handler = ExitHandler()
    future_tracker = TrackedResource(limit=n_workers * 2)
    sqs = boto3.resource("sqs", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    with ProcessPoolExecutor(
        max_workers=n_workers, initializer=initializer
    ) as executor:
        while not exit_handler.should_exit:
            if future_tracker.has_available_space:
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
                            "queue": queue_name,
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
                            queue_name,
                            message,
                        )
                    )
            else:
                time.sleep(0.1)
