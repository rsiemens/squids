import functools
import inspect
import json

import boto3

from squids.consumer import Consumer


class App:
    def __init__(self, name, config=None):
        """
        :param name: name for the app
        :param config: Dict of kwargs from https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.resource
        """
        self.config = config or {}
        self.name = name
        self.sqs = boto3.resource("sqs", **self.config)
        self._tasks = {}
        self._pre_task = None
        self._post_task = None
        self._pre_send = None
        self._post_send = None
        self._report_queue_stats = None

    def task(self, queue):
        def wrapper(func):
            task = Task(
                self,
                queue,
                func=func,
                pre_task=self._pre_task,
                post_task=self._post_task,
            )
            self._tasks[task.name] = task
            # We need to return func to get around some pickling issues where pickle
            # will need to see the original function.
            # https://stackoverflow.com/questions/52185507/pickle-and-decorated-classes-picklingerror-not-the-same-object
            func.send = task.send
            func.send_job = task.send_job
            return func

        return wrapper

    def add_task(self, task_cls):
        task = task_cls(
            self,
            task_cls.queue,
            pre_task=self._pre_task,
            post_task=self._post_task,
        )
        self._tasks[task.name] = task
        return task

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

    def report_queue_stats(self, func):
        self._report_queue_stats = func
        return func

    @functools.lru_cache()
    def get_queue_by_name(self, queue_name):
        return self.sqs.get_queue_by_name(QueueName=queue_name)

    @functools.lru_cache()
    def get_consumer(self, queue_name):
        queue = self.get_queue_by_name(queue_name)
        return Consumer(self, queue)


class Task:
    def __init__(self, app, queue=None, func=None, pre_task=None, post_task=None):
        self.queue = queue
        if func:
            self.name = f"{func.__module__}.{func.__qualname__}"
        else:
            self.name = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        self.func = func
        self.pre_task = pre_task
        self.post_task = post_task
        # NB: This isn't available on the consumer side because of pickling.
        #  see the note in __getstate__
        self.app = app
        self.signature = inspect.signature(func if func else self.run)
        # set on the consumer side via __call__
        self.id = None

    def send_job(self, args, kwargs, options=None):
        if options is None:
            options = {}

        queue = self.app.get_queue_by_name(self.queue)
        # will raise TypeError if the signature doesn't match
        bound = self.signature.bind(*args, **kwargs)
        body = {
            "task": self.name,
            "args": bound.args,
            "kwargs": bound.kwargs,
        }

        if self.app._pre_send is not None:
            self.app._pre_send(self.queue, body)

        response = queue.send_message(MessageBody=json.dumps(body), **options)

        if self.app._post_send is not None:
            self.app._post_send(self.queue, body, response)

        return response

    def send(self, *args, **kwargs):
        return self.send_job(args, kwargs)

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
