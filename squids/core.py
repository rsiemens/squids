from __future__ import annotations

import functools
import inspect
import json
import sys
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Type

import boto3

from squids.consumer import Consumer

if TYPE_CHECKING:
    from mypy_boto3_sqs.literals import QueueAttributeNameType
    from mypy_boto3_sqs.service_resource import Queue
    from mypy_boto3_sqs.type_defs import SendMessageResultTypeDef

    PreTaskCallback = Callable[[Task], None]
    PostTaskCallback = Callable[[Task], None]
    PreSendCallback = Callable[[str, Dict], None]
    PostSendCallback = Callable[[str, Dict, SendMessageResultTypeDef], None]
    ReportQueueStatsCallback = Callable[[str, Dict[QueueAttributeNameType, str]], None]


class App:
    def __init__(self, name: str, config: Optional[Dict] = None):
        self.config = config or {}
        self.name = name
        self.sqs = boto3.resource("sqs", **self.config)
        self._tasks: Dict[str, Task] = {}
        self._pre_task: Optional[PreTaskCallback] = None
        self._post_task: Optional[PostTaskCallback] = None
        self._pre_send: Optional[PreSendCallback] = None
        self._post_send: Optional[PostSendCallback] = None
        self._report_queue_stats: Optional[ReportQueueStatsCallback] = None

    def task(self, queue: str) -> Callable:
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

    def add_task(self, task_cls: Type[Task]) -> Task:
        task = task_cls(
            self,
            task_cls.queue,
            pre_task=self._pre_task,
            post_task=self._post_task,
        )
        self._tasks[task.name] = task
        return task

    def pre_task(self, func: PreTaskCallback) -> PreTaskCallback:
        self._pre_task = func
        return func

    def post_task(self, func: PostTaskCallback) -> PostTaskCallback:
        self._post_task = func
        return func

    def pre_send(self, func: PreSendCallback) -> PreSendCallback:
        self._pre_send = func
        return func

    def post_send(self, func: PostSendCallback) -> PostSendCallback:
        self._post_send = func
        return func

    def report_queue_stats(
        self, func: ReportQueueStatsCallback
    ) -> ReportQueueStatsCallback:
        self._report_queue_stats = func
        return func

    @functools.lru_cache()
    def get_queue_by_name(self, queue_name: str) -> Queue:
        return self.sqs.get_queue_by_name(QueueName=queue_name)

    def create_consumer(self, queue_name: str) -> Consumer:
        queue = self.get_queue_by_name(queue_name)
        return Consumer(self, queue)

    def __getstate__(self):
        state = {**self.__dict__}
        state.pop("sqs")
        return state

    def __setstate__(self, state):
        state["sqs"] = boto3.resource("sqs", **state["config"])
        return state


class Task:
    def __init__(
        self,
        app: App,
        queue: str,
        func: Optional[Callable] = None,
        pre_task: Optional[PreTaskCallback] = None,
        post_task: Optional[PostTaskCallback] = None,
    ):
        self.queue = queue

        mod = func.__module__ if func else self.__class__.__module__
        if mod == "__main__":
            mod = sys.argv[0].rsplit("/", 1)[-1].rstrip(".py")

        if func:
            self.name = f"{mod}.{func.__qualname__}"
        else:
            self.name = f"{mod}.{self.__class__.__qualname__}"

        self.func = func
        self.pre_task = pre_task
        self.post_task = post_task
        # NB: This isn't available on the consumer side because of pickling.
        #  see the note in __getstate__
        self.app = app
        self.signature = inspect.signature(func if func else self.run)
        # set on the consumer side via __call__
        self.id: Optional[str] = None

    def send_job(
        self,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        options: Dict = None,
    ) -> SendMessageResultTypeDef:
        if options is None:
            options = {}
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = {}

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

    def send(self, *args, **kwargs) -> SendMessageResultTypeDef:
        return self.send_job(args, kwargs)

    def run(self, *args, **kwargs) -> Any:
        if self.func is not None:
            return self.func(*args, **kwargs)

    def __getstate__(self):
        state = {**self.__dict__}
        # We only need the `app` for sending to SQS.
        state.pop("app")
        return state

    def __call__(self, message_id: str, *args, **kwargs) -> Any:
        self.id = message_id

        if self.pre_task is not None:
            self.pre_task(self)

        result = self.run(*args, **kwargs)

        if self.post_task is not None:
            self.post_task(self)

        return result
