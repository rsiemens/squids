from __future__ import annotations

import functools
import inspect
import json
import sys
from copy import copy
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import boto3

from squids import routing
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
    RoutingStrategy = Callable[[List[str], Dict[str, Any]], Iterable[str]]


class App:
    """
    The central object for registering tasks and creating consumers.

    :param name: An identifier for the application.
    :param config: An optional configuration dict which takes the same values as
        `boto3.session.Session.resource <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.resource>`_.
    """

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

    def task(
        self,
        queue: Union[str, List[str]] = None,
        routing_strategy: RoutingStrategy = routing.random_strategy,
    ) -> Callable:
        """
        A decorator method which takes a queue name and registers the decorated function with the
        app.

        :param queue: The name of the queue that this task should go to when being sent. This can
            also be a list of the queues that this task should go to. When it's a list, the actual
            queue that it goes to is based on the ``routing_strategy``.
        :param routing_strategy: When supplying multiple queues the routing strategy will determine
            which queue(s) will actually be used at :meth:`.Task.send_job` time.
        :return: The decorated function which is augmented to have a ``send`` and ``send_job``
            attribute.
        """

        def wrapper(func):
            task = Task(
                self,
                queue,
                routing_strategy,
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
        """
        Provides a way for custom :class:`.Task` subclasses to be registered and created as
        runnable tasks. Usage looks like:

        .. code-block:: python

            class MyTask(squids.Task):
                queue = "some-queue"

                def run(some_arg):
                    # The task body goes here
                    ...

            my_task_instance = app.add_task(MyTask)
            my_task.send('some_value')

        :param task_cls: A subclass of :class:`.Task` which usually will have overridden the
            :meth:`.Task.run` method.
        :return: An instance of ``task_cls`` that can be used for sending tasks to
            ``task_cls.queue``.
        """
        task = task_cls(
            self,
            task_cls.queue,  # type: ignore
            pre_task=self._pre_task,
            post_task=self._post_task,
        )
        self._tasks[task.name] = task
        return task

    def pre_task(self, func: PreTaskCallback) -> PreTaskCallback:
        """
        Decorator for registering a callback that is invoked consumer side after the message is
        consumed, but right before the task is run.

        The callback takes a single argument, ``task``, which is an instance of :class:`.Task`.

        :param func: The function to be decorated.
        :return: The decorated function.
        """
        self._pre_task = func
        return func

    def post_task(self, func: PostTaskCallback) -> PostTaskCallback:
        """
        Decorator for registering a callback that is invoked consumer side after the message is
        consumed and the task is run.

        The callback takes a single argument, ``task``, which is an instance of :class:`.Task`.

        :param func: The function to be decorated.
        :return: The decorated function.
        """
        self._post_task = func
        return func

    def pre_send(self, func: PreSendCallback) -> PreSendCallback:
        """
        Decorator for registering a callback that is invoked producer side before the message is
        sent to the queue.

        The callback takes two arguments:

        - ``queue`` - A string indicating the queue the message will be sent to.
        - ``body`` - A dict that will be serialized and sent into the queue.

        :param func: The function to be decorated.
        :return: The decorated function.
        """
        self._pre_send = func
        return func

    def post_send(self, func: PostSendCallback) -> PostSendCallback:
        """
        Decorator for registering a callback that is invoked producer side after the message is sent
        to the queue.

        The callback takes three arguments:

        - ``queue`` - A string indicating the queue the message will be sent to.
        - ``body`` - The dict that was serialized and sent into the queue.
        - ``response`` - A dict response which is the return value from
            `SQS.Queue.send_message <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.send_message>`_.

        :param func: The function to be decorated.
        :return: The decorated function.
        """
        self._post_send = func
        return func

    def report_queue_stats(
        self, func: ReportQueueStatsCallback
    ) -> ReportQueueStatsCallback:
        """
        Decorator for registering a callback that is invoked periodically by the
        :ref:`command line consumer<Command Line Consumer>`.

        The callback takes two arguments:

        - ``queue`` - A string indicating the queue that the stats are for.
        - ``queue_stats`` - A dict containing attributes about the queue.

        :param func: The function to be decorated.
        :return: The decorated function.
        """
        self._report_queue_stats = func
        return func

    @functools.lru_cache()
    def get_queue_by_name(self, queue_name: str) -> Queue:
        """
        A convenience method for getting an `SQS.Queue <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue>`_
        by queue name.

        :param queue_name: A string identifying the queue.
        :return: An instance of `SQS.Queue <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue>`_.
        """
        return self.sqs.get_queue_by_name(QueueName=queue_name)

    def create_consumer(self, queue_name: str) -> Consumer:
        """
        A convenience method for creating a :class:`.Consumer` instance.

        :param queue_name: A string identifying the queue.
        :return: An instance of :class:`.Consumer`.
        """
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
    """
    An object that wraps some task to be done, usually a function, or some callable. You'll rarely,
    if ever ever, need to instantiate an instance of this yourself, but instead will use
    :meth:`.App.task` or :meth:`.App.add_task` to handle instantiating it.

    :param app: An instance of :class:`.App`.
    :param queue: A string or list indicating the queue or queues that this task should be sent to.
    :param routing_strategy: When supplying multiple queues the routing strategy will determine
        which queue(s) will actually be used at :meth:`.Task.send_job` time.
    :param func: The job to be done. If this is None then :meth:`.Task.run` should be overridden.
    :param pre_task: An optional callback function to be invoked right before the task is run. See
        :meth:`.App.pre_task` for more details on the callback.
    :param post_task: An optional callback function to be invoked right after the task is run. See
        :meth:`.App.post_task` for more details on the callback.
    """

    def __init__(
        self,
        app: App,
        queue: Union[str, List[str]],
        routing_strategy: RoutingStrategy = routing.random_strategy,
        func: Optional[Callable] = None,
        pre_task: Optional[PreTaskCallback] = None,
        post_task: Optional[PostTaskCallback] = None,
    ):
        self.queues = queue if isinstance(queue, List) else [queue]
        self.routing_strategy = routing_strategy

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
        options: Optional[Dict] = None,
        queue: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        .. _SQS.Queue.send_message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.send_message

        Send a task into the associated SQS queue.

        :param args: Positional arguments for the task.
        :param kwargs: Keyword arguments for the task.
        :param options: A dict of optional arguments when sending into the queue. Takes the same
            values as `SQS.Queue.send_message`_ minus the ``MessageBody``.
        :param queue: Forces sending a message to specific queue regardless of ``routing_strategy``.
        :return: List of dicts containing the queue name and the response from SQS which is the same
            returned by `SQS.Queue.send_message`_.
        """
        if options is None:
            options = {}
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = {}

        # will raise TypeError if the signature doesn't match
        bound = self.signature.bind(*args, **kwargs)
        body = {
            "task": self.name,
            "args": bound.args,
            "kwargs": bound.kwargs,
        }

        if len(self.queues) > 1:
            target_queues = self.routing_strategy(copy(self.queues), body)
        else:
            target_queues = self.queues

        if queue is not None and queue not in self.queues:
            raise ValueError(f'Forced queue "{queue}" is not an option for this task.')
        elif queue is not None:
            target_queues = [queue]

        responses = []
        for queue_name in target_queues:
            sqs_queue = self.app.get_queue_by_name(queue_name)

            if self.app._pre_send is not None:
                self.app._pre_send(queue_name, body)

            response = sqs_queue.send_message(MessageBody=json.dumps(body), **options)
            responses.append({"queue": queue_name, "sqs_response": response})

            if self.app._post_send is not None:
                self.app._post_send(queue_name, body, response)

        return responses

    def send(self, *args, **kwargs) -> List[Dict[str, Any]]:
        """
        Splat args and kwargs version of :meth:`.Task.send_job` which does not support the extra
        ``options`` or ``queue`` argument provided by :meth:`.Task.send_job`.

        :param args: Positional arguments for the task.
        :param kwargs: Keyword arguments for the task.
        :return: List of dicts containing the queue name and the response from SQS which is the same
            returned by `SQS.Queue.send_message <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.send_message>`_.
        """
        return self.send_job(args, kwargs)

    def run(self, *args, **kwargs) -> Any:
        """
        Executes the provided task. If you are creating your own Task subclass then this method
        should be overridden.

        :param args: Positional arguments for the task.
        :param kwargs: Keyword arguments for the task.
        :return: Any
        """
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
