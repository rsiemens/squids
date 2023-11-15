from __future__ import annotations

import functools
import inspect
import sys
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Type

import boto3

from squids.consumer import Consumer
from squids.serde import JSONSerde, Serde

PreTaskCallback = Callable[["Task"], None]
PostTaskCallback = Callable[["Task"], None]
PreSendCallback = Callable[[str, Dict], None]
PostSendCallback = Callable[[str, Dict, "SendMessageResultTypeDef"], None]

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import SendMessageResultTypeDef


class App:
    """
    The central object for registering tasks and creating consumers.

    :param name: An identifier for the application.
    :param boto_config: An optional configuration dict which takes the same values as
        `boto3.session.Session.client <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client>`_.
    :param serde: An optional :class:`.Serde` subclass that is used to serialize and deserialize
        message bodies when sending and consuming. If not provided, :class:`.JSONSerde` will be
        used.
    :param delete_late: An optional boolean that determines when a message is deleted from SQS. If
        the value is ``True`` then the message is deleted *after* processing the task. This means an
        unhandled exception in the task could lead to the message not being deleted and
        re-delivered when the visibility timeout expires. It also means a task must finish
        processing it's task within the visibility timeout window or it could be re-delivered.
        The default value of ``False`` is generally encouraged unless your tasks are idempotent or you
        have appropriate DLQ handling and or de-duping configured.
    """

    def __init__(
        self,
        name: str,
        boto_config: Optional[Dict] = None,
        serde: Type[Serde] = JSONSerde,
        delete_late: bool = False,
    ):
        self.name = name
        self.boto_config = boto_config or {}
        self.sqs = boto3.client("sqs", **self.boto_config)
        self._serde = serde
        self._delete_late = delete_late
        self._tasks: Dict[str, Task] = {}
        self._pre_task: Optional[PreTaskCallback] = None
        self._post_task: Optional[PostTaskCallback] = None
        self._pre_send: Optional[PreSendCallback] = None
        self._post_send: Optional[PostSendCallback] = None

    def task(
        self,
        queue: str,
    ) -> Callable:
        """
        A decorator method which takes a queue name and registers the decorated function with the
        app.

        :param queue: The name of the queue that this task should go to by default when being sent.
        :return: The decorated function which is augmented to have a ``send`` and ``send_job``
            attribute.
        """

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
            `SQS.Client.send_message <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message.html>`_.

        :param func: The function to be decorated.
        :return: The decorated function.
        """
        self._post_send = func
        return func

    @functools.lru_cache()
    def get_queue_by_name(self, queue_name: str) -> str:
        """
        A convenience method for getting a queue URL by name.

        :param queue_name: A string identifying the queue.
        :return: The queue URL.
        """
        return self.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

    def create_consumer(self, queue_name: str) -> Consumer:
        """
        A convenience method for creating a :class:`.Consumer` instance.

        :param queue_name: A string identifying the queue.
        :return: An instance of :class:`.Consumer`.
        """
        queue_url = self.get_queue_by_name(queue_name)
        return Consumer(self, queue_url)

    def __getstate__(self):
        state = {**self.__dict__}
        state.pop("sqs")
        return state

    def __setstate__(self, state):
        state["sqs"] = boto3.client("sqs", **state["boto_config"])
        self.__dict__ = state


class Task:
    """
    An object that wraps some task to be done, usually a function, or some callable. You'll rarely,
    if ever ever, need to instantiate an instance of this yourself, but instead will use
    :meth:`.App.task` or :meth:`.App.add_task` to handle instantiating it.

    :param app: An instance of :class:`.App`.
    :param queue: A queue name that this task should be sent to by default.
    :param func: The job to be done. If this is None then :meth:`.Task.run` should be overridden.
    :param pre_task: An optional callback function to be invoked right before the task is run. See
        :meth:`.App.pre_task` for more details on the callback.
    :param post_task: An optional callback function to be invoked right after the task is run. See
        :meth:`.App.post_task` for more details on the callback.
    """

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
    ) -> SendMessageResultTypeDef:
        """
        .. _SQS.Client.send_message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message.html

        Send a task into the associated SQS queue.

        :param args: Positional arguments for the task.
        :param kwargs: Keyword arguments for the task.
        :param options: A dict of optional arguments when sending into the queue. Takes the same
            values as `SQS.Client.send_message`_ minus the ``MessageBody``.
        :param queue: Forces sending a message to specific queue. This overrides the default queue.
        :return: The response from SQS which is the same returned by `SQS.Client.send_message`_.
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

        if queue is None:
            queue = self.queue

        queue_url = self.app.get_queue_by_name(queue)

        if self.app._pre_send is not None:
            self.app._pre_send(queue, body)

        response = self.app.sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=self.app._serde.serialize(body),
            **options,
        )

        if self.app._post_send is not None:
            self.app._post_send(queue, body, response)

        return response

    def send(self, *args, **kwargs) -> SendMessageResultTypeDef:
        """
        Splat args and kwargs version of :meth:`.Task.send_job` which does not support the extra
        ``options`` or ``queue`` argument provided by :meth:`.Task.send_job`.

        :param args: Positional arguments for the task.
        :param kwargs: Keyword arguments for the task.
        :return: The response from SQS which is the same returned by `SQS.Client.send_message <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message.html>`_.
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

    def __call__(self, message_id: str, *args, **kwargs) -> Any:
        self.id = message_id

        if self.pre_task is not None:
            self.pre_task(self)

        result = self.run(*args, **kwargs)

        if self.post_task is not None:
            self.post_task(self)

        return result
