User's Guide
============

Application
-----------

The :class:`squids.App` serves as the central object for configuration, registering tasks, and creating consumers. It is
also responsible for knowing how to connect to SQS. It takes several arguments, but the ones you will use most often are
``name`` and ``boto_config``, the later of which is optional. ``name`` is a string identifier for your application while
``boto_config`` is a dictionary of configuration values that are passed to `boto3.session.Session.client <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client>`_.

.. code-block:: python

    app = squids.App(
        "my-app",
        boto_config={"aws_access_key_id": "abc", "aws_secret_access_key": "secret"}
    )

Once you have created a :class:`squids.App` instance then you can begin registering tasks, sending
tasks, and consuming tasks. Registering a task looks like this:

.. code-block:: python

    @app.task("emails")
    def email_customer(to_addr, body):
        ...

This will register the ``email_customer`` function as a task with the app. The :meth:`.App.task`
decorator takes a ``queue`` argument, which is the name of the SQS queue where the task should be sent to.

Sending Tasks
-------------

Once you have registered your tasks with the application you can begin to send your task into the
specified queue using :meth:`.Task.send` or :meth:`.Task.send_job` as demonstrated below:

.. code-block:: python

    email_customer.send("foo@domain.com", "Hello!")
    email_customer.send_job(
        args=("foo@domain.com", "Hello!"),
        kwargs={},
        options={"DelaySeconds": 5}
    )

Calling ``send`` or ``send_job`` will ensure that any arguments and keyword arguments match the
signature of the function. If they don't a ``TypeError`` will be raised. Both ``send`` and ``send_job``
will return a response of the same form, a `SQS.Client.send_message <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message.html>`_.
The difference between ``send`` and ``send_job`` is that ``send_job`` also accepts an ``options``
dict which accepts all the same arguments as `SQS.Client.send_message <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message.html>`_
except for the ``MessageBody``.

    When you send a task it will, by default, serialize the arguments and keyword arguments using `json <https://docs.python.org/3/library/json.html>`_.
    This means that anything unable to be json serialized cannot be passed to ``send`` or ``send_job``. This behavior
    can be customized by providing the ``serde`` keyword argument to :class:`squids.App` with a class that is of type
    :class:`squids.serde.Serde`.

You can still run your functions synchronously if you want.

.. code-block:: python

    email_customer("foo@domain.com", "Hello!")

Doing this will **not** send a task through the SQS queue, but instead simply call the function and
execute it in the calling process like normal.

Consuming Tasks
---------------

Once you have sent a task into an SQS queue you'll likely want to run it eventually. To run the task
you need to consume it. We can get a consumer for a queue by calling :meth:`.App.create_consumer`.
``create_consumer`` takes a single argument which is the queue name. Once we have the consumer we
can begin to consume and run our tasks like so:

.. code-block:: python

    consumer = app.create_consumer("emails")
    while True:
        consumer.consume(
            options={"WaitTimeSeconds": 5, "MaxNumberOfMessages": 10, "VisibilityTimeout": 30}
        )

:meth:`.Consumer.consume` will fetch messages from the ``emails`` SQS queue and run the function
associated with each received message. In our case it'll run the ``email_customer`` function. The
``options`` keyword argument is an optional dict that takes the same values as `SQS.Client.receive_message <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html>`_.

Often you'll want to be consuming your tasks in another process to keep from blocking your main
program. If you don't want to build your own consumer clients you can look at using the ``squids`` :ref:`command line consumer<Command Line Consumer>`
tool which makes simple consuming of tasks easy.

Application Hooks
-----------------

There are a couple of hooks you can register with your application.

- :meth:`.App.pre_send` - Runs producer side just before the task is sent to the SQS queue.
- :meth:`.App.post_send` - Runs producer side just after the task is sent to the SQS queue.
- :meth:`.App.pre_task` - Runs consumer side after the message is consumed, but just before the task is run.
- :meth:`.App.post_task` - Runs consumer side after the message is consumed and the task is run.

.. code-block:: python

    @app.pre_send
    def before_send(queue_name, body):
        ...

    @app.post_send
    def after_send(queue_name, body, response):
        ...

    @app.pre_task
    def before_task(task):
        ...

    @app.after_task
    def after_task(task):
        ...

These hooks provide a good opportunity for performing logging or metrics related to the production
and consumption of tasks.


Command Line Consumer
---------------------

SQuidS ships with a command line consumer, ``squids``. You can always build your own consumers
(See Consuming Tasks), but this one provides a great starting point that you can use to quickly
scale out your rate of consumption.

.. code-block::

    usage: squids [-h] -q QUEUE [-w WORKERS] -a APP [--polling-wait-time {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}] [--visibility-timeout VISIBILITY_TIMEOUT]
                  [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}]

    optional arguments:
      -h, --help            show this help message and exit
      -q QUEUE, --queue QUEUE
                            The name of the SQS queue to process.
      -w WORKERS, --workers WORKERS
                            The number of workers to run. Defaults to the number of CPUs in the system
      -a APP, --app APP     Path to the application class something like package.module:app where app is an instance of squids.App
      --polling-wait-time {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}
                            The WaitTimeSeconds for polling for messages from the queue. Consult the AWS SQS docs on long polling for more information about this setting. https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-
                            short-and-long-polling.html#sqs-long-polling
      --visibility-timeout VISIBILITY_TIMEOUT
                            The VisibilityTimeout duration (in seconds) that the received messages are hidden from subsequent retrieve requests after being retrieved by a ReceiveMessage request.
      --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                            Set the logging level for the consumer. Logs will be handled using the logging.SteamHandler with the stream set to stdout

It works by creating a pool of worker processes. The consumer then passes the tasks it receives to be run
by the workers. This allows for increased consumption throughput. The consumer will never consumer
more than 2x the number of workers to prevent feeding tasks faster than the workers can process them.

If you need to increase the consumption rate then you can run the consumer on additonal machines or pods.
