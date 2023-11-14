import json
import pickle
import unittest
from unittest.mock import Mock, patch

from squids import App, Task
from squids.serde import JSONSerde, Serde


@patch("squids.core.boto3.client")
class AppTestCases(unittest.TestCase):
    def test_init(self, _):
        app = App("unittests")
        self.assertEqual(app.name, "unittests")
        self.assertEqual(app._serde, JSONSerde)

    def test_task(self, _):
        app = App("unittests")

        @app.task("test-queue")
        def test_task():
            pass

        self.assertIn(
            "tests.test_core.AppTestCases.test_task.<locals>.test_task", app._tasks
        )
        task = app._tasks["tests.test_core.AppTestCases.test_task.<locals>.test_task"]
        self.assertIsInstance(task, Task)
        self.assertEqual(
            task.name, "tests.test_core.AppTestCases.test_task.<locals>.test_task"
        )
        self.assertEqual(task.queue, "test-queue")
        self.assertEqual(task.func, test_task)
        self.assertEqual(task.send, test_task.send)

    def test_add_task(self, _):
        app = App("unittests")

        class MyTask(Task):
            queue = "test-queue"

            def run(self, arg1, arg2):
                pass

        task = app.add_task(MyTask)
        self.assertIn(
            "tests.test_core.AppTestCases.test_add_task.<locals>.MyTask", app._tasks
        )
        self.assertIsInstance(task, MyTask)
        self.assertEqual(
            task.name, "tests.test_core.AppTestCases.test_add_task.<locals>.MyTask"
        )
        self.assertEqual(task.queue, "test-queue")
        self.assertEqual(task.func, None)

    def test_pre_task(self, _):
        app = App("unittests")

        @app.pre_task
        def before_task(task):
            pass

        self.assertEqual(app._pre_task, before_task)

    def test_post_task(self, _):
        app = App("unittests")

        @app.post_task
        def after_task(task):
            pass

        self.assertEqual(app._post_task, after_task)

    def test_pre_send(self, _):
        app = App("unittests")

        @app.pre_send
        def before_send(queue, body):
            pass

        self.assertEqual(app._pre_send, before_send)

    def test_post_send(self, _):
        app = App("unittests")

        @app.post_send
        def after_send(queue, body, response):
            pass

        self.assertEqual(app._post_send, after_send)


def dummy_task():
    pass


@patch("squids.core.boto3.client")
class TaskTestCases(unittest.TestCase):
    def test_send(self, _):
        app = App("unittests")
        queue_url = "http://fake/queue"
        app.sqs.get_queue_url.return_value = {"QueueUrl": queue_url}

        def dummy_job(arg1, kwarg1=None):
            self.fail("dummy_job shouldn't be called on `send`")

        task = Task(app, "test-queue", func=dummy_job)
        with self.assertRaises(TypeError):
            # validates the `send` arguments match the `dummy_job` parameters
            task.send(kwarg1="kwarg1")

        task.send("arg1val", kwarg1="kwarg1val")
        app.sqs.send_message.assert_called_once_with(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {
                    "task": "tests.test_core.TaskTestCases.test_send.<locals>.dummy_job",
                    "args": ["arg1val", "kwarg1val"],
                    "kwargs": {},
                }
            ),
        )

    def test_send_job(self, _):
        app = App("unittests")
        queue_url = "http://fake/queue"
        app.sqs.get_queue_url.return_value = {"QueueUrl": queue_url}

        def dummy_job(arg1, kwarg1=None):
            self.fail("dummy_job shouldn't be called on `delay`")

        task = Task(app, "test-queue", func=dummy_job)

        task.send_job(
            args=("arg1val",),
            kwargs={"kwarg1": "kwarg1val"},
            options={"DelaySeconds": 10},
        )
        app.sqs.send_message.assert_called_once_with(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {
                    "task": "tests.test_core.TaskTestCases.test_send_job.<locals>.dummy_job",
                    "args": ["arg1val", "kwarg1val"],
                    "kwargs": {},
                }
            ),
            DelaySeconds=10,
        )

    def test_send_job_queue_override(self, _):
        app = App("unittests")
        queue_url = "http://fake/queue"
        app.sqs.get_queue_url.return_value = {"QueueUrl": queue_url}

        def dummy_job(arg1, kwarg1=None):
            self.fail("dummy_job shouldn't be called on `delay`")

        task = Task(app, "q1", func=dummy_job)

        task.send_job(
            args=("arg1val",),
            kwargs={"kwarg1": "kwarg1val"},
            queue="q2",
        )
        app.sqs.send_message.assert_called_once_with(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {
                    "task": "tests.test_core.TaskTestCases.test_send_job_queue_override.<locals>.dummy_job",
                    "args": ["arg1val", "kwarg1val"],
                    "kwargs": {},
                }
            ),
        )

    def test_send_job_uses_app_serde_to_serialize(self, _):
        expected_job_body = {
            "task": "tests.test_core.TaskTestCases.test_send_job_uses_app_serde_to_serialize.<locals>.dummy_job",
            "args": ("arg1val", "kwarg1val"),
            "kwargs": {},
        }

        class DumbSerde(Serde):
            @classmethod
            def serialize(cls, body):
                self.assertEqual(body, expected_job_body)
                return "I'm serialized"

        app = App("unittests", serde=DumbSerde)
        queue_url = "http://fake/queue"
        app.sqs.get_queue_url.return_value = {"QueueUrl": queue_url}

        def dummy_job(arg1, kwarg1=None):
            self.fail("dummy_job shouldn't be called on `delay`")

        task = Task(app, "test-queue", func=dummy_job)

        task.send_job(
            args=("arg1val",),
            kwargs={"kwarg1": "kwarg1val"},
        )
        app.sqs.send_message.assert_called_once_with(
            QueueUrl=queue_url,
            MessageBody="I'm serialized",
        )

    def test_send_with_pre_and_post_hooks(self, _):
        app = App("unittests")
        hook_call_order = []
        expected_body = {
            "task": "tests.test_core.TaskTestCases.test_send_with_pre_and_post_hooks.<locals>.dummy_job",
            "args": (),
            "kwargs": {},
        }

        @app.pre_send
        def before_send(queue, body):
            hook_call_order.append("pre_send")

            self.assertEqual(queue, "test-queue")
            self.assertEqual(body, expected_body)

        @app.post_send
        def after_send(queue, body, response):
            hook_call_order.append("post_send")

            self.assertEqual(queue, "test-queue")
            self.assertEqual(body, expected_body)
            self.assertIsInstance(response, Mock)

        def dummy_job():
            self.fail("dummy_job shouldn't be called on `send`")

        task = Task(app, "test-queue", func=dummy_job)
        task.send()

        self.assertEqual(hook_call_order, ["pre_send", "post_send"])

    def test_run(self, _):
        app = App("unittests")
        dummy_job_called = False

        def dummy_job():
            nonlocal dummy_job_called
            dummy_job_called = True

        task = Task(app, "test-queue", func=dummy_job)
        task.run()

        self.assertTrue(dummy_job_called)

    def test_pickle_task(self, _):
        app = App("unittests")

        task = Task(app, "http://fake/test-queue", func=dummy_task)
        unpickled_task = pickle.loads(pickle.dumps(task))

        self.assertTrue(hasattr(unpickled_task.app, "sqs"))

    def test_call(self, _):
        app = App("unittests")
        hook_call_order = []
        message_id = "abc-123"

        def dummy_job():
            hook_call_order.append("dummy_job")

        def before_task(task):
            hook_call_order.append("pre_task")

            self.assertEqual(task.queue, "test-queue")
            self.assertEqual(task.id, message_id)

        def after_task(task):
            hook_call_order.append("post_task")

            self.assertEqual(task.queue, "test-queue")
            self.assertEqual(task.id, message_id)

        task = Task(
            app,
            "test-queue",
            func=dummy_job,
            pre_task=before_task,
            post_task=after_task,
        )
        task(message_id)

        self.assertEqual(hook_call_order, ["pre_task", "dummy_job", "post_task"])

    def test_custom_task_class(self, _):
        app = App("unittests")
        run_called = False

        class MyTask(Task):
            def run(self, arg1, kwarg1=None):
                nonlocal run_called
                run_called = True

        task = MyTask(app, "test-queue")
        self.assertEqual(
            task.name,
            "tests.test_core.TaskTestCases.test_custom_task_class.<locals>.MyTask",
        )

        with self.assertRaises(TypeError):
            task.send(kwarg1="kwarg1")

        task.send("arg1", kwarg1="kwarg1")
        task("messageid-123", "arg1", kwarg1="kwarg1")

        self.assertTrue(run_called)
