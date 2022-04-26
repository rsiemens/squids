import json
import pickle
import unittest
from unittest.mock import Mock, patch

from squids import App, Task


def dummy_task():
    pass


@patch("squids.boto3")
class TaskTestCases(unittest.TestCase):
    def test_send(self, _):
        app = App("unittests")
        mock_queue = Mock()
        app.sqs.get_queue_by_name.return_value = mock_queue

        def dummy_job(arg1, kwarg1=None):
            self.fail("dummy_job shouldn't be called on `send`")

        task = Task("test-queue", dummy_job, app=app)
        with self.assertRaises(TypeError):
            # validates the `send` arguments match the `dummy_job` parameters
            task.send(kwarg1="kwarg1")

        task.send("arg1val", kwarg1="kwarg1val")
        mock_queue.send_message.assert_called_once_with(
            MessageBody=json.dumps(
                {
                    "task": "dummy_job",
                    "args": ["arg1val", "kwarg1val"],
                    "kwargs": {},
                }
            )
        )

    def test_send_with_pre_and_post_hooks(self, _):
        app = App("unittests")
        hook_call_order = []
        expected_body = {
            "task": "dummy_job",
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

        task = Task("test-queue", dummy_job, app=app)
        task.send()

        self.assertEqual(hook_call_order, ["pre_send", "post_send"])

    def test_run(self, _):
        app = App("unittests")
        dummy_job_called = False

        def dummy_job():
            nonlocal dummy_job_called
            dummy_job_called = True

        task = Task("test-queue", dummy_job, app=app)
        task.run()

        self.assertTrue(dummy_job_called)

    def test_pickle_task(self, _):
        app = App("unittests")

        task = Task("test-queue", dummy_task, app=app)
        unpickled_task = pickle.loads(pickle.dumps(task))

        self.assertEqual(task.app, app)
        with self.assertRaises(AttributeError):
            unpickled_task.app

    def test_call(self, _):
        app = App("unittests")
        hook_call_order = []
        message_id = "abc-123"

        def dummy_job():
            hook_call_order.append("dummy_job")

        def before_task(task):
            hook_call_order.append("pre_task")

            self.assertEqual(task.queue_name, "test-queue")
            self.assertEqual(task.id, message_id)

        def after_task(task):
            hook_call_order.append("post_task")

            self.assertEqual(task.queue_name, "test-queue")
            self.assertEqual(task.id, message_id)

        task = Task(
            "test-queue", dummy_job, app, pre_task=before_task, post_task=after_task
        )
        task(message_id)

        self.assertEqual(hook_call_order, ["pre_task", "dummy_job", "post_task"])
