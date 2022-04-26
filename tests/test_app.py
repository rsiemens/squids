import unittest
from unittest.mock import patch

from squids import App, Task


@patch("squids.boto3")
class AppTestCases(unittest.TestCase):
    def test_init(self, _):
        app = App("unittests")
        self.assertEqual(app.name, "unittests")

    def test_task(self, _):
        app = App("unittests")

        @app.task("test-queue")
        def test_task():
            pass

        self.assertIn(
            "tests.test_app.AppTestCases.test_task.<locals>.test_task", app._tasks
        )
        task = app._tasks["tests.test_app.AppTestCases.test_task.<locals>.test_task"]
        self.assertIsInstance(task, Task)
        self.assertEqual(
            task.name, "tests.test_app.AppTestCases.test_task.<locals>.test_task"
        )
        self.assertEqual(task.queue_name, "test-queue")
        self.assertEqual(task.func, test_task)
        self.assertEqual(task.send, test_task.send)

    def test_add_task(self, _):
        app = App("unittests")

        class MyTask(Task):
            queue_name = "test-queue"

            def run(self, arg1, arg2):
                pass

        app.add_task(MyTask)
        self.assertIn(
            "tests.test_app.AppTestCases.test_add_task.<locals>.MyTask", app._tasks
        )
        task = app._tasks["tests.test_app.AppTestCases.test_add_task.<locals>.MyTask"]
        self.assertIsInstance(task, MyTask)
        self.assertEqual(
            task.name, "tests.test_app.AppTestCases.test_add_task.<locals>.MyTask"
        )
        self.assertEqual(task.queue_name, "test-queue")
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
