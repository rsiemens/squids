import json
import unittest
from unittest.mock import Mock, call, patch

from squids.consumer import Consumer, ResourceLimitExceeded, ResourceTracker
from squids.core import App
from squids.serde import Serde


class ResourceTrackerTestCases(unittest.TestCase):
    def test_add(self):
        tracker = ResourceTracker(3)
        tracker.add("a")
        tracker.add("b")
        tracker.add("c")
        tracker.add("a")
        self.assertEqual(tracker._resources, {"a", "b", "c"})

        with self.assertRaises(ResourceLimitExceeded):
            # over the limit of 3 resources
            tracker.add("d")

    def test_remove(self):
        tracker = ResourceTracker(3)
        tracker.add("a")
        tracker.add("b")
        tracker.add("c")
        self.assertEqual(tracker._resources, {"a", "b", "c"})

        tracker.remove("a")
        tracker.remove("b")
        self.assertEqual(tracker._resources, {"c"})

    def test_available_space(self):
        limit = 5
        tracker = ResourceTracker(limit)

        for i in range(1, limit + 1):
            tracker.add(i)
            self.assertEqual(tracker.available_space(), limit - i)

    def test_has_available_space(self):
        limit = 5
        tracker = ResourceTracker(limit)

        for i in range(1, limit):
            tracker.add(i)
            self.assertTrue(tracker.has_available_space)

        tracker.add(5)
        self.assertFalse(tracker.has_available_space)


@patch("squids.core.boto3.client")
class ConsumeTestCases(unittest.TestCase):
    def test_prepare_task(self, _):
        app = App("test")
        fake_task = Mock()
        app._tasks["fake_task"] = fake_task
        body = {
            "task": "fake_task",
            "args": (1, 2, 3),
            "kwargs": {},
        }

        consumer = Consumer(app, "http://fake/queue")
        task, message_id, args, kwargs = consumer._prepare_task(
            {"MessageId": "123", "Body": json.dumps(body)}
        )

        self.assertEqual(task, fake_task)
        self.assertEqual(message_id, "123")
        self.assertEqual(args, [1, 2, 3])
        self.assertEqual(kwargs, {})

    def test_prepare_task_uses_app_serde_to_deserialize(self, _):
        class DumbSerde(Serde):
            @classmethod
            def deserialize(cls, body):
                self.assertEqual(body, "I'm serialized")
                return {
                    "task": "fake_task",
                    "args": [1, 2, 3],
                    "kwargs": {},
                }

        app = App("test", serde=DumbSerde)
        fake_task = Mock()
        app._tasks["fake_task"] = fake_task

        consumer = Consumer(app, "http://fake/queue")
        task, message_id, args, kwargs = consumer._prepare_task(
            {"MessageId": "123", "Body": "I'm serialized"}
        )

        self.assertEqual(task, fake_task)
        self.assertEqual(message_id, "123")
        self.assertEqual(args, [1, 2, 3])
        self.assertEqual(kwargs, {})

    def test_consume_messages(self, _):
        app = App("unittests")
        app.sqs.receive_message.return_value = {"Messages": [1, 2, 3]}
        consumer = Consumer(app, "http://fake/queue")

        self.assertEqual([m for m in consumer.consume_messages()], [1, 2, 3])

    def test_consume(self, _):
        app = App("test")
        fake_task = Mock()
        app._tasks["fake_task"] = fake_task
        body = {
            "task": "fake_task",
            "args": (1, "a"),
            "kwargs": {},
        }
        app.sqs.receive_message.return_value = {
            "Messages": [
                {"MessageId": "1", "ReceiptHandle": "1", "Body": json.dumps(body)},
                {"MessageId": "2", "ReceiptHandle": "2", "Body": json.dumps(body)},
                {"MessageId": "3", "ReceiptHandle": "3", "Body": json.dumps(body)},
            ]
        }

        consumer = Consumer(app, "http://fake/queue")
        consumer.consume()

        self.assertEqual(fake_task.call_count, 3)
        self.assertEqual(
            fake_task.call_args_list,
            [
                call("1", 1, "a"),
                call("2", 1, "a"),
                call("3", 1, "a"),
            ],
        )
