import unittest

from squids.consumer import (ResourceLimitExceeded, ResourceTracker,
                             RoundRobinScheduler)


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


class RoundRobinSchedulerTestCases(unittest.TestCase):
    def test_next(self):
        scheduler = RoundRobinScheduler(["a", "b", "c"])

        self.assertEqual(scheduler.next(), "a")
        self.assertEqual(scheduler.next(), "b")
        self.assertEqual(scheduler.next(), "c")
        self.assertEqual(scheduler.next(), "a")
        self.assertEqual(scheduler.next(), "b")
