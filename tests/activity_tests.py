import unittest

import mock
import redis

import reddit_service_activity as activity
from reddit_service_activity.counter import ActivityCounter
from reddit_service_activity.activity_thrift import ActivityService


class ActivityInfoTests(unittest.TestCase):
    def test_fuzzed_if_small(self):
        with mock.patch("random.randint") as randint:
            randint.return_value = 3
            info = activity.ActivityInfo.from_count(fuzz_threshold=100, count=99)
        self.assertEqual(info.count, 102)
        self.assertTrue(info.is_fuzzed)

    def test_not_fuzzed_if_large(self):
        info = activity.ActivityInfo.from_count(fuzz_threshold=100, count=101)
        self.assertEqual(info.count, 101)
        self.assertFalse(info.is_fuzzed)

    def test_range_of_fuzzing(self):
        samples = [activity.ActivityInfo.from_count(fuzz_threshold=100, count=10).count
                   for i in range(1000)]
        self.assertTrue(all(sample >= 10 for sample in samples))
        self.assertTrue(all(sample <= 15 for sample in samples))

    def test_json_roundtrip(self):
        info = activity.ActivityInfo(count=42, is_fuzzed=True)

        serialized = info.to_json()
        deserialized = activity.ActivityInfo.from_json(serialized)

        self.assertEqual(deserialized.count, 42)
        self.assertTrue(deserialized.is_fuzzed)


class ActivityServiceTests(unittest.TestCase):
    def setUp(self):
        self.mock_counter = mock.Mock(spec=ActivityCounter)
        self.handler = activity.Handler(
            fuzz_threshold=10,
            counter=self.mock_counter,
        )
        self.mock_context = mock.Mock()
        self.mock_context.redis = mock.Mock(spec=redis.StrictRedis)

    def test_health_check(self):
        self.handler.is_healthy(self.mock_context)
        self.mock_context.redis.ping.assert_called_with()

    def test_record_activity(self):
        self.handler.record_activity(self.mock_context, "context", "visitor")

        self.mock_counter.record_activity.assert_called_with(
            self.mock_context.redis, "context", "visitor")

    def test_record_activity_bad_id(self):
        self.handler.record_activity(self.mock_context, u"\u2603", "visitor")
        self.assertFalse(self.mock_counter.record_activity.called)

        self.handler.record_activity(self.mock_context, "context", u"\u2603")
        self.assertFalse(self.mock_counter.record_activity.called)

    def test_count_activity_bad_id(self):
        with self.assertRaises(ActivityService.InvalidContextIDException):
            self.handler.count_activity(self.mock_context, u"\u2603")

    @mock.patch("reddit_service_activity.ActivityInfo", autospec=True)
    def test_count_activity_cache_hit(self, MockActivityInfo):
        self.mock_context.redis.get.return_value = "serialized"
        deserialized = MockActivityInfo.from_json.return_value
        deserialized.count = 33
        deserialized.is_fuzzed = True

        result = self.handler.count_activity(self.mock_context, "context")

        MockActivityInfo.from_json.assert_called_with("serialized")
        self.assertEqual(result.count, 33)
        self.assertTrue(result.is_fuzzed)

    @mock.patch("reddit_service_activity.ActivityInfo", autospec=True)
    def test_count_activity_cache_miss(self, MockActivityInfo):
        fuzzed = MockActivityInfo.from_count.return_value
        fuzzed.count = 33
        fuzzed.is_fuzzed = True
        fuzzed.to_json.return_value = "DATA"
        self.mock_context.redis.get.return_value = None

        result = self.handler.count_activity(self.mock_context, "context")

        self.mock_context.redis.get.assert_called_with("context/cached")
        self.assertEqual(result.count, 33)
        self.assertTrue(result.is_fuzzed)

        # 60 is how long we cache for.
        self.mock_context.redis.setex.assert_called_with("context/cached", 60, "DATA")
