import unittest

import mock
import redis

import baseplate.context.redis

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


class MockActivityInfo(activity.ActivityInfo):
    def to_json(self):
        return (self.count, self.is_fuzzed)


class ActivityServiceTests(unittest.TestCase):
    def setUp(self):
        self.mock_counter = mock.Mock(spec=ActivityCounter)
        self.handler = activity.Handler(
            fuzz_threshold=10,
            counter=self.mock_counter,
        )
        self.mock_context = mock.Mock()

        redis_ = mock.Mock(spec=redis.StrictRedis)
        pipeline = redis_.pipeline.return_value = mock.MagicMock(
            spec=baseplate.context.redis.MonitoredRedisPipeline)
        self.mock_context.redis = redis_
        self.mock_pipe = pipeline.__enter__.return_value

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
        self.mock_context.redis.mget.return_value = ["serialized"]
        deserialized = MockActivityInfo.from_json.return_value
        deserialized.count = 33
        deserialized.is_fuzzed = True

        result = self.handler.count_activity(self.mock_context, "context")

        MockActivityInfo.from_json.assert_called_with("serialized")
        self.assertEqual(result.count, 33)
        self.assertTrue(result.is_fuzzed)

    @mock.patch("reddit_service_activity.ActivityInfo", new=MockActivityInfo)
    def test_count_activity_cache_miss(self):
        self.mock_context.redis.mget.return_value = [None]

        self.mock_pipe.execute.return_value = [125]
        result = self.handler.count_activity(self.mock_context, "context")

        self.mock_context.redis.mget.assert_called_with(["context/cached"])
        self.assertEqual(result.count, 125)
        self.assertFalse(result.is_fuzzed)

        # 30 is how long we cache for.
        self.mock_pipe.setex.assert_called_with("context/cached", 30, (125, False))

    @mock.patch("reddit_service_activity.ActivityInfo", new=MockActivityInfo)
    def test_count_activity_multi_cache_miss(self):
        self.mock_context.redis.mget.return_value = [None, None]
        self.mock_pipe.execute.return_value = [500, 600]

        self.handler.count_activity_multi(self.mock_context, ["one", "two"])

        self.mock_pipe.setex.assert_has_calls([
            mock.call("one/cached", 30, (500, False)),
            mock.call("two/cached", 30, (600, False)),
        ], any_order=True)
