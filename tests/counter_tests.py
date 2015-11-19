import unittest

import mock
import redis

from reddit_service_activity.counter import ActivityCounter


class ActivityCounterTests(unittest.TestCase):
    def setUp(self):
        self.counter = ActivityCounter(activity_window=15*60)

    @mock.patch("time.time")
    def test_record_activity(self, mock_time):
        mock_time.return_value = 1200
        mock_redis = mock.Mock(spec=redis.StrictRedis)

        self.counter.record_activity(mock_redis, "context", "visitor")

        # 20 is the current time slice; 1200 / 60 = time.time() / SLICE_LENGTH
        mock_redis.execute_command.assert_called_with(
            "PFADD", "context/20", "visitor")

    @mock.patch("time.time")
    def test_count_activity(self, mock_time):
        mock_time.return_value = 1200
        mock_redis = mock.Mock(spec=redis.StrictRedis)
        mock_redis.execute_command.return_value = 28

        result = self.counter.count_activity(mock_redis, "context")

        # the counter should merge the most recent 15 slices, starting at
        # the current slice #20 (see above for why it's #20).
        mock_redis.execute_command.assert_called_with("PFCOUNT",
            "context/20", "context/19", "context/18",
            "context/17", "context/16", "context/15",
            "context/14", "context/13", "context/12",
            "context/11", "context/10", "context/9",
            "context/8", "context/7", "context/6",
        )
        self.assertEqual(result, 28)
