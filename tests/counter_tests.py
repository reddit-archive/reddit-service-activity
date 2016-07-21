import unittest

import mock
import redis

import baseplate.context.redis

from reddit_service_activity.counter import ActivityCounter


class ActivityCounterTests(unittest.TestCase):
    def setUp(self):
        self.counter = ActivityCounter(activity_window=15*60)

    @mock.patch("time.time")
    def test_record_activity(self, mock_time):
        mock_time.return_value = 1202
        mock_redis = mock.Mock(spec=redis.StrictRedis)
        pipeline = mock_redis.pipeline.return_value = mock.MagicMock(
            spec=baseplate.context.redis.MonitoredRedisPipeline)
        pipe = pipeline.__enter__.return_value

        self.counter.record_activity(mock_redis, "context", "visitor")

        # 80 is the current time slice; 1200 / 15 = time.time() / SLICE_LENGTH
        pipe.execute_command.assert_called_with(
            "PFADD", "context/80", "visitor")

        # 2115 = current slice + number of slices we need as a time
        # i.e. 15 minutes in the future + a tiny buffer
        pipe.expireat.assert_called_with("context/80", 2115)

    @mock.patch("time.time")
    def test_count_activity(self, mock_time):
        mock_time.return_value = 1200
        mock_redis = mock.Mock(spec=redis.StrictRedis)
        mock_redis.execute_command.return_value = 28

        result = self.counter.count_activity(mock_redis, "context")

        # the counter should merge the most recent 15*4 slices, starting at
        # the current slice #20 (see above for why it's #20).
        mock_redis.execute_command.assert_called_with("PFCOUNT",
            "context/80", "context/79", "context/78", "context/77",
            "context/76", "context/75", "context/74", "context/73",
            "context/72", "context/71", "context/70", "context/69",
            "context/68", "context/67", "context/66", "context/65",
            "context/64", "context/63", "context/62", "context/61",

            "context/60", "context/59", "context/58", "context/57",
            "context/56", "context/55", "context/54", "context/53",
            "context/52", "context/51", "context/50", "context/49",
            "context/48", "context/47", "context/46", "context/45",
            "context/44", "context/43", "context/42", "context/41",

            "context/40", "context/39", "context/38", "context/37",
            "context/36", "context/35", "context/34", "context/33",
            "context/32", "context/31", "context/30", "context/29",
            "context/28", "context/27", "context/26", "context/25",
            "context/24", "context/23", "context/22", "context/21",
        )
        self.assertEqual(result, 28)
