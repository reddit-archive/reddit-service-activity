import json
import logging
import math
import random
import re

import redis

from baseplate import Baseplate, make_metrics_client, config
from baseplate.context.redis import RedisContextFactory
from baseplate.integration.thrift import BaseplateProcessorEventHandler

from .activity_thrift import ActivityService, ttypes
from .counter import ActivityCounter


logger = logging.getLogger(__name__)
_ID_RE = re.compile("^[A-Za-z0-9_]{,50}$")
_CACHE_TIME = 60  # seconds


class ActivityInfo(ttypes.ActivityInfo):
    @classmethod
    def from_count(cls, fuzz_threshold, count):
        if count >= fuzz_threshold:
            return cls(count=count, is_fuzzed=False)

        decay = math.exp(float(-count) / 60)
        jitter = round(5 * decay)
        return cls(count=count + random.randint(0, jitter), is_fuzzed=True)

    def to_json(self):
        return json.dumps(
            {"count": self.count, "is_fuzzed": self.is_fuzzed},
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, value):
        deserialized = json.loads(value)
        return cls(
            count=deserialized["count"],
            is_fuzzed=deserialized["is_fuzzed"],
        )


class Handler(ActivityService.ContextIface):
    def __init__(self, fuzz_threshold, counter):
        self.fuzz_threshold = fuzz_threshold
        self.counter = counter

    def is_healthy(self, context):
        return context.redis.ping()

    def record_activity(self, context, context_id, visitor_id):
        if not _ID_RE.match(context_id) or not _ID_RE.match(visitor_id):
            return

        self.counter.record_activity(context.redis, context_id, visitor_id)

    def count_activity(self, context, context_id):
        if not _ID_RE.match(context_id):
            raise ActivityService.InvalidContextIDException

        cache_key = "{context}/cached".format(context=context_id)
        cached_result = context.redis.get(cache_key)
        if cached_result:
            return ActivityInfo.from_json(cached_result.decode())
        else:
            count = self.counter.count_activity(context.redis, context_id)
            info = ActivityInfo.from_count(self.fuzz_threshold, count)
            context.redis.setex(cache_key, _CACHE_TIME, info.to_json())
            return info

    def count_activity_multi(self, context, context_ids):
        for context_id in context_ids:
            if not _ID_RE.match(context_id):
                raise ActivityService.InvalidContextIDException

        by_key = {"{context}/cached".format(context=context_id): context_id
            for context_id in context_ids}
        cached_results = context.redis.mget(by_key.keys())

        missing = []
        for context_id in context_ids:



def make_processor(app_config):  # pragma: nocover
    cfg = config.parse_config(app_config, {
        "activity": {
            "window": config.Timespan,
            "fuzz_threshold": config.Integer,
        },

        "redis": {
            "url": config.String,
        },
    })

    metrics_client = make_metrics_client(app_config)
    redis_pool = redis.ConnectionPool.from_url(cfg.redis.url)

    baseplate = Baseplate()
    baseplate.configure_logging()
    baseplate.configure_metrics(metrics_client)
    baseplate.add_to_context("redis", RedisContextFactory(redis_pool))

    counter = ActivityCounter(cfg.activity.window.total_seconds())
    handler = Handler(
        fuzz_threshold=cfg.activity.fuzz_threshold,
        counter=counter,
    )
    processor = ActivityService.ContextProcessor(handler)
    event_handler = BaseplateProcessorEventHandler(logger, baseplate)
    processor.setEventHandler(event_handler)

    return processor
