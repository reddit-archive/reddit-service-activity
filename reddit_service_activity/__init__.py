import json
import logging
import math
import random
import re

import redis

from baseplate import (
    Baseplate,
    config,
    error_reporter_from_config,
    metrics_client_from_config,
    tracing_client_from_config,
)
from baseplate.context.redis import RedisContextFactory
from baseplate.integration.thrift import BaseplateProcessorEventHandler

from .activity_thrift import ActivityService, ttypes
from .counter import ActivityCounter


logger = logging.getLogger(__name__)
_ID_RE = re.compile("^[A-Za-z0-9_]{,50}$")
_CACHE_TIME = 30  # seconds


class ActivityInfo(ttypes.ActivityInfo):
    @classmethod
    def from_count(cls, count):
        # keep a minimum jitter range of 5 for counts over 100
        decay = math.exp(float(-min(count, 100)) / 60)
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
    def __init__(self, counter):
        self.counter = counter

    def is_healthy(self, context):
        return context.redis.ping()

    def record_activity(self, context, context_id, visitor_id):
        if not _ID_RE.match(context_id) or not _ID_RE.match(visitor_id):
            return

        self.counter.record_activity(context.redis, context_id, visitor_id)

    def count_activity(self, context, context_id):
        results = self.count_activity_multi(context, [context_id])
        return results[context_id]

    def count_activity_multi(self, context, context_ids):
        if not all(_ID_RE.match(context_id) for context_id in context_ids):
            raise ActivityService.InvalidContextIDException

        activity = {}

        # read cached activity
        cache_keys = [context_id + "/cached" for context_id in context_ids]
        cached_info = context.redis.mget(cache_keys)
        for context_id, cached_value in zip(context_ids, cached_info):
            if cached_value is None:
                continue
            activity[context_id] = ActivityInfo.from_json(cached_value.decode())

        # count any ones that were not cached
        missing_ids = [id_ for id_ in context_ids if id_ not in activity]
        if not missing_ids:
            return activity

        with context.redis.pipeline("count", transaction=False) as pipe:
            for context_id in missing_ids:
                self.counter.count_activity(pipe, context_id)
            counts = pipe.execute()

        # update the cache with the ones we just counted
        to_cache = {}
        for context_id, count in zip(missing_ids, counts):
            if count is not None:
                info = ActivityInfo.from_count(count)
                to_cache[context_id] = info
        activity.update(to_cache)

        if to_cache:
            with context.redis.pipeline("cache", transaction=False) as pipe:
                for context_id, info in to_cache.items():
                    pipe.setex(context_id + "/cached", _CACHE_TIME, info.to_json())
                pipe.execute()

        return activity


def make_processor(app_config):  # pragma: nocover
    cfg = config.parse_config(app_config, {
        "activity": {
            "window": config.Timespan,
        },
        "tracing": {
            "endpoint": config.Optional(config.Endpoint),
            "service_name": config.String,
        },
        "redis": {
            "url": config.String,
            "max_connections": config.Optional(config.Integer, default=100),
        },
    })

    metrics_client = metrics_client_from_config(app_config)
    tracing_client = tracing_client_from_config(app_config)
    error_reporter = error_reporter_from_config(app_config, __name__)
    redis_pool = redis.BlockingConnectionPool.from_url(
        cfg.redis.url,
        max_connections=cfg.redis.max_connections,
        timeout=0.1,
    )

    baseplate = Baseplate()
    baseplate.configure_logging()
    baseplate.configure_metrics(metrics_client)
    baseplate.configure_tracing(tracing_client)
    baseplate.configure_error_reporting(error_reporter)
    baseplate.add_to_context("redis", RedisContextFactory(redis_pool))

    counter = ActivityCounter(cfg.activity.window.total_seconds())
    handler = Handler(counter=counter)
    processor = ActivityService.ContextProcessor(handler)
    event_handler = BaseplateProcessorEventHandler(logger, baseplate)
    processor.setEventHandler(event_handler)

    return processor
