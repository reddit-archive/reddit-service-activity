"""Count activity using Redis HyperLogLogs.

This module implements a real-time activity counter backed by Redis using its
HyperLogLog data structures. HLL allows us to estimate, with known accuracy,
the cardinality of a set of data while using a small fixed amount of memory.
This is perfect for counting how many visitors are active within a context.

However, HLLs do not have a way of expiring items out of the set after a period
of time. To approximate this, we keep a different HLL for each minute of time.
When activity is recorded, we add it to the current time slice's HLL. To count
the activity in the past N minutes, we merge the HLLs for the most recent N
minutes.

(It's also possible to do the opposite and write to multiple slices in the
future and do a read on only the current slice, but since we're caching the
read-result, this keeps the uncached path more efficient.)

For more info on Redis's HLL support, see: http://antirez.com/news/75

"""
import time


_SLICE_KEY_FORMAT = "{context_id}/{slice:d}"
_SLICE_LENGTH = 15  # seconds


def _current_slice():
    return int(time.time() // _SLICE_LENGTH)


def _make_key(context_id, slice, offset=0):
    slice += offset
    return _SLICE_KEY_FORMAT.format(context_id=context_id, slice=slice)


class ActivityCounter(object):
    def __init__(self, activity_window):
        slice_count, remainder = divmod(activity_window, _SLICE_LENGTH)
        assert remainder == 0
        self.slice_count = int(slice_count)

    def record_activity(self, redis, context_id, visitor_id):
        current_slice = _current_slice()
        key = _make_key(context_id, current_slice)
        expiration = (current_slice + self.slice_count + 1) * _SLICE_LENGTH

        with redis.pipeline("record") as pipe:
            pipe.execute_command("PFADD", key, visitor_id)
            pipe.expireat(key, expiration)
            pipe.execute()

    def count_activity(self, redis, context_id):
        slice = _current_slice()
        keys = [_make_key(context_id, slice, -i)
            for i in range(self.slice_count)]
        return redis.execute_command("PFCOUNT", *keys)
