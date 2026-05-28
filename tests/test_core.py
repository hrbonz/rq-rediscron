from __future__ import annotations

import fnmatch
import time
import unittest
from datetime import timedelta
from typing import Any
from unittest.mock import patch

from rq.cron import CronScheduler
from rq.utils import now

from rediscron.core import (
    CRON_JOBS_INDEX_KEY,
    CRON_JOBS_LAST_UPDATE_KEY,
    CRON_JOBS_LOCK_KEY,
    RedisCronJob,
    RedisCronScheduler,
)


ENQUEUED: list[tuple[int, str | None]] = []


def sample_task(value: int, marker: str | None = None) -> None:
    ENQUEUED.append((value, marker))


class FakeQueue:
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def __init__(self, name: str, connection: Any):
        self.name = name
        self.connection = connection

    def enqueue(self, func, *args, **kwargs):
        self.calls.append((self.name, args, kwargs))
        return {"func": func, "args": args, "kwargs": kwargs}


class MemoryRedis:
    def __init__(self):
        self.hashes: dict[str, dict[str, Any]] = {}
        self.strings: dict[str, Any] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.events: list[tuple[str, str]] = []

    def hset(self, key, mapping=None, *args, **kwargs):
        self.hashes.setdefault(key, {}).update(mapping or {})
        return len(mapping or {})

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def delete(self, key):
        removed = key in self.hashes or key in self.strings or key in self.zsets
        self.hashes.pop(key, None)
        self.strings.pop(key, None)
        self.zsets.pop(key, None)
        return int(removed)

    def zadd(self, key, mapping, **kwargs):
        self.zsets.setdefault(key, {}).update({member: float(score) for member, score in mapping.items()})
        return len(mapping)

    def zrem(self, key, *members):
        zset = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            member = member.decode() if isinstance(member, bytes) else member
            if member in zset:
                removed += 1
                del zset[member]
        return removed

    def zrange(self, key, start, end, withscores=False, **kwargs):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
        if end == -1:
            selected = items[start:]
        else:
            selected = items[start : end + 1]
        if withscores:
            return selected
        return [member for member, _score in selected]

    def zrangebyscore(self, key, min, max, **kwargs):
        min_score = float("-inf") if min == "-inf" else float(min)
        max_score = float("inf") if max == "+inf" else float(max)
        return [
            member
            for member, score in sorted(self.zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
            if min_score <= score <= max_score
        ]

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.strings:
            return False
        self.strings[key] = value
        return True

    def get(self, key):
        return self.strings.get(key)

    def publish(self, channel, payload):
        self.events.append((channel, payload))
        return 1

    def eval(self, script, numkeys, key, token, *args):
        if self.strings.get(key) != token:
            return 0
        if args:
            return 1
        if self.strings.get(key) == token:
            del self.strings[key]
            return 1
        return 0

    def keys(self, pattern):
        return [key for key in set(self.hashes) | set(self.strings) | set(self.zsets) if fnmatch.fnmatch(key, pattern)]


class RedisCronJobTests(unittest.TestCase):
    def test_save_and_fetch_round_trip_executable_fields(self):
        redis = MemoryRedis()
        job = RedisCronJob(
            id="air-quality",
            queue_name="metrics",
            func=sample_task,
            args=(42,),
            kwargs={"marker": "pm25"},
            interval=30,
            result_ttl=10,
            meta={"source": "sensor"},
            connection=redis,
        )

        job.save()
        fetched = RedisCronJob.fetch("air-quality", redis)

        self.assertEqual(fetched.id, "air-quality")
        self.assertEqual(fetched.func, sample_task)
        self.assertEqual(fetched.func_name, f"{__name__}.sample_task")
        self.assertEqual(fetched.queue_name, "metrics")
        self.assertEqual(fetched.args, (42,))
        self.assertEqual(fetched.kwargs, {"marker": "pm25"})
        self.assertEqual(fetched.interval, 30)
        self.assertEqual(fetched.job_options["result_ttl"], 10)
        self.assertEqual(fetched.job_options["meta"], {"source": "sensor"})
        self.assertIn("air-quality", redis.zsets[CRON_JOBS_INDEX_KEY])

    def test_delete_removes_hash_and_index_entry(self):
        redis = MemoryRedis()
        job = RedisCronJob(id="stale", queue_name="default", func=sample_task, interval=60, connection=redis)
        job.save()

        job.delete()

        self.assertEqual(redis.hgetall(job.key), {})
        self.assertNotIn("stale", redis.zsets[CRON_JOBS_INDEX_KEY])


class RedisCronSchedulerTests(unittest.TestCase):
    def setUp(self):
        FakeQueue.calls = []

    def test_register_same_id_edits_existing_job(self):
        redis = MemoryRedis()
        scheduler = RedisCronScheduler(redis)
        scheduler.register(sample_task, "default", id="metric", args=(1,), interval=60)
        first = RedisCronJob.fetch("metric", redis)
        scheduler.register(sample_task, "critical", id="metric", args=(2,), interval=120)

        edited = RedisCronJob.fetch("metric", redis)
        self.assertEqual(edited.created_at, first.created_at)
        self.assertEqual(edited.queue_name, "critical")
        self.assertEqual(edited.args, (2,))
        self.assertEqual(edited.interval, 120)
        self.assertEqual(list(redis.zsets[CRON_JOBS_INDEX_KEY]), ["metric"])

    def test_enqueue_jobs_uses_lock_and_updates_next_schedule(self):
        redis = MemoryRedis()
        scheduler = RedisCronScheduler(redis)
        job = scheduler.register(sample_task, "default", id="metric", args=(1,), interval=60)
        job.next_enqueue_time = now() - timedelta(seconds=1)
        job.save()
        self.assertIsNotNone(scheduler._acquire_lock())

        import rediscron.core as redis_cron

        original_queue = redis_cron.Queue
        redis_cron.Queue = FakeQueue
        try:
            enqueued = scheduler.enqueue_jobs()
        finally:
            redis_cron.Queue = original_queue

        self.assertEqual([job.id for job in enqueued], ["metric"])
        self.assertEqual(FakeQueue.calls[0][0], "default")
        fetched = RedisCronJob.fetch("metric", redis)
        self.assertIsNotNone(fetched.latest_enqueue_time)
        self.assertGreater(redis.zsets[CRON_JOBS_INDEX_KEY]["metric"], time.time())
        self.assertIn(CRON_JOBS_LOCK_KEY, redis.strings)

    def test_enqueue_without_owned_scheduler_lock_does_not_enqueue(self):
        redis = MemoryRedis()
        scheduler = RedisCronScheduler(redis)
        job = scheduler.register(sample_task, "default", id="metric", args=(1,), interval=60)
        job.next_enqueue_time = now() - timedelta(seconds=1)
        job.save()
        redis.set(CRON_JOBS_LOCK_KEY, "other-owner")

        self.assertEqual(scheduler.enqueue_jobs(), [])
        self.assertEqual(FakeQueue.calls, [])

    def test_register_birth_acquires_lifetime_lock(self):
        redis = MemoryRedis()
        scheduler = RedisCronScheduler(redis)

        with patch.object(CronScheduler, "register_birth") as register_birth:
            scheduler.register_birth()

        register_birth.assert_called_once_with()
        self.assertEqual(redis.get(CRON_JOBS_LOCK_KEY), scheduler._lock_token)

    def test_register_birth_fails_when_another_scheduler_owns_lock(self):
        redis = MemoryRedis()
        redis.set(CRON_JOBS_LOCK_KEY, "other-owner")
        scheduler = RedisCronScheduler(redis)

        with patch.object(CronScheduler, "register_birth") as register_birth:
            with self.assertRaises(RuntimeError):
                scheduler.register_birth()

        register_birth.assert_not_called()

    def test_register_death_releases_owned_lifetime_lock(self):
        redis = MemoryRedis()
        scheduler = RedisCronScheduler(redis)
        token = scheduler._acquire_lock()
        self.assertIsNotNone(token)

        with patch.object(CronScheduler, "register_death") as register_death:
            scheduler.register_death()

        register_death.assert_called_once_with(None)
        self.assertNotIn(CRON_JOBS_LOCK_KEY, redis.strings)

    def test_release_lock_does_not_delete_another_owner(self):
        redis = MemoryRedis()
        scheduler = RedisCronScheduler(redis)
        redis.set(CRON_JOBS_LOCK_KEY, "other-owner")

        self.assertFalse(scheduler._release_lock("mine"))
        self.assertEqual(redis.get(CRON_JOBS_LOCK_KEY), "other-owner")

    def test_calculate_sleep_interval_reads_sorted_set(self):
        redis = MemoryRedis()
        scheduler = RedisCronScheduler(redis)
        redis.zadd(CRON_JOBS_INDEX_KEY, {"metric": time.time() + 5})

        sleep = scheduler.calculate_sleep_interval()

        self.assertGreater(sleep, 0)
        self.assertLessEqual(sleep, 5)

    def test_update_marker_changes_when_job_saved(self):
        redis = MemoryRedis()
        scheduler = RedisCronScheduler(redis)
        self.assertIsNone(scheduler.last_seen_update)

        RedisCronJob(id="metric", queue_name="default", func=sample_task, interval=60, connection=redis).save()

        self.assertTrue(scheduler._refresh_update_marker())
        self.assertIsNotNone(redis.get(CRON_JOBS_LAST_UPDATE_KEY))


if __name__ == "__main__":
    unittest.main()
