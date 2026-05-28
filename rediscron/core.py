from __future__ import annotations

import importlib
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from redis import Redis
from rq import Queue
from rq.cron import CronJob, CronScheduler
from rq.defaults import DEFAULT_RESULT_TTL
from rq.exceptions import NoSuchJobError, StopRequested
from rq.serializers import resolve_serializer
from rq.utils import now, utcformat, utcparse


CRON_JOB_KEY_PREFIX = "rq:cron_job:"
CRON_JOBS_INDEX_KEY = "rq:cron_jobs"
CRON_JOBS_LOCK_KEY = "rq:cron_jobs:lock"
CRON_JOBS_LAST_UPDATE_KEY = "rq:cron_jobs:last_update"
CRON_JOBS_EVENTS_CHANNEL = "rq:cron_jobs:events"

ACTIVE_STATUS = "active"
DELETED_STATUS = "deleted"

_RELEASE_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

_EXTEND_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
end
return 0
"""


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode()
    return value


def _decode_hash(data: dict[Any, Any]) -> dict[str, Any]:
    return {str(_decode(key)): _decode(value) for key, value in data.items()}


def _dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _loads(value: Any, default: Any) -> Any:
    value = _decode(value)
    if value in (None, ""):
        return default
    return json.loads(value)


def _optional_int(value: Any) -> int | None:
    value = _decode(value)
    if value in (None, ""):
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    value = _decode(value)
    if value in (None, ""):
        return None
    return float(value)


def _optional_datetime(value: Any) -> datetime | None:
    value = _decode(value)
    if value in (None, ""):
        return None
    return utcparse(value)


def _import_attribute(path: str) -> Callable:
    module_name, _, attribute_name = path.rpartition(".")
    if not module_name or not attribute_name:
        raise ValueError(f"Function path must be a dotted import path: {path!r}")
    module = importlib.import_module(module_name)
    func = getattr(module, attribute_name)
    if not callable(func):
        raise TypeError(f"Imported object is not callable: {path!r}")
    return func


def _timestamp(value: datetime | None) -> float:
    if value is None:
        return 0.0
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


class RedisCronJob(CronJob):
    """A Redis-persisted RQ cron job with a stable application-owned id.

    `RedisCronJob` is the persisted form of one recurring job. The job is stored
    in a Redis hash at `rq:cron_job:{id}` and indexed in `rq:cron_jobs` by its
    next enqueue timestamp.

    Create or update jobs through `RedisCronScheduler.register()` for normal
    use. Use this class directly when you are building administration tools that
    need to fetch, refresh, or delete one job.

    Example:

    .. code-block:: python

        from redis import Redis
        from rediscron import RedisCronJob

        def rebuild_metric(metric_id: str) -> None:

        redis = Redis.from_url("redis://localhost:6379/0")
        job = RedisCronJob(
            id="metric:pm25",
            queue_name="metrics",
            func=rebuild_metric,
            args=("pm25",),
            interval=300,
            connection=redis,
        )
        job.save(event="created")

        same_job = RedisCronJob.fetch("metric:pm25", redis)
        same_job.delete()

    """

    def __init__(
        self,
        id: str,
        queue_name: str,
        func: Callable | None = None,
        func_name: str | None = None,
        args: tuple | None = None,
        kwargs: dict | None = None,
        interval: int | None = None,
        cron: str | None = None,
        job_timeout: int | None = None,
        result_ttl: int = DEFAULT_RESULT_TTL,
        ttl: int | None = None,
        failure_ttl: int | None = None,
        meta: dict | None = None,
        connection: Redis | None = None,
        latest_enqueue_time: datetime | None = None,
        next_enqueue_time: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        status: str = ACTIVE_STATUS,
    ):
        if not id:
            raise ValueError("RedisCronJob requires an explicit stable id")

        super().__init__(
            queue_name=queue_name,
            func=func,
            func_name=func_name,
            args=args,
            kwargs=kwargs,
            interval=interval,
            cron=cron,
            job_timeout=job_timeout,
            result_ttl=result_ttl,
            ttl=ttl,
            failure_ttl=failure_ttl,
            meta=meta,
        )
        self.id = id
        self.connection = connection
        self.created_at = created_at or now()
        self.updated_at = updated_at or self.created_at
        self.status = status

        if latest_enqueue_time is not None:
            self.latest_enqueue_time = latest_enqueue_time
        if next_enqueue_time is not None:
            self.next_enqueue_time = next_enqueue_time
        elif latest_enqueue_time is not None:
            self.next_enqueue_time = self.get_next_enqueue_time()

    @property
    def key(self) -> str:
        """Redis hash key for this cron job.

        Example:

        .. code-block:: python

            job = RedisCronJob.fetch("metric:pm25", redis)
            assert job.key == "rq:cron_job:metric:pm25"
        """
        return f"{CRON_JOB_KEY_PREFIX}{self.id}"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the cron job into Redis hash fields.

        Values are intentionally primitive strings, integers, or JSON strings so
        the hash can be inspected with `redis-cli hgetall rq:cron_job:{id}`.
        `args`, `kwargs`, and `meta` are JSON encoded; datetimes are stored in
        RQ's UTC datetime format.
        """
        data: dict[str, Any] = {
            "id": self.id,
            "func_name": self.func_name,
            "queue_name": self.queue_name,
            "args": _dumps(self.args),
            "kwargs": _dumps(self.kwargs),
            "interval": self.interval if self.interval is not None else "",
            "cron": self.cron or "",
            "latest_enqueue_time": utcformat(self.latest_enqueue_time) if self.latest_enqueue_time else "",
            "next_enqueue_time": utcformat(self.next_enqueue_time) if self.next_enqueue_time else "",
            "created_at": utcformat(self.created_at),
            "updated_at": utcformat(self.updated_at),
            "status": self.status,
        }
        for key in ("job_timeout", "result_ttl", "ttl", "failure_ttl"):
            data[key] = self.job_options.get(key, "")
        data["meta"] = _dumps(self.job_options.get("meta", {}))
        return data

    def save(self, pipeline: Any | None = None, event: str = "updated") -> None:
        """Persist this cron job and notify schedulers about the change.

        `save()` writes the hash, updates the sorted-set index, bumps
        `rq:cron_jobs:last_update`, and publishes an event on
        `rq:cron_jobs:events`.

        Example:

        .. code-block:: python

            job = RedisCronJob.fetch("metric:pm25", redis)
            job.interval = 600
            job.save(event="updated")
        """
        connection = pipeline or self.connection
        if connection is None:
            raise ValueError("RedisCronJob.save() requires a Redis connection")

        self.updated_at = now()
        logging.getLogger(__name__).debug(
            "Saving Redis cron job id=%s event=%s queue=%s func=%s interval=%s cron=%s next=%s",
            self.id,
            event,
            self.queue_name,
            self.func_name,
            self.interval,
            self.cron,
            self.next_enqueue_time,
        )
        connection.hset(self.key, mapping=self.to_dict())
        if self.status == ACTIVE_STATUS:
            connection.zadd(CRON_JOBS_INDEX_KEY, {self.id: _timestamp(self.next_enqueue_time)})
            logging.getLogger(__name__).debug(
                "Indexed Redis cron job id=%s score=%s", self.id, _timestamp(self.next_enqueue_time)
            )
        else:
            connection.zrem(CRON_JOBS_INDEX_KEY, self.id)
            logging.getLogger(__name__).debug("Removed inactive Redis cron job id=%s from index", self.id)
        self._bump_update(connection, event, self.id)

    def refresh(self) -> None:
        """Reload this job from Redis in place.

        Example:

        .. code-block:: python

            job = RedisCronJob.fetch("metric:pm25", redis)
            # Another process edits the job.
            job.refresh()
        """
        if self.connection is None:
            raise ValueError("RedisCronJob.refresh() requires a Redis connection")
        logging.getLogger(__name__).debug("Refreshing Redis cron job id=%s", self.id)
        fresh = self.fetch(self.id, self.connection)
        self.__dict__.update(fresh.__dict__)

    def delete(self, pipeline: Any | None = None) -> None:
        """Delete this cron job from Redis and notify running schedulers.

        Deletes remove both the job hash and its sorted-set index entry. Running
        schedulers notice the change through Pub/Sub when available and through
        the update marker as the correctness fallback.

        Example:

        .. code-block:: python

            RedisCronJob.fetch("metric:pm25", redis).delete()
        """
        connection = pipeline or self.connection
        if connection is None:
            raise ValueError("RedisCronJob.delete() requires a Redis connection")

        logging.getLogger(__name__).debug("Deleting Redis cron job id=%s key=%s", self.id, self.key)
        connection.delete(self.key)
        connection.zrem(CRON_JOBS_INDEX_KEY, self.id)
        self.status = DELETED_STATUS
        self._bump_update(connection, "deleted", self.id)

    def enqueue(self, connection: Redis):
        """Enqueue one concrete RQ job for this cron definition.

        Restored Redis cron jobs are executable because `restore()` imports the
        stored dotted `func_name` back into a Python callable before enqueueing.
        """
        if not self.func:
            raise ValueError("RedisCronJob has no function to enqueue")

        queue = Queue(self.queue_name, connection=connection)
        logging.getLogger(__name__).debug(
            "Enqueueing Redis cron job id=%s queue=%s func=%s args=%r kwargs=%r options=%r",
            self.id,
            self.queue_name,
            self.func_name,
            self.args,
            self.kwargs,
            self.job_options,
        )
        job = queue.enqueue(self.func, *self.args, **self.kwargs, **self.job_options)
        logging.getLogger(__name__).info("Enqueued cron job %s to queue %s", self.id, self.queue_name)
        return job

    @classmethod
    def fetch(cls, id: str, connection: Redis) -> "RedisCronJob":
        """Fetch one persisted cron job by id.

        Example:

        .. code-block:: python

            job = RedisCronJob.fetch("metric:pm25", redis)
            print(job.next_enqueue_time)
        """
        logging.getLogger(__name__).debug("Fetching Redis cron job id=%s", id)
        raw_data = connection.hgetall(f"{CRON_JOB_KEY_PREFIX}{id}")
        if not raw_data:
            raise NoSuchJobError(f"RedisCronJob {id!r} does not exist")
        job = cls.restore(raw_data, connection=connection)
        if job.id != id:
            raise ValueError(f"Fetched cron job id mismatch: expected {id!r}, got {job.id!r}")
        return job

    @classmethod
    def restore(cls, raw_data: dict[Any, Any], connection: Redis | None = None) -> "RedisCronJob":
        """Restore a `RedisCronJob` from Redis hash data.

        Unlike RQ's monitoring-only `CronJob.from_dict()`, this method imports
        `func_name` and returns a cron job that can be enqueued immediately.
        """
        data = _decode_hash(raw_data)
        func_name = str(data["func_name"])
        logging.getLogger(__name__).debug("Restoring Redis cron job id=%s func=%s", data.get("id"), func_name)
        func = _import_attribute(func_name)
        return cls(
            id=str(data["id"]),
            queue_name=str(data["queue_name"]),
            func=func,
            args=tuple(_loads(data.get("args"), ())),
            kwargs=dict(_loads(data.get("kwargs"), {})),
            interval=_optional_int(data.get("interval")),
            cron=str(data.get("cron") or "") or None,
            job_timeout=_optional_int(data.get("job_timeout")),
            result_ttl=_optional_int(data.get("result_ttl")) or DEFAULT_RESULT_TTL,
            ttl=_optional_int(data.get("ttl")),
            failure_ttl=_optional_int(data.get("failure_ttl")),
            meta=dict(_loads(data.get("meta"), {})),
            connection=connection,
            latest_enqueue_time=_optional_datetime(data.get("latest_enqueue_time")),
            next_enqueue_time=_optional_datetime(data.get("next_enqueue_time")),
            created_at=_optional_datetime(data.get("created_at")) or now(),
            updated_at=_optional_datetime(data.get("updated_at")) or now(),
            status=str(data.get("status") or ACTIVE_STATUS),
        )

    def set_enqueue_time(self, time: datetime) -> None:
        """Record an enqueue timestamp and persist the recalculated next run."""
        super().set_enqueue_time(time)
        if self.connection is not None:
            logging.getLogger(__name__).debug(
                "Set enqueue time for Redis cron job id=%s latest=%s next=%s",
                self.id,
                self.latest_enqueue_time,
                self.next_enqueue_time,
            )
            self.save()

    @staticmethod
    def _bump_update(connection: Any, event: str, job_id: str | None = None) -> None:
        update_value = time.time()
        logging.getLogger(__name__).debug(
            "Bumping Redis cron update marker event=%s job_id=%s value=%s", event, job_id, update_value
        )
        connection.set(CRON_JOBS_LAST_UPDATE_KEY, update_value)
        payload = _dumps({"event": event, "job_id": job_id})
        connection.publish(CRON_JOBS_EVENTS_CHANNEL, payload)


class RedisCronScheduler(CronScheduler):
    """Cron scheduler that uses Redis as the cron job source of truth.

    The scheduler keeps RQ's scheduler registry and heartbeat behavior, but it
    does not use RQ's in-memory `_cron_jobs` list to decide what should run.
    Each loop reads due job ids from the Redis sorted set and fetches the
    corresponding job hash before enqueueing.

    Only one `RedisCronScheduler` process is allowed to be alive at a time. The
    scheduler acquires `rq:cron_jobs:lock` during `register_birth()` and releases
    it during `register_death()`. Heartbeats extend the lock TTL while the
    scheduler is healthy. If another scheduler already owns the lock, startup
    fails before the scheduler registers itself in RQ's registry.

    Example:

    .. code-block:: python

        from redis import Redis
        from rediscron import RedisCronScheduler

        def rebuild_metric(metric_id: str) -> None:

        redis = Redis.from_url("redis://localhost:6379/0")
        scheduler = RedisCronScheduler(redis, lock_ttl=120)
        scheduler.register(
            rebuild_metric,
            queue_name="metrics",
            id="metric:pm25",
            args=("pm25",),
            interval=300,
        )
        scheduler.start()
    """

    def __init__(
        self,
        connection: Redis,
        logging_level: str | int = logging.INFO,
        name: str = "",
        lock_ttl: int = 120,
        max_sleep_interval: float = 60.0,
    ):
        super().__init__(connection=connection, logging_level=logging_level, name=name)
        self.lock_ttl = lock_ttl
        self.max_sleep_interval = max_sleep_interval
        self.last_seen_update = self._get_last_update()
        self._lock_token: str | None = None
        self.serializer = resolve_serializer()

    def register(
        self,
        func: Callable,
        queue_name: str,
        id: str,
        args: tuple | None = None,
        kwargs: dict | None = None,
        interval: int | None = None,
        cron: str | None = None,
        job_timeout: int | None = None,
        result_ttl: int = DEFAULT_RESULT_TTL,
        ttl: int | None = None,
        failure_ttl: int | None = None,
        meta: dict | None = None,
    ) -> RedisCronJob:
        """Create or update one persisted cron job.

        `id` is required and stable. Calling `register()` again with the same id
        edits the existing job instead of adding a duplicate. Runtime edits are
        persisted immediately and published to running schedulers.

        Example:

        .. code-block:: python

            scheduler.register(rebuild_metric, "metrics", id="metric:pm25", interval=300)

            # Later, from another process:
            scheduler.register(rebuild_metric, "metrics", id="metric:pm25", interval=600)
        """
        self.log.debug(
            "Register requested id=%s queue=%s func=%s interval=%s cron=%s args=%r kwargs=%r",
            id,
            queue_name,
            f"{func.__module__}.{func.__name__}",
            interval,
            cron,
            args,
            kwargs,
        )
        try:
            existing = RedisCronJob.fetch(id, self.connection)
            created_at = existing.created_at
            latest_enqueue_time = existing.latest_enqueue_time
            event = "updated"
            self.log.debug(
                "Updating existing Redis cron job id=%s previous_queue=%s previous_interval=%s previous_cron=%s",
                id,
                existing.queue_name,
                existing.interval,
                existing.cron,
            )
        except NoSuchJobError:
            created_at = now()
            latest_enqueue_time = None
            event = "created"
            self.log.debug("Creating new Redis cron job id=%s", id)

        cron_job = RedisCronJob(
            id=id,
            queue_name=queue_name,
            func=func,
            args=args,
            kwargs=kwargs,
            interval=interval,
            cron=cron,
            job_timeout=job_timeout,
            result_ttl=result_ttl,
            ttl=ttl,
            failure_ttl=failure_ttl,
            meta=meta,
            connection=self.connection,
            latest_enqueue_time=latest_enqueue_time,
            created_at=created_at,
            updated_at=now(),
            status=ACTIVE_STATUS,
        )
        cron_job.save(event=event)
        self.log.info("Redis cron job %s id=%s queue=%s", event, id, queue_name)
        return cron_job

    def get_jobs(self) -> list[RedisCronJob]:
        """Return all active jobs currently indexed in Redis.

        Missing hashes are pruned from the sorted set as stale index entries.
        This makes Redis the source of truth while keeping the index tidy.
        """
        ids = self.connection.zrange(CRON_JOBS_INDEX_KEY, 0, -1)
        self.log.debug("Loading Redis cron jobs from index count=%s", len(ids))
        jobs: list[RedisCronJob] = []
        for raw_id in ids:
            job_id = str(_decode(raw_id))
            try:
                job = RedisCronJob.fetch(job_id, self.connection)
            except NoSuchJobError:
                self.log.debug("Pruning stale Redis cron index entry id=%s", job_id)
                self.connection.zrem(CRON_JOBS_INDEX_KEY, job_id)
                continue
            if job.status == ACTIVE_STATUS:
                jobs.append(job)
            else:
                self.log.debug("Skipping inactive Redis cron job id=%s status=%s", job.id, job.status)
        self.log.debug("Loaded active Redis cron jobs count=%s", len(jobs))
        return jobs

    def enqueue_jobs(self) -> list[RedisCronJob]:
        """Enqueue all Redis cron jobs currently due.

        This method assumes the scheduler has already acquired the singleton
        scheduler lock during `register_birth()`. If the lock is missing or was
        lost, no jobs are enqueued.
        """
        if not self._owns_lock():
            self.log.warning(
                "RedisCronScheduler %s: not enqueueing jobs because scheduler lock is not owned", self.name
            )
            return []

        enqueue_time = now()
        enqueued_jobs: list[RedisCronJob] = []
        due_ids = self.connection.zrangebyscore(CRON_JOBS_INDEX_KEY, "-inf", enqueue_time.timestamp())
        self.log.debug(
            "RedisCronScheduler %s: found due cron jobs count=%s at=%s",
            self.name,
            len(due_ids),
            enqueue_time,
        )
        for raw_id in due_ids:
            job_id = str(_decode(raw_id))
            try:
                job = RedisCronJob.fetch(job_id, self.connection)
            except NoSuchJobError:
                self.log.debug("Pruning due cron job id=%s because hash is missing", job_id)
                self.connection.zrem(CRON_JOBS_INDEX_KEY, job_id)
                continue

            if job.status != ACTIVE_STATUS:
                self.log.debug("Removing inactive due cron job id=%s status=%s", job.id, job.status)
                self.connection.zrem(CRON_JOBS_INDEX_KEY, job_id)
                continue
            if not job.should_run():
                self.log.debug(
                    "Due index entry id=%s was not runnable; reindexing next=%s", job.id, job.next_enqueue_time
                )
                self.connection.zadd(CRON_JOBS_INDEX_KEY, {job.id: _timestamp(job.next_enqueue_time)})
                continue

            job.enqueue(self.connection)
            job.set_enqueue_time(enqueue_time)
            enqueued_jobs.append(job)
        self.log.debug("RedisCronScheduler %s: enqueued cron jobs count=%s", self.name, len(enqueued_jobs))
        return enqueued_jobs

    def calculate_sleep_interval(self) -> float:
        """Return seconds until the next indexed cron job is due.

        The value is capped by `max_sleep_interval` so runtime updates are still
        observed even if no Pub/Sub message is received.
        """
        next_job = self.connection.zrange(CRON_JOBS_INDEX_KEY, 0, 0, withscores=True)
        if not next_job:
            self.log.debug("RedisCronScheduler %s: no jobs indexed; using max sleep", self.name)
            return self.max_sleep_interval

        score = float(next_job[0][1])
        seconds_until_next = score - time.time()
        if seconds_until_next <= 0:
            self.log.debug("RedisCronScheduler %s: next job is due now", self.name)
            return 0
        sleep = min(seconds_until_next, self.max_sleep_interval)
        self.log.debug("RedisCronScheduler %s: next job in %.3fs; sleep %.3fs", self.name, seconds_until_next, sleep)
        return sleep

    def save_jobs_data(self) -> None:
        """No-op compatibility hook.

        RQ's `CronScheduler` persists the in-memory `_cron_jobs` list into the
        scheduler hash. `RedisCronScheduler` stores each job independently, so
        there is no scheduler-local job list to save.
        """
        return None

    def start(self):
        """Start the singleton Redis cron scheduler loop.

        Startup acquires the scheduler lock and registers birth. Shutdown
        unregisters from RQ's registry and releases the lock only if this
        scheduler still owns it.
        """
        self.log.info("RedisCronScheduler %s: starting...", self.name)
        self._install_signal_handlers()
        self.register_birth()

        pubsub = self.connection.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(CRON_JOBS_EVENTS_CHANNEL)
        try:
            while True:
                self._refresh_update_marker()
                self.enqueue_jobs()
                self.heartbeat()
                sleep_time = min(self.calculate_sleep_interval(), max(1.0, self.lock_ttl / 2))
                self.log.debug("RedisCronScheduler %s: sleep for %.3fs", self.name, sleep_time)
                if sleep_time > 0:
                    message = pubsub.get_message(timeout=sleep_time)
                    if message is not None:
                        self._refresh_update_marker(force=True)
        except KeyboardInterrupt:
            self.log.info('RedisCronScheduler %s: received KeyboardInterrupt', self.name)
        except StopRequested:
            self.log.info('RedisCronScheduler %s: stop requested', self.name)
        finally:
            pubsub.close()
            self.register_death()
            self.log.info("RedisCronScheduler %s: shutdown complete", self.name)

    def register_birth(self) -> None:
        """Acquire the singleton scheduler lock and register this scheduler.

        If the lock is already owned, this method raises `RuntimeError` before
        registering the scheduler with RQ. This prevents two scheduler processes
        from running at the same time and enqueueing duplicate work.
        """
        token = self._acquire_lock()
        if token is None:
            self.log.warning("RedisCronScheduler %s: another scheduler already owns %s", self.name, CRON_JOBS_LOCK_KEY)
            raise RuntimeError(f"Another RedisCronScheduler already owns {CRON_JOBS_LOCK_KEY}")

        self.log.debug("RedisCronScheduler %s: acquired scheduler lock token=%s", self.name, token)
        try:
            super().register_birth()
        except Exception:
            self._release_lock(token)
            raise

    def register_death(self, pipeline: Any | None = None) -> None:
        """Unregister this scheduler and release the singleton lock."""
        try:
            super().register_death(pipeline)
        finally:
            if self._lock_token is not None:
                self._release_lock(self._lock_token)

    def heartbeat(self) -> None:
        """Heartbeat RQ's scheduler registry and extend the scheduler lock.

        If the scheduler no longer owns the lock, `StopRequested` is raised so
        the process exits instead of continuing alongside another scheduler.
        """
        super().heartbeat()
        if self._lock_token is not None and not self._extend_lock(self._lock_token):
            self.log.warning("RedisCronScheduler %s: lost scheduler lock; stopping", self.name)
            raise StopRequested()

    def _acquire_lock(self) -> str | None:
        token = f"{self.name}:{uuid.uuid4().hex}"
        acquired = self.connection.set(CRON_JOBS_LOCK_KEY, token, nx=True, ex=self.lock_ttl)
        if acquired:
            self._lock_token = token
            self.log.debug(
                "RedisCronScheduler %s: lock acquired key=%s ttl=%s",
                self.name,
                CRON_JOBS_LOCK_KEY,
                self.lock_ttl,
            )
            return token
        self.log.debug("RedisCronScheduler %s: lock acquisition failed key=%s", self.name, CRON_JOBS_LOCK_KEY)
        return None

    def _release_lock(self, token: str) -> bool:
        released = bool(self.connection.eval(_RELEASE_LOCK_SCRIPT, 1, CRON_JOBS_LOCK_KEY, token))
        if released and self._lock_token == token:
            self._lock_token = None
        self.log.debug("RedisCronScheduler %s: lock release token=%s released=%s", self.name, token, released)
        return released

    def _extend_lock(self, token: str) -> bool:
        extended = bool(self.connection.eval(_EXTEND_LOCK_SCRIPT, 1, CRON_JOBS_LOCK_KEY, token, self.lock_ttl))
        self.log.debug("RedisCronScheduler %s: lock extend token=%s extended=%s", self.name, token, extended)
        return extended

    def _owns_lock(self) -> bool:
        if self._lock_token is None:
            return False
        return _decode(self.connection.get(CRON_JOBS_LOCK_KEY)) == self._lock_token

    def _get_last_update(self) -> float | None:
        return _optional_float(self.connection.get(CRON_JOBS_LAST_UPDATE_KEY))

    def _refresh_update_marker(self, force: bool = False) -> bool:
        latest = self._get_last_update()
        changed = force or latest != self.last_seen_update
        if changed:
            self.log.debug(
                "RedisCronScheduler %s: observed cron update marker old=%s new=%s force=%s",
                self.name,
                self.last_seen_update,
                latest,
                force,
            )
            self.last_seen_update = latest
        return changed
