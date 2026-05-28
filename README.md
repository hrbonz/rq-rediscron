# rq-rediscron

`rq-rediscron` provides Redis-backed subclasses for RQ cron scheduling:

- `RedisCronJob`
- `RedisCronScheduler`

Cron job definitions are stored in Redis hashes and indexed in a sorted set by
next enqueue time. This lets scheduler processes pick up job creates, edits, and
deletes at runtime without restart.

Only one `RedisCronScheduler` process runs at a time. The scheduler acquires
`rq:cron_jobs:lock` during `register_birth()`, extends that lock on heartbeat,
and releases it during `register_death()`. A second scheduler can start after
the lock TTL expires, which provides failover without duplicate enqueue loops.

```python
from redis import Redis
from rediscron import RedisCronScheduler


def rebuild_metric(metric_id: str) -> None:
    ...


connection = Redis.from_url("redis://localhost:6379/0")
scheduler = RedisCronScheduler(connection)
scheduler.register(
    rebuild_metric,
    queue_name="metrics",
    id="metric:pm25",
    args=("pm25",),
    interval=300,
)
scheduler.start()
```

Redis keys:

- `rq:cron_job:{id}` stores one cron job hash.
- `rq:cron_jobs` indexes job ids by next enqueue timestamp.
- `rq:cron_jobs:lock` allows one live scheduler process at a time.
- `rq:cron_jobs:last_update` records runtime changes.
- `rq:cron_jobs:events` publishes create, update, and delete events.
