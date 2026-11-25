# Deployment

Build the image locally:

```sh
docker build -t job-broker:$(git rev-parse --short HEAD) .
```

Use the Git SHA as the image tag, for example `job-broker:<git-sha>`.
Docker Compose mounts a named `/data` volume and reads local configuration
from `.env`; copy `.env.example` and set a real `ADMIN_TOKEN`.

Roll out a new tag, wait for `/healthz` and `/readyz`, and keep the existing
named volume attached. Roll back by starting the previous image tag against
the same volume.

SQLite schema changes are guarded by `meta.schema_version`: an image refuses to
start if the database schema is newer than it supports, so a rollback cannot
silently corrupt data.

Backup SQLite by stopping writes, copying `job-broker.db` plus `-wal` and
`-shm`, or by taking an online SQLite backup with `sqlite3`. Restore by placing
all files on `/data` and starting the matching or newer compatible image.

| Config | Purpose | Default |
|---|---|---:|
| `DB_PATH` | SQLite file | `/data/job-broker.db` |
| `ADMIN_TOKEN` | Protects `/admin/advance` | unset |
| `MAX_NODES` | Alive launch/node cap | 10 |
| `IDLE_TIMEOUT_MINUTES` | Ready idle reaper | 10 |
| `FAIRNESS_POLICY` | `fair` or `fifo` | fair |
| `SEED` | Deterministic provider RNG | 42 |

Next for production: move state from SQLite to Postgres, run workers behind a
job queue, deploy on Kubernetes/ECS, and use a secrets manager for admin
credentials. Add team-scoped authentication and distributed scheduling. Add
object storage for trace/artifact retention and a real metrics backend.
