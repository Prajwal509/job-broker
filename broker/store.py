import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .models import Job, JobStatus, LedgerSegment, Node, NodeState, Priority

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  team TEXT NOT NULL,
  gpus INTEGER NOT NULL,
  estimated_minutes INTEGER NOT NULL,
  priority TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status TEXT NOT NULL,
  submitted_at INTEGER NOT NULL,
  started_at INTEGER,
  finished_at INTEGER,
  ends_at INTEGER,
  actual_minutes INTEGER,
  outcome TEXT NOT NULL,
  node_id TEXT,
  UNIQUE(team, idempotency_key)
);
CREATE TABLE IF NOT EXISTS nodes (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  state TEXT NOT NULL,
  launched_at INTEGER NOT NULL,
  ready_at INTEGER,
  terminated_at INTEGER,
  tenant TEXT,
  failure_at INTEGER,
  idle_since INTEGER,
  last_tenant TEXT
);
-- One row per interval in which a node's tenant and GPUs-in-use did not change.
CREATE TABLE IF NOT EXISTS ledger (
  id INTEGER PRIMARY KEY,
  node_id TEXT NOT NULL,
  start INTEGER NOT NULL,
  end INTEGER,
  tenant TEXT,
  gpus_used INTEGER NOT NULL
);
-- Simulated clock, retry/backoff state, provider RNG state, schema version.
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

JOB_COLUMNS = (
    "id", "team", "gpus", "estimated_minutes", "priority", "idempotency_key",
    "status", "submitted_at", "started_at", "finished_at", "ends_at",
    "actual_minutes", "outcome", "node_id",
)
NODE_COLUMNS = (
    "id", "type", "state", "launched_at", "ready_at", "terminated_at",
    "tenant", "failure_at", "idle_since", "last_tenant",
)


def _upsert_sql(table: str, columns: tuple[str, ...]) -> str:
    updates = ", ".join(f"{c}=excluded.{c}" for c in columns[1:])
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)}) "
        f"ON CONFLICT(id) DO UPDATE SET {updates}"
    )


class Store:
    SCHEMA_VERSION = 1

    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        version = self.get_meta("schema_version")
        if version is None:
            self.set_meta({"schema_version": self.SCHEMA_VERSION})
        elif version > self.SCHEMA_VERSION:
            # Refuse to run an older image against a newer database (safe rollback).
            raise RuntimeError(
                f"database schema {version} is newer than supported {self.SCHEMA_VERSION}"
            )

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Commit everything inside as one unit. Nested calls join the outer one."""
        with self._lock:
            if self.conn.in_transaction:
                yield
                return
            self.conn.execute("BEGIN")
            try:
                yield
            except Exception:
                self.conn.rollback()
                raise
            self.conn.commit()

    def save_job(self, job: Job) -> None:
        values = [getattr(job, c) for c in JOB_COLUMNS]
        with self.transaction():
            self.conn.execute(_upsert_sql("jobs", JOB_COLUMNS), values)

    def save_node(self, node: Node) -> None:
        values = [getattr(node, c) for c in NODE_COLUMNS]
        with self.transaction():
            self.conn.execute(_upsert_sql("nodes", NODE_COLUMNS), values)

    def get_job_by_key(self, team: str, key: str) -> Job | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM jobs WHERE team=? AND idempotency_key=?", (team, key)
            ).fetchone()
        return self._job(row) if row else None

    def jobs(self) -> list[Job]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
        return [self._job(row) for row in rows]

    def nodes(self) -> list[Node]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM nodes ORDER BY rowid").fetchall()
        return [
            Node(**{c: row[c] for c in NODE_COLUMNS} | {"state": NodeState(row["state"])})
            for row in rows
        ]

    def sync_ledger_segment(
        self, node_id: str, timestamp: int, tenant: str | None, gpus_used: int, active: bool
    ) -> None:
        """Close the node's open segment and open a new one if anything changed."""
        with self.transaction():
            row = self.conn.execute(
                "SELECT id,tenant,gpus_used FROM ledger "
                "WHERE node_id=? AND end IS NULL ORDER BY id DESC LIMIT 1",
                (node_id,),
            ).fetchone()
            if active and row and row["tenant"] == tenant and row["gpus_used"] == gpus_used:
                return
            if row:
                self.conn.execute("UPDATE ledger SET end=? WHERE id=?", (timestamp, row["id"]))
            if active:
                self.conn.execute(
                    "INSERT INTO ledger(node_id,start,end,tenant,gpus_used) VALUES (?,?,NULL,?,?)",
                    (node_id, timestamp, tenant, gpus_used),
                )

    def ledger_segments(self) -> list[LedgerSegment]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT node_id,start,end,tenant,gpus_used FROM ledger ORDER BY id"
            ).fetchall()
        return [LedgerSegment(**dict(row)) for row in rows]

    def set_meta(self, values: dict[str, object]) -> None:
        with self.transaction():
            for key, value in values.items():
                self.conn.execute(
                    "INSERT INTO meta(key,value) VALUES (?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, json.dumps(value)),
                )

    def get_meta(self, key: str, default=None):
        with self._lock:
            row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return default if row is None else json.loads(row["value"])

    def ping(self) -> None:
        with self._lock:
            self.conn.execute("SELECT 1").fetchone()

    @staticmethod
    def _job(row: sqlite3.Row) -> Job:
        return Job(
            **{c: row[c] for c in JOB_COLUMNS}
            | {"priority": Priority(row["priority"]), "status": JobStatus(row["status"])}
        )
