import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from .models import Job, JobStatus, LedgerSegment, Node, NodeState, Priority


class Store:
    SCHEMA_VERSION = 1

    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        self._lock.acquire()
        try:
            if self.conn.in_transaction:
                yield
                return
            self.conn.execute("BEGIN")
            try:
                yield
            except Exception:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()
        finally:
            self._lock.release()

    def _create_tables(self) -> None:
        self.conn.executescript(
            """
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
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ledger (
              id INTEGER PRIMARY KEY,
              node_id TEXT NOT NULL,
              start INTEGER NOT NULL,
              end INTEGER,
              tenant TEXT,
              gpus_used INTEGER NOT NULL
            );
            """
        )
        self.conn.commit()
        job_columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        if "node_id" not in job_columns:
            self.conn.execute("ALTER TABLE jobs ADD COLUMN node_id TEXT")
            self.conn.commit()
        node_columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(nodes)").fetchall()
        }
        if "idle_since" not in node_columns:
            self.conn.execute("ALTER TABLE nodes ADD COLUMN idle_since INTEGER")
            self.conn.commit()
        if "last_tenant" not in node_columns:
            self.conn.execute("ALTER TABLE nodes ADD COLUMN last_tenant TEXT")
            self.conn.commit()
        ledger_columns = {
            row["name"]: row
            for row in self.conn.execute("PRAGMA table_info(ledger)").fetchall()
        }
        if "id" not in ledger_columns or ledger_columns["end"]["notnull"]:
            self.conn.executescript(
                """
                ALTER TABLE ledger RENAME TO ledger_old;
                CREATE TABLE ledger (
                  id INTEGER PRIMARY KEY,
                  node_id TEXT NOT NULL,
                  start INTEGER NOT NULL,
                  end INTEGER,
                  tenant TEXT,
                  gpus_used INTEGER NOT NULL
                );
                INSERT INTO ledger(node_id,start,end,tenant,gpus_used)
                  SELECT node_id,start,end,tenant,gpus_used FROM ledger_old;
                DROP TABLE ledger_old;
                """
            )
        schema_version = self.get_meta("schema_version")
        if schema_version is None:
            self.set_meta("schema_version", self.SCHEMA_VERSION)
        elif schema_version > self.SCHEMA_VERSION:
            raise RuntimeError(
                f"database schema {schema_version} is newer than supported "
                f"schema {self.SCHEMA_VERSION}"
            )

    def save_job(self, job: Job) -> None:
        with self.transaction():
            self.conn.execute(
                """INSERT INTO jobs (
                   id, team, gpus, estimated_minutes, priority, idempotency_key,
                   status, submitted_at, started_at, finished_at, ends_at,
                   actual_minutes, outcome, node_id
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(id) DO UPDATE SET
                   status=excluded.status, started_at=excluded.started_at,
                   finished_at=excluded.finished_at, ends_at=excluded.ends_at,
                   actual_minutes=excluded.actual_minutes, outcome=excluded.outcome,
                   node_id=excluded.node_id""",
                (
                    job.id,
                    job.team,
                    job.gpus,
                    job.estimated_minutes,
                    job.priority.value,
                    job.idempotency_key,
                    job.status.value,
                    job.submitted_at,
                    job.started_at,
                    job.finished_at,
                    job.ends_at,
                    job.actual_minutes,
                    job.outcome,
                    job.node_id,
                ),
            )

    def save_node(self, node: Node) -> None:
        with self.transaction():
            self.conn.execute(
                """INSERT INTO nodes (
                   id, type, state, launched_at, ready_at, terminated_at, tenant, failure_at
                   , idle_since, last_tenant
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(id) DO UPDATE SET
                   state=excluded.state, ready_at=excluded.ready_at,
                   terminated_at=excluded.terminated_at, tenant=excluded.tenant,
                   failure_at=excluded.failure_at, idle_since=excluded.idle_since,
                   last_tenant=excluded.last_tenant""",
                (
                    node.id,
                    node.type,
                    node.state.value,
                    node.launched_at,
                    node.ready_at,
                    node.terminated_at,
                    node.tenant,
                    node.failure_at,
                    node.idle_since,
                    node.last_tenant,
                ),
            )

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
        return self._job(row) if row else None

    def get_job_by_key(self, team: str, key: str) -> Job | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM jobs WHERE team=? AND idempotency_key=?", (team, key)
            ).fetchone()
        return self._job(row) if row else None

    def jobs(self) -> list[Job]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM jobs").fetchall()
        return [self._job(row) for row in rows]

    def nodes(self) -> list[Node]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM nodes").fetchall()
        return [
            Node(
                id=row["id"],
                type=row["type"],
                state=NodeState(row["state"]),
                launched_at=row["launched_at"],
                ready_at=row["ready_at"],
                terminated_at=row["terminated_at"],
                tenant=row["tenant"],
                failure_at=row["failure_at"],
                idle_since=row["idle_since"] if "idle_since" in row.keys() else None,
                last_tenant=row["last_tenant"] if "last_tenant" in row.keys() else None,
            )
            for row in rows
        ]

    def set_meta(self, key: str, value) -> None:
        encoded = json.dumps(value) if not isinstance(value, str) else value
        self.set_meta_values({key: encoded}, encoded=True)

    def set_meta_values(self, values: dict[str, object], encoded: bool = False) -> None:
        with self.transaction():
            for key, value in values.items():
                stored = (
                    value
                    if encoded and isinstance(value, str)
                    else json.dumps(value)
                )
                self.conn.execute(
                    "INSERT INTO meta(key,value) VALUES (?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, stored),
                )

    def sync_ledger_segment(
        self,
        node_id: str,
        timestamp: int,
        tenant: str | None,
        gpus_used: int,
        active: bool,
    ) -> None:
        with self.transaction():
            row = self.conn.execute(
                "SELECT id,tenant,gpus_used FROM ledger "
                "WHERE node_id=? AND end IS NULL ORDER BY id DESC LIMIT 1",
                (node_id,),
            ).fetchone()
            if (
                active
                and row is not None
                and row["tenant"] == tenant
                and row["gpus_used"] == gpus_used
            ):
                return
            if row is not None:
                self.conn.execute(
                    "UPDATE ledger SET end=? WHERE id=?",
                    (timestamp, row["id"]),
                )
            if active:
                self.conn.execute(
                    "INSERT INTO ledger(node_id,start,end,tenant,gpus_used) "
                    "VALUES (?,?,NULL,?,?)",
                    (node_id, timestamp, tenant, gpus_used),
                )

    def ledger_segments(self) -> list[LedgerSegment]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT node_id,start,end,tenant,gpus_used FROM ledger ORDER BY id"
            ).fetchall()
        return [
            LedgerSegment(
                node_id=row["node_id"],
                start=row["start"],
                end=row["end"],
                tenant=row["tenant"],
                gpus_used=row["gpus_used"],
            )
            for row in rows
        ]

    def get_meta(self, key: str, default=None):
        with self._lock:
            row = self.conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)
            ).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return row["value"]

    def ping(self) -> None:
        with self._lock:
            self.conn.execute("SELECT 1").fetchone()

    @staticmethod
    def _job(row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            team=row["team"],
            gpus=row["gpus"],
            estimated_minutes=row["estimated_minutes"],
            priority=Priority(row["priority"]),
            idempotency_key=row["idempotency_key"],
            status=JobStatus(row["status"]),
            submitted_at=row["submitted_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            ends_at=row["ends_at"],
            actual_minutes=row["actual_minutes"],
            outcome=row["outcome"],
            node_id=row["node_id"] if "node_id" in row.keys() else None,
        )
