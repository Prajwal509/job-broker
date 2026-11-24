import json
import sqlite3
from pathlib import Path

from .models import Job, JobStatus, Node, NodeState, Priority


class Store:
    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

    def close(self) -> None:
        self.conn.close()

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
              idle_since INTEGER
            );
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ledger (
              node_id TEXT NOT NULL,
              start INTEGER NOT NULL,
              end INTEGER NOT NULL,
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

    def save_job(self, job: Job) -> None:
        with self.conn:
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
        with self.conn:
            self.conn.execute(
                """INSERT INTO nodes (
                   id, type, state, launched_at, ready_at, terminated_at, tenant, failure_at
                   , idle_since
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(id) DO UPDATE SET
                   state=excluded.state, ready_at=excluded.ready_at,
                   terminated_at=excluded.terminated_at, tenant=excluded.tenant,
                   failure_at=excluded.failure_at, idle_since=excluded.idle_since""",
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
                ),
            )

    def get_job(self, job_id: str) -> Job | None:
        row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._job(row) if row else None

    def get_job_by_key(self, team: str, key: str) -> Job | None:
        row = self.conn.execute(
            "SELECT * FROM jobs WHERE team=? AND idempotency_key=?", (team, key)
        ).fetchone()
        return self._job(row) if row else None

    def jobs(self) -> list[Job]:
        return [self._job(row) for row in self.conn.execute("SELECT * FROM jobs")]

    def nodes(self) -> list[Node]:
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
            )
            for row in rows
        ]

    def set_meta(self, key: str, value) -> None:
        encoded = json.dumps(value) if not isinstance(value, str) else value
        with self.conn:
            self.conn.execute(
                "INSERT INTO meta(key,value) VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, encoded),
            )

    def get_meta(self, key: str, default=None):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return row["value"]

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
