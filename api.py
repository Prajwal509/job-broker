"""HTTP API for the Job Broker. Run with: uvicorn --factory api:create_app"""
import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from secrets import compare_digest
from typing import Annotated, Literal

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, model_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

from broker.billing import cost_report
from broker.broker import (
    Broker,
    BrokerConfig,
    ConflictError,
    IllegalTransitionError,
    KeyLookupError,
    ValidationError,
)
from broker.models import NODE_GPUS, Job, JobStatus, NodeState

MAX_ADVANCE_MINUTES = 7 * 24 * 60  # keep one request from blocking the server for long

# Broker exceptions -> (HTTP status, error code)
ERRORS = {
    ValidationError: (422, "validation_error"),
    ConflictError: (409, "conflict"),
    IllegalTransitionError: (409, "conflict"),
    KeyLookupError: (404, "not_found"),
}
HTTP_ERROR_CODES = {401: "unauthorized", 404: "not_found", 503: "unavailable"}


class JobSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    team: StrictStr
    gpus: StrictInt
    estimated_minutes: Annotated[StrictInt, Field(gt=0)]
    priority: Literal["low", "normal", "high"]
    idempotency_key: StrictStr


class Completion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: Literal["succeeded", "failed"] = "succeeded"


class AdvanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    minutes: Annotated[StrictInt, Field(ge=0, le=MAX_ADVANCE_MINUTES)] | None = None
    to: Annotated[StrictInt, Field(ge=0)] | None = None

    @model_validator(mode="after")
    def require_one_target(self):
        if (self.minutes is None) == (self.to is None):
            raise ValueError("provide exactly one of minutes or to")
        return self


def _job_payload(job: Job) -> dict:
    return {
        "id": job.id,
        "team": job.team,
        "gpus": job.gpus,
        "estimated_minutes": job.estimated_minutes,
        "priority": job.priority.value,
        "status": job.status.value,
        "submitted_at": job.submitted_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "node_id": job.node_id,
    }


def _parse_utc_seconds(value: str) -> int:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError("time must be ISO8601 UTC") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValidationError("time must include a UTC offset")
    return int(parsed.timestamp())


def _iso_utc(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z")


def _configure_event_logging() -> None:
    """Broker events are written to stderr as one JSON object per line."""
    logger = logging.getLogger("job_broker.events")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)


def _error_handler(status: int, code: str):
    async def handler(_request, exc: Exception):
        return JSONResponse(status_code=status, content={"error": code, "detail": str(exc)})
    return handler


def create_app(broker: Broker | None = None) -> FastAPI:
    service = broker or Broker(config=BrokerConfig())
    _configure_event_logging()
    app = FastAPI(title="Northwind Labs Job Broker")
    app.state.broker = service

    for exc_type, (status, code) in ERRORS.items():
        app.add_exception_handler(exc_type, _error_handler(status, code))

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(_request, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={"error": "validation_error", "detail": jsonable_encoder(exc.errors())},
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_request, exc: StarletteHTTPException):
        code = HTTP_ERROR_CODES.get(exc.status_code, "http_error")
        return JSONResponse(status_code=exc.status_code, content={"error": code, "detail": str(exc.detail)})

    def require_admin(provided: str | None) -> None:
        expected = service.config.admin_token
        if not expected:
            raise HTTPException(503, "ADMIN_TOKEN is not set")
        if provided is None or not compare_digest(provided, expected):
            raise HTTPException(401, "invalid admin token")

    @app.post("/jobs")
    async def submit_job(payload: JobSubmission):
        existed = service.store.get_job_by_key(payload.team.strip(), payload.idempotency_key)
        job = service.submit(
            team=payload.team,
            gpus=payload.gpus,
            estimated_minutes=payload.estimated_minutes,
            priority=payload.priority,
            idempotency_key=payload.idempotency_key,
        )
        return JSONResponse(status_code=200 if existed else 201, content=_job_payload(job))

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str):
        return _job_payload(service.get_job(job_id))

    @app.post("/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str):
        return _job_payload(service.cancel(job_id))

    @app.post("/jobs/{job_id}/complete")
    async def complete_job(job_id: str, payload: Completion | None = None):
        outcome = payload.outcome if payload else "succeeded"
        return _job_payload(service.finish_job(job_id, outcome))

    @app.get("/queue")
    async def get_queue():
        return [_job_payload(job) for job in service.queue()]

    @app.get("/nodes")
    async def get_nodes():
        return [
            {
                "id": node.id,
                "type": node.type,
                "state": node.state.value,
                "tenant": node.tenant,
                "free_gpus": service.free_gpus(node) if node.state == NodeState.ready else 0,
                "idle_since": node.idle_since,
            }
            for node in service.nodes.values()
        ]

    @app.get("/cost")
    async def get_cost(
        team: str | None = None,
        from_time: str = Query(alias="from"),
        to_time: str = Query(alias="to"),
    ):
        start, end = _parse_utc_seconds(from_time), _parse_utc_seconds(to_time)
        if end < start:
            raise ValidationError("to must not be before from")
        report = cost_report(service.store, start, end, service.clock.now)
        teams = {
            name: {
                "gpu_hours": round(cost.gpu_hours, 4),
                "direct_cost": round(cost.direct_cost, 2),
                "idle_cost": round(cost.idle_cost, 2),
                "total_cost": round(cost.total_cost, 2),
            }
            for name, cost in sorted(report.teams.items())
            if team is None or name == team
        }
        return {
            "from": _iso_utc(start),
            "to": _iso_utc(end),
            "teams": teams,
            "unattributed_cost": round(report.unattributed_cost, 2),
            "idle_cost": round(report.idle_cost, 2),
            "idle_percent": round(report.idle_percent, 2),
            "total_cost": round(report.total_cost, 2),
        }

    @app.post("/admin/advance")
    async def advance_time(
        payload: AdvanceRequest,
        x_admin_token: Annotated[str | None, Header(alias="X-Admin-Token")] = None,
    ):
        require_admin(x_admin_token)
        now = service.clock.now
        target = now + payload.minutes * 60 if payload.minutes is not None else payload.to
        if target < now:
            raise ValidationError("cannot move simulated clock backwards")
        if target - now > MAX_ADVANCE_MINUTES * 60:
            raise ValidationError(f"advance at most {MAX_ADVANCE_MINUTES} minutes per request")
        service.advance_to(target)
        return {"sim_time": service.clock.now}

    @app.get("/healthz")
    async def health():
        return {"status": "ok"}

    @app.get("/readyz")
    async def ready():
        try:
            service.store.ping()
        except sqlite3.Error:
            return JSONResponse(
                status_code=503, content={"status": "not_ready", "detail": "database unavailable"}
            )
        return {"status": "ready"}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics():
        now = service.clock.now
        jobs = list(service.jobs.values())
        nodes = list(service.nodes.values())
        lines = [
            f'queue_depth{{priority="{p}"}} '
            f"{sum(j.status == JobStatus.queued and j.priority.value == p for j in jobs)}"
            for p in ("high", "normal", "low")
        ]
        lines += [f'nodes{{state="{s.value}"}} {sum(n.state == s for n in nodes)}' for s in NodeState]
        idle_nodes = sum(
            n.state == NodeState.ready and service.free_gpus(n) == NODE_GPUS[n.type] for n in nodes
        )
        lines.append(f"idle_nodes {idle_nodes}")
        lines += [f'jobs{{state="{s.value}"}} {sum(j.status == s for j in jobs)}' for s in JobStatus]
        total = cost_report(service.store, 0, now + 1, now).total_cost
        last_hour = cost_report(service.store, max(0, now - 3600), now + 1, now).total_cost
        oldest = max((now - j.submitted_at for j in jobs if j.status == JobStatus.queued), default=0)
        lines += [
            f"spend_total_usd {total:.2f}",
            f"spend_rate_usd_per_hour {last_hour * 3600 / max(1, min(3600, now)):.2f}",
            f"launch_failures_total {service.launch_failures}",
            f"oldest_queued_job_age_seconds {oldest}",
        ]
        return PlainTextResponse(
            "\n".join(lines) + "\n", media_type="text/plain; version=0.0.4; charset=utf-8"
        )

    return app
