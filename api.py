import logging
import sqlite3
import sys
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal
from secrets import compare_digest

from fastapi import FastAPI, Header, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator
from pydantic import model_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

from broker.billing import Billing
from broker.broker import (
    Broker,
    BrokerConfig,
    ConflictError,
    IllegalTransitionError,
    KeyLookupError,
    ValidationError,
)
from broker.models import Job, JobStatus, NodeState


class JobSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    team: StrictStr
    gpus: StrictInt
    estimated_minutes: Annotated[StrictInt, Field(gt=0)]
    priority: Literal["low", "normal", "high"]
    idempotency_key: StrictStr

    @field_validator("team", "idempotency_key")
    @classmethod
    def require_nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("gpus")
    @classmethod
    def require_supported_gpu_count(cls, value: int) -> int:
        if value not in {1, 2, 4, 8}:
            raise ValueError("gpus must be one of 1, 2, 4, 8")
        return value


class AdvanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    minutes: Annotated[StrictInt, Field(ge=0)] | None = None
    to: Annotated[StrictInt, Field(ge=0)] | None = None

    @model_validator(mode="after")
    def require_one_target(self):
        if (self.minutes is None) == (self.to is None):
            raise ValueError("provide exactly one of minutes or to")
        return self


class AdminTokenError(Exception):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message


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
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _configure_event_logging() -> None:
    logger = logging.getLogger("job_broker.events")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)


def create_app(broker: Broker | None = None) -> FastAPI:
    service = broker or Broker(config=BrokerConfig())
    _configure_event_logging()
    app = FastAPI(title="Northwind Labs Job Broker")
    app.state.broker = service

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(_request, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={
                "error": "validation_error",
                "detail": jsonable_encoder(exc.errors()),
            },
        )

    @app.exception_handler(ValidationError)
    async def broker_validation_error(_request, exc: ValidationError):
        return JSONResponse(
            status_code=422,
            content={"error": "validation_error", "detail": str(exc)},
        )

    @app.exception_handler(ConflictError)
    async def conflict_error(_request, exc: ConflictError):
        return JSONResponse(
            status_code=409,
            content={"error": "conflict", "detail": str(exc)},
        )

    @app.exception_handler(IllegalTransitionError)
    async def illegal_transition_error(_request, exc: IllegalTransitionError):
        return JSONResponse(
            status_code=409,
            content={"error": "conflict", "detail": str(exc)},
        )

    @app.exception_handler(KeyLookupError)
    async def not_found_error(_request, exc: KeyLookupError):
        return JSONResponse(
            status_code=404,
            content={"error": "not_found", "detail": str(exc)},
        )

    @app.exception_handler(AdminTokenError)
    async def admin_token_error(_request, exc: AdminTokenError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.code, "detail": exc.message},
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_request, exc: StarletteHTTPException):
        code = "not_found" if exc.status_code == 404 else "http_error"
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": code, "detail": str(exc.detail)},
        )

    def require_admin(provided: str | None) -> None:
        expected = service.config.admin_token
        if not expected:
            raise AdminTokenError(503, "admin_unavailable", "ADMIN_TOKEN is not set")
        if provided is None or not compare_digest(provided, expected):
            raise AdminTokenError(401, "unauthorized", "invalid admin token")

    @app.post("/jobs")
    async def submit_job(payload: JobSubmission):
        existing = service.store.get_job_by_key(
            payload.team.strip(), payload.idempotency_key
        )
        job = service.submit(
            team=payload.team,
            gpus=payload.gpus,
            estimated_minutes=payload.estimated_minutes,
            priority=payload.priority,
            idempotency_key=payload.idempotency_key,
        )
        status_code = 200 if existing is not None else 201
        return JSONResponse(status_code=status_code, content=_job_payload(job))

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str):
        return _job_payload(service.get_job(job_id))

    @app.post("/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str):
        return _job_payload(service.cancel(job_id))

    @app.get("/queue")
    async def get_queue():
        queued = service.scheduler.queue_order(
            list(service.jobs.values()),
            service._rolling_usage(),
            service.clock.now,
        )
        return [_job_payload(job) for job in queued]

    @app.get("/nodes")
    async def get_nodes():
        response = []
        for node in service.nodes.values():
            running_gpus = sum(
                job.gpus
                for job in service.jobs.values()
                if job.status == JobStatus.running and job.node_id == node.id
            )
            capacity = service.NODE_GPUS[node.type]
            free_gpus = (
                capacity - running_gpus if node.state == NodeState.ready else 0
            )
            response.append(
                {
                    "id": node.id,
                    "type": node.type,
                    "state": node.state.value,
                    "tenant": node.tenant,
                    "free_gpus": free_gpus,
                    "idle_since": node.idle_since,
                }
            )
        return response

    @app.get("/cost")
    async def get_cost(
        team: str | None = None,
        from_time: str = Query(alias="from"),
        to_time: str = Query(alias="to"),
    ):
        start = _parse_utc_seconds(from_time)
        end = _parse_utc_seconds(to_time)
        if end < start:
            raise ValidationError("to must not be before from")
        report = Billing(service.store).report(start, end, service.clock.now)
        result = asdict(report)
        for team_cost in result["teams"].values():
            team_cost["total_cost"] = (
                team_cost["direct_cost"] + team_cost["idle_cost"]
            )
        result.pop("from_time")
        result.pop("to_time")
        result["from"] = _iso_utc(start)
        result["to"] = _iso_utc(end)
        if team is not None:
            result["teams"] = {
                team: result["teams"][team]
            } if team in result["teams"] else {}
        return result

    @app.post("/admin/advance")
    async def advance_time(
        payload: AdvanceRequest,
        x_admin_token: Annotated[str | None, Header(alias="X-Admin-Token")] = None,
    ):
        require_admin(x_admin_token)
        target = payload.to
        if payload.minutes is not None:
            target = service.clock.now + payload.minutes * 60
        if target is None:
            raise ValidationError("provide minutes or absolute to time")
        try:
            service.advance_to(target)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
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
                status_code=503,
                content={"status": "not_ready", "detail": "database unavailable"},
            )
        return {"status": "ready"}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics():
        now = service.clock.now
        jobs = list(service.jobs.values())
        nodes = list(service.nodes.values())
        lines: list[str] = []
        for priority in ("high", "normal", "low"):
            count = sum(
                job.status == JobStatus.queued and job.priority.value == priority
                for job in jobs
            )
            lines.append(f'queue_depth{{priority="{priority}"}} {count}')
        for state in NodeState:
            count = sum(node.state == state for node in nodes)
            lines.append(f'nodes{{state="{state.value}"}} {count}')
        idle_nodes = sum(
            node.state == NodeState.ready
            and service._free_gpus(node) == service.NODE_GPUS[node.type]
            for node in nodes
        )
        lines.append(f"idle_nodes {idle_nodes}")
        for state in JobStatus:
            count = sum(job.status == state for job in jobs)
            lines.append(f'jobs{{state="{state.value}"}} {count}')

        first_launch = min((node.launched_at for node in nodes), default=now)
        total = Billing(service.store).report(first_launch, now + 1, now).total_cost
        recent_start = max(0, now - 3600)
        recent = Billing(service.store).report(recent_start, now + 1, now).total_cost
        elapsed = max(1, min(3600, now - recent_start))
        lines.extend(
            (
                f"spend_total_usd {total:.2f}",
                f"spend_rate_usd_per_hour {recent * 3600 / elapsed:.2f}",
                f"launch_failures_total {service.provider.launch_failures}",
                "oldest_queued_job_age_seconds "
                f"{max((now - job.submitted_at for job in jobs if job.status == JobStatus.queued), default=0)}",
            )
        )
        return PlainTextResponse(
            "\n".join(lines) + "\n",
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    return app


app = create_app()
