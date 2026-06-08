from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class WorkerRegisterRequest(BaseModel):
    worker_id: UUID | None = None
    endpoint: str
    cpus: int
    memory: int
    disk_space: int
    max_concurrent_queries: int = 1


class WorkerRegisterResponse(BaseModel):
    worker_id: UUID
    init_scripts: list[str]


class WorkerHeartbeatRequest(BaseModel):
    cpus: int | None = None
    memory: int | None = None
    disk_space: int | None = None
    status: str | None = None  # running or shutting_down
    memory_used_mb: int | None = None
    cpu_usage: float | None = None
    running_queries: int | None = None


class SubmitQueryRequest(BaseModel):
    query: str
    cpus: int | None = None
    memory: int | None = None
    disk_space: int | None = None


class RunQueryRequest(BaseModel):
    query_id: UUID
    query: str
    cpus: int | None = None
    memory: int | None = None
    disk_space: int | None = None


class CompleteQueryRequest(BaseModel):
    status: str = Field(pattern="^(completed|failed|cancelled)$")
    error: str | None = None
    result_row_count: int | None = None


class CancelQueryRequest(BaseModel):
    query_id: UUID | None = None
    reason: str | None = None


class QueryResponse(BaseModel):
    query_id: UUID
    worker_id: UUID | None = None
    status: str
    query: str
    error: str | None = None
    cpus: int | None = None
    memory: int | None = None
    disk_space: int | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result_row_count: int | None = None


class WorkerResponse(BaseModel):
    worker_id: UUID
    endpoint: str
    status: str
    cpus: int
    memory: int
    disk_space: int
    max_concurrent_queries: int
    running_queries: int
    memory_used_mb: int | None = None
    cpu_usage: float | None = None
    started_at: datetime
    last_heartbeat_at: datetime


class QueryResultPage(BaseModel):
    query_id: UUID
    offset: int
    limit: int
    total_rows: int
    columns: list[str]
    rows: list[list]


class Metrics(BaseModel):
    memory_used_mb: int
    memory_left_mb: int
    cpu_count: int
    cpu_usage: float
