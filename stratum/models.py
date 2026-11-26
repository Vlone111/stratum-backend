from typing import Any, List, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field, SecretStr, conint, conlist, constr

Kind = Literal["csv", "sqlite", "rest", "webhook"]
Status = Literal["queued", "running", "succeeded", "partial", "failed"]
Role = Literal["editor", "viewer"]
Identifier = constr(strip_whitespace=True, min_length=1, max_length=100)
Interval = conint(ge=10, le=604800)


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Login(Input):
    workspace: Identifier
    login: Identifier
    password: SecretStr = Field(min_length=1, max_length=256)


class Identity(BaseModel):
    workspace: str
    login: str
    role: Role


class Session(Identity):
    access_token: str
    expires: int


class Binding(BaseModel):
    name: str
    kind: Kind


class NewSource(Input):
    name: Identifier
    kind: Kind
    binding: Optional[Identifier] = None
    interval_seconds: Optional[Interval] = None


class Source(BaseModel):
    id: str
    name: str
    kind: Kind
    binding: Optional[str] = None
    enabled: bool
    interval_seconds: Optional[int] = None
    next_at: Optional[int] = None
    cursor: int


class SourceUpdate(Input):
    name: Optional[Identifier] = None
    enabled: Optional[bool] = None
    interval_seconds: Optional[Interval] = None


class ApiError(BaseModel):
    detail: str


class Health(BaseModel):
    status: Literal["ok"]


class JobRef(BaseModel):
    job_id: str


class Job(BaseModel):
    id: str
    source_id: str
    cause: Literal["manual", "schedule", "webhook", "continuation", "retry"]
    status: Status
    attempt: int
    created: int
    finished: Optional[int] = None
    received: int
    applied: int
    skipped: int
    rejected: int
    error: Optional[str] = None


class EventBatch(Input):
    events: conlist(Any, min_length=1, max_length=500) = Field(
        description="До 500 строк. Worker проверяет каждую отдельно и сохраняет брак в карантин.",
        examples=[[{"event_id":"sample-12","revision":1,"station":"north","metric":"pm10",
                  "value":0.023,"unit":"mg/m3","observed_at":"2026-11-01T08:00:00Z"}]])


class HookKey(BaseModel):
    key: str


class Reading(BaseModel):
    source_id: str
    event_id: str
    revision: int
    station: str
    metric: str
    observed_at: str
    value: float
    unit: str
    job_id: str


class Rejection(BaseModel):
    row_number: int
    reason: str
    raw: Any = Field(...)


class Overview(BaseModel):
    sources: int
    current_readings: int
    queued: int
    running: int
    failed: int
    rejected_rows: int
