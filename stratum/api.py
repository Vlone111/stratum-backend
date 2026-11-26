import hmac
import json
import os
import secrets
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from . import models as m
from .connectors import catalog
from .db import connection, enqueue, initialize, now, uid
from .normalize import timestamp
from .security import authenticate, digest, issue


@asynccontextmanager
async def lifespan(_):
    initialize()
    yield


app = FastAPI(title="Stratum • экологические измерения", version="1.0.0", lifespan=lifespan,
              description="Приём, нормализация и история ревизий промышленных экологических измерений.",
              contact={"name": "Евгений Славкин • backend"})



class BodyLimit:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] not in ("POST", "PATCH"):
            return await self.app(scope, receive, send)
        body = bytearray()
        while True:
            msg = await receive()
            if msg["type"] == "http.disconnect":
                return
            if len(body) + len(msg["body"]) > 1024 * 1024:
                return await JSONResponse({"detail": "лимит тела 1 МиБ"}, 413)(scope, receive, send)
            body.extend(msg["body"])
            if not msg.get("more_body"):
                break
        delivered = False

        async def replay():
            nonlocal delivered
            if delivered:
                return await receive()
            delivered = True
            return {"type": "http.request", "body": bytes(body), "more_body": False}
        await self.app(scope, replay, send)


app.add_middleware(BodyLimit)
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("STRATUM_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173").split(","), allow_methods=["GET","POST","PATCH","DELETE"],
    allow_headers=["Authorization","Content-Type","X-Source-Key"])
bearer = HTTPBearer(auto_error=False)
hook_header = APIKeyHeader(name="X-Source-Key", auto_error=False)
# List endpoints exclude raw input blobs from their database reads.
JOB_COLUMNS = "j.id,j.source_id,j.cause,j.status,j.attempt,j.created,j.finished,j.received,j.applied,j.skipped,j.rejected,j.error"
READING_COLUMNS = "v.source_id,v.event_id,v.revision,v.station,v.metric,v.observed_at,v.value,v.unit,v.job_id"
Limit = Query(50, ge=1, le=200)
Offset = Query(0, ge=0, le=2147483647)


def user(auth: Optional[HTTPAuthorizationCredentials] = Depends(bearer)):
    if auth:
        with connection() as db:
            row = db.execute("""SELECT u.tenant AS workspace,u.login,u.role FROM users u
                                JOIN sessions s ON s.user_id=u.id WHERE s.digest=? AND s.expires>?""",
                             (digest(auth.credentials), now())).fetchone()
            if row:
                return dict(row)
    raise HTTPException(401, "требуется действующая сессия", headers={"WWW-Authenticate": "Bearer"})


def editor(me=Depends(user)):
    if me["role"] != "editor":
        raise HTTPException(403, "нужна роль editor")
    return me


def source_for(db, tenant, sid):
    row = db.execute("SELECT * FROM sources WHERE tenant=? AND id=?", (tenant, sid)).fetchone()
    if not row:
        raise HTTPException(404, "источник не найден")
    return dict(row)


def job_for(db, tenant, jid):
    row = db.execute("""SELECT j.* FROM jobs j JOIN sources s ON s.id=j.source_id
                        WHERE s.tenant=? AND j.id=?""", (tenant, jid)).fetchone()
    if not row:
        raise HTTPException(404, "загрузка не найдена")
    return dict(row)


@app.exception_handler(RequestValidationError)
async def invalid_request(_, exc):
    parts = []
    for issue in exc.errors():
        field = ".".join(str(part) for part in issue["loc"])
        parts.append(f"{field} — {issue['msg']}")
    return JSONResponse({"detail": "\n".join(parts)}, status_code=422)


def errors(*codes):
    descriptions = {401: "Требуется действующая сессия, пароль или ключ", 403: "Нужна роль editor",
                    404: "Объект отсутствует в рабочем пространстве", 409: "Операция недоступна",
                    413: "Тело запроса превышает 1 МиБ", 422: "Неверные параметры или тело запроса"}
    return {code: {"model": m.ApiError, "description": descriptions[code]} for code in (*codes, 422)}


@app.get("/health", response_model=m.Health, tags=["service"])
def health():
    with connection() as db:
        db.execute("SELECT 1 FROM tenants LIMIT 1").fetchone()
    return {"status": "ok"}


@app.post("/v1/session", response_model=m.Session, tags=["session"], responses=errors(401, 413))
def login(body: m.Login):
    with connection() as db:
        u = authenticate(db, body.workspace, body.login, body.password.get_secret_value())
    if not u:
        raise HTTPException(401, "неверные данные входа")
    with connection(True) as db:
        token, expires = issue(db, u["id"])
    return dict(workspace=u["tenant"], login=u["login"], role=u["role"], access_token=token, expires=expires)


@app.get("/v1/me", response_model=m.Identity, tags=["session"], responses=errors(401))
def whoami(me=Depends(user)):
    return me


@app.delete("/v1/session", status_code=204, tags=["session"], responses=errors(401))
def logout(me=Depends(user), auth=Depends(bearer)):
    with connection(True) as db:
        db.execute("DELETE FROM sessions WHERE digest=?", (digest(auth.credentials),))
    return Response(status_code=204)


@app.get("/v1/bindings", response_model=List[m.Binding], tags=["sources"], responses=errors(401, 403))
def bindings(me=Depends(editor)):
    return [{"name": name, "kind": cfg["kind"]} for name, cfg in catalog(me["workspace"]).items()]


@app.get("/v1/sources", response_model=List[m.Source], tags=["sources"], responses=errors(401))
def sources(limit: int = Limit, offset: int = Offset, me=Depends(user)):
    with connection() as db:
        return [dict(s) for s in db.execute("SELECT * FROM sources WHERE tenant=? ORDER BY created,id LIMIT ? OFFSET ?",
                                             (me["workspace"], limit, offset))]


@app.post("/v1/sources", response_model=m.Source, status_code=201, tags=["sources"], responses=errors(401, 403, 413))
def create_source(body: m.NewSource, me=Depends(editor)):
    if body.kind == "webhook":
        if body.binding or body.interval_seconds:
            raise HTTPException(422, "webhook принимает события без привязки и расписания")
    elif catalog(me["workspace"]).get(body.binding, {}).get("kind") != body.kind:
        raise HTTPException(422, "выберите привязку соответствующего типа")
    sid = uid()
    with connection(True) as db:
        db.execute("""INSERT INTO sources(id,tenant,name,kind,binding,interval_seconds,enabled,next_at,created)
                      VALUES(?,?,?,?,?,?,1,?,?)""", (sid, me["workspace"], body.name, body.kind, body.binding,
                      body.interval_seconds, now()+body.interval_seconds if body.interval_seconds else None, now()))
        return source_for(db, me["workspace"], sid)


@app.patch("/v1/sources/{sid}", response_model=m.Source, tags=["sources"], responses=errors(401, 403, 404, 413))
def update_source(sid: str, body: m.SourceUpdate, me=Depends(editor)):
    with connection(True) as db:
        src = source_for(db, me["workspace"], sid)
        fields = body.model_fields_set
        if not fields or any(getattr(body, k) is None for k in fields & {"name","enabled"}):
            raise HTTPException(422, "укажите изменяемые поля, name и enabled не могут быть null")
        interval = body.interval_seconds if "interval_seconds" in fields else src["interval_seconds"]
        if interval is not None and src["kind"] == "webhook":
            raise HTTPException(422, "у webhook нет расписания")
        enabled = body.enabled if "enabled" in fields else bool(src["enabled"])
        due = src["next_at"]
        if interval != src["interval_seconds"] or enabled != bool(src["enabled"]):
            due = now() + interval if enabled and interval else None
        db.execute("UPDATE sources SET enabled=?,next_at=?,interval_seconds=?,name=? WHERE id=?",
                   (enabled, due, interval, body.name or src["name"], sid))
        return source_for(db, me["workspace"], sid)


@app.post("/v1/sources/{sid}/runs", response_model=m.JobRef, status_code=202, tags=["runs"], responses=errors(401, 403, 404, 409, 413))
def start(sid: str, me=Depends(editor)):
    with connection(True) as db:
        src = source_for(db, me["workspace"], sid)
        if not src["enabled"] or src["kind"] == "webhook":
            raise HTTPException(409, "ручная загрузка недоступна")
        active = db.execute("SELECT id FROM jobs WHERE source_id=? AND status IN ('queued','running') LIMIT 1",
                            (sid,)).fetchone()
        return {"job_id": active["id"] if active else enqueue(db, sid, "manual")}


@app.post("/v1/sources/{sid}/key", response_model=m.HookKey, tags=["sources"], responses=errors(401, 403, 404, 409, 413))
def rotate(sid: str, me=Depends(editor)):
    with connection(True) as db:
        if source_for(db, me["workspace"], sid)["kind"] != "webhook":
            raise HTTPException(409, "ключ поддерживается только для webhook")
        key = secrets.token_urlsafe(32)
        db.execute("INSERT OR REPLACE INTO hook_keys VALUES(?,?)", (sid, digest(key)))
        return {"key": key}


@app.post("/v1/hooks/{sid}", response_model=m.JobRef, status_code=202, tags=["ingress"], responses=errors(401, 409, 413))
def receive(sid: str, body: m.EventBatch, key=Depends(hook_header)):
    with connection(True) as db:
        row = db.execute("""SELECT k.digest,s.enabled FROM hook_keys k JOIN sources s ON s.id=k.source_id
                            WHERE s.id=?""", (sid,)).fetchone()
        if not row or not key or not hmac.compare_digest(row["digest"], digest(key)):
            raise HTTPException(401, "неверный ключ источника")
        if not row["enabled"]:
            raise HTTPException(409, "источник на паузе")
        try:
            payload = json.dumps(body.events, allow_nan=False)
        except ValueError:
            raise HTTPException(422, "числа должны быть конечными")
        return {"job_id": enqueue(db, sid, "webhook", payload)}


@app.get("/v1/runs", response_model=List[m.Job], tags=["runs"], responses=errors(401))
def runs(source_id: Optional[str] = None, status: Optional[m.Status] = None,
         limit: int = Limit, offset: int = Offset, me=Depends(user)):
    with connection() as db:
        return [dict(r) for r in db.execute(f"""SELECT {JOB_COLUMNS} FROM jobs j JOIN sources s ON s.id=j.source_id
          WHERE s.tenant=? AND (? IS NULL OR s.id=?) AND (? IS NULL OR j.status=?)
          ORDER BY j.created DESC,j.rowid DESC LIMIT ? OFFSET ?""",
          (me["workspace"], source_id, source_id, status, status, limit, offset))]


@app.get("/v1/runs/{jid}", response_model=m.Job, tags=["runs"], responses=errors(401, 404))
def run_detail(jid: str, me=Depends(user)):
    with connection() as db:
        return job_for(db, me["workspace"], jid)


@app.get("/v1/runs/{jid}/rejects", response_model=List[m.Rejection], tags=["runs"], responses=errors(401, 404))
def rejects(jid: str, limit: int = Limit, offset: int = Offset, me=Depends(user)):
    with connection() as db:
        job_for(db, me["workspace"], jid)
        return [{"row_number": r["row_number"], "reason": r["reason"], "raw": json.loads(r["raw"])}
                for r in db.execute("SELECT * FROM rejects WHERE job_id=? ORDER BY id LIMIT ? OFFSET ?",
                                    (jid, limit, offset))]


@app.post("/v1/runs/{jid}/retry", response_model=m.JobRef, status_code=202, tags=["runs"], responses=errors(401, 403, 404, 409, 413))
def retry(jid: str, me=Depends(editor)):
    with connection(True) as db:
        j = job_for(db, me["workspace"], jid)
        if j["status"] != "failed" or not source_for(db, me["workspace"], j["source_id"])["enabled"]:
            raise HTTPException(409, "повтор доступен для failed при включенном источнике")
        return {"job_id": enqueue(db, j["source_id"], "retry", j["payload"])}


@app.get("/v1/readings", response_model=List[m.Reading], tags=["measurements"], responses=errors(401))
def readings(source_id: Optional[str] = None, station: Optional[str] = None, metric: Optional[str] = None,
             since: Optional[str] = None, until: Optional[str] = None, limit: int = Limit,
             offset: int = Offset, me=Depends(user)):
    filters = ["s.tenant=?"]
    values = [me["workspace"]]
    for field, value in (("v.source_id", source_id), ("v.station", station), ("v.metric", metric)):
        if value is not None:
            filters.append(field + "=?")
            values.append(value)
    for comparison, value in ((">=", since), ("<", until)):
        if value is not None:
            try:
                values.append(timestamp(value))
            except ValueError as exc:
                raise HTTPException(422, str(exc))
            filters.append("v.observed_at" + comparison + "?")
    with connection() as db:
        return [dict(r) for r in db.execute(f"""SELECT {READING_COLUMNS} FROM heads h JOIN versions v
            USING(source_id,event_id,revision) JOIN sources s ON s.id=v.source_id WHERE """ +
            " AND ".join(filters) + " ORDER BY v.observed_at DESC,v.source_id,v.event_id LIMIT ? OFFSET ?",
            (*values, limit, offset))]


@app.get("/v1/sources/{sid}/events/{event_id}/versions", response_model=List[m.Reading], tags=["measurements"], responses=errors(401, 404))
def versions(sid: str, event_id: str, limit: int = Limit, offset: int = Offset, me=Depends(user)):
    with connection() as db:
        source_for(db, me["workspace"], sid)
        return [dict(r) for r in db.execute(f"""SELECT {READING_COLUMNS} FROM versions v WHERE source_id=? AND event_id=?
          ORDER BY revision DESC LIMIT ? OFFSET ?""", (sid, event_id, limit, offset))]


@app.get("/v1/overview", response_model=m.Overview, tags=["monitoring"], responses=errors(401))
def overview(me=Depends(user)):
    tenant = (me["workspace"],)
    with connection() as db:
        counts = dict(db.execute("""SELECT j.status,COUNT(*) FROM jobs j JOIN sources s ON s.id=j.source_id
                                    WHERE s.tenant=? GROUP BY j.status""", tenant))
        return dict(sources=db.execute("SELECT COUNT(*) FROM sources WHERE tenant=?", tenant).fetchone()[0],
                    current_readings=db.execute("""SELECT COUNT(*) FROM heads h JOIN sources s
                      ON s.id=h.source_id WHERE s.tenant=?""", tenant).fetchone()[0],
                    rejected_rows=db.execute("""SELECT COALESCE(SUM(j.rejected),0) FROM jobs j JOIN sources s
                      ON s.id=j.source_id WHERE s.tenant=?""", tenant).fetchone()[0],
                    **{k: counts.get(k, 0) for k in ("queued", "running", "failed")})
