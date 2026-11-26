import csv
import io
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .db import data_dir

PAGE = 250
MAX_BYTES = 2 * 1024 * 1024
MAX_ROWS = 5000


class InputError(Exception):
    """Safe diagnostic for the run log."""


@dataclass
class Batch:
    rows: list
    cursor: int
    more: bool = False


def catalog(tenant):
    path = data_dir() / "bindings.json"
    return json.loads(path.read_text(encoding="utf-8")).get(tenant, {}) if path.exists() else {}


def read_bounded(stream):
    blob = stream.read(MAX_BYTES + 1)
    if len(blob) > MAX_BYTES:
        raise InputError("пакет превышает 2 МиБ")
    return blob


def decode_json(blob):
    def invalid(_):
        raise ValueError("invalid JSON constant")
    return json.loads(blob, parse_constant=invalid)


def csv_batch(config):
    with open(config["path"], "rb") as stream:
        blob = read_bounded(stream)
    rows = csv.DictReader(io.StringIO(blob.decode("utf-8-sig"), newline=""), strict=True)
    required = {"event_id", "station", "metric", "value", "unit", "observed_at"}
    if not required.issubset(rows.fieldnames or []) or len(set(rows.fieldnames)) != len(rows.fieldnames):
        raise InputError("неверный заголовок CSV")
    records = []
    for row in rows:
        if len(records) >= MAX_ROWS:
            raise InputError("CSV превышает лимит в 5000 строк")
        if None in row:
            raise InputError("в строке CSV больше полей, чем в заголовке")
        records.append(row)
    return Batch(records, 0)


def paged(rows, cursor, more):
    if not isinstance(rows, list) or len(rows) > PAGE or type(more) is not bool or (more and not rows):
        raise InputError("неверная структура страницы источника")
    for row in rows:
        seq = row.get("seq") if isinstance(row, dict) else None
        if type(seq) is not int or not cursor < seq <= 2**63 - 1:
            raise InputError("seq должен строго возрастать в пределах int64")
        cursor = seq
    return Batch(rows, cursor, more)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def fetch(source, payload):
    kind, cursor = source["kind"], source["cursor"]
    if kind == "webhook":
        return Batch(json.loads(payload), cursor)
    config = catalog(source["tenant"]).get(source["binding"])
    if not config or config["kind"] != kind:
        raise InputError("привязка отсутствует в каталоге оператора")
    if kind == "csv":
        return csv_batch(config)
    if kind == "sqlite":
        uri = Path(config["path"]).resolve().as_uri() + "?mode=ro"
        db = sqlite3.connect(uri, uri=True, timeout=5)
        try:
            db.row_factory = sqlite3.Row
            records = db.execute("""SELECT seq,event_id,revision,station,metric,value,unit,observed_at
                                    FROM readings WHERE seq>? ORDER BY seq LIMIT ?""",
                                 (cursor, PAGE + 1)).fetchall()
        finally:
            db.close()
        return paged([dict(row) for row in records[:PAGE]], cursor, len(records) > PAGE)
    url = urlsplit(config["url"])
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password:
        raise InputError("неверный адрес в каталоге оператора")
    params = [(k, v) for k, v in parse_qsl(url.query) if k not in ("after", "limit")]
    params.extend([("after", cursor), ("limit", PAGE)])
    headers = {"Accept": "application/json"}
    if config.get("token_env"):
        token = os.getenv(config["token_env"])
        if not token:
            raise InputError("не настроен секрет внешнего API")
        headers["Authorization"] = "Bearer " + token
    req = Request(urlunsplit(url._replace(query=urlencode(params))), headers=headers)
    with build_opener(NoRedirect()).open(req, timeout=5) as response:
        page = decode_json(read_bounded(response))
    if not isinstance(page, dict):
        raise InputError("внешний API вернул неверный документ")
    return paged(page.get("items"), cursor, page.get("has_more"))
