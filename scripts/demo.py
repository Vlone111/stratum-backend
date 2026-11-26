"""Start an isolated local API, worker and simulated upstream REST service."""
import argparse
import json
import multiprocessing
import os
import secrets
import shutil
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import uvicorn

from stratum.db import connection, data_dir, enqueue, initialize, now, uid
from stratum.security import provision

ROOT = Path(__file__).resolve().parents[1]


class Feed(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        if url.path != "/readings":
            return self.send_error(404)
        try:
            args = parse_qs(url.query)
            after = int(args.get("after", ["0"])[0])
            limit = max(1, min(250, int(args.get("limit", ["250"])[0])))
        except ValueError:
            return self.send_error(400)
        records = [{"seq": i, "event_id": f"ref-{i}", "revision": 1, "station": "west",
                    "metric": "pm10", "value": 14 + i, "unit": "ug/m3",
                    "observed_at": f"2026-10-14T0{i}:00:00Z"} for i in range(1, 4) if i > after]
        blob = json.dumps({"items": records[:limit], "has_more": len(records) > limit}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(blob)))
        self.end_headers()
        self.wfile.write(blob)

    def log_message(self, *_):
        pass


def prepare(feed_url):
    initialize()
    path = data_dir()
    with connection(True) as db:
        old = db.execute("SELECT id FROM tenants LIMIT 1").fetchone()
        if old and not (path / "demo-login.txt").is_file():
            raise SystemExit("Выберите пустой каталог демо. Здесь уже существуют рабочие пространства.")
        configs = {"lab-export": {"kind": "csv", "path": str(path / "laboratory.csv")},
                   "archive": {"kind": "sqlite", "path": str(path / "archive.sqlite3")},
                   "network": {"kind": "rest", "url": feed_url}}
        (path / "bindings.json").write_text(json.dumps({"ural": configs}), encoding="utf-8")
        if old:
            return
        shutil.copyfile(ROOT / "examples/laboratory.csv", path / "laboratory.csv")
        archive = sqlite3.connect(path / "archive.sqlite3")
        try:
            archive.execute("""CREATE TABLE IF NOT EXISTS readings(seq INTEGER PRIMARY KEY,event_id TEXT,
              revision INTEGER,station TEXT,metric TEXT,value REAL,unit TEXT,observed_at TEXT)""")
            archive.execute("""INSERT OR REPLACE INTO readings VALUES
              (1,'logger-8',1,'south','noise',61.2,'dBA','2026-10-14T03:15:00Z')""")
            archive.commit()
        finally:
            archive.close()
        lines = []
        for tenant, roles in (("ural", ("editor", "viewer")), ("volga", ("editor",))):
            db.execute("INSERT INTO tenants VALUES(?,?)", (tenant, tenant))
            for role in roles:
                password = secrets.token_urlsafe(15)
                provision(db, tenant, role, password, role)
                lines.append(f"workspace={tenant}  login={role}  password={password}")
        for kind, name, binding in (("csv", "Протоколы лаборатории", "lab-export"),
                                    ("sqlite", "Архив шумомера", "archive"),
                                    ("rest", "Опорная сеть наблюдения", "network"),
                                    ("webhook", "Передвижной пост", None)):
            sid = uid()
            interval = 300 if binding else None
            db.execute("""INSERT INTO sources(id,tenant,name,kind,binding,interval_seconds,enabled,next_at,created)
                          VALUES(?,'ural',?,?,?,?,1,?,?)""",
                       (sid, name, kind, binding, interval, now()+interval if interval else None, now()))
            if binding:
                enqueue(db, sid, "manual")
            else:
                payload = json.loads((ROOT / "examples/webhook.json").read_text())["events"]
                enqueue(db, sid, "webhook", json.dumps(payload))
        with os.fdopen(os.open(path / "demo-login.txt", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
            f.write("\n".join(lines) + "\n")


def worker_loop():
    import time
    from stratum.worker import process_one
    while True:
        if not process_one():
            time.sleep(0.25)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data", default=".demo")
    args = parser.parse_args()
    os.environ["STRATUM_DATA"] = str(Path(args.data).resolve())
    feed = ThreadingHTTPServer(("127.0.0.1", 0), Feed)
    try:
        prepare(f"http://127.0.0.1:{feed.server_port}/readings")
        threading.Thread(target=feed.serve_forever, daemon=True).start()
        worker = multiprocessing.get_context("spawn").Process(target=worker_loop, daemon=True)
        worker.start()
        print(f"Учетные записи стенда: {data_dir() / 'demo-login.txt'}", flush=True)
        print(f"Swagger UI: http://127.0.0.1:{args.port}/docs", flush=True)
        try:
            uvicorn.run("stratum.api:app", host=args.host, port=args.port)
        finally:
            worker.terminate()
            worker.join(5)
    finally:
        feed.server_close()


if __name__ == "__main__":
    main()
