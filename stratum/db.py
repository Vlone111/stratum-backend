"""Persistence shared by API, provisioning commands and workers."""
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path


def data_dir():
    return Path(os.environ.get("STRATUM_DATA", "var")).resolve()


def uid():
    return uuid.uuid4().hex


def now():
    return int(time.time())


SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants(id TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS users(
 id TEXT PRIMARY KEY, tenant TEXT NOT NULL REFERENCES tenants(id), login TEXT NOT NULL,
 salt BLOB NOT NULL, password BLOB NOT NULL, role TEXT NOT NULL, UNIQUE(tenant, login));
CREATE TABLE IF NOT EXISTS sessions(
 digest TEXT PRIMARY KEY, user_id TEXT REFERENCES users(id), expires INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS sources(
 id TEXT PRIMARY KEY, tenant TEXT REFERENCES tenants(id), name TEXT NOT NULL,
 kind TEXT NOT NULL, binding TEXT, interval_seconds INTEGER, enabled INTEGER NOT NULL,
 next_at INTEGER, cursor INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS hook_keys(source_id TEXT PRIMARY KEY REFERENCES sources(id), digest TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS source_tenant ON sources(tenant, created);
CREATE TABLE IF NOT EXISTS jobs(
 id TEXT PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(id), cause TEXT NOT NULL,
 status TEXT NOT NULL, payload TEXT, attempt INTEGER NOT NULL DEFAULT 0, ready INTEGER NOT NULL,
 lease TEXT, expires INTEGER, created INTEGER NOT NULL, finished INTEGER,
 received INTEGER NOT NULL DEFAULT 0, applied INTEGER NOT NULL DEFAULT 0,
 skipped INTEGER NOT NULL DEFAULT 0, rejected INTEGER NOT NULL DEFAULT 0, error TEXT);
CREATE INDEX IF NOT EXISTS job_queue ON jobs(status, ready, created);
CREATE INDEX IF NOT EXISTS job_source ON jobs(source_id, status);
CREATE TABLE IF NOT EXISTS versions(
 source_id TEXT NOT NULL REFERENCES sources(id), event_id TEXT NOT NULL, revision INTEGER NOT NULL,
 station TEXT NOT NULL, metric TEXT NOT NULL, observed_at TEXT NOT NULL, value REAL NOT NULL,
 unit TEXT NOT NULL, raw TEXT NOT NULL, job_id TEXT NOT NULL REFERENCES jobs(id),
 PRIMARY KEY(source_id,event_id,revision));
CREATE TABLE IF NOT EXISTS heads(
 source_id TEXT NOT NULL, event_id TEXT NOT NULL, revision INTEGER NOT NULL,
 PRIMARY KEY(source_id,event_id),
 FOREIGN KEY(source_id,event_id,revision) REFERENCES versions(source_id,event_id,revision));
CREATE TABLE IF NOT EXISTS rejects(
 id INTEGER PRIMARY KEY, job_id TEXT REFERENCES jobs(id), row_number INTEGER NOT NULL,
 reason TEXT NOT NULL, raw TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS rejects_job ON rejects(job_id,id);
CREATE INDEX IF NOT EXISTS version_time ON versions(observed_at, source_id);
"""


@contextmanager
def connection(write=False):
    db = sqlite3.connect(data_dir() / "stratum.sqlite3", timeout=15, isolation_level=None)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        yield db
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def initialize():
    data_dir().mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect(data_dir() / "stratum.sqlite3", timeout=15)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript(SCHEMA)
    finally:
        db.close()


def enqueue(db, source_id, cause, payload=None):
    job_id = uid()
    db.execute("""INSERT INTO jobs(id,source_id,cause,status,payload,ready,created)
                  VALUES(?,?,?,'queued',?,?,?)""",
               (job_id, source_id, cause, payload, now(), now()))
    return job_id
