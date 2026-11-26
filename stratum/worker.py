import json
import logging
import os
import time
from urllib.error import HTTPError

from .connectors import InputError, fetch
from .db import connection, enqueue, initialize, now, uid
from .normalize import normalize

ATTEMPTS = 3
LEASE = 90
LOG = logging.getLogger("stratum.worker")


def claim():
    t = now()
    with connection(True) as db:
        db.execute("""UPDATE jobs SET status='failed',finished=?,error='истекла последняя аренда',
                      lease=NULL,expires=NULL WHERE status='running' AND expires<=? AND attempt>=?""",
                   (t, t, ATTEMPTS))
        db.execute("""UPDATE jobs SET status='queued',lease=NULL,expires=NULL
                      WHERE status='running' AND expires<=?""", (t,))
        for src in db.execute("""SELECT id,interval_seconds FROM sources
                                 WHERE enabled=1 AND next_at<=?""", (t,)).fetchall():
            if not db.execute("SELECT 1 FROM jobs WHERE source_id=? AND status IN ('queued','running')",
                              (src["id"],)).fetchone():
                enqueue(db, src["id"], "schedule")
            db.execute("UPDATE sources SET next_at=? WHERE id=?", (t + src["interval_seconds"], src["id"]))
        job = db.execute("""SELECT j.* FROM jobs j WHERE j.status='queued' AND j.ready<=?
                            AND NOT EXISTS (SELECT 1 FROM jobs k WHERE k.source_id=j.source_id
                            AND k.status='running') ORDER BY j.ready,j.created,j.rowid LIMIT 1""", (t,)).fetchone()
        if not job:
            return None
        lease = uid()
        db.execute("UPDATE jobs SET status='running',attempt=attempt+1,lease=?,expires=? WHERE id=?",
                   (lease, t + LEASE, job["id"]))
        src = db.execute("SELECT * FROM sources WHERE id=?", (job["source_id"],)).fetchone()
        return dict(job), dict(src), lease


def owns(db, job, lease):
    return db.execute("SELECT 1 FROM jobs WHERE id=? AND status='running' AND lease=? AND expires>?",
                      (job["id"], lease, now())).fetchone() is not None


def apply_row(db, source_id, job_id, row, raw):
    fields = normalize(row)
    event_id, revision = fields[:2]
    old = db.execute("""SELECT station,metric,observed_at,value,unit FROM versions
                        WHERE source_id=? AND event_id=? AND revision=?""",
                     (source_id, event_id, revision)).fetchone()
    if old:
        if tuple(old) != fields[2:]:
            raise ValueError("конфликт содержимого одной ревизии")
        return "skipped"
    db.execute("INSERT INTO versions VALUES(?,?,?,?,?,?,?,?,?,?)", (source_id, *fields, raw, job_id))
    db.execute("""INSERT INTO heads VALUES(?,?,?) ON CONFLICT(source_id,event_id)
                  DO UPDATE SET revision=excluded.revision WHERE excluded.revision>heads.revision""",
               (source_id, event_id, revision))
    return "applied"


def process_one():
    task = claim()
    if not task:
        return False
    job, src, lease = task
    try:
        batch = fetch(src, job["payload"])
        with connection(True) as db:
            if not owns(db, job, lease):
                return True
            counts = {"applied": 0, "skipped": 0, "rejected": 0}
            for number, row in enumerate(batch.rows, 1):
                raw = json.dumps(row, ensure_ascii=True, allow_nan=False)
                try:
                    counts[apply_row(db, src["id"], job["id"], row, raw)] += 1
                except ValueError as exc:
                    db.execute("INSERT INTO rejects(job_id,row_number,reason,raw) VALUES(?,?,?,?)",
                               (job["id"], number, str(exc), raw))
                    counts["rejected"] += 1
            db.execute("UPDATE sources SET cursor=? WHERE id=?", (batch.cursor, src["id"]))
            if batch.more:
                enqueue(db, src["id"], "continuation")
            status = "partial" if counts["rejected"] else "succeeded"
            db.execute("""UPDATE jobs SET status=?,finished=?,received=?,applied=?,skipped=?,rejected=?,
                          error=NULL,lease=NULL,expires=NULL WHERE id=?""",
                       (status, now(), len(batch.rows), counts["applied"], counts["skipped"],
                        counts["rejected"], job["id"]))
    except Exception as exc:
        if isinstance(exc, InputError):
            message = str(exc)
        elif isinstance(exc, HTTPError):
            message = f"внешний API вернул HTTP {exc.code}"
        else:
            message = f"ошибка чтения или обработки ({type(exc).__name__})"
        LOG.warning("job=%s %s", job["id"], message)
        with connection(True) as db:
            if owns(db, job, lease):
                attempt = job["attempt"] + 1
                failed = attempt >= ATTEMPTS
                db.execute("""UPDATE jobs SET status=?,ready=?,finished=?,lease=NULL,expires=NULL,error=?
                              WHERE id=?""", ("failed" if failed else "queued", now() + 2**attempt,
                                              now() if failed else None, message, job["id"]))
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    initialize()
    while True:
        if not process_one():
            time.sleep(float(os.getenv("STRATUM_POLL", "1")))
