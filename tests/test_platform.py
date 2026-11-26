import json
import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from fastapi.testclient import TestClient

from scripts.demo import Feed, prepare
from stratum import worker
from stratum.api import app
from stratum.db import connection, data_dir, now


class PlatformTests(unittest.TestCase):
    def setUp(self):
        self.store = tempfile.TemporaryDirectory()
        self.addCleanup(self.store.cleanup)
        self.env = patch.dict(os.environ, STRATUM_DATA=self.store.name)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.feed = ThreadingHTTPServer(("127.0.0.1", 0), Feed)
        thread = threading.Thread(target=self.feed.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        self.addCleanup(self.feed.server_close)
        self.addCleanup(self.feed.shutdown)
        prepare(f"http://127.0.0.1:{self.feed.server_port}/readings")
        self.client = TestClient(app)
        self.addCleanup(self.client.close)
        self.tokens = {}
        for line in (data_dir() / "demo-login.txt").read_text().splitlines():
            credentials = dict(field.split("=", 1) for field in line.split())
            response = self.client.post("/v1/session", json=credentials)
            self.assertEqual(response.status_code, 200, response.text)
            self.tokens[(credentials["workspace"], credentials["login"])] = {
                "Authorization": "Bearer " + response.json()["access_token"]}
        self.editor = self.tokens["ural", "editor"]
        self.viewer = self.tokens["ural", "viewer"]
        self.other = self.tokens["volga", "editor"]
        self.sources = {s["kind"]: s["id"] for s in self.get("/v1/sources")}
        self.drain()

    def drain(self):
        for _ in range(50):
            if not worker.process_one():
                return
        self.fail("очередь не опустела")

    def get(self, url, headers=None, **kwargs):
        r = self.client.get(url, headers=headers or self.editor, **kwargs)
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    def start(self, kind):
        r = self.client.post(f"/v1/sources/{self.sources[kind]}/runs", headers=self.editor)
        self.assertEqual(r.status_code, 202, r.text)
        return r.json()["job_id"]

    def hook(self):
        url = "/v1/sources/" + self.sources["webhook"] + "/key"
        return {"X-Source-Key": self.client.post(url, headers=self.editor).json()["key"]}

    def test_four_kinds_normalization_quarantine_and_idempotency(self):
        self.assertEqual(self.get("/v1/overview")["current_readings"], 7)
        jobs = self.get("/v1/runs")
        self.assertCountEqual([j["status"] for j in jobs], ["partial", "succeeded", "succeeded", "succeeded"])
        csv_id = self.sources["csv"]
        lab = next(j for j in jobs if j["source_id"] == csv_id)
        self.assertEqual([lab[k] for k in ("received", "applied", "skipped", "rejected")], [5, 3, 1, 1])
        self.assertEqual(self.get("/v1/runs/" + lab["id"] + "/rejects")[0]["raw"]["event_id"], "lab-043")
        versions = self.get(f"/v1/sources/{csv_id}/events/lab-041/versions")
        self.assertEqual([(v["revision"], v["value"]) for v in versions], [(2, 21), (1, 23)])
        self.assertEqual(versions[0]["observed_at"], "2026-10-14T03:00:00.000000Z")
        jid = self.start("csv")
        self.assertEqual(jid, self.start("csv"))
        self.drain()
        run = self.get("/v1/runs/" + jid)
        self.assertEqual((run["applied"], run["skipped"], run["rejected"]), (0, 4, 1))

    def test_access_isolation_and_revocation(self):
        self.assertEqual(self.get("/v1/runs", self.other), [])
        self.assertEqual(self.get("/v1/readings", self.other), [])
        self.assertEqual(self.get("/v1/bindings", self.other), [])
        job = self.get("/v1/runs")[0]["id"]
        self.assertEqual(self.client.get(f"/v1/runs/{job}/rejects", headers=self.other).status_code, 404)
        path = f"/v1/sources/{self.sources['csv']}/runs"
        for header, status in ((self.viewer, 403), (self.other, 404), ({}, 401)):
            self.assertEqual(self.client.post(path, headers=header).status_code, status)
        self.assertEqual(self.client.delete("/v1/session", headers=self.editor).status_code, 204)
        self.assertEqual(self.client.get("/v1/runs", headers=self.editor).status_code, 401)

    def test_webhook_revisions_and_key_rotation(self):
        sid = self.sources["webhook"]
        url = "/v1/hooks/" + sid
        old, key = self.hook(), self.hook()
        base = {"event_id":"sensor-17","station":"east","metric":"pm10","unit":"mg/m3",
                "observed_at":"2026-10-14T08:00:00+05:00"}
        events = [dict(base, revision=n, value=v) for n,v in ((3,0.05),(1,0.02),(3,0.05),(3,0.06))]
        self.assertEqual(self.client.post(url, headers=old, json={"events":events}).status_code, 401)
        r = self.client.post(url, headers=key, json={"events":events})
        self.assertEqual(r.status_code, 202)
        self.drain()
        j = self.get("/v1/runs/" + r.json()["job_id"])
        self.assertEqual((j["applied"], j["skipped"], j["rejected"]), (2, 1, 1))
        rows = self.get("/v1/readings", params={"source_id":sid,"metric":"pm10"})
        self.assertEqual((len(rows), rows[0]["revision"], rows[0]["value"]), (1, 3, 50))
        self.client.patch("/v1/sources/"+sid, headers=self.editor, json={"enabled":False})
        self.assertEqual(self.client.post(url, headers=key, json={"events":events}).status_code, 409)

    def test_pagination_and_rollback_before_checkpoint(self):
        src = self.sources["sqlite"]
        db = sqlite3.connect(data_dir() / "archive.sqlite3")
        try:
            db.executemany("INSERT INTO readings VALUES(?,?,1,'a','noise',20,'dBA','2026-01-01T00:00:00Z')",
                           [(i, "j"+str(i)) for i in range(2, 303)])
            db.commit()
        finally:
            db.close()
        jid = self.start("sqlite")
        original = worker.apply_row
        def stop_mid_page(db, source_id, job_id, row, raw):
            if row["seq"] == 4:
                raise RuntimeError("simulated worker failure")
            return original(db, source_id, job_id, row, raw)
        with patch.object(worker, "apply_row", stop_mid_page):
            worker.process_one()
        self.assertEqual(self.get("/v1/runs/"+jid)["status"], "queued")
        with connection(True) as db:
            self.assertEqual(db.execute("SELECT cursor FROM sources WHERE id=?", (src,)).fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM versions WHERE source_id=?", (src,)).fetchone()[0], 1)
            db.execute("UPDATE jobs SET ready=0 WHERE id=?", (jid,))
        self.drain()
        jobs = self.get("/v1/runs", params={"source_id":src})
        self.assertEqual((jobs[0]["cause"], jobs[0]["applied"]), ("continuation", 51))
        self.assertEqual(self.get("/v1/runs/"+jid)["applied"], 250)
        current = next(s for s in self.get("/v1/sources") if s["id"] == src)
        self.assertEqual(current["cursor"], 302)

    def test_parallel_claim_and_expired_lease(self):
        jid = self.start("csv")
        with ThreadPoolExecutor(max_workers=4) as pool:
            claims = [c for c in pool.map(lambda _: worker.claim(), range(4)) if c]
        self.assertEqual(len(claims), 1)
        job, _, lease = claims[0]
        with connection(True) as db:
            db.execute("UPDATE jobs SET expires=0 WHERE id=?", (jid,))
        recovered = worker.claim()
        with connection() as db:
            self.assertFalse(worker.owns(db, job, lease))
            self.assertTrue(worker.owns(db, recovered[0], recovered[2]))

    def test_schedule_failed_run_and_manual_retry(self):
        sid = self.sources["csv"]
        with connection(True) as db:
            db.execute("UPDATE sources SET next_at=? WHERE id=?", (now()-1, sid))
        path = data_dir() / "laboratory.csv"
        tmp = path.with_suffix(".saved")
        path.rename(tmp)
        for _ in range(3):
            worker.process_one()
            with connection(True) as db:
                db.execute("UPDATE jobs SET ready=0 WHERE status='queued'")
        failed = self.get("/v1/runs", params={"status":"failed"})[0]
        self.assertEqual((failed["cause"],failed["attempt"]), ("schedule",3))
        tmp.rename(path)
        r = self.client.post("/v1/runs/"+failed["id"]+"/retry", headers=self.editor)
        self.assertEqual(r.status_code, 202)
        self.drain()
        self.assertEqual(self.get("/v1/runs/"+r.json()["job_id"])["status"], "partial")

    def test_create_binding_and_limit(self):
        cfg = {"name":"копия","kind":"rest","binding":"network","interval_seconds":45}
        created = self.client.post("/v1/sources", headers=self.editor, json=cfg)
        self.assertEqual(created.status_code, 201)
        self.assertTrue(created.json()["next_at"] > now())
        self.assertEqual(self.client.post("/v1/sources", headers=self.other, json=cfg).status_code, 422)
        self.assertEqual(self.client.post("/v1/hooks/unknown", content=b"x" * (1024*1024+1)).status_code, 413)

    def test_schedule_edit_and_period_removal(self):
        path = "/v1/sources/" + self.sources["csv"]
        r = self.client.patch(path, headers=self.editor, json={"interval_seconds":10,"name":"Север"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual((r.json()["interval_seconds"], r.json()["name"]), (10,"Север"))
        r = self.client.patch(path, headers=self.editor, json={"interval_seconds":None})
        self.assertIsNone(r.json()["next_at"])
        self.assertTrue(r.json()["enabled"])
        self.assertEqual(self.client.patch(path, headers=self.editor, json={"enabled":None}).status_code, 422)
        r = self.client.patch(path, headers=self.editor, json={"interval_seconds":5})
        self.assertIsInstance(r.json()["detail"], str)
