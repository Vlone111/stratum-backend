"""Measure the durable webhook -> worker -> current readings path in a temporary database."""
import json
import os
import tempfile
import time

from fastapi.testclient import TestClient

from stratum.api import app
from stratum.db import connection, initialize
from stratum.security import digest
from stratum.worker import process_one


def main():
    with tempfile.TemporaryDirectory() as directory:
        os.environ["STRATUM_DATA"] = directory
        initialize()
        with connection(True) as db:
            db.execute("INSERT INTO tenants VALUES('bench','bench')")
            db.execute("""INSERT INTO sources(id,tenant,name,kind,enabled,created)
                       VALUES('s','bench','bench','webhook',1,0)""")
            db.execute("INSERT INTO hook_keys VALUES('s',?)", (digest("benchmark"),))
        template = {"revision":1,"station":"east","metric":"pm10","observed_at":"2026-10-14T03:00:00Z",
                    "value":0.021,"unit":"mg/m3"}
        batches = [[dict(template, event_id=str(batch*500+i)) for i in range(500)] for batch in range(20)]
        with TestClient(app) as client:
            start = time.perf_counter()
            for events in batches:
                response = client.post("/v1/hooks/s", json={"events":events}, headers={"X-Source-Key":"benchmark"})
                response.raise_for_status()
            accepted = time.perf_counter()
            while process_one():
                pass
            end = time.perf_counter()
        with connection() as db:
            count = db.execute("SELECT COUNT(*) FROM heads").fetchone()[0]
            ok = db.execute("SELECT COUNT(*) FROM jobs WHERE status='succeeded'").fetchone()[0]
        if (count, ok) != (10000, 20):
            raise SystemExit("Проверка полноты загрузки не прошла")
        print(json.dumps(dict(rows=count, batches=ok, enqueue_s=round(accepted-start,3),
                              processing_s=round(end-accepted,3), total_s=round(end-start,3),
                              rows_per_s=round(count/(end-start))), indent=2))


if __name__ == "__main__":
    main()
