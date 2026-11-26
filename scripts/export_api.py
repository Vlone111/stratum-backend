import json
from pathlib import Path
from stratum.api import app


if __name__ == "__main__":
    path = Path(__file__).resolve().parents[1] / "docs/openapi.json"
    path.write_text(json.dumps(app.openapi(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
