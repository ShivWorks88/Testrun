"""Write the API contract to web/src/api/openapi.json (the console's types are generated from it)."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.main import create_app  # noqa: E402

out = Path(__file__).resolve().parents[2] / "web" / "src" / "api" / "openapi.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(create_app().openapi(), indent=2) + "\n")
print(f"wrote {out}")
