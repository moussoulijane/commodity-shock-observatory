"""Export aggregate evidence only. Source observations and forecast rows stay local."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def report(name):
    return json.loads((ROOT / "artifacts" / name).read_text(encoding="utf-8"))


macro = report("macro-exploratory.json")
macro_summary = {k: v for k, v in macro.items() if k != "predictions"}
macro_summary["evaluation_periods"] = {
    str(h): {
        "first_origin": min(r["origin"] for r in macro["predictions"] if r["horizon"] == h),
        "last_origin": max(r["origin"] for r in macro["predictions"] if r["horizon"] == h),
    }
    for h in (1, 3)
}
detectors = report("detector-evaluation.json")
detectors["real_data"] = [
    {k: v for k, v in row.items() if k != "top_events"} for row in detectors["real_data"]
]
paths = [
    *ROOT.glob("src/**/*.py"),
    *ROOT.glob("frontend/src/*"),
    ROOT / "compose.yaml",
    ROOT / "Dockerfile",
    ROOT / "uv.lock",
    ROOT / "frontend/package-lock.json",
]
hashes = {
    p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
    for p in sorted(paths)
    if p.is_file()
}
evidence = {
    "created_at": datetime.now(UTC).isoformat(),
    "scope": "Local Docker Desktop validation; aggregate evidence only; no production or independent accuracy claim",
    "source_counts": json.loads((ROOT / "data/source-report.json").read_text()),
    "snapshot_parity": report("batch-stream-parity.json"),
    "recovery": report("reliability.json"),
    "load": report("load-test.json"),
    "macro": macro_summary,
    "detectors": detectors,
    "strict_macro": "not run: no FRED API key supplied",
    "live_feed": "adapter implemented; not exercised against an entitled provider account",
    "source_file_sha256": hashes,
}
target = ROOT / "docs/evidence/local-reference.json"
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8", newline="\n")
print(target)
