"""Export V2 aggregate evidence, preserving the frozen V1 reference."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def report(name):
    path = ROOT / "artifacts" / name
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"status": "not_run"}


macro = report("macro-exploratory.json")
predictions = macro.pop("predictions", [])
macro["evaluation_periods"] = {
    str(h): {
        "first_origin": min(r["origin"] for r in predictions if r["horizon"] == h),
        "last_origin": max(r["origin"] for r in predictions if r["horizon"] == h),
    }
    for h in (1, 3)
    if any(r["horizon"] == h for r in predictions)
}
detectors = report("detector-evaluation-v2.json")
if isinstance(detectors.get("historical_review"), dict):
    # Reference case studies remain described, without copying quote payloads.
    detectors["historical_review"].pop("top_events", None)
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
    "version": 2,
    "created_at": datetime.now(UTC).isoformat(),
    "scope": "Local Docker Desktop validation; aggregate evidence only; no production or independent economic accuracy claim",
    "v1_reference": "docs/evidence/local-reference.json, tag v1-local-validation",
    "source_counts": json.loads((ROOT / "data/source-report.json").read_text()),
    "spark_oracle_and_clock_isolation": report("spark-checks-v2.json"),
    "feature_parity": report("parity-v2.json"),
    "recovery": report("reliability-v2.json"),
    "partial_commit_recovery": report("commit-recovery-v2.json"),
    "burst_load": report("load-test-v2.json"),
    "sustained_load": report("sustained-load-v2.json"),
    "macro": macro,
    "detectors": detectors,
    "strict_macro": "No real FRED API key supplied; unverified CSV cannot claim strict provenance",
    "live_market": "Persistent adapter implemented; not exercised against an entitled provider account",
    "ci": "Configured; not run on GitHub before publication",
    "source_file_sha256": hashes,
}
target = ROOT / "docs/evidence/local-v2-reference.json"
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8", newline="\n")
print(target)
