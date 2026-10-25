"""Downloaded snapshots retain source URL, UTC download time and SHA256."""

import hashlib
import io
import json
from datetime import UTC, datetime

import httpx
import pandas as pd

from observatory.config import DATA, INSTRUMENTS
from observatory.contracts import make_quote
from observatory.store import save_report

WB_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Historical-Data-Monthly.xlsx"
)


def download(url, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
    target.write_bytes(response.content)
    meta = {
        "url": str(response.url),
        "downloaded_at": datetime.now(UTC).isoformat(),
        "sha256": hashlib.sha256(response.content).hexdigest(),
        "bytes": len(response.content),
    }
    save_report(target.with_suffix(target.suffix + ".manifest.json"), meta)
    return response.content, meta


def fetch_history(start="2000-01-01", end=None):
    end = end or datetime.now(UTC).date().isoformat()
    all_quotes, failures = [], {}
    for name in ("BRENT", "WTI"):
        series = INSTRUMENTS[name]["series"]
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}&cosd={start}&coed={end}"
        try:
            content, meta = download(url, DATA / "raw" / f"{series}.csv")
            frame = pd.read_csv(io.BytesIO(content), na_values=["."])
            date_col = frame.columns[0]
            for _, row in frame.dropna(subset=[series]).iterrows():
                date = pd.Timestamp(row[date_col], tz="UTC").to_pydatetime()
                all_quotes.append(
                    make_quote(
                        name,
                        date,
                        float(row[series]),
                        meta["sha256"][:16],
                        datetime.fromisoformat(meta["downloaded_at"]),
                    )
                )
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            failures[name] = type(exc).__name__
    try:
        content, meta = download(WB_URL, DATA / "raw" / "world-bank-monthly.xlsx")
        raw = pd.read_excel(io.BytesIO(content), sheet_name="Monthly Prices", header=None)
        header_row = next(i for i, row in raw.iterrows() if "Gold" in row.values)
        columns = raw.iloc[header_row].tolist()
        for name in ("GOLD", "COPPER", "WHEAT"):
            col = columns.index(INSTRUMENTS[name]["series"])
            for _, row in raw.iloc[header_row + 1 :].iterrows():
                label = str(row.iloc[0])
                if len(label) != 7 or "M" not in label:
                    continue
                date = pd.Timestamp(label.replace("M", "-") + "-01", tz="UTC")
                # Monthly average belongs to the month END, never the first day.
                date = date + pd.offsets.MonthEnd(0)
                if not (pd.Timestamp(start, tz="UTC") <= date <= pd.Timestamp(end, tz="UTC")):
                    continue
                value = pd.to_numeric(row.iloc[col], errors="coerce")
                if pd.notna(value):
                    all_quotes.append(
                        make_quote(
                            name,
                            date.to_pydatetime(),
                            float(value),
                            meta["sha256"][:16],
                            datetime.fromisoformat(meta["downloaded_at"]),
                        )
                    )
    except (httpx.HTTPError, ValueError, KeyError, StopIteration) as exc:
        failures["WORLD_BANK"] = type(exc).__name__
    all_quotes.sort(key=lambda x: (x.event_time, x.instrument))
    target = DATA / "quotes.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(q.model_dump_json() for q in all_quotes) + "\n", encoding="utf-8")
    report = {
        "quotes": len(all_quotes),
        "counts": pd.Series([q.instrument for q in all_quotes]).value_counts().to_dict(),
        "failures": failures,
        "historical_publication_times": "unknown; observed_at is download time",
        "mode": "REPLAY",
        "created_at": datetime.now(UTC).isoformat(),
    }
    save_report(DATA / "source-report.json", report)
    print(json.dumps(report, indent=2))
    if not all_quotes:
        raise RuntimeError("No data downloaded; no synthetic substitution was performed")
    return report
