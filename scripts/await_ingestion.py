from observatory.config import DATA
from observatory.store import connect
from observatory.validation import verify, wait_for

expected = sum(bool(line.strip()) for line in (DATA / "quotes.jsonl").read_text().splitlines())


def complete():
    with connect() as conn:
        return (
            conn.execute("SELECT count(*) AS n FROM observations WHERE mode='REPLAY'").fetchone()[
                "n"
            ]
            >= expected
        )


wait_for(complete, timeout=900)
verify()
