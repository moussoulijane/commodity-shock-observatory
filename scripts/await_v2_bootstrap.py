import psycopg

from observatory.store import connect
from observatory.validation import wait_for
from observatory.validation_v2 import verify


def complete():
    try:
        with connect() as conn:
            expected = conn.execute("SELECT count(*) AS n FROM (SELECT DISTINCT mode,instrument,event_time FROM observations) c").fetchone()["n"]
            actual = conn.execute("SELECT count(*) AS n FROM signals_v2").fetchone()["n"]
            return expected > 0 and actual >= expected
    except psycopg.errors.UndefinedTable:
        return False


wait_for(complete, timeout=1200)
verify()
