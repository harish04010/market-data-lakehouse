"""
Bronze: fetch each source and keep the raw bytes exactly as received.

Files are addressed by their SHA-256, so fetching an unchanged file is a
no-op, and every version that was ever loaded can be replayed later. For
tests and offline runs, LAKE_FIXTURES points at a folder of <source>.csv
files that stand in for the network.
"""
import hashlib
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from .sources import Source
from .storage import meta, path


class FetchError(Exception):
    pass


def fetch(source: Source, attempts: int = 3) -> bytes:
    fixtures = os.getenv("LAKE_FIXTURES")
    if fixtures:
        return (Path(fixtures) / f"{source.name}.csv").read_bytes()
    last = None
    for i in range(attempts):
        try:
            with urllib.request.urlopen(source.url, timeout=30) as r:
                return r.read()
        except Exception as e:  # network hiccups are expected; back off and retry
            last = e
            time.sleep(2 ** i)
    raise FetchError(f"{source.name}: {last}")


def ingest(source: Source) -> dict:
    data = fetch(source)
    digest = hashlib.sha256(data).hexdigest()
    with meta() as con:
        seen = con.execute("select path from raw_files where source = ? and sha256 = ?", [source.name, digest]).fetchone()
        if seen:
            return {"status": "unchanged", "sha256": digest, "bytes": len(data), "path": seen[0]}
        now = datetime.now(timezone.utc)
        p = path("bronze", source.name, f"ingest_date={now:%Y-%m-%d}", f"{digest[:16]}.csv")
        p.write_bytes(data)
        con.execute("insert into raw_files values (?, ?, ?, ?, ?)", [source.name, digest, str(p), len(data), now])
    return {"status": "new", "sha256": digest, "bytes": len(data), "path": str(p)}


def latest_raw(source_name: str) -> str | None:
    with meta() as con:
        row = con.execute("select path from raw_files where source = ? order by fetched_at desc limit 1",
                          [source_name]).fetchone()
    return row[0] if row else None
