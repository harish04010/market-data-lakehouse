"""
A small orchestrator: tasks, dependencies, retries, and a run log.

Airflow or Dagster would do this in production. The point here is the
behavior they provide, in code you can read in one sitting:
  - tasks run in dependency order
  - a failed task is retried with backoff, then marked failed
  - anything downstream of a failure is skipped, not run on stale inputs
  - a blocking quality failure stops the gold tables that read that source
  - every task's outcome, timing and row counts land in meta.duckdb
"""
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from . import gold, ingest, quality, silver
from .sources import BY_NAME, SOURCES
from .storage import meta, path, read_parquet, write_parquet


@dataclass
class Task:
    name: str
    fn: callable
    deps: list[str] = field(default_factory=list)
    retries: int = 0


def build_dag(only: list[str] | None = None, force: bool = False) -> list[Task]:
    sources = [s for s in SOURCES if not only or s.name in only]
    tasks = []
    for s in sources:
        tasks.append(Task(f"ingest:{s.name}", lambda s=s: ingest.ingest(s), retries=2))
        tasks.append(Task(f"silver:{s.name}", lambda s=s: load_silver(s, force), [f"ingest:{s.name}"]))
    names = {s.name for s in sources}
    for table, (inputs, _) in gold.TABLES.items():
        if set(inputs) <= names:
            tasks.append(Task(f"gold:{table}", lambda t=table: {"rows_out": gold.build(t)},
                              [f"silver:{i}" for i in inputs]))
    return tasks


def load_silver(source, force: bool) -> dict:
    raw = ingest.latest_raw(source.name)
    target = path("silver", f"{source.name}.parquet")
    with meta() as con:
        prev = con.execute("select rows, high_water from watermarks where source = ?", [source.name]).fetchone()
        last_sha = con.execute("select sha256 from raw_files where source = ? order by fetched_at desc limit 1",
                               [source.name]).fetchone()[0]
        processed = con.execute("select message from task_log where task = ? and status = 'ok' "
                                "order by finished_at desc limit 1", [f"silver:{source.name}"]).fetchone()
    if not force and target.exists() and processed and last_sha[:16] in (processed[0] or ""):
        return {"status": "unchanged", "rows_out": prev[0] if prev else None, "message": f"raw {last_sha[:16]} already loaded"}

    good, bad = silver.standardize(source, raw)
    result = silver.merge(source, good)
    silver.write_quarantine(source, bad)
    current = read_parquet(target)
    checks = quality.check(source, current, len(bad), prev[0] if prev else None)
    high = str(current[source.key[0]].max())
    with meta() as con:
        con.execute("insert or replace into watermarks values (?, ?, ?, ?)",
                    [source.name, high, len(current), datetime.now(timezone.utc)])
    blocking = [c for c in checks if c["severity"] == "block"]
    return {"rows_in": len(good) + len(bad), "rows_out": result["rows"], **result, "quarantined": len(bad), "repaired": good.attrs.get("repaired", 0),
            "checks": checks, "blocked": bool(blocking),
            "message": f"raw {last_sha[:16]}: +{result['inserted']} new, {result['revised']} revised, "
                       f"{len(bad)} quarantined, {good.attrs.get('repaired', 0)} repaired"}


def run(only=None, force=False, trigger="manual", note="") -> dict:
    tasks = build_dag(only, force)
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    started = datetime.now(timezone.utc)
    with meta() as con:
        con.execute("insert into runs values (?, ?, null, 'running', ?, ?)", [run_id, started, trigger, note])

    state: dict[str, str] = {}
    results: dict[str, dict] = {}
    for t in tasks:  # build_dag emits tasks in dependency order
        bad_deps = [d for d in t.deps if state.get(d) not in ("ok", "unchanged")]
        if bad_deps:
            state[t.name] = "skipped"
            _log(run_id, t, "skipped", 0, {}, f"upstream not ok: {', '.join(bad_deps)}", started)
            continue
        t0 = datetime.now(timezone.utc)
        for attempt in range(1, t.retries + 2):
            try:
                out = t.fn() or {}
                status = "blocked" if out.get("blocked") else ("unchanged" if out.get("status") == "unchanged" else "ok")
                state[t.name] = status
                results[t.name] = out
                _log(run_id, t, status, attempt, out, out.get("message", out.get("status", "")), t0)
                if "checks" in out:
                    with meta() as con:
                        for c in out["checks"]:
                            con.execute("insert into quality_results values (?, ?, ?, ?, ?, ?)",
                                        [run_id, t.name.split(":")[1], c["check"], c["passed"], c["failing_rows"], c["detail"]])
                break
            except Exception as e:
                if attempt <= t.retries:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                state[t.name] = "failed"
                _log(run_id, t, "failed", attempt, {}, f"{type(e).__name__}: {e}\n{traceback.format_exc(limit=3)}", t0)

    status = "failed" if "failed" in state.values() else ("partial" if {"skipped", "blocked"} & set(state.values()) else "ok")
    with meta() as con:
        con.execute("update runs set finished_at = ?, status = ? where run_id = ?",
                    [datetime.now(timezone.utc), status, run_id])
    return {"run_id": run_id, "status": status, "tasks": state, "results": results}


def _log(run_id, t, status, attempts, out, message, t0):
    with meta() as con:
        con.execute("insert into task_log values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, t.name, t0, datetime.now(timezone.utc), status, attempts,
                     out.get("rows_in"), out.get("rows_out"), (message or "")[:2000]])


def backfill(source_name: str, since: str) -> dict:
    """
    Rebuild one source's silver rows from `since` onward, from the raw file
    already in bronze. For when a bug in parsing is fixed, or a vendor says
    "everything after March was wrong". Nothing is re-downloaded.
    """
    source = BY_NAME[source_name]
    target = path("silver", f"{source.name}.parquet")
    good, bad = silver.standardize(source, ingest.latest_raw(source.name))
    key = source.key[0]
    current = read_parquet(target)
    cutoff = pd.Timestamp(since).date()
    kept = current[pd.to_datetime(current[key]).dt.date < cutoff]
    fresh = good[pd.to_datetime(good[key]).dt.date >= cutoff]
    write_parquet(pd.concat([kept, fresh]).sort_values(key), target)
    return {"source": source_name, "since": since, "kept_before": len(kept), "rebuilt": len(fresh)}
