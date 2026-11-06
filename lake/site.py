"""Exports the platform's own metadata to site/status.json for the status page."""
import json
from datetime import date
from pathlib import Path

from .gold import TABLES
from .sources import SOURCES
from .storage import meta, path, read_parquet

SITE = Path(__file__).resolve().parent.parent / "site"


def export() -> str:
    with meta() as con:
        run = con.execute("select run_id, status, started_at, finished_at from runs "
                          "where status in ('ok', 'partial') order by started_at desc limit 1").fetchone()
        first_load = con.execute("select run_id from runs order by started_at limit 1").fetchone()[0]
        checks = con.execute("select source, check_name, passed, detail from quality_results where run_id = ("
                             "select max(run_id) from quality_results)").fetchall()
        history = con.execute("select run_id, status, started_at, "
                              "date_diff('millisecond', started_at, finished_at) as ms from runs order by started_at desc limit 8").fetchall()
        loads = {t.split(":")[1]: m for t, m in con.execute(
            "select task, message from task_log where run_id = ? and task like 'silver:%'", [first_load]).fetchall()}
        marks = dict(con.execute("select source, high_water from watermarks").fetchall())

    sources = []
    for s in SOURCES:
        df = read_parquet(path("silver", f"{s.name}.parquet"))
        q = path("quarantine", f"{s.name}.parquet")
        mine = [c for c in checks if c[0] == s.name]
        fails = [c for c in mine if not c[2]]
        msg = loads.get(s.name, "")
        repaired = int(msg.split(", ")[-1].split(" ")[0]) if "repaired" in msg else 0
        sources.append({"name": s.name, "grain": s.grain, "description": s.description, "rows": len(df),
                        "latest": str(marks.get(s.name, ""))[:10] if s.grain != "snapshot" else None,
                        "quarantined": len(read_parquet(q)) if q.exists() else 0, "repaired": repaired,
                        "checks": len(mine), "failed": [{"check": c[1], "detail": c[3]} for c in fails]})

    macro = read_parquet(path("gold", "macro_monthly.parquet"))
    macro = macro[macro["month"].dt.month == 12]
    daily = read_parquet(path("gold", "market_daily.parquet"))
    daily["month"] = daily["date"].dt.to_period("Q").astype(str)
    monthly = daily.groupby("month").agg(vix=("vix", "mean"), brent=("brent", "mean")).reset_index()
    gold = [{"name": n, "inputs": inputs, "rows": len(read_parquet(path("gold", f"{n}.parquet")))}
            for n, (inputs, _) in TABLES.items()]

    payload = {
        "generated": date.today().isoformat(),
        "run": {"id": run[0], "status": run[1], "ms": int((run[3] - run[2]).total_seconds() * 1000)},
        "history": [{"id": h[0], "status": h[1], "at": h[2].isoformat(timespec="minutes"), "ms": int(h[3])} for h in history],
        "sources": sources, "gold": gold,
        "cape": [{"y": int(r.month.year), "v": round(float(r.cape), 1)} for r in macro.itertuples() if r.cape == r.cape],
        "market": [{"m": r.month, "vix": round(float(r.vix), 1) if r.vix == r.vix else None,
                    "brent": round(float(r.brent), 1) if r.brent == r.brent else None} for r in monthly.itertuples()],
    }
    SITE.mkdir(exist_ok=True)
    out = SITE / "status.json"
    out.write_text(json.dumps(payload, separators=(",", ":")))
    return f"wrote {out} ({out.stat().st_size:,} bytes)"
