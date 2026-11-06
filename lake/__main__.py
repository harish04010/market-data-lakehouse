"""
python -m lake run [--force] [--only vix_daily,cpi_monthly]
python -m lake backfill --source sp500_monthly --since 2020-01-01
python -m lake status
python -m lake export-site          # status page data for site/
"""
import argparse
import json

from . import orchestrate
from .storage import meta


def status():
    with meta() as con:
        runs = con.execute("select run_id, status, started_at from runs order by started_at desc limit 5").fetchall()
        print("Recent runs:")
        for r in runs:
            print(f"  {r[0]}  {r[1]:<8} {r[2]:%Y-%m-%d %H:%M}")
        last = runs[0][0] if runs else None
        if not last:
            return
        print("\nQuality, latest run:")
        for s, c, ok, d in con.execute("select source, check_name, passed, detail from quality_results where run_id = ? "
                                       "order by passed, source", [last]).fetchall():
            print(f"  {'ok  ' if ok else 'FAIL'} {s:<22} {c:<22} {d}")


def main():
    ap = argparse.ArgumentParser(prog="lake")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--force", action="store_true")
    r.add_argument("--only")
    b = sub.add_parser("backfill")
    b.add_argument("--source", required=True)
    b.add_argument("--since", required=True)
    sub.add_parser("status")
    sub.add_parser("export-site")
    a = ap.parse_args()
    if a.cmd == "run":
        out = orchestrate.run(a.only.split(",") if a.only else None, a.force)
        print(f"run {out['run_id']}: {out['status']}")
        for t, s in out["tasks"].items():
            msg = out["results"].get(t, {}).get("message", "")
            print(f"  {s:<9} {t:<34} {msg}")
    elif a.cmd == "backfill":
        print(json.dumps(orchestrate.backfill(a.source, a.since)))
    elif a.cmd == "status":
        status()
    else:
        from .site import export
        print(export())


if __name__ == "__main__":
    main()
