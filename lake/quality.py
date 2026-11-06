"""
Checks that run after every silver load. Each one passes, warns, or blocks;
a blocking failure stops gold tables that depend on the source from building.
"""
from datetime import date

import pandas as pd

from .sources import Source


def check(source: Source, silver: pd.DataFrame, quarantined: int, previous_rows: int | None,
          today: date | None = None) -> list[dict]:
    today = today or date.today()
    out = []

    def add(name, passed, detail, failing=0, blocking=False):
        out.append({"check": name, "passed": bool(passed), "failing_rows": int(failing), "detail": detail,
                    "severity": "ok" if passed else ("block" if blocking else "warn")})

    total = len(silver) + quarantined
    rate = quarantined / total if total else 0
    add("contract_violations", rate <= 0.01, f"{quarantined} of {total} rows quarantined ({rate:.2%})",
        quarantined, blocking=rate > 0.05)

    if source.grain in ("daily", "monthly"):
        col = source.key[0]
        last = pd.to_datetime(silver[col]).max().date()
        if source.grain == "daily":
            age = (today - last).days
            add("freshness", age <= source.fresh_within_days,
                f"latest {last}, {age} days old (allowed {source.fresh_within_days})")
        else:
            behind = (today.year - last.year) * 12 + today.month - last.month
            add("freshness", behind <= 2, f"latest month {last:%Y-%m}, {behind} months behind (allowed 2)")

    if previous_rows:
        drop = (previous_rows - len(silver)) / previous_rows
        add("row_count_stable", drop <= 0.01, f"{previous_rows} -> {len(silver)} rows", blocking=drop > 0.1)

    if "column_freshness" in source.rules:
        # A table can look fresh because one column keeps updating while others
        # quietly stopped. Check each nullable column's latest non-null period.
        key = pd.to_datetime(silver[source.key[0]])
        last_row = key.max()
        for c in source.columns:
            if c.nullable:
                filled = key[silver[c.name].notna()]
                last = filled.max() if len(filled) else None
                gap = (last_row.year - last.year) * 12 + last_row.month - last.month if last is not None else None
                add(f"column_fresh:{c.name}", gap is not None and gap <= 3,
                    f"last value {last:%Y-%m}, {gap} months behind the table" if last is not None else "never populated")

    for c in source.columns:
        if c.nullable and c.name in silver:
            share = silver[c.name].isna().mean()
            add(f"nulls:{c.name}", share <= 0.5, f"{share:.1%} null")
    return out
