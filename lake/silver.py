"""
Silver: typed, deduplicated, contract-checked tables, merged incrementally.

Each run compares the latest raw file against what silver already holds:
  - keys silver has never seen are inserts
  - keys whose values changed are revisions (sources do revise history)
  - everything else is left alone
So running twice changes nothing the second time, and a revision upstream
shows up as a counted, logged update rather than a silent overwrite.
"""
from datetime import date

import duckdb
import pandas as pd

from .sources import Source
from .storage import path, read_parquet, write_parquet

PARSE = {
    "date": "coalesce(try_strptime({c}, '%Y-%m-%d'), try_strptime({c}, '%m/%d/%Y'))::date",
    "month": "date_trunc('month', coalesce(try_strptime({c}, '%Y-%m-%d'), try_strptime({c}, '%Y-%m')))::date",
    "float": "try_cast({c} as double)",
    "str": "nullif(trim({c}), '')",
}


def standardize(source: Source, raw_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (valid rows, quarantined rows with a reason)."""
    con = duckdb.connect()
    con.execute(f"create table raw as select * from read_csv('{raw_path}', all_varchar = true, header = true)")
    selects, checks = [], []
    for c in source.columns:
        raw_col = '"' + (c.source or c.name).replace('"', '""') + '"'
        trimmed = f"trim(replace({raw_col}, chr(13), ''))"
        expr = PARSE[c.type].format(c=trimmed)
        if c.zero_is_null:
            # Placeholder zeros become nulls before any range check sees them.
            expr = f"nullif({expr}, 0)"
        selects.append(f"{expr} as {c.name}")
        # A value that was present but didn't parse is a contract break, not a null.
        parse_only = PARSE[c.type].format(c=trimmed)
        checks.append(f"case when {trimmed} is not null and {trimmed} <> '' and {parse_only} is null "
                      f"then '{c.name}: could not parse \"' || {trimmed} || '\"' end")
        if not c.nullable:
            checks.append(f"case when {expr} is null then '{c.name}: missing' end")
        if c.min is not None:
            checks.append(f"case when {expr} < {c.min} then '{c.name}: below {c.min}' end")
        if c.max is not None:
            checks.append(f"case when {expr} > {c.max} then '{c.name}: above {c.max}' end")
    reason = "concat_ws('; ', " + ", ".join(checks) + ")"
    df = con.execute(f"select {', '.join(selects)}, nullif({reason}, '') as _reason from raw").df()

    df = apply_rules(source, df)
    bad = df[df["_reason"].notna()].drop(columns="_repaired").copy()
    good = df[df["_reason"].isna()].drop(columns="_reason")
    good.attrs["repaired"] = int(good["_repaired"].notna().sum())
    good = good.drop(columns="_repaired")

    # Duplicate keys: identical copies collapse; conflicting copies are quarantined.
    key = list(source.key)
    dup = good.duplicated(key, keep=False)
    if dup.any():
        groups = good[dup].groupby(key, dropna=False)
        conflicting = groups.filter(lambda g: len(g.drop_duplicates()) > 1)
        if len(conflicting):
            bad = pd.concat([bad, conflicting.assign(_reason="conflicting duplicate key")])
            good = good.drop(conflicting.index)
        good = good.drop_duplicates(key, keep="last")
    repaired = good.attrs.get("repaired", 0)
    good = good.sort_values(key).reset_index(drop=True)
    good.attrs["repaired"] = repaired
    return good, bad.reset_index(drop=True)


def apply_rules(source: Source, df: pd.DataFrame) -> pd.DataFrame:
    df["_repaired"] = None
    if "open_within_range" in source.rules:
        # Before 2003 the VIX "open" wasn't a real opening print, and dozens of
        # days have an open outside that day's high-low range. The close is
        # fine, so the row stays; only the open is blanked, and the repair is logged.
        outside = (df["open"] > df["high"]) | (df["open"] < df["low"])
        df.loc[outside & df["_reason"].isna(), "_repaired"] = "open outside high-low range, set to null"
        df.loc[outside & df["_reason"].isna(), "open"] = None
        broken = (df["low"] > df["high"]) | (df["close"] > df["high"]) | (df["close"] < df["low"])
        df.loc[broken & df["_reason"].isna(), "_reason"] = "close outside high-low range"
    return df


def row_hash(df: pd.DataFrame) -> pd.Series:
    return pd.util.hash_pandas_object(df.astype(str), index=False)


def merge(source: Source, incoming: pd.DataFrame) -> dict:
    target = path("silver", f"{source.name}.parquet")
    key = list(source.key)
    if not target.exists():
        write_parquet(incoming, target)
        return {"inserted": len(incoming), "revised": 0, "unchanged": 0, "removed_upstream": 0, "rows": len(incoming)}

    current = read_parquet(target)
    cur = current.set_index(key)
    inc = incoming.set_index(key)
    cur_h = row_hash(cur.reset_index()).set_axis(cur.index)
    inc_h = row_hash(inc.reset_index()).set_axis(inc.index)
    new_keys = inc.index.difference(cur.index)
    common = inc.index.intersection(cur.index)
    revised = common[(inc_h.loc[common] != cur_h.loc[common]).to_numpy()]
    gone = cur.index.difference(inc.index)  # kept: a source dropping history isn't a reason to lose it

    merged = pd.concat([cur.drop(index=revised), inc.loc[new_keys.union(revised)]]).sort_index().reset_index()
    write_parquet(merged, target)
    return {"inserted": len(new_keys), "revised": len(revised), "unchanged": len(common) - len(revised),
            "removed_upstream": len(gone), "rows": len(merged)}


def write_quarantine(source: Source, bad: pd.DataFrame) -> None:
    target = path("quarantine", f"{source.name}.parquet")
    if len(bad):
        write_parquet(bad.assign(_quarantined_on=date.today().isoformat()).astype(str), target)
    elif target.exists():
        target.unlink()
