"""
Where things live, and the run/state metadata that makes loads incremental.

    warehouse/
      bronze/<source>/ingest_date=YYYY-MM-DD/<sha256>.csv   raw bytes, never modified
      silver/<source>.parquet                               typed, deduplicated, contract-checked
      quarantine/<source>.parquet                           rows that broke the contract, with reasons
      gold/<table>.parquet                                  analysis-ready
      meta.duckdb                                           runs, task log, watermarks, file registry
"""
import os
from contextlib import contextmanager
from pathlib import Path

import duckdb

ROOT = Path(os.getenv("LAKE_ROOT", Path(__file__).resolve().parent.parent / "warehouse"))

META_SCHEMA = """
create table if not exists runs (
    run_id varchar primary key, started_at timestamp, finished_at timestamp,
    status varchar, trigger varchar, note varchar);
create table if not exists task_log (
    run_id varchar, task varchar, started_at timestamp, finished_at timestamp,
    status varchar, attempts integer, rows_in bigint, rows_out bigint, message varchar);
create table if not exists raw_files (
    source varchar, sha256 varchar, path varchar, bytes bigint, fetched_at timestamp,
    primary key (source, sha256));
create table if not exists watermarks (
    source varchar primary key, high_water varchar, rows bigint, updated_at timestamp);
create table if not exists quality_results (
    run_id varchar, source varchar, check_name varchar, passed boolean, failing_rows bigint, detail varchar);
"""


def path(*parts) -> Path:
    p = ROOT.joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


@contextmanager
def meta():
    ROOT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(ROOT / "meta.duckdb"))
    try:
        con.execute(META_SCHEMA)
        yield con
    finally:
        con.close()


def write_parquet(df, target) -> None:
    """Parquet through DuckDB, so the platform doesn't need pyarrow."""
    # pandas 3 has its own string dtype that DuckDB 1.1 can't read; plain objects it can.
    df = df.astype({c: object for c in df.columns if str(df[c].dtype) in ("str", "string")})
    con = duckdb.connect()
    con.register("frame", df)
    con.execute(f"copy frame to '{target}' (format parquet)")
    con.close()


def read_parquet(source):
    con = duckdb.connect()
    df = con.execute(f"select * from read_parquet('{source}')").df()
    con.close()
    return df
