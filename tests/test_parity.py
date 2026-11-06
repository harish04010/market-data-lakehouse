"""The local engine (DuckDB) and the cluster engine (Spark) must agree."""
import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")


def test_market_daily_matches_across_engines(lake):
    orchestrate, _ = lake
    orchestrate.run()
    from pyspark.sql import SparkSession
    from lake.gold import market_daily_duckdb, market_daily_spark
    spark = (SparkSession.builder.master("local[1]").appName("parity")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
    try:
        a = market_daily_duckdb()
        b = market_daily_spark(spark)[a.columns]
    finally:
        spark.stop()
    for df in (a, b):
        df["date"] = pd.to_datetime(df["date"])
    a, b = a.sort_values("date").reset_index(drop=True), b.sort_values("date").reset_index(drop=True)
    assert len(a) == len(b)
    for c in a.columns:
        same = (a[c] == b[c]) | (a[c].isna() & b[c].isna())
        assert same.all(), f"{c} differs on {int((~same).sum())} rows"
