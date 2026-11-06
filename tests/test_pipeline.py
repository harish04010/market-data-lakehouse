import pandas as pd

from lake.sources import BY_NAME


def test_full_run_builds_every_layer(lake):
    orchestrate, _ = lake
    out = orchestrate.run()
    assert out["status"] == "ok", out["tasks"]
    assert all(s in ("ok", "unchanged") for s in out["tasks"].values())


def test_second_run_is_a_no_op(lake):
    orchestrate, _ = lake
    orchestrate.run()
    again = orchestrate.run()
    silver = {k: v for k, v in again["tasks"].items() if k.startswith(("ingest:", "silver:"))}
    assert set(silver.values()) == {"unchanged"}


def test_new_rows_upstream_are_merged_incrementally(lake):
    orchestrate, feed = lake
    orchestrate.run()
    f = feed / "gold_monthly.csv"
    f.write_text(f.read_text() + "2099-01,9999.0\n")
    out = orchestrate.run(only=["gold_monthly"])
    r = out["results"]["silver:gold_monthly"]
    assert r["inserted"] == 1 and r["revised"] == 0


def test_revised_history_is_counted_not_silently_overwritten(lake):
    orchestrate, feed = lake
    orchestrate.run()
    f = feed / "treasury10y_monthly.csv"
    lines = f.read_text().splitlines()
    date, rate = lines[5].split(",")
    lines[5] = f"{date},{float(rate) + 1:.2f}"
    f.write_text("\n".join(lines) + "\n")
    r = orchestrate.run(only=["treasury10y_monthly"])["results"]["silver:treasury10y_monthly"]
    assert r["revised"] == 1 and r["inserted"] == 0


def test_bad_rows_go_to_quarantine_with_a_reason(lake):
    orchestrate, feed = lake
    f = feed / "brent_daily.csv"
    f.write_text(f.read_text() + "2099-01-02,not-a-number\n2099-01-03,-5\n")
    orchestrate.run(only=["brent_daily"])
    from lake.storage import path, read_parquet
    q = read_parquet(path("quarantine", "brent_daily.parquet"))
    reasons = " ".join(q["_reason"])
    assert len(q) == 2 and "could not parse" in reasons and "below" in reasons


def test_a_failed_source_skips_only_its_downstream(lake):
    orchestrate, feed = lake
    (feed / "vix_daily.csv").unlink()  # the "network" is down for one source
    out = orchestrate.run()
    assert out["tasks"]["ingest:vix_daily"] == "failed"
    assert out["tasks"]["gold:market_daily"] == "skipped"
    assert out["tasks"]["gold:macro_monthly"] == "ok"


def test_placeholder_zeros_become_nulls_not_quarantine(lake):
    orchestrate, _ = lake
    out = orchestrate.run(only=["sp500_monthly"])
    assert out["results"]["silver:sp500_monthly"]["quarantined"] == 0
    from lake.storage import path, read_parquet
    s = read_parquet(path("silver", "sp500_monthly.parquet"))
    assert (s["earnings"] == 0).sum() == 0 and s["earnings"].isna().any()


def test_backfill_rebuilds_from_bronze_without_refetching(lake):
    orchestrate, feed = lake
    orchestrate.run(only=["cpi_monthly"])
    (feed / "cpi_monthly.csv").unlink()  # backfill must not need the network
    r = orchestrate.backfill("cpi_monthly", "2020-01-01")
    assert r["rebuilt"] > 0 and r["kept_before"] > 0
