"""
Every source the platform ingests, with its contract.

A contract is what downstream users are promised: which columns, what types,
what keys are unique, what ranges are plausible, and how fresh the data
should be. Rows that break it don't reach silver. They go to quarantine with
the reason attached, so a person can look.

All sources are public datasets maintained by the Datahub project on GitHub,
under the Open Data Commons PDDL unless noted.
"""
from dataclasses import dataclass, field

BASE = "https://raw.githubusercontent.com/datasets"


@dataclass(frozen=True)
class Column:
    name: str
    type: str                     # date | month | float | str
    source: str | None = None     # name in the raw file, if different
    nullable: bool = False
    min: float | None = None
    max: float | None = None
    zero_is_null: bool = False   # the source writes 0 for "not published yet"


@dataclass(frozen=True)
class Source:
    name: str
    url: str
    grain: str                    # daily | monthly | snapshot
    key: tuple[str, ...]
    columns: tuple[Column, ...]
    fresh_within_days: int        # older than this and the source is flagged stale
    description: str
    rules: tuple[str, ...] = field(default_factory=tuple)  # extra named checks, see quality.py


SOURCES = [
    Source("vix_daily", f"{BASE}/finance-vix/main/data/vix-daily.csv", "daily", ("date",), (
        Column("date", "date", "DATE"),
        Column("open", "float", "OPEN", min=1, max=200),
        Column("high", "float", "HIGH", min=1, max=200),
        Column("low", "float", "LOW", min=1, max=200),
        Column("close", "float", "CLOSE", min=1, max=200),
    ), 5, "CBOE Volatility Index, daily, since 1990.", ("open_within_range",)),
    Source("brent_daily", f"{BASE}/oil-prices/main/data/brent-daily.csv", "daily", ("date",), (
        Column("date", "date", "Date"),
        Column("price", "float", "Price", min=1, max=500),
    ), 10, "Brent crude spot price, US dollars per barrel."),
    Source("wti_daily", f"{BASE}/oil-prices/main/data/wti-daily.csv", "daily", ("date",), (
        Column("date", "date", "Date"),
        # WTI really did settle at -$37.63 on 2020-04-20, so the floor allows it.
        Column("price", "float", "Price", min=-50, max=500),
    ), 10, "West Texas Intermediate crude spot price, US dollars per barrel."),
    Source("natgas_daily", f"{BASE}/natural-gas/main/data/daily.csv", "daily", ("date",), (
        Column("date", "date", "Date"),
        Column("price", "float", "Price", min=0, max=100),
    ), 10, "Henry Hub natural gas spot price, US dollars per million BTU."),
    Source("cpi_monthly", f"{BASE}/cpi-us/main/data/cpiai.csv", "monthly", ("month",), (
        Column("month", "month", "Date"),
        Column("cpi", "float", "Index", min=1, max=10000),
        Column("inflation_yoy", "float", "Inflation", nullable=True, min=-30, max=50),
    ), 45, "US consumer price index, all items, and year-over-year inflation."),
    Source("gold_monthly", f"{BASE}/gold-prices/main/data/monthly.csv", "monthly", ("month",), (
        Column("month", "month", "Date"),
        Column("price", "float", "Price", min=1, max=100000),
    ), 45, "Gold price, US dollars per troy ounce, monthly average."),
    Source("treasury10y_monthly", f"{BASE}/bond-yields-us-10y/main/data/monthly.csv", "monthly", ("month",), (
        Column("month", "month", "Date"),
        Column("yield_pct", "float", "Rate", min=-5, max=25),
    ), 45, "10-year US Treasury yield, percent, monthly."),
    Source("sp500_monthly", f"{BASE}/s-and-p-500/main/data/data.csv", "monthly", ("month",), (
        Column("month", "month", "Date"),
        Column("price", "float", "SP500", min=1, max=100000),
        Column("dividend", "float", "Dividend", nullable=True, min=0, max=1000, zero_is_null=True),
        Column("earnings", "float", "Earnings", nullable=True, min=-1000, max=10000, zero_is_null=True),
        Column("cpi", "float", "Consumer Price Index", nullable=True, min=1, max=10000, zero_is_null=True),
        Column("long_rate", "float", "Long Interest Rate", nullable=True, min=-5, max=25, zero_is_null=True),
    ), 45, "Robert Shiller's S&P composite: price, dividends, earnings, CPI and long rates, monthly since 1871.",
        ("column_freshness",)),
    Source("sp500_constituents", f"{BASE}/s-and-p-500-companies/main/data/constituents.csv", "snapshot", ("symbol",), (
        Column("symbol", "str", "Symbol"),
        Column("name", "str", "Security"),
        Column("sector", "str", "GICS Sector"),
        Column("sub_industry", "str", "GICS Sub-Industry"),
        Column("headquarters", "str", "Headquarters Location", nullable=True),
        Column("date_added", "date", "Date added", nullable=True),
        Column("cik", "str", "CIK", nullable=True),
        Column("founded", "str", "Founded", nullable=True),
    ), 120, "Current S&P 500 members with GICS sector, from Wikipedia via Datahub."),
]

BY_NAME = {s.name: s for s in SOURCES}
