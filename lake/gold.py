"""
Gold: tables people actually query.

market_daily     one row per trading day: VIX, Brent, WTI and natural gas side by side.
                 A price missing on a day the others traded (a holiday in one market)
                 is carried forward for up to 3 days and flagged, never silently invented.
macro_monthly    S&P 500 with dividends, earnings, inflation, 10-year yields and gold;
                 real (inflation-adjusted) returns and Shiller's CAPE ratio computed here.
sector_summary   S&P 500 membership by GICS sector.

market_daily has two implementations: DuckDB SQL (what runs locally) and
PySpark (what you'd run on a cluster). tests/test_parity.py checks they agree.
"""
import duckdb
import pandas as pd

from .storage import path, read_parquet, write_parquet

CARRY_DAYS = 3


def _silver(name):
    return str(path("silver", f"{name}.parquet"))


MARKET_DAILY_SQL = f"""
with days as (
    select date::date as date from read_parquet('{{vix}}')
    union select date::date from read_parquet('{{brent}}')
    union select date::date from read_parquet('{{wti}}')
    union select date::date from read_parquet('{{gas}}')
),
joined as (
    select d.date,
           v.close as vix, b.price as brent, w.price as wti, g.price as natgas
    from days d
    left join (select date::date as date, close from read_parquet('{{vix}}')) v using (date)
    left join (select date::date as date, price from read_parquet('{{brent}}')) b using (date)
    left join (select date::date as date, price from read_parquet('{{wti}}')) w using (date)
    left join (select date::date as date, price from read_parquet('{{gas}}')) g using (date)
    where d.date >= date '1990-01-02'
),
marked as (
    select *,
        {{carry}}
    from joined
)
select date,
       {{select_cols}}
from marked
order by date
"""


def _carry_sql(col: str) -> tuple[str, str]:
    """Last known value and the date it was observed, so we can cap how far it's carried."""
    grp = f"count({col}) over (order by date rows unbounded preceding)"
    marks = (f"{grp} as {col}_grp, "
             f"max(case when {col} is not null then date end) over (order by date rows unbounded preceding) as {col}_seen")
    select = (f"case when {col} is not null then {col} "
              f"when datediff('day', {col}_seen, date) <= {CARRY_DAYS} then first_value({col}) over (partition by {col}_grp order by date) end as {col}, "
              f"{col} is null and coalesce(datediff('day', {col}_seen, date) <= {CARRY_DAYS}, false) as {col}_carried")
    return marks, select


def market_daily_duckdb() -> pd.DataFrame:
    cols = ["vix", "brent", "wti", "natgas"]
    marks, selects = zip(*[_carry_sql(c) for c in cols])
    sql = MARKET_DAILY_SQL.format(vix=_silver("vix_daily"), brent=_silver("brent_daily"),
                                  wti=_silver("wti_daily"), gas=_silver("natgas_daily"),
                                  carry=", ".join(marks), select_cols=", ".join(selects))
    return duckdb.connect().execute(sql).df()


def market_daily_spark(spark) -> pd.DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    def read(name, value_col, alias):
        return spark.read.parquet(_silver(name)).select("date", F.col(value_col).alias(alias))

    frames = [read("vix_daily", "close", "vix"), read("brent_daily", "price", "brent"),
              read("wti_daily", "price", "wti"), read("natgas_daily", "price", "natgas")]
    days = frames[0].select("date")
    for f in frames[1:]:
        days = days.union(f.select("date"))
    df = days.distinct().where(F.col("date") >= F.lit("1990-01-02").cast("date"))
    for f in frames:
        df = df.join(f, "date", "left")

    w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    for c in ["vix", "brent", "wti", "natgas"]:
        seen = F.max(F.when(F.col(c).isNotNull(), F.col("date"))).over(w)
        last = F.last(F.col(c), ignorenulls=True).over(w)
        df = df.withColumn(f"{c}_seen", seen).withColumn(f"{c}_last", last)
        within = F.datediff(F.col("date"), F.col(f"{c}_seen")) <= CARRY_DAYS
        # Before a series starts there's nothing to carry: that's False, not null,
        # to match the DuckDB version (and so NOT x_carried filters keep those rows).
        df = (df.withColumn(f"{c}_carried", F.coalesce(F.col(c).isNull() & within, F.lit(False)))
                .withColumn(c, F.when(F.col(c).isNotNull(), F.col(c)).when(within, F.col(f"{c}_last")))
                .drop(f"{c}_seen", f"{c}_last"))
    cols = ["date"] + [x for c in ["vix", "brent", "wti", "natgas"] for x in (c, f"{c}_carried")]
    return df.select(*cols).orderBy("date").toPandas()


MACRO_MONTHLY_SQL = """
with sp as (select * from read_parquet('{sp}')),
cpi as (select * from read_parquet('{cpi}')),
latest_cpi as (select cpi from cpi order by month desc limit 1),
base as (
    select sp.month, sp.price, sp.dividend, sp.earnings,
           coalesce(c.cpi, sp.cpi) as cpi, c.inflation_yoy,
           t.yield_pct as treasury_10y, g.price as gold
    from sp
    left join cpi c using (month)
    left join read_parquet('{t10}') t using (month)
    left join read_parquet('{gold}') g using (month)
),
real as (
    select *,
        price * (select cpi from latest_cpi) / cpi as real_price,
        earnings * (select cpi from latest_cpi) / cpi as real_earnings,
        -- Shiller's dividend is annualized; a month earns a twelfth of it.
        (price + coalesce(dividend, 0) / 12) / lag(price) over (order by month) - 1 as nominal_return
    from base
),
cape as (
    select *,
        -- CAPE: real price over the average of the last 10 years of real earnings.
        -- Needs all 120 months; otherwise it's not CAPE, so it's left null.
        case when count(real_earnings) over w = 120 then real_price / avg(real_earnings) over w end as cape
    from real
    window w as (order by month rows between 119 preceding and current row)
)
select month, price, dividend, earnings, cpi, inflation_yoy, treasury_10y, gold,
       real_price, nominal_return,
       (1 + nominal_return) / (cpi / lag(cpi) over (order by month)) - 1 as real_return,
       cape,
       coalesce(dividend, 0) / price as dividend_yield
from cape
order by month
"""


def macro_monthly() -> pd.DataFrame:
    sql = MACRO_MONTHLY_SQL.format(sp=_silver("sp500_monthly"), cpi=_silver("cpi_monthly"),
                                   t10=_silver("treasury10y_monthly"), gold=_silver("gold_monthly"))
    return duckdb.connect().execute(sql).df()


def sector_summary() -> pd.DataFrame:
    return duckdb.connect().execute(f"""
        select sector, count(*) as companies,
               min(date_added) as earliest_added, max(date_added) as latest_added
        from read_parquet('{_silver("sp500_constituents")}')
        group by sector order by companies desc""").df()


TABLES = {
    "market_daily": (["vix_daily", "brent_daily", "wti_daily", "natgas_daily"], market_daily_duckdb),
    "macro_monthly": (["sp500_monthly", "cpi_monthly", "treasury10y_monthly", "gold_monthly"], macro_monthly),
    "sector_summary": (["sp500_constituents"], sector_summary),
}


def build(name: str) -> int:
    df = TABLES[name][1]()
    write_parquet(df, path("gold", f"{name}.parquet"))
    return len(df)
