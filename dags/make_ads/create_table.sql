create or replace table `{BQ_PROJECT}.ads.{SYMBOL_LOWER}`
partition by date(time) as
with distinct_rows as (
  select distinct
    symbol,
    period,
    time,
    open,
    high,
    low,
    close,
    tick_volume
  from `{BQ_PROJECT}.raw.raw`
  where date(time) > '2000-01-01'
  and symbol = '{SYMBOL}'
  and period in ({PERIODS})
),
ads as (
  select distinct
    *,
    extract(day from time) as day_of_month,
    extract(month from time) as month,
    extract(dayofweek from cast(time as date)) as day_of_week,
    extract(year from time) as year,
    extract(hour from time) as hour,
    extract(minute from time) as minute,
    extract(second from time) as second
  from distinct_rows
)
select
  *
from ads
;


