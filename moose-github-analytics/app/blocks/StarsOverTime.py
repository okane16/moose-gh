from moose_lib import Blocks, create_aggregation, drop_aggregation, TableCreateOptions, AggregationCreateOptions, AggregationDropOptions


TABLE_NAME = "StarsOverTime"
MV_NAME = "StarsOverTimeMV"


CREATE_TABLE = TableCreateOptions(
    name=TABLE_NAME,
    columns={
        "date": "DateTime",
        "daily_stars": "Int64",
        "cumulative_stars": "Int64"
    },
    order_by="date"
)

QUERY = """
 WITH daily_stars AS (
   SELECT toStartOfDay(starred_at) AS date, SUM(stars_added) as daily_stars
   FROM HistoricalStargazer_0_0
   GROUP BY date
 )
 SELECT 
   date,
   daily_stars,
   SUM(daily_stars) OVER (ORDER BY date) as cumulative_stars
 FROM daily_stars
 ORDER BY date
 WITH FILL STEP INTERVAL 1 DAY
"""
setup = create_aggregation(AggregationCreateOptions(
    materialized_view_name=MV_NAME,
    select=QUERY,
    table_create_options=CREATE_TABLE
))
teardown = drop_aggregation(AggregationDropOptions(
    view_name=MV_NAME,
    table_name=TABLE_NAME
))

block = Blocks(
    setup=setup,
    teardown=teardown
)