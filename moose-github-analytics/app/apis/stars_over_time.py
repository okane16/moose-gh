from typing import Optional
from moose_lib import MooseClient
from dataclasses import dataclass
from datetime import datetime

@dataclass 
class QueryParams:
    start_date: datetime = None
    end_date: datetime = None
    interval: str = "day"  # "day" | "week" | "month"

def run(client: MooseClient, params: QueryParams):
    # Build date filters
    date_filters = []
    if params.start_date:
        date_filters.append(f"starred_at >= '{params.start_date.isoformat()}'")
    if params.end_date:
        date_filters.append(f"starred_at <= '{params.end_date.isoformat()}'")
    
    where_clause = f"WHERE {' AND '.join(date_filters)}" if date_filters else ""

    # Map interval to SQL date truncation
    interval_mapping = {
        "day": "day",
        "week": "week",
        "month": "month"
    }

    trunc_interval = interval_mapping.get(params.interval)
    if not trunc_interval:
        raise ValueError(f"Invalid interval. Must be one of: {', '.join(interval_mapping.keys())}")

    query = f"""
    WITH daily_stars AS (
        SELECT 
            date_trunc('{trunc_interval}', starred_at) as period,
            SUM(stars_added) as stars_in_period
        FROM HistoricalStargazer_0_0
        {where_clause}
        GROUP BY date_trunc('{trunc_interval}', starred_at)
        ORDER BY period WITH FILL STEP INTERVAL 1 {trunc_interval}
    )
    SELECT 
        period,
        stars_in_period,
        SUM(stars_in_period) OVER (ORDER BY period) as cumulative_stars
    FROM daily_stars
    """

    return client.query(query, {
        "trunc_interval": trunc_interval,
        "where_clause": where_clause
    })