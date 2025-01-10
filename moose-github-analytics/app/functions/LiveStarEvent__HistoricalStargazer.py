from moose_lib import StreamingFunction
from app.datamodels.LiveStarEvent import LiveStarEvent
from app.datamodels.HistoricalStargazer import HistoricalStargazer
from datetime import datetime

def transform(event: LiveStarEvent) -> HistoricalStargazer:
    if event.action == "created":
        stars_added = 1
        starred_at = event.starred_at
    else:
        stars_added = -1
        starred_at = datetime.now()
        
    return HistoricalStargazer(
        login=event.sender.login,
        avatar_url=event.sender.avatar_url,
        repos_url=event.sender.repos_url,
        starred_at=starred_at,
        stars_added=stars_added,
    )


func = StreamingFunction(run=transform)