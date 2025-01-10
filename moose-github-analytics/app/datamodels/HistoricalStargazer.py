from dataclasses import dataclass
from datetime import datetime
from moose_lib import Key, moose_data_model, DataModelConfig, IngestionConfig, IngestionFormat
from typing import Optional

# Configuration for batch loading stargazer data
# IngestionFormat.JSON_ARRAY enables the ingestion endpoint to accept arrays of records
# This is more efficient than sending individual records when batch loading
batch_load_config = DataModelConfig(
    ingestion=IngestionConfig(
        format=IngestionFormat.JSON_ARRAY,
    )
)
 
@moose_data_model(batch_load_config)
@dataclass
class HistoricalStargazer:
    starred_at: datetime
    login: Key[str]
    avatar_url: str
    repos_url: str
    stars_added: int ## either 1 or -1