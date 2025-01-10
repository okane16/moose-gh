from moose_lib import (
    AggregationCreateOptions,
    AggregationDropOptions,
    Blocks,
    ClickHouseEngines,
    TableCreateOptions,
    create_aggregation,
    drop_aggregation,
)

# Names for the materialized view and underlying table
MV_NAME = "StargazerProjectsDeduped_MV"
TABLE_NAME = "StargazerProjectsDeduped"

# Define schema for the deduped table
TABLE_OPTIONS = TableCreateOptions(
    name=TABLE_NAME,
    columns={
        "stargazer_login": "String",
        "repo_name": "String",
        "starred_at": "DateTime",
        "repo_description": "String",
        "repo_size_kb": "Int64",
        "language": "String",
        "repo_stars": "Int64",
        "repo_watchers": "Int64",
    },
    engine=ClickHouseEngines.MergeTree,
    order_by="(repo_name, stargazer_login)",
)

# SQL query that powers the materialized view
# Gets only the most recent entry for each repository
QUERY = '''
SELECT
    stargazer_login,
    repo_name,
    starred_at,
    argMax(COALESCE(repo_description, ''), starred_at) AS repo_description,
    argMax(repo_size_kb, starred_at) AS repo_size_kb,
    argMax(language, starred_at) AS language,
    argMax(repo_stars, starred_at) AS repo_stars,
    argMax(repo_watchers, starred_at) AS repo_watchers
FROM StargazerProjectInfo_0_0
GROUP BY repo_name, stargazer_login, starred_at
'''

# Cleanup queries
teardown_queries = drop_aggregation(AggregationDropOptions(
    view_name=MV_NAME,
    table_name=TABLE_NAME
))

# Setup queries
setup_queries = create_aggregation(AggregationCreateOptions(
    materialized_view_name=MV_NAME,
    select=QUERY,
    table_create_options=TABLE_OPTIONS,
))

# Create Blocks instance
block = Blocks(teardown=teardown_queries, setup=setup_queries)
