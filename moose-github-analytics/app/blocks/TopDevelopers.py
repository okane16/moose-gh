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
MV_NAME = "TopDevelopers_MV"
TABLE_NAME = "TopDevelopers"

# Define schema for the developer stats table
TABLE_OPTIONS = TableCreateOptions(
    name=TABLE_NAME,
    columns={
        "developer_login": "String",
        "total_stars": "Int64",
        "total_watchers": "Int64",
        "top_languages": "Array(String)",
        "most_popular_repo": "String",
        "most_popular_repo_stars": "Int64",
        "most_popular_repo_description": "String",
        "most_popular_repo_language": "String"
    },
    engine=ClickHouseEngines.MergeTree,
    order_by="(total_stars, most_popular_repo_stars)"
)

# SQL query that powers the materialized view
QUERY = '''
WITH 
    -- Get total stars and watchers per developer
    developer_totals AS (
        SELECT
            stargazer_login,
            count() as total_stars,
            sum(repo_watchers) as total_watchers
        FROM StargazerProjectsDeduped
        GROUP BY stargazer_login
    ),
    
    -- Get top 3 languages per developer
    developer_languages AS (
        SELECT
            stargazer_login,
            arraySlice(
                groupArray(language),
                1,
                3
            ) as top_languages
        FROM (
            SELECT
                stargazer_login,
                language,
                count() as lang_count
            FROM StargazerProjectsDeduped
            GROUP BY stargazer_login, language
            ORDER BY stargazer_login, lang_count DESC
        )
        GROUP BY stargazer_login
    ),
    
    -- Get most popular repo info per developer
    popular_repos AS (
        SELECT
            stargazer_login,
            any(repo_name) as most_popular_repo,
            max(repo_stars) as most_popular_repo_stars,
            any(repo_description) as most_popular_repo_description,
            any(language) as most_popular_repo_language
        FROM (
            SELECT
                stargazer_login,
                repo_name,
                repo_stars,
                repo_description,
                language
            FROM StargazerProjectsDeduped
            WHERE (stargazer_login, repo_stars) IN (
                SELECT
                    stargazer_login,
                    max(repo_stars) as max_stars
                FROM StargazerProjectsDeduped
                GROUP BY stargazer_login
            )
        )
        GROUP BY stargazer_login
    )

SELECT
    dt.stargazer_login as developer_login,
    dt.total_stars,
    dt.total_watchers,
    dl.top_languages,
    pr.most_popular_repo,
    pr.most_popular_repo_stars,
    pr.most_popular_repo_description,
    pr.most_popular_repo_language
FROM developer_totals dt
LEFT JOIN developer_languages dl ON dt.stargazer_login = dl.stargazer_login
LEFT JOIN popular_repos pr ON dt.stargazer_login = pr.stargazer_login
ORDER BY total_stars, total_watchers, most_popular_repo_stars
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
