from typing import Literal
from moose_lib import MooseClient
from dataclasses import dataclass

@dataclass
class QueryParams:
    rank_by: str = "total_stars"  # "starred_repos" | "total_stars" | "avg_stars"
    sort_desc: bool = True
    limit: int = 10
def run(client: MooseClient, params: QueryParams):
    # Map friendly parameter values to database values
    rank_by_mapping = {
        "total_stars": "total_stars", 
        "total_watchers": "total_watchers",
        "top_repo": "most_popular_repo_stars",
    }
    

    # Convert friendly values to database values
    db_rank_by = rank_by_mapping.get(params.rank_by)
    db_sort_order = "desc" if params.sort_desc else "asc"

    # Validate parameters
    if db_rank_by is None:
        raise ValueError(f"Invalid rank_by value. Must be one of: {', '.join(rank_by_mapping.keys())}")

    # Build and execute query to get ranked stargazers
    query = f'''
    SELECT
        developer_login,
        total_stars,
        total_watchers,
        top_languages,
        most_popular_repo,
        most_popular_repo_stars,
        most_popular_repo_description,
        most_popular_repo_language
    FROM
        TopDevelopers
    ORDER BY
        {db_rank_by} {db_sort_order}
    LIMIT {params.limit}
    '''

    return client.query(query, {"rank_by": db_rank_by, "limit": params.limit})