# This file is where you can define your API templates for consuming your data
# All query_params are passed in as strings, and are used within the sql tag to parameterize you queries

from typing import Literal
from moose_lib import MooseClient
from dataclasses import dataclass
# Define the expected query parameters for this API endpoint
# rank_by: Determines how to sort the results, defaults to sorting by total number of projects
@dataclass
class QueryParams:
    rank_by: str = "projects"  # "projects" | "total_size" | "average_size" | "developers"
    sort_order: str = "descending"  # "descending" | "ascending"
 
def run(client: MooseClient, params: QueryParams):
    # Map friendly parameter values to database values
    rank_by_mapping = {
        "projects": "total_projects",
        "total_size": "total_repo_size_kb",
        "average_size": "avg_repo_size_kb",
        "developers": "unique_developers"
    }
    
    sort_order_mapping = {
        "ascending": "asc",
        "descending": "desc"
    }

    # Convert friendly values to database values
    db_rank_by = rank_by_mapping.get(params.rank_by)
    db_sort_order = sort_order_mapping.get(params.sort_order)

    # Validate parameters
    if db_sort_order is None:
        raise ValueError(f"Invalid sort_order value. Must be one of: {', '.join(sort_order_mapping.keys())}")
    
    if db_rank_by is None:
        raise ValueError(f"Invalid rank_by value. Must be one of: {', '.join(rank_by_mapping.keys())}")
    
    # Build and execute query to get ranked programming languages
    query = f'''
    SELECT 
        language, 
        countMerge(total_projects) AS total_projects, 
        sumMerge(total_repo_size_kb) AS total_repo_size_kb, 
        avgMerge(avg_repo_size_kb) AS avg_repo_size_kb,
        uniqMerge(unique_developers) AS unique_developers
    FROM 
        TopLanguages 
    GROUP BY 
        language 
    ORDER BY 
        {db_rank_by} {db_sort_order}
    '''
    # Execute the query, passing rank_by as a parameter for safety
    return client.query(query, { "rank_by": db_rank_by })
 