
# Add your models & start the development server to import these types
from app.datamodels.HistoricalStargazer import HistoricalStargazer
from app.datamodels.StargazerProjectInfo import StargazerProjectInfo
from moose_lib import StreamingFunction
from typing import Optional
import requests
from dotenv import load_dotenv
import os

load_dotenv()

token = os.getenv('GITHUB_ACCESS_TOKEN')
def call_github_api(url: str) -> dict:
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    return response.json()
 
def fn(source: HistoricalStargazer) -> Optional[list[StargazerProjectInfo]]:
    repositories = call_github_api(source.repos_url)
    
    data = []
    for repo in repositories:
        data.append(
            StargazerProjectInfo(
                starred_at=source.starred_at,
                stargazer_login=source.login,
                repo_name=repo["name"],
                repo_full_name=repo["full_name"],
                description=repo["description"],
                repo_url=repo["html_url"],
                repo_stars=repo["stargazers_count"],
                repo_watchers=repo["watchers_count"],
                language=repo["language"] or "Multiple Languages",
                repo_size_kb=repo["size"],
                created_at=repo["created_at"],
                updated_at=repo["updated_at"],
            )
        )
    return data
 
my_function = StreamingFunction(
    run=fn
)