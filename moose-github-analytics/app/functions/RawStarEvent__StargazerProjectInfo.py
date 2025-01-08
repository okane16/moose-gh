# Add your models & start the development server to import these types
from app.datamodels.RawStarEvent import RawStarEvent
from app.datamodels.StargazerProjectInfo import StargazerProjectInfo
from moose_lib import StreamingFunction, cli_log, CliLogData
from typing import Optional
from datetime import datetime
import requests
from dotenv import load_dotenv
import os

load_dotenv()

token = os.getenv('GITHUB_ACCESS_TOKEN')
def call_github_api(url: str) -> dict:
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    return response.json()
 
def fn(source: RawStarEvent) -> Optional[list[StargazerProjectInfo]]:
    if source.action == "deleted" or not source.starred_at:
        cli_log(CliLogData(action=source.action, message=f"Skipping deleted or without starred_at", message_type="Info"))
        return None
    
    repositories = call_github_api(source.sender.repos_url)
    cli_log(CliLogData(action="Got repositories", message=f"{len(repositories)}", message_type="Info"))
    
    data=[]
    for repo in repositories:
        data.append(
            StargazerProjectInfo(
                starred_at=source.starred_at,
                stargazer_login=source.sender.login,
                repo_name=repo["name"],
                repo_full_name=repo["full_name"],
                description=repo["description"],
                repo_url=repo["html_url"],
                repo_stars=repo["stargazers_count"],
                repo_watchers=repo["watchers_count"],
                language=repo["language"],
                repo_size_kb=repo["size"],
                created_at=repo["created_at"],
                updated_at=repo["updated_at"],
            )
        )
    return data
    
my_function = StreamingFunction(
    run=fn
)